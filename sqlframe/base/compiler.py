from __future__ import annotations

import typing as t
import uuid
import zlib
from dataclasses import dataclass, field

from sqlglot import expressions as exp
from sqlglot import maybe_parse

from sqlframe.base.operations import Operation
from sqlframe.base.plan import (
    AliasNode,
    CacheNode,
    DistinctNode,
    DropDuplicatesNode,
    DropNaNode,
    DropNode,
    FillNaNode,
    FilterNode,
    GroupAggNode,
    GroupByNode,
    HintNode,
    JoinNode,
    LimitNode,
    PivotAggNode,
    PivotNode,
    PlanNode,
    RenameColumnsNode,
    ReplaceNode,
    SelectNode,
    SetOpNode,
    SortNode,
    SourceNode,
    ToDFNode,
    UnionByNameNode,
    UnpivotNode,
    WithColumnsNode,
)
from sqlframe.base.transforms import replace_id_value
from sqlframe.base.util import (
    get_func_from_session,
    get_tables_from_expression_with_join,
    quote_preserving_alias_or_name,
)

JOIN_TYPE_MAPPING = {
    "outer": "full_outer",
    "full": "full_outer",
    "fullouter": "full_outer",
    "left": "left_outer",
    "leftouter": "left_outer",
    "right": "right_outer",
    "rightouter": "right_outer",
    "semi": "left_semi",
    "leftsemi": "left_semi",
    "anti": "left_anti",
    "leftanti": "left_anti",
}

if t.TYPE_CHECKING:
    from sqlframe.base.column import Column
    from sqlframe.base.session import _BaseSession


@dataclass
class CompilationState:
    """Intermediate state tracked during plan compilation."""

    expression: exp.Select
    last_op: Operation
    branch_id: str
    sequence_id: str
    pending_hints: t.List[exp.Expression] = field(default_factory=list)
    display_name_mapping: t.Dict[str, str] = field(default_factory=dict)
    _newly_added_columns: t.Set[str] = field(default_factory=set)


class PlanCompiler:
    """Compiles a plan node tree into a sqlglot expression.

    Walks the plan tree and produces an exp.Select. Each plan node type has a
    corresponding _compile method. CTE wrapping logic (previously in the
    @operation decorator) is handled by _maybe_wrap_cte.
    """

    def __init__(self, session: _BaseSession):
        self.session = session

    def compile(self, node: PlanNode) -> exp.Select:
        state = self._compile_node(node)
        return state.expression

    def compile_with_state(self, node: PlanNode) -> CompilationState:
        return self._compile_node(node)

    # -------------------------------------------------------------------
    # Node dispatch
    # -------------------------------------------------------------------

    def _compile_node(self, node: PlanNode) -> CompilationState:
        return node.compile(self)

    # -------------------------------------------------------------------
    # CTE wrapping (port of @operation decorator logic)
    # -------------------------------------------------------------------

    def _maybe_wrap_cte(self, state: CompilationState, new_op: Operation) -> CompilationState:
        """Apply CTE wrapping based on operation ordering.

        This replicates the logic from the @operation decorator:
        - INIT operations are always wrapped in a CTE first
        - If the new operation is "lower" than the last, wrap in CTE
        - Two consecutive SELECT operations trigger CTE wrapping
        """
        if state.last_op == Operation.INIT:
            state = self._convert_leaf_to_cte(state)
            state.last_op = Operation.NO_OP
            state._newly_added_columns = set()
        effective_op = new_op if new_op != Operation.NO_OP else state.last_op
        if effective_op < state.last_op or (state.last_op == effective_op == Operation.SELECT):
            state = self._convert_leaf_to_cte(state)
            state._newly_added_columns = set()
        return state

    def _convert_leaf_to_cte(
        self,
        state: CompilationState,
        sequence_id: t.Optional[str] = None,
        name: t.Optional[str] = None,
    ) -> CompilationState:
        """Port of BaseDataFrame._convert_leaf_to_cte."""
        state = self._resolve_pending_hints(state)
        sequence_id = sequence_id or state.sequence_id
        expression = state.expression

        # Track join FROM-clause sequence_ids for alias propagation
        has_joins = bool(expression.args.get("joins"))
        from_sequence_ids: t.Set[str] = set()
        if has_joins:
            from_table_names = {
                table.alias_or_name for table in get_tables_from_expression_with_join(expression)
            }
            from_sequence_ids = {
                cte.args["sequence_id"]
                for cte in expression.ctes
                if cte.alias_or_name in from_table_names and "sequence_id" in cte.args
            }

        cte_expression, cte_name = self._create_cte_from_expression(
            expression=expression,
            branch_id=state.branch_id,
            sequence_id=sequence_id,
            name=name,
        )
        new_expression = self._add_ctes_to_expression(
            exp.Select(), expression.ctes + [cte_expression]
        )
        sel_columns = self._get_outer_select_columns(cte_expression)
        new_expression = new_expression.from_(cte_name).select(*[x.expression for x in sel_columns])

        # Propagate alias mappings for join expressions
        if from_sequence_ids:
            for seq_ids in self.session.name_to_sequence_id_mapping.values():
                if any(sid in from_sequence_ids for sid in seq_ids) and sequence_id not in seq_ids:
                    seq_ids.append(sequence_id)

        return CompilationState(
            expression=new_expression,
            last_op=state.last_op,
            branch_id=state.branch_id,
            sequence_id=sequence_id,
            pending_hints=list(state.pending_hints),
            display_name_mapping=dict(state.display_name_mapping),
        )

    def _resolve_pending_hints(self, state: CompilationState) -> CompilationState:
        """Port of BaseDataFrame._resolve_pending_hints."""
        if not state.pending_hints:
            return state

        expression = state.expression
        hint_expression = expression.args.get("hint") or exp.Hint(expressions=[])
        remaining_hints = list(state.pending_hints)

        # Partition hints
        partition_hints = [h for h in remaining_hints if isinstance(h, exp.Anonymous)]
        for hint in partition_hints:
            hint_expression.append("expressions", hint)
            remaining_hints.remove(hint)

        # Join hints
        join_aliases = {
            join_table.alias_or_name
            for join_table in get_tables_from_expression_with_join(expression)
        }
        if join_aliases:
            join_hints = [h for h in list(remaining_hints) if isinstance(h, exp.JoinHint)]
            for hint in join_hints:
                for seq_id_expr in hint.expressions:
                    seq_id_or_name = seq_id_expr.alias_or_name
                    seq_ids_to_match = [seq_id_or_name]
                    if seq_id_or_name in self.session.name_to_sequence_id_mapping:
                        seq_ids_to_match = self.session.name_to_sequence_id_mapping[seq_id_or_name]
                    matching_ctes = [
                        cte
                        for cte in reversed(expression.ctes)
                        if cte.args["sequence_id"] in seq_ids_to_match
                    ]
                    for cte in matching_ctes:
                        if cte.alias_or_name in join_aliases:
                            seq_id_expr.set("this", cte.args["alias"].this)
                            if hint in remaining_hints:
                                remaining_hints.remove(hint)
                            break
                hint_expression.append("expressions", hint)

        if hint_expression.expressions:
            expression.set("hint", hint_expression)

        return CompilationState(
            expression=expression,
            last_op=state.last_op,
            branch_id=state.branch_id,
            sequence_id=state.sequence_id,
            pending_hints=remaining_hints,
            display_name_mapping=dict(state.display_name_mapping),
        )

    # -------------------------------------------------------------------
    # CTE helper methods (ported from BaseDataFrame)
    # -------------------------------------------------------------------

    def _create_cte_from_expression(
        self,
        expression: exp.Expression,
        branch_id: str,
        sequence_id: str,
        name: t.Optional[str] = None,
        **kwargs,
    ) -> t.Tuple[exp.CTE, str]:
        """Port of BaseDataFrame._create_cte_from_expression."""
        name = name or self._create_hash_from_expression(expression)
        expression_to_cte = expression.copy()
        expression_to_cte.set("with_", None)
        cte = exp.Select().with_(name, as_=expression_to_cte, **kwargs).ctes[0]
        cte.set("branch_id", branch_id)
        cte.set("sequence_id", sequence_id)
        return cte, name

    def _add_ctes_to_expression(self, expression: exp.Select, ctes: t.List[exp.CTE]) -> exp.Select:
        """Port of BaseDataFrame._add_ctes_to_expression."""
        expression = expression.copy()
        with_expression = expression.args.get("with_")
        if with_expression:
            existing_ctes = with_expression.expressions
            existing_cte_names = {x.alias_or_name for x in existing_ctes}
            replaced_cte_names = {}
            for cte in ctes:
                if replaced_cte_names:
                    cte = cte.transform(replace_id_value, replaced_cte_names)
                if cte.alias_or_name in existing_cte_names:
                    existing_cte = next(
                        c for c in existing_ctes if c.alias_or_name == cte.alias_or_name
                    )
                    if self._create_hash_from_expression(
                        existing_cte.this
                    ) == self._create_hash_from_expression(cte.this) and existing_cte.args.get(
                        "sequence_id"
                    ) == cte.args.get("sequence_id"):
                        continue
                    random_filter = exp.Literal.string(uuid.uuid4().hex)
                    cte.set(
                        "this",
                        cte.this.where(
                            exp.EQ(
                                this=random_filter,
                                expression=random_filter,
                            )
                        ),
                    )
                    new_cte_alias = self._create_hash_from_expression(cte.this)
                    replaced_cte_names[cte.args["alias"].this] = maybe_parse(
                        new_cte_alias,
                        dialect=self.session.input_dialect,
                        into=exp.Identifier,
                    )
                    cte.set(
                        "alias",
                        maybe_parse(
                            new_cte_alias,
                            dialect=self.session.input_dialect,
                            into=exp.TableAlias,
                        ),
                    )
                    existing_cte_names.add(new_cte_alias)
                existing_ctes.append(cte)
        else:
            existing_ctes = ctes
        expression.set("with_", exp.With(expressions=existing_ctes))
        return expression

    def _get_outer_select_columns(self, item: exp.Expression) -> t.List[Column]:
        """Port of BaseDataFrame._get_outer_select_columns."""
        from sqlframe.base.session import _BaseSession

        col = get_func_from_session("col", _BaseSession())
        outer_select = item.find(exp.Select)
        if outer_select:
            return [col(quote_preserving_alias_or_name(x)) for x in outer_select.expressions]
        return []

    def _create_hash_from_expression(self, expression: exp.Expression) -> str:
        """Port of BaseDataFrame._create_hash_from_expression."""
        from sqlframe.base.session import _BaseSession

        value = expression.sql(dialect=_BaseSession().input_dialect).encode("utf-8")
        hash = f"t{zlib.crc32(value)}"[:9]
        return self.session._normalize_string(hash)

    # -------------------------------------------------------------------
    # Temporary DataFrame for column processing
    # -------------------------------------------------------------------

    def _create_temp_df(self, state: CompilationState):
        """Create a temporary DataFrame for column normalization/expansion.

        Used during compilation of operations that need column processing
        (select, where, orderBy, etc.). The temp DataFrame provides access
        to existing helper methods without deep-copying expressions.
        """
        df_class = self.session._df
        return df_class(
            session=self.session,
            expression=state.expression,
            branch_id=state.branch_id,
            sequence_id=state.sequence_id,
            display_name_mapping=dict(state.display_name_mapping),
        )

    # -------------------------------------------------------------------
    # Source compilation
    # -------------------------------------------------------------------

    def _compile_source(self, node: SourceNode) -> CompilationState:
        return CompilationState(
            expression=node.expression,
            last_op=node.last_op,
            branch_id=node.branch_id,
            sequence_id=node.sequence_id,
        )

    # -------------------------------------------------------------------
    # Simple operation compilation
    # -------------------------------------------------------------------

    def _compile_limit(self, node: LimitNode) -> CompilationState:
        state = self._compile_node(node.parent)
        state = self._maybe_wrap_cte(state, Operation.LIMIT)
        num = node.num
        if limit_exp := state.expression.args.get("limit"):
            num = min(num, int(limit_exp.expression.this))
        state.expression = state.expression.limit(num)
        state.last_op = Operation.LIMIT
        return state

    def _compile_distinct(self, node: DistinctNode) -> CompilationState:
        state = self._compile_node(node.parent)
        state = self._maybe_wrap_cte(state, Operation.SELECT)
        state.expression = state.expression.distinct()
        state.last_op = Operation.SELECT
        return state

    # -------------------------------------------------------------------
    # Column-processing operation compilation
    # -------------------------------------------------------------------

    def _compile_filter(self, node: FilterNode) -> CompilationState:
        import sqlglot

        state = self._compile_node(node.parent)
        state = self._maybe_wrap_cte(state, Operation.WHERE)
        temp_df = self._create_temp_df(state)

        condition = node.condition
        if isinstance(condition, str):
            col = temp_df._ensure_and_normalize_col(
                sqlglot.parse_one(condition, dialect=self.session.input_dialect)
            )
        else:
            col = temp_df._ensure_and_normalize_col(condition)
        if isinstance(col.expression, exp.Alias):
            col.expression = col.expression.this

        state.expression = state.expression.where(col.expression)
        state.last_op = Operation.WHERE
        return state

    def _compile_select(self, node: SelectNode) -> CompilationState:
        state = self._compile_node(node.parent)
        kwargs = dict(node.kwargs)

        # Methods that already handle CTE wrapping (e.g., withColumns, join, drop)
        # pass _skip_cte_wrap=True to avoid double wrapping.
        if not kwargs.pop("_skip_cte_wrap", False):
            state = self._maybe_wrap_cte(state, Operation.SELECT)

        cols = node.cols

        if not cols:
            state.last_op = Operation.SELECT
            return state

        if isinstance(cols[0], list):
            cols = cols[0]

        temp_df = self._create_temp_df(state)
        columns = temp_df._ensure_and_normalize_cols(cols)

        if "skip_update_display_name_mapping" not in kwargs:
            unexpanded_columns = temp_df._ensure_and_normalize_cols(cols, skip_star_expansion=True)
            user_cols = list(cols)
            star_columns = []
            for index, user_col in enumerate(cols):
                if "*" in (user_col if isinstance(user_col, str) else user_col.alias_or_name):
                    star_columns.append(index)
            for index in star_columns:
                unexpanded_columns.pop(index)
                user_cols.pop(index)
            temp_df._update_display_name_mapping(unexpanded_columns, user_cols)
            state.display_name_mapping = temp_df.display_name_mapping

        kwargs["append"] = kwargs.get("append", False)

        # If an expression is `CAST(x AS DATETYPE)` then alias so that `x` is the result column name
        columns = [
            col.alias(col.expression.alias_or_name)
            if isinstance(col.expression, exp.Cast) and col.expression.alias_or_name
            else col
            for col in columns
        ]

        state.expression = state.expression.select(*[x.expression for x in columns], **kwargs)
        state.last_op = Operation.SELECT
        return state

    def _compile_sort(self, node: SortNode) -> CompilationState:
        import sqlglot

        state = self._compile_node(node.parent)
        state = self._maybe_wrap_cte(state, Operation.ORDER_BY)
        temp_df = self._create_temp_df(state)

        columns = temp_df._ensure_and_normalize_cols(node.cols)
        pre_ordered_col_indexes = [
            i for i, col in enumerate(columns) if isinstance(col.expression, exp.Ordered)
        ]
        ascending = node.ascending
        if ascending is None:
            ascending = [True] * len(columns)
        elif not isinstance(ascending, list):
            ascending = [ascending] * len(columns)
        ascending = [bool(x) for i, x in enumerate(ascending)]
        assert len(columns) == len(ascending), (
            "The length of items in ascending must equal the number of columns provided"
        )
        col_and_ascending = list(zip(columns, ascending))
        order_by_columns = [
            (
                sqlglot.parse_one(
                    f"{col.expression.sql(dialect=self.session.input_dialect)} {'DESC' if not asc else ''}",
                    dialect=self.session.input_dialect,
                    into=exp.Ordered,
                )
                if i not in pre_ordered_col_indexes
                else columns[i].column_expression
            )
            for i, (col, asc) in enumerate(col_and_ascending)
        ]

        state.expression = state.expression.order_by(*order_by_columns)
        state.last_op = Operation.ORDER_BY
        return state

    def _can_skip_with_columns_cte_wrap(
        self, state: CompilationState, node: WithColumnsNode
    ) -> bool:
        """Check if CTE wrapping can be skipped for a withColumns operation.

        Safe to skip when:
        1. The last operation was SELECT (we'd wrap due to SELECT==SELECT)
        2. Previous withColumns ops tracked newly added columns
        3. All columns being added are NEW (not replacing existing ones)
        4. Value expressions don't reference any newly added column
        """
        if state.last_op != Operation.SELECT:
            return False
        if not state._newly_added_columns:
            return False

        cols_map = node.cols_map
        if len(cols_map) != 1:
            return False

        from sqlframe.base.column import Column

        # Check existing column names in the expression
        existing_col_names = {
            e.alias_or_name for e in state.expression.expressions if hasattr(e, "alias_or_name")
        }

        for col_name, col_value in cols_map[0].items():
            # Check if replacing an existing column
            normalized_name = self.session._normalize_string(col_name)
            if normalized_name in existing_col_names:
                return False

            # Check value expression for references to newly added columns
            if isinstance(col_value, Column):
                for col_ref in col_value.expression.find_all(exp.Column):
                    if col_ref.alias_or_name in state._newly_added_columns:
                        return False
            elif isinstance(col_value, str) and col_value in state._newly_added_columns:
                return False

        return True

    def _compile_with_columns(self, node: WithColumnsNode) -> CompilationState:
        state = self._compile_node(node.parent)

        skip_cte_wrap = self._can_skip_with_columns_cte_wrap(state, node)

        if skip_cte_wrap:
            # Fast path: append new column expressions directly to existing SELECT.
            # Safe because all columns are NEW and don't reference newly added columns.
            temp_df = self._create_temp_df(state)
            cols_map = node.cols_map
            if len(cols_map) != 1:
                raise ValueError("Only a single map is supported")

            for col_name, col_value in cols_map[0].items():
                normalized_value = temp_df._ensure_and_normalize_col(col_value)
                new_expr = normalized_value.alias(col_name).expression
                state.expression = state.expression.select(new_expr, append=True)
                normalized_name = self.session._normalize_string(col_name)
                state._newly_added_columns.add(normalized_name)
                state.display_name_mapping[normalized_name] = col_name

            state.last_op = Operation.SELECT
            return state

        # Standard path: CTE wrap then rebuild SELECT
        state = self._maybe_wrap_cte(state, Operation.SELECT)
        temp_df = self._create_temp_df(state)

        cols_map = node.cols_map
        if len(cols_map) != 1:
            raise ValueError("Only a single map is supported")
        col_map = {
            temp_df._ensure_and_normalize_col(k): (temp_df._ensure_and_normalize_col(v), k)
            for k, v in cols_map[0].items()
        }
        existing_cols = self._get_outer_select_columns(state.expression)
        existing_col_names = [x.alias_or_name for x in existing_cols]
        select_columns = existing_cols
        all_new = True
        for col, (col_value, display_name) in col_map.items():
            column_name = col.alias_or_name
            existing_col_index = (
                existing_col_names.index(column_name) if column_name in existing_col_names else None
            )
            if existing_col_index is not None:
                select_columns[existing_col_index] = col_value.alias(display_name)
                all_new = False
            else:
                select_columns.append(col_value.alias(display_name))
        temp_df._update_display_name_mapping(
            [col for col in col_map], [name for _, name in col_map.values()]
        )
        state.display_name_mapping = temp_df.display_name_mapping

        # Normalize columns (resolves ambiguous references in join contexts)
        # then apply CAST aliasing
        columns = temp_df._ensure_and_normalize_cols(select_columns)
        columns = [
            col.alias(col.expression.alias_or_name)
            if isinstance(col.expression, exp.Cast) and col.expression.alias_or_name
            else col
            for col in columns
        ]

        state.expression = state.expression.select(*[x.expression for x in columns], append=False)
        state.last_op = Operation.SELECT

        # Track newly added columns for potential future skip
        if all_new:
            for col in col_map:
                state._newly_added_columns.add(col.alias_or_name)

        return state

    def _compile_drop(self, node: DropNode) -> CompilationState:
        from sqlframe.base.util import partition_to

        state = self._compile_node(node.parent)
        state = self._maybe_wrap_cte(state, Operation.SELECT)
        temp_df = self._create_temp_df(state)

        cols = node.cols
        # Separate string column names from Column objects for different handling
        column_objs, column_names = partition_to(lambda x: isinstance(x, str), cols, list, set)

        # Normalize only the Column objects (strings will be handled as unqualified)
        drop_cols = temp_df._ensure_and_normalize_cols(column_objs) if column_objs else []

        # Work directly with the expression's select columns to preserve table qualifiers
        current_expressions = state.expression.expressions
        drop_sql = {drop_col.expression.sql() for drop_col in drop_cols}

        # Create a more sophisticated matching function that considers table qualifiers
        def should_drop_expression(expr: exp.Expression) -> bool:
            if expr.sql() in drop_sql:
                return True

            if isinstance(expr, exp.Column) and (alias_or_name := expr.alias_or_name):
                if alias_or_name in column_names:
                    return True
                for col_name in column_names:
                    if ("." in col_name) and alias_or_name == (col_name.split(".", maxsplit=1)[-1]):
                        return True
                for drop_col in drop_cols:
                    if ((drop_expression := drop_col.expression).alias_or_name) == alias_or_name:
                        if expr_table := expr.table:
                            drop_table = drop_expression.args.get("table")
                            if (not drop_table) or (expr_table == drop_table):
                                return True
                        else:
                            return True

            return False

        new_expressions = [expr for expr in current_expressions if not should_drop_expression(expr)]

        state.expression = state.expression.select(*new_expressions, append=False)
        state.last_op = Operation.SELECT
        return state

    def _compile_to_df(self, node: ToDFNode) -> CompilationState:
        state = self._compile_node(node.parent)
        cols = node.col_names

        # Validate column count
        existing_expressions = state.expression.expressions
        if len(cols) != len(existing_expressions):
            raise ValueError(
                f"Number of column names does not match number of columns: "
                f"{len(cols)} != {len(existing_expressions)}"
            )

        expression = state.expression.copy()
        expression = expression.select(
            *[exp.alias_(col, new_col) for col, new_col in zip(expression.expressions, cols)],
            append=False,
        )
        state.expression = expression
        return state

    def _compile_rename_columns(self, node: RenameColumnsNode) -> CompilationState:
        """Port of BaseDataFrame._rename_columns.

        Uses a custom CTE-wrapping rule: wraps only if last_op > SELECT,
        not on SELECT == SELECT (to avoid losing table-qualified column references).
        """
        state = self._compile_node(node.parent)

        # Custom CTE wrapping for rename: only wrap on INIT or when last_op > SELECT
        if state.last_op == Operation.INIT:
            state = self._convert_leaf_to_cte(state)
            state.last_op = Operation.NO_OP
        if Operation.SELECT < state.last_op:
            state = self._convert_leaf_to_cte(state)

        expression = state.expression.copy()
        outer_select = expression.find(exp.Select)
        if not outer_select:
            if node.raise_on_missing:
                raise ValueError("Tried to rename a column that doesn't exist")
            state.expression = expression
            return state

        normalized_map = {self.session._normalize_string(k): v for k, v in node.cols_map.items()}
        found_any = False
        display_updates: t.Dict[str, str] = {}
        for i, select_expr in enumerate(outer_select.expressions):
            new_name = normalized_map.get(select_expr.alias_or_name)
            if new_name is not None:
                new_identifier = exp.to_identifier(new_name)
                if isinstance(select_expr, exp.Alias):
                    select_expr.set("alias", new_identifier)
                else:
                    outer_select.expressions[i] = exp.Alias(this=select_expr, alias=new_identifier)
                display_updates[self.session._normalize_string(new_name)] = new_name
                found_any = True

        if node.raise_on_missing and not found_any:
            raise ValueError("Tried to rename a column that doesn't exist")

        state.expression = expression
        state.last_op = Operation.SELECT
        state.display_name_mapping.update(display_updates)
        return state

    def _compile_drop_duplicates(self, node: DropDuplicatesNode) -> CompilationState:
        state = self._compile_node(node.parent)

        if not node.subset:
            # No subset = distinct()
            state = self._maybe_wrap_cte(state, Operation.SELECT)
            state.expression = state.expression.distinct()
            state.last_op = Operation.SELECT
            return state

        # With subset: use window function approach
        # We need to build this as a chain: withColumn("row_num", ...).where(...).drop("row_num")
        # The simplest approach is to create a temp DataFrame and chain operations
        temp_df = self._create_temp_df(state)
        temp_df.last_op = state.last_op

        from sqlglot.helper import ensure_list

        from sqlframe.base import functions as F
        from sqlframe.base.window import Window

        column_names = ensure_list(node.subset)
        window = Window.partitionBy(*column_names).orderBy(*column_names)
        result_df = (
            temp_df.withColumn("row_num", F.row_number().over(window))
            .where(F.col("row_num") == F.lit(1))
            .drop("row_num")
        )
        # result_df is now a DataFrame with a plan tree; compile it
        result_state = self.compile_with_state(result_df._plan)
        return result_state

    # -------------------------------------------------------------------
    # Set operation compilation (FROM-level)
    # -------------------------------------------------------------------

    def _compile_set_op(self, node: SetOpNode) -> CompilationState:
        """Port of _set_operation for union/intersect/except."""
        left_state = self._compile_node(node.parent)
        left_state = self._maybe_wrap_cte(left_state, Operation.FROM)
        right_state = self._compile_node(node.other)
        right_state = self._convert_leaf_to_cte(right_state)

        base_expression = left_state.expression.copy()
        base_expression = self._add_ctes_to_expression(base_expression, right_state.expression.ctes)
        all_ctes = base_expression.ctes
        right_state.expression.set("with_", None)
        base_expression.set("with_", None)
        operation = node.op_class(
            this=base_expression, distinct=node.distinct, expression=right_state.expression
        )
        operation.set("with_", exp.With(expressions=all_ctes))

        # Wrap the set operation result in a CTE
        result_state = CompilationState(
            expression=operation,
            last_op=Operation.FROM,
            branch_id=left_state.branch_id,
            sequence_id=left_state.sequence_id,
            pending_hints=list(left_state.pending_hints),
            display_name_mapping=dict(left_state.display_name_mapping),
        )
        result_state = self._convert_leaf_to_cte(result_state)
        result_state.last_op = Operation.FROM
        return result_state

    def _compile_union_by_name(self, node: UnionByNameNode) -> CompilationState:
        """Port of unionByName."""
        left_state = self._compile_node(node.parent)
        left_state = self._maybe_wrap_cte(left_state, Operation.FROM)
        right_state = self._compile_node(node.other)

        l_temp_df = self._create_temp_df(left_state)
        l_temp_df.last_op = left_state.last_op
        r_temp_df = self._create_temp_df(right_state)
        r_temp_df.last_op = right_state.last_op

        l_columns = l_temp_df._columns
        r_columns = r_temp_df._columns

        if not node.allow_missing_columns:
            l_expressions = l_columns
            r_expressions = l_columns
        else:
            from copy import copy as shallow_copy

            l_expressions = []
            r_expressions = []
            r_columns_unused = shallow_copy(r_columns)
            for l_column in l_columns:
                l_expressions.append(l_column)
                if l_column in r_columns:
                    r_expressions.append(l_column)
                    r_columns_unused.remove(l_column)
                else:
                    r_expressions.append(exp.alias_(exp.Null(), l_column, copy=False))
            for r_column in r_columns_unused:
                l_expressions.append(exp.alias_(exp.Null(), r_column, copy=False))
                r_expressions.append(r_column)

        # Build right side: CTE + select
        r_df = r_temp_df._convert_leaf_to_cte().select(
            *r_temp_df._ensure_list_of_columns(r_expressions)
        )
        # Build left side
        l_df = l_temp_df
        if node.allow_missing_columns:
            l_df = l_temp_df._convert_leaf_to_cte().select(
                *l_temp_df._ensure_list_of_columns(l_expressions)
            )
        # Perform the union using _set_operation on the DataFrames
        result_df = l_df._set_operation(exp.Union, r_df, False)
        # Compile the result's plan
        result_state = self.compile_with_state(result_df._plan)
        return result_state

    # -------------------------------------------------------------------
    # NA operation compilation (FROM-level)
    # -------------------------------------------------------------------

    def _compile_dropna(self, node: DropNaNode) -> CompilationState:
        state = self._compile_node(node.parent)
        state = self._maybe_wrap_cte(state, Operation.FROM)
        temp_df = self._create_temp_df(state)
        temp_df.last_op = state.last_op

        from sqlframe.base import functions as F

        minimum_non_null = node.thresh or 0
        all_columns = self._get_outer_select_columns(temp_df.expression)
        if node.subset:
            null_check_columns = temp_df._ensure_and_normalize_cols(node.subset)
        else:
            null_check_columns = all_columns
        if node.thresh is None:
            minimum_num_nulls = 1 if node.how == "any" else len(null_check_columns)
        else:
            minimum_num_nulls = len(null_check_columns) - minimum_non_null + 1
        if minimum_num_nulls > len(null_check_columns):
            raise RuntimeError(
                f"The minimum num nulls for dropna must be less than or equal to the number of columns. "
                f"Minimum num nulls: {minimum_num_nulls}, Num Columns: {len(null_check_columns)}"
            )
        import functools

        if_null_checks = [
            F.when(column.isNull(), F.lit(1)).otherwise(F.lit(0)) for column in null_check_columns
        ]
        nulls_added_together = functools.reduce(lambda x, y: x + y, if_null_checks)
        num_nulls = nulls_added_together.alias("num_nulls")
        result_df = (
            temp_df.select(num_nulls, append=True)
            .where(F.col("num_nulls") < F.lit(minimum_num_nulls))
            .select(*all_columns)
        )
        result_state = self.compile_with_state(result_df._plan)
        result_state.last_op = Operation.FROM
        return result_state

    def _compile_fillna(self, node: FillNaNode) -> CompilationState:
        state = self._compile_node(node.parent)
        state = self._maybe_wrap_cte(state, Operation.FROM)
        temp_df = self._create_temp_df(state)
        temp_df.last_op = state.last_op

        from sqlframe.base import functions as F

        values = None
        columns = None
        all_columns = self._get_outer_select_columns(temp_df.expression)
        all_column_mapping = {column.alias_or_name: column for column in all_columns}
        if isinstance(node.value, dict):
            values = list(node.value.values())
            columns = temp_df._ensure_and_normalize_cols(list(node.value))
        if not columns:
            columns = (
                temp_df._ensure_and_normalize_cols(node.subset) if node.subset else all_columns
            )
        if not values:
            assert not isinstance(node.value, dict)
            values = [node.value] * len(columns)
        value_columns = [F.lit(value) for value in values]

        null_replacement_mapping = {
            column.alias_or_name: (
                F.when(column.isNull(), value).otherwise(column).alias(column.alias_or_name)
            )
            for column, value in zip(columns, value_columns)
        }
        null_replacement_mapping = {**all_column_mapping, **null_replacement_mapping}
        null_replacement_columns = [
            null_replacement_mapping[column.alias_or_name] for column in all_columns
        ]
        result_df = temp_df.select(*null_replacement_columns)
        result_state = self.compile_with_state(result_df._plan)
        result_state.last_op = Operation.FROM
        return result_state

    def _compile_replace(self, node: ReplaceNode) -> CompilationState:
        state = self._compile_node(node.parent)
        state = self._maybe_wrap_cte(state, Operation.FROM)
        temp_df = self._create_temp_df(state)
        temp_df.last_op = state.last_op

        from sqlframe.base import functions as F

        old_values = None
        all_columns = self._get_outer_select_columns(temp_df.expression)
        all_column_mapping = {column.alias_or_name: column for column in all_columns}

        columns = temp_df._ensure_and_normalize_cols(node.subset) if node.subset else all_columns
        to_replace = node.to_replace
        value = node.value
        if isinstance(to_replace, dict):
            old_values = list(to_replace)
            new_values = list(to_replace.values())
        elif not old_values and isinstance(to_replace, list):
            assert isinstance(value, list), "value must be a list since the replacements are a list"
            assert len(to_replace) == len(value), (
                "the replacements and values must be the same length"
            )
            old_values = to_replace
            new_values = value
        else:
            old_values = [to_replace] * len(columns)
            new_values = [value] * len(columns)
        old_values = [F.lit(v) for v in old_values]
        new_values = [F.lit(v) for v in new_values]

        replacement_mapping = {}
        for column in columns:
            expression = F.lit(None)
            for i, (old_value, new_value) in enumerate(zip(old_values, new_values)):
                if i == 0:
                    expression = F.when(column == old_value, new_value)
                else:
                    expression = expression.when(column == old_value, new_value)
            replacement_mapping[column.alias_or_name] = expression.otherwise(column).alias(
                column.expression.alias_or_name
            )

        replacement_mapping = {**all_column_mapping, **replacement_mapping}
        replacement_columns = [replacement_mapping[column.alias_or_name] for column in all_columns]
        result_df = temp_df.select(*replacement_columns)
        result_state = self.compile_with_state(result_df._plan)
        result_state.last_op = Operation.FROM
        return result_state

    # -------------------------------------------------------------------
    # Unpivot compilation
    # -------------------------------------------------------------------

    def _compile_unpivot(self, node: UnpivotNode) -> CompilationState:
        import functools as ft

        from sqlframe.base import functions as F

        state = self._compile_node(node.parent)
        state = self._maybe_wrap_cte(state, Operation.SELECT)
        temp_df = self._create_temp_df(state)
        temp_df.last_op = state.last_op

        id_columns = temp_df._ensure_and_normalize_cols(node.ids)
        values = node.values
        if not values:
            outer_selects = self._get_outer_select_columns(state.expression)
            values = [
                column
                for column in outer_selects
                if column.alias_or_name not in {x.alias_or_name for x in id_columns}
            ]
        value_columns = temp_df._ensure_and_normalize_cols(values)

        df = temp_df._convert_leaf_to_cte()
        selects = []
        for value in value_columns:
            selects.append(
                exp.select(
                    *[x.column_expression for x in id_columns],
                    F.lit(value.alias_or_name).alias(node.variable_column_name).expression,
                    value.alias(node.value_column_name).expression,
                ).from_(df.expression.ctes[-1].alias_or_name)
            )
        unioned_expression = ft.reduce(lambda x, y: x.union(y, distinct=False), selects)
        final_expression = df._add_ctes_to_expression(unioned_expression, df.expression.ctes)
        result_df = df.copy(expression=final_expression)._convert_leaf_to_cte()
        result_state = self.compile_with_state(result_df._plan)
        return result_state

    # -------------------------------------------------------------------
    # NO_OP-level compilation (hint, cache, alias)
    # -------------------------------------------------------------------

    def _compile_hint(self, node: HintNode) -> CompilationState:
        state = self._compile_node(node.parent)
        state = self._maybe_wrap_cte(state, Operation.NO_OP)
        state.pending_hints.append(node.hint_expression)
        return state

    def _compile_cache(self, node: CacheNode) -> CompilationState:
        state = self._compile_node(node.parent)
        state = self._maybe_wrap_cte(state, Operation.NO_OP)
        state = self._convert_leaf_to_cte(state)
        state.expression.ctes[-1].set("cache_storage_level", node.storage_level)
        return state

    def _compile_alias(self, node: AliasNode) -> CompilationState:
        state = self._compile_node(node.parent)
        state = self._maybe_wrap_cte(state, Operation.NO_OP)
        state = self._convert_leaf_to_cte(state, sequence_id=node.new_sequence_id)
        return state

    # -------------------------------------------------------------------
    # Join compilation (FROM-level)
    # -------------------------------------------------------------------

    def _compile_join(self, node: JoinNode) -> CompilationState:
        """Port of BaseDataFrame.join().

        Compiles both sides, merges CTEs, builds the join expression,
        handles self-join column resolution, and applies the final select.
        """
        import functools

        how = node.how

        # 1. Compile left side and apply CTE wrapping for FROM-level operation
        left_state = self._compile_node(node.parent)
        left_state = self._maybe_wrap_cte(left_state, Operation.FROM)

        # 2. Compile right side and convert to CTE
        right_state = self._compile_node(node.other)
        right_state = self._convert_leaf_to_cte(right_state)

        # 3. Get the right side's leaf CTE info (before merging)
        right_leaf_cte = right_state.expression.ctes[-1]
        right_leaf_name = right_leaf_cte.alias_or_name
        right_leaf_seq_id = right_leaf_cte.args.get("sequence_id")

        # 4. Merge CTEs from both sides
        join_expression = self._add_ctes_to_expression(
            left_state.expression, right_state.expression.ctes
        )

        # 5. Determine join type
        join_type = JOIN_TYPE_MAPPING.get(how, how).replace("_", " ")

        # 6. Find effective join alias (CTE might be renamed during merge)
        effective_join_alias = next(
            (
                c.alias_or_name
                for c in join_expression.ctes
                if c.alias_or_name == right_leaf_name
                and c.args.get("sequence_id") == right_leaf_seq_id
            ),
            join_expression.ctes[-1].alias,
        )

        # 7. Add join to expression (without ON clause initially)
        join_expression = join_expression.join(effective_join_alias, join_type=join_type)

        # 8. Get columns from both sides
        self_columns = self._get_outer_select_columns(join_expression)
        other_columns = self._get_outer_select_columns(right_state.expression)

        # 9. Normalize join columns against the left expression
        left_temp_df = self._create_temp_df(left_state)
        join_columns = (
            left_temp_df._ensure_and_normalize_cols(node.on) if node.on is not None else []
        )

        # 10. Handle self-join: resolve column table qualifiers for the right side
        if node.left_branch_id == node.right_branch_id and join_columns:
            other_df_unique_uuids = node.right_known_uuids - node.left_known_uuids
            for col in join_columns:
                for col_expr in col.expression.find_all(exp.Column):
                    if (
                        "join_on_uuid" in col_expr.meta
                        and col_expr.meta["join_on_uuid"] in other_df_unique_uuids
                    ):
                        col_expr.set("table", exp.to_identifier(effective_join_alias))

        # 11. Build select columns
        select_columns = (
            self_columns
            if join_type in ["left anti", "left semi"]
            else self_columns + other_columns
        )

        join_clause = None
        if join_type != "cross":
            if isinstance(join_columns[0].expression, exp.Column):
                # Column-name join: put join columns first, deduplicate
                from sqlframe.base.functions import coalesce

                table_names = [
                    table.alias_or_name
                    for table in get_tables_from_expression_with_join(join_expression)
                ]

                # Port of _handle_join_column_names_only
                potential_ctes = [
                    cte
                    for cte in join_expression.ctes
                    if cte.alias_or_name in table_names
                    and cte.alias_or_name != effective_join_alias
                ]
                join_column_pairs = []
                for join_column in join_columns:
                    num_matching_ctes = 0
                    for cte in potential_ctes:
                        if join_column.alias_or_name in cte.this.named_selects or any(
                            isinstance(sel, exp.Star) for sel in cte.this.expressions
                        ):
                            left_column = join_column.copy().set_table_name(cte.alias_or_name)
                            right_column = join_column.copy().set_table_name(effective_join_alias)
                            join_column_pairs.append((left_column, right_column))
                            num_matching_ctes += 1
                            break
                    if num_matching_ctes == 0:
                        raise ValueError(
                            f"Column `{join_column.alias_or_name}` does not exist in any of the tables."
                        )
                join_clause = functools.reduce(
                    lambda x, y: x & y,
                    [left == right for left, right in join_column_pairs],
                )

                join_column_names: list = [
                    (
                        coalesce(
                            left_col.sql(dialect=self.session.input_dialect),
                            right_col.sql(dialect=self.session.input_dialect),
                        ).alias(left_col.alias_or_name)
                        if join_type == "full outer"
                        else left_col.alias_or_name
                    )
                    for left_col, right_col in join_column_pairs
                ]
                select_column_names: list = [
                    (
                        column.alias_or_name
                        if not isinstance(column.expression.this, exp.Star)
                        else column.sql()
                    )
                    for column in select_columns
                ]
                select_column_names = [
                    column_name
                    for column_name in select_column_names
                    if column_name
                    not in [
                        x.alias_or_name if not isinstance(x, str) else x for x in join_column_names
                    ]
                ]
                select_column_names = join_column_names + select_column_names
            else:
                # Expression join: normalize against the full join expression
                normalized_join_columns = left_temp_df._ensure_and_normalize_cols(
                    join_columns,
                    join_expression,
                    remove_identifier_if_possible=True,
                )
                if len(normalized_join_columns) > 1:
                    normalized_join_columns = [
                        functools.reduce(lambda x, y: x & y, normalized_join_columns)
                    ]
                join_clause = normalized_join_columns[0]
                select_column_names = [column.alias_or_name for column in select_columns]
        else:
            select_column_names = [column.alias_or_name for column in select_columns]

        # 12. Set the actual join ON clause
        join_expression.args["joins"][-1].set("on", join_clause.expression if join_clause else None)

        # 13. Build result state with merged hints
        result_state = CompilationState(
            expression=join_expression,
            last_op=Operation.FROM,
            branch_id=left_state.branch_id,
            sequence_id=left_state.sequence_id,
            pending_hints=list(left_state.pending_hints) + list(right_state.pending_hints),
            display_name_mapping=dict(left_state.display_name_mapping),
        )

        # 14. Apply final select (using temp df + plan-based select to reuse logic)
        temp_df = self._create_temp_df(result_state)
        temp_df.last_op = Operation.FROM
        result_df = temp_df.select(
            *select_column_names, skip_update_display_name_mapping=True, _skip_cte_wrap=True
        )
        final_state = self.compile_with_state(result_df._plan)
        final_state.pending_hints = result_state.pending_hints
        final_state.last_op = Operation.FROM
        return final_state

    # -------------------------------------------------------------------
    # Group aggregation compilation (GROUP_BY + SELECT)
    # -------------------------------------------------------------------

    def _compile_group_agg(self, node: GroupAggNode) -> CompilationState:
        """Port of groupBy() + GroupedData.agg() for non-pivot aggregation.

        Applies two rounds of CTE wrapping (GROUP_BY then SELECT),
        normalizes group-by columns and agg expressions, builds the
        GROUP BY + SELECT expression.
        """
        from sqlframe.base.normalize import (
            _extract_column_name,
            is_column_unambiguously_available,
        )

        # 1. Compile the base DataFrame's plan
        state = self._compile_node(node.parent.parent)

        # 2. Extra SELECT CTE wrapping when called from DataFrame.agg()
        if node.df_agg:
            state = self._maybe_wrap_cte(state, Operation.SELECT)

        # 3. GROUP_BY CTE wrapping (port of @operation(GROUP_BY) on groupBy)
        state = self._maybe_wrap_cte(state, Operation.GROUP_BY)

        # 4. Normalize group-by columns
        group_temp_df = self._create_temp_df(state)
        group_by_cols = (
            group_temp_df._ensure_and_normalize_cols(node.parent.cols) if node.parent.cols else []
        )
        # Post-process: remove table qualifiers for unambiguous columns
        for col in group_by_cols:
            if col.column_expression.args.get("table"):
                column_name = _extract_column_name(this) if (this := col.expression.this) else None
                if column_name and is_column_unambiguously_available(state.expression, column_name):
                    col.column_expression.set("table", None)

        # 5. SELECT CTE wrapping (port of @group_operation(SELECT) on agg)
        state = self._maybe_wrap_cte(state, Operation.SELECT)

        # 6. Normalize agg expressions
        agg_temp_df = self._create_temp_df(state)
        agg_cols = agg_temp_df._ensure_and_normalize_cols(node.agg_exprs)

        # 7. Handle display name mapping for DataFrame.agg()
        if node.df_agg:
            agg_temp_df._update_display_name_mapping(agg_cols, node.agg_exprs)
            state.display_name_mapping = agg_temp_df.display_name_mapping

        # 8. Build the expression
        if not node.is_cube:
            expression = state.expression.group_by(
                *[x.column_expression for x in group_by_cols]
            ).select(
                *[x.expression for x in group_by_cols + agg_cols],  # type: ignore
                append=False,
            )
            effective_group_by_cols = group_by_cols
        else:
            import itertools

            expression = state.expression
            all_grouping_sets = []
            for i in reversed(range(len(group_by_cols) + 1)):
                for combo in itertools.combinations(group_by_cols, i):
                    all_grouping_sets.append(
                        exp.Tuple(expressions=[x.column_expression for x in combo])
                    )
            group_by = exp.Group(grouping_sets=[exp.GroupingSets(expressions=all_grouping_sets)])
            expression.set("group", group_by)
            effective_group_by_cols = group_by_cols

        # Expand GROUPING_ID to include all group-by columns
        for col in agg_cols:
            if col.column_expression.this == "GROUPING_ID":
                col.column_expression.set(
                    "expressions", [x.expression for x in effective_group_by_cols]
                )

        if node.is_cube:
            expression = expression.select(
                *[x.expression for x in effective_group_by_cols + agg_cols],
                append=False,
            )

        state.expression = expression
        state.last_op = Operation.SELECT
        return state

    def _compile_pivot_agg(self, node: PivotAggNode) -> CompilationState:
        """Port of GroupedData.agg() for pivot aggregation.

        Applies CTE wrapping, normalizes columns, builds the pivot
        expression with subquery.
        """
        from sqlframe.base.column import Column

        # 1. Compile the base DataFrame's plan (through PivotNode → GroupByNode)
        state = self._compile_node(node.parent.parent.parent)

        # 2. GROUP_BY CTE wrapping
        state = self._maybe_wrap_cte(state, Operation.GROUP_BY)

        # 3. SELECT CTE wrapping
        state = self._maybe_wrap_cte(state, Operation.SELECT)

        # 4. Normalize group-by columns and agg expressions
        temp_df = self._create_temp_df(state)
        group_by_cols = (
            temp_df._ensure_and_normalize_cols(node.group_by_cols) if node.group_by_cols else []
        )
        cols = temp_df._ensure_and_normalize_cols(node.agg_exprs)

        pivot_col = node.parent.pivot_col
        pivot_values = node.parent.pivot_values

        if self.session._is_snowflake and len(cols) > 1:
            raise ValueError(
                "Snowflake does not support multiple aggregation functions in a single group by operation."
            )

        # 5. Build the base query with group by columns, pivot column, and agg columns
        select_cols: list = []
        for col in group_by_cols:
            select_cols.append(col.expression)  # type: ignore
        select_cols.append(Column.ensure_col(pivot_col).expression)
        for agg_col in cols:
            if (
                isinstance(agg_col.column_expression, exp.AggFunc)
                and agg_col.column_expression.this
            ):
                if agg_col.column_expression.this not in select_cols:
                    select_cols.append(agg_col.column_expression.this)

        base_query = state.expression.select(*select_cols, append=False)

        # 6. Build pivot expressions
        pivot_expressions = []
        for agg_col in cols:
            if isinstance(agg_col.column_expression, exp.AggFunc):
                agg_func = (
                    agg_col.column_expression.copy()
                    if self.session._is_snowflake
                    else agg_col.expression.copy()
                )
                pivot_expressions.append(agg_func)

        # 7. Create IN clause with pivot values
        in_values = []
        for v in pivot_values:
            if isinstance(v, str):
                in_values.append(exp.Literal.string(v))
            else:
                in_values.append(exp.Literal.number(v))

        pivot = exp.Pivot(
            expressions=pivot_expressions,
            fields=[
                exp.In(
                    this=Column.ensure_col(pivot_col).column_expression,
                    expressions=in_values,
                )
            ],
        )

        subquery = base_query.subquery()
        subquery.set("pivots", [pivot])

        # 8. Build final select with dialect-specific column handling
        final_select_in_values: list = []
        for col_val in in_values:
            for agg_col in cols:
                original_name = col_val.alias_or_name
                if self.session._is_snowflake:
                    new_col = exp.to_column(
                        col_val.alias_or_name,
                        quoted=True,
                        dialect=self.session.execution_dialect,
                    )
                    new_col.this.set("this", f"'{new_col.this.this}'")
                    new_col = exp.alias_(new_col, original_name)
                    new_col.unalias()._meta = {"case_sensitive": True}
                elif self.session._is_bigquery:
                    new_col = exp.to_column(
                        f"{agg_col.alias_or_name}_{original_name}",
                        dialect=self.session.execution_dialect,
                    )
                    new_col = (
                        exp.alias_(new_col, original_name)
                        if len(cols) == 1
                        else exp.alias_(new_col, f"{original_name}_{agg_col.alias_or_name}")
                    )
                elif self.session._is_duckdb:
                    new_col = exp.column(f"{original_name}_{agg_col.expression.alias_or_name}")
                    if len(cols) == 1:
                        new_col = exp.alias_(new_col, original_name)
                else:
                    new_col = (
                        exp.column(original_name)
                        if len(cols) == 1
                        else exp.column(f"{original_name}_{agg_col.expression.alias_or_name}")
                    )
                final_select_in_values.append(new_col)

        expression = exp.select(
            *[x.column_expression for x in group_by_cols] + final_select_in_values  # type: ignore
        ).from_(subquery)

        state.expression = expression
        state.last_op = Operation.SELECT
        return state
