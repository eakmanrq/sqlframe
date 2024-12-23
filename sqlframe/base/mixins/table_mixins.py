import functools
import logging
import typing as t

from sqlglot import exp

try:
    from sqlglot.expressions import Whens

    whens: t.Union[t.Type["Whens"], None] = Whens
except ImportError:
    whens = None
from sqlglot.helper import object_to_dict

from sqlframe.base.column import Column
from sqlframe.base.table import (
    DF,
    Clause,
    LazyExpression,
    WhenMatched,
    WhenNotMatched,
    WhenNotMatchedBySource,
    _BaseTable,
)

if t.TYPE_CHECKING:
    from sqlframe.base._typing import ColumnOrLiteral


logger = logging.getLogger(__name__)


def ensure_cte() -> t.Callable[[t.Callable], t.Callable]:
    def decorator(func: t.Callable) -> t.Callable:
        @functools.wraps(func)
        def wrapper(self: _BaseTable, *args, **kwargs) -> t.Any:
            if len(self.expression.ctes) > 0:
                return func(self, *args, **kwargs)  # type: ignore
            self_class = self.__class__
            self = self._convert_leaf_to_cte()
            self = self_class(**object_to_dict(self))
            return func(self, *args, **kwargs)  # type: ignore

        wrapper.__wrapped__ = func  # type: ignore
        return wrapper

    return decorator


class UpdateSupportMixin(_BaseTable, t.Generic[DF]):
    @ensure_cte()
    def update(
        self,
        _set: t.Dict[t.Union[Column, str], t.Union[Column, "ColumnOrLiteral", exp.Expression]],
        where: t.Optional[t.Union[Column, str, bool]] = None,
    ) -> LazyExpression:
        self_name = self.expression.ctes[0].this.args["from"].this.alias_or_name
        self_expr = self.expression.ctes[0].this.args["from"].this

        if where is None:
            logger.warning("Empty value for `where`clause.")
            condition: exp.Expression = exp.Boolean(this=True)
        else:
            condition_list = self._ensure_and_normalize_cols(where, self.expression)
            if len(condition_list) > 1:
                condition_list = [functools.reduce(lambda x, y: x & y, condition_list)]
            for col_expr in condition_list[0].expression.find_all(exp.Column):
                if col_expr.table == self.expression.args["from"].this.alias_or_name:
                    col_expr.set("table", exp.to_identifier(self_name))
            condition = condition_list[0].expression

        update_set = self._ensure_and_normalize_update_set(_set)

        # exp.update

        update_expr = exp.Update(
            this=self_expr,
            expressions=[
                exp.EQ(
                    this=key,
                    expression=val,
                )
                for key, val in update_set.items()
            ],
            where=exp.Where(this=condition),
        )

        return LazyExpression(update_expr, self.session)

    def _ensure_and_normalize_update_set(
        self,
        set_: t.Dict[t.Union[Column, str], t.Union[Column, "ColumnOrLiteral", exp.Expression]],
    ) -> t.Dict[str, exp.Expression]:
        self_name = self.expression.ctes[0].this.args["from"].this.alias_or_name
        update_set = {}
        for key, val in set_.items():
            key_column: Column = self._ensure_and_normalize_col(key)
            key_expr = list(key_column.expression.find_all(exp.Column))
            if len(key_expr) > 1:
                raise ValueError(f"Can only update one a single column at a time.")
            key = key_expr[0].alias_or_name

            val_column: Column = self._ensure_and_normalize_col(val)
            for col_expr in val_column.expression.find_all(exp.Column):
                if col_expr.table == self.expression.args["from"].this.alias_or_name:
                    col_expr.set("table", exp.to_identifier(self_name))
                else:
                    raise ValueError(
                        f"Column `{col_expr.alias_or_name}` does not exist in the table."
                    )

            update_set[key] = val_column.expression
        return update_set


class DeleteSupportMixin(_BaseTable, t.Generic[DF]):
    @ensure_cte()
    def delete(
        self,
        where: t.Optional[t.Union[Column, str, bool]] = None,
    ) -> LazyExpression:
        self_name = self.expression.ctes[0].this.args["from"].this.alias_or_name
        self_expr = self.expression.ctes[0].this.args["from"].this

        if where is None:
            logger.warning("Empty value for `where`clause. defaults to `True`.")
            condition: exp.Expression = exp.Boolean(this=True)
        else:
            condition_list = self._ensure_and_normalize_cols(where, self.expression)
            if len(condition_list) > 1:
                condition_list = [functools.reduce(lambda x, y: x & y, condition_list)]
            for col_expr in condition_list[0].expression.find_all(exp.Column):
                if col_expr.table == self.expression.args["from"].this.alias_or_name:
                    col_expr.set("table", exp.to_identifier(self_name))
            condition = condition_list[0].expression

        delete_expr = exp.Delete(
            this=self_expr,
            where=exp.Where(this=condition),
        )

        return LazyExpression(delete_expr, self.session)


class MergeSupportMixin(_BaseTable, t.Generic[DF]):
    _merge_supported_clauses: t.Iterable[
        t.Union[t.Type[WhenMatched], t.Type[WhenNotMatched], t.Type[WhenNotMatchedBySource]]
    ]
    _merge_support_star: bool

    @ensure_cte()
    def merge(
        self,
        other_df: DF,
        condition: t.Union[str, t.List[str], Column, t.List[Column]],
        clauses: t.Iterable[t.Union[WhenMatched, WhenNotMatched, WhenNotMatchedBySource]],
    ) -> LazyExpression:
        self_name = self.expression.ctes[0].this.args["from"].this.alias_or_name
        self_expr = self.expression.ctes[0].this.args["from"].this

        other_df = other_df._convert_leaf_to_cte()

        if condition is None:
            raise ValueError("condition cannot be None")

        condition_columns: Column = self._ensure_and_normalize_condition(condition, other_df)
        other_name = self._create_hash_from_expression(other_df.expression)
        other_expr = exp.Subquery(
            this=other_df.expression, alias=exp.TableAlias(this=exp.to_identifier(other_name))
        )

        for col_expr in condition_columns.expression.find_all(exp.Column):
            if col_expr.table == self.expression.args["from"].this.alias_or_name:
                col_expr.set("table", exp.to_identifier(self_name))
            if col_expr.table == other_df.latest_cte_name:
                col_expr.set("table", exp.to_identifier(other_name))

        merge_expressions = []
        for clause in clauses:
            if not isinstance(clause, tuple(self._merge_supported_clauses)):
                raise ValueError(
                    f"Unsupported clause type {type(clause.clause)} for merge operation"
                )
            expression = None

            if clause.clause.condition is not None:
                cond_clause = self._ensure_and_normalize_condition(
                    clause.clause.condition, other_df, True
                )
                for col_expr in cond_clause.expression.find_all(exp.Column):
                    if col_expr.table == self.expression.args["from"].this.alias_or_name:
                        col_expr.set("table", exp.to_identifier(self_name))
                    if col_expr.table == other_df.latest_cte_name:
                        col_expr.set("table", exp.to_identifier(other_name))
            else:
                cond_clause = None
            if clause.clause.clause_type == Clause.UPDATE:
                update_set = self._ensure_and_normalize_assignments(
                    clause.clause.assignments, other_df
                )
                expression = exp.When(
                    matched=clause.clause.matched,
                    source=clause.clause.by_source,
                    condition=cond_clause.expression if cond_clause else None,
                    then=exp.Update(
                        expressions=[
                            exp.EQ(
                                this=key,
                                expression=val,
                            )
                            for key, val in update_set.items()
                        ]
                    ),
                )
            if clause.clause.clause_type == Clause.UPDATE_ALL:
                if not self._support_star:
                    raise ValueError("Merge operation does not support UPDATE_ALL")
                expression = exp.When(
                    matched=clause.clause.matched,
                    source=clause.clause.by_source,
                    condition=cond_clause.expression if cond_clause else None,
                    then=exp.Update(expressions=[exp.Star()]),
                )
            elif clause.clause.clause_type == Clause.INSERT:
                insert_values = self._ensure_and_normalize_assignments(
                    clause.clause.assignments, other_df
                )
                expression = exp.When(
                    matched=clause.clause.matched,
                    source=clause.clause.by_source,
                    condition=cond_clause.expression if cond_clause else None,
                    then=exp.Insert(
                        this=exp.Tuple(expressions=[key for key in insert_values.keys()]),
                        expression=exp.Tuple(expressions=[val for val in insert_values.values()]),
                    ),
                )
            elif clause.clause.clause_type == Clause.INSERT_ALL:
                if not self._support_star:
                    raise ValueError("Merge operation does not support INSERT_ALL")
                expression = exp.When(
                    matched=clause.clause.matched,
                    source=clause.clause.by_source,
                    condition=cond_clause.expression if cond_clause else None,
                    then=exp.Insert(expression=exp.Star()),
                )
            elif clause.clause.clause_type == Clause.DELETE:
                expression = exp.When(
                    matched=clause.clause.matched,
                    source=clause.clause.by_source,
                    condition=cond_clause.expression if cond_clause else None,
                    then=exp.var("DELETE"),
                )

            if expression:
                merge_expressions.append(expression)

        if whens is None:
            merge_expr = exp.merge(
                *merge_expressions,
                into=self_expr,
                using=other_expr,
                on=condition_columns.expression,
            )
        else:
            merge_expr = exp.merge(
                whens(expressions=merge_expressions),
                into=self_expr,
                using=other_expr,
                on=condition_columns.expression,
            )

        return LazyExpression(merge_expr, self.session)

    def _ensure_and_normalize_condition(
        self,
        condition: t.Union[str, t.List[str], Column, t.List[Column]],
        other_df: DF,
        clause: t.Optional[bool] = False,
    ):
        join_expression = self._add_ctes_to_expression(
            self.expression, other_df.expression.copy().ctes
        )
        condition = self._ensure_and_normalize_cols(condition, self.expression)

        if self.branch_id == other_df.branch_id:
            other_df_unique_uuids = other_df.known_uuids - self.known_uuids
            for col in condition:
                for col_expr in col.expression.find_all(exp.Column):
                    if (
                        "join_on_uuid" in col_expr.meta
                        and col_expr.meta["join_on_uuid"] in other_df_unique_uuids
                    ):
                        col_expr.set("table", exp.to_identifier(other_df.latest_cte_name))

        if isinstance(condition[0].expression, exp.Column) and not clause:
            """
            Unique characteristics of join on column names only:
            * The column names are put at the front of the select list
            * The column names are deduplicated across the entire select list and only the column names (other dups are allowed)
            """
            table_names = [
                table.alias_or_name
                for table in [
                    self.expression.args["from"].this,
                    other_df.expression.args["from"].this,
                ]
            ]
            potential_ctes = [
                cte
                for cte in join_expression.ctes
                if cte.alias_or_name in table_names
                and cte.alias_or_name != other_df.latest_cte_name
            ]
            # Determine the table to reference for the left side of the join by checking each of the left side
            # tables and see if they have the column being referenced.
            join_column_pairs = []
            for join_column in condition:
                num_matching_ctes = 0
                for cte in potential_ctes:
                    if join_column.alias_or_name in cte.this.named_selects:
                        left_column = join_column.copy().set_table_name(cte.alias_or_name)
                        right_column = join_column.copy().set_table_name(other_df.latest_cte_name)
                        join_column_pairs.append((left_column, right_column))
                        num_matching_ctes += 1
                        # We only want to match one table to the column and that should be matched left -> right
                        # so we break after the first match
                        break
                if num_matching_ctes == 0:
                    raise ValueError(
                        f"Column `{join_column.alias_or_name}` does not exist in any of the tables."
                    )
            join_clause = functools.reduce(
                lambda x, y: x & y,
                [left_column == right_column for left_column, right_column in join_column_pairs],
            )
        else:
            """
            Unique characteristics of join on expressions:
            * There is no deduplication of the results.
            * The left join dataframe columns go first and right come after. No sort preference is given to join columns
            """
            condition = self._ensure_and_normalize_cols(condition, join_expression)
            if len(condition) > 1:
                condition = [functools.reduce(lambda x, y: x & y, condition)]
            join_clause = condition[0]
        return join_clause

    def _ensure_and_normalize_assignments(
        self,
        assignments: t.Dict[
            t.Union[Column, str], t.Union[Column, "ColumnOrLiteral", exp.Expression]
        ],
        other_df,
    ) -> t.Dict[str, exp.Expression]:
        self_name = self.expression.ctes[0].this.args["from"].this.alias_or_name
        other_name = self._create_hash_from_expression(other_df.expression)
        update_set = {}
        for key, val in assignments.items():
            key_column: Column = self._ensure_and_normalize_col(key)
            key_expr = list(key_column.expression.find_all(exp.Column))
            if len(key_expr) > 1:
                raise ValueError(f"Target expression `{key_expr}` should be a single column.")
            key = key_expr[0].alias_or_name

            val = self._ensure_and_normalize_col(val)
            val = self._ensure_and_normalize_cols(val, other_df.expression)[0]
            if self.branch_id == other_df.branch_id:
                other_df_unique_uuids = other_df.known_uuids - self.known_uuids
                for col_expr in val.expression.find_all(exp.Column):
                    if (
                        "join_on_uuid" in col_expr.meta
                        and col_expr.meta["join_on_uuid"] in other_df_unique_uuids
                    ):
                        col_expr.set("table", exp.to_identifier(other_df.latest_cte_name))

            for col_expr in val.expression.find_all(exp.Column):
                if not col_expr.table or col_expr.table == other_df.latest_cte_name:
                    col_expr.set("table", exp.to_identifier(other_name))
                elif col_expr.table == self.expression.args["from"].this.alias_or_name:
                    col_expr.set("table", exp.to_identifier(self_name))
                else:
                    raise ValueError(
                        f"Column `{col_expr.alias_or_name}` does not exist in any of the tables."
                    )

            update_set[key] = val.expression
        return update_set
