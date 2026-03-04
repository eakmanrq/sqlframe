from __future__ import annotations

import typing as t
from dataclasses import dataclass, field

from sqlglot import expressions as exp

from sqlframe.base.operations import Operation

if t.TYPE_CHECKING:
    from sqlframe.base._typing import ColumnOrLiteral, ColumnOrName, OutputExpressionContainer
    from sqlframe.base.column import Column
    from sqlframe.base.compiler import CompilationState, PlanCompiler


@dataclass
class PlanNode:
    """Base class for all nodes in a DataFrame's logical plan.

    The plan is a tree of PlanNode instances. Each node records a single operation
    (select, filter, join, etc.) along with its raw parameters. The tree is compiled
    into a sqlglot expression only when SQL output is requested.
    """

    _operation: Operation = field(default=Operation.INIT, repr=False)

    @property
    def operation(self) -> Operation:
        return self._operation

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        raise NotImplementedError(f"Compilation of {type(self).__name__} not yet implemented.")


# ---------------------------------------------------------------------------
# Source node (root of every plan tree)
# ---------------------------------------------------------------------------


@dataclass
class SourceNode(PlanNode):
    """Root node: wraps a pre-built sqlglot Select expression.

    Created by session.createDataFrame(), session.sql(), reader.table(), etc.
    Also created when an eagerly-built expression is wrapped (e.g., from
    unconverted @operation methods). In that case, last_op carries the
    operation level of the expression so the compiler can make correct
    CTE wrapping decisions.
    """

    expression: exp.Select = field(default_factory=exp.Select)
    branch_id: str = ""
    sequence_id: str = ""
    last_op: Operation = Operation.INIT
    output_expression_container: t.Optional[OutputExpressionContainer] = None

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_source(self)


# ---------------------------------------------------------------------------
# SELECT-level nodes (Operation.SELECT)
# ---------------------------------------------------------------------------


@dataclass
class SelectNode(PlanNode):
    """df.select(*cols)"""

    parent: PlanNode = field(default_factory=PlanNode)
    cols: t.Tuple[t.Any, ...] = ()
    kwargs: t.Dict[str, t.Any] = field(default_factory=dict)
    _operation: Operation = field(default=Operation.SELECT, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_select(self)


@dataclass
class WithColumnsNode(PlanNode):
    """df.withColumn(name, col) / df.withColumns(colsMap)"""

    parent: PlanNode = field(default_factory=PlanNode)
    cols_map: t.Tuple[t.Dict[str, t.Any], ...] = ()
    _operation: Operation = field(default=Operation.SELECT, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_with_columns(self)


@dataclass
class DropNode(PlanNode):
    """df.drop(*cols)"""

    parent: PlanNode = field(default_factory=PlanNode)
    cols: t.Tuple[t.Any, ...] = ()
    _operation: Operation = field(default=Operation.SELECT, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_drop(self)


@dataclass
class DistinctNode(PlanNode):
    """df.distinct()"""

    parent: PlanNode = field(default_factory=PlanNode)
    _operation: Operation = field(default=Operation.SELECT, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_distinct(self)


@dataclass
class DropDuplicatesNode(PlanNode):
    """df.dropDuplicates(subset)"""

    parent: PlanNode = field(default_factory=PlanNode)
    subset: t.Optional[t.List[str]] = None
    _operation: Operation = field(default=Operation.SELECT, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_drop_duplicates(self)


@dataclass
class UnpivotNode(PlanNode):
    """df.unpivot(ids, values, variableColumnName, valueColumnName)"""

    parent: PlanNode = field(default_factory=PlanNode)
    ids: t.Any = None
    values: t.Any = None
    variable_column_name: str = ""
    value_column_name: str = ""
    _operation: Operation = field(default=Operation.SELECT, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_unpivot(self)


@dataclass
class RenameColumnsNode(PlanNode):
    """df.withColumnRenamed() / df.withColumnsRenamed()

    Uses a custom CTE-wrapping rule (wraps only if last_op > SELECT, not on SELECT==SELECT).
    """

    parent: PlanNode = field(default_factory=PlanNode)
    cols_map: t.Dict[str, str] = field(default_factory=dict)
    raise_on_missing: bool = True
    _operation: Operation = field(default=Operation.SELECT, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_rename_columns(self)


@dataclass
class ToDFNode(PlanNode):
    """df.toDF(*cols) — rename all columns"""

    parent: PlanNode = field(default_factory=PlanNode)
    col_names: t.Tuple[str, ...] = ()
    _operation: Operation = field(default=Operation.SELECT, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_to_df(self)


# ---------------------------------------------------------------------------
# WHERE-level nodes (Operation.WHERE)
# ---------------------------------------------------------------------------


@dataclass
class FilterNode(PlanNode):
    """df.where(condition) / df.filter(condition)"""

    parent: PlanNode = field(default_factory=PlanNode)
    condition: t.Any = None  # str, Column, bool, or exp.Expression
    _operation: Operation = field(default=Operation.WHERE, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_filter(self)


# ---------------------------------------------------------------------------
# GROUP_BY-level nodes (Operation.GROUP_BY)
# ---------------------------------------------------------------------------


@dataclass
class GroupByNode(PlanNode):
    """Intermediate node for df.groupBy(*cols) — returns GroupedData, not a DataFrame.

    This node stores the group-by columns. The actual aggregation is applied
    by GroupAggNode or PivotAggNode.
    """

    parent: PlanNode = field(default_factory=PlanNode)
    cols: t.Tuple[t.Any, ...] = ()
    _operation: Operation = field(default=Operation.GROUP_BY, repr=False)


@dataclass
class GroupAggNode(PlanNode):
    """GroupedData.agg(*exprs) — aggregation without pivot"""

    parent: GroupByNode = field(default_factory=GroupByNode)
    group_by_cols: t.List[t.Any] = field(default_factory=list)
    agg_exprs: t.Tuple[t.Any, ...] = ()
    df_agg: bool = False  # True when created by DataFrame.agg() (extra SELECT wrapping)
    is_cube: bool = (
        False  # True when created by DataFrame.cube() (compiler generates grouping sets)
    )
    _operation: Operation = field(default=Operation.SELECT, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_group_agg(self)


@dataclass
class PivotNode(PlanNode):
    """GroupedData.pivot(pivot_col, values) — intermediate pivot node"""

    parent: GroupByNode = field(default_factory=GroupByNode)
    pivot_col: t.Any = None
    pivot_values: t.Optional[t.List[t.Any]] = None


@dataclass
class PivotAggNode(PlanNode):
    """GroupedData.agg(*exprs) after pivot()"""

    parent: PivotNode = field(default_factory=PivotNode)
    group_by_cols: t.List[t.Any] = field(default_factory=list)
    agg_exprs: t.Tuple[t.Any, ...] = ()
    _operation: Operation = field(default=Operation.SELECT, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_pivot_agg(self)


# ---------------------------------------------------------------------------
# ORDER_BY-level nodes (Operation.ORDER_BY)
# ---------------------------------------------------------------------------


@dataclass
class SortNode(PlanNode):
    """df.orderBy(*cols, ascending=...) / df.sort(...)"""

    parent: PlanNode = field(default_factory=PlanNode)
    cols: t.Tuple[t.Any, ...] = ()
    ascending: t.Optional[t.Union[t.Any, t.List[t.Any]]] = None
    _operation: Operation = field(default=Operation.ORDER_BY, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_sort(self)


# ---------------------------------------------------------------------------
# LIMIT-level nodes (Operation.LIMIT)
# ---------------------------------------------------------------------------


@dataclass
class LimitNode(PlanNode):
    """df.limit(n)"""

    parent: PlanNode = field(default_factory=PlanNode)
    num: int = 0
    _operation: Operation = field(default=Operation.LIMIT, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_limit(self)


# ---------------------------------------------------------------------------
# FROM-level nodes (Operation.FROM)
# ---------------------------------------------------------------------------


@dataclass
class JoinNode(PlanNode):
    """df.join(other, on, how) / df.crossJoin(other)"""

    parent: PlanNode = field(default_factory=PlanNode)
    other: PlanNode = field(default_factory=PlanNode)
    on: t.Any = None  # str, List[str], Column, List[Column], or None
    how: str = "inner"
    # Metadata for self-join handling (from the DataFrames at join creation time)
    left_branch_id: str = ""
    right_branch_id: str = ""
    left_join_on_uuid: str = ""
    right_join_on_uuid: str = ""
    left_known_uuids: t.Set[str] = field(default_factory=set)
    right_known_uuids: t.Set[str] = field(default_factory=set)
    # Pending hints from both sides
    left_pending_hints: t.List[t.Any] = field(default_factory=list)
    right_pending_hints: t.List[t.Any] = field(default_factory=list)
    _operation: Operation = field(default=Operation.FROM, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_join(self)


@dataclass
class SetOpNode(PlanNode):
    """df.union(other) / df.intersect(other) / df.exceptAll(other)"""

    parent: PlanNode = field(default_factory=PlanNode)
    other: PlanNode = field(default_factory=PlanNode)
    op_class: t.Any = None  # exp.Union, exp.Intersect, or exp.Except
    distinct: bool = False
    _operation: Operation = field(default=Operation.FROM, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_set_op(self)


@dataclass
class UnionByNameNode(PlanNode):
    """df.unionByName(other, allowMissingColumns)"""

    parent: PlanNode = field(default_factory=PlanNode)
    other: PlanNode = field(default_factory=PlanNode)
    allow_missing_columns: bool = False
    _operation: Operation = field(default=Operation.FROM, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_union_by_name(self)


@dataclass
class DropNaNode(PlanNode):
    """df.dropna(how, thresh, subset)"""

    parent: PlanNode = field(default_factory=PlanNode)
    how: str = "any"
    thresh: t.Optional[int] = None
    subset: t.Optional[t.Union[str, t.Tuple[str, ...], t.List[str]]] = None
    _operation: Operation = field(default=Operation.FROM, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_dropna(self)


@dataclass
class FillNaNode(PlanNode):
    """df.fillna(value, subset)"""

    parent: PlanNode = field(default_factory=PlanNode)
    value: t.Any = None
    subset: t.Optional[t.Union[str, t.Tuple[str, ...], t.List[str]]] = None
    _operation: Operation = field(default=Operation.FROM, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_fillna(self)


@dataclass
class ReplaceNode(PlanNode):
    """df.replace(to_replace, value, subset)"""

    parent: PlanNode = field(default_factory=PlanNode)
    to_replace: t.Any = None
    value: t.Any = None
    subset: t.Any = None
    _operation: Operation = field(default=Operation.FROM, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_replace(self)


# ---------------------------------------------------------------------------
# NO_OP-level nodes (Operation.NO_OP)
# ---------------------------------------------------------------------------


@dataclass
class AliasNode(PlanNode):
    """df.alias(name)"""

    parent: PlanNode = field(default_factory=PlanNode)
    name: str = ""
    new_sequence_id: str = ""
    _operation: Operation = field(default=Operation.NO_OP, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_alias(self)


@dataclass
class HintNode(PlanNode):
    """df.hint(name, *parameters) / df.repartition(...) / df.coalesce(...)"""

    parent: PlanNode = field(default_factory=PlanNode)
    hint_expression: t.Any = None  # exp.JoinHint or exp.Anonymous
    _operation: Operation = field(default=Operation.NO_OP, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_hint(self)


@dataclass
class CacheNode(PlanNode):
    """df.cache() / df.persist(storageLevel)"""

    parent: PlanNode = field(default_factory=PlanNode)
    storage_level: str = "MEMORY_AND_DISK"
    _operation: Operation = field(default=Operation.NO_OP, repr=False)

    def compile(self, compiler: PlanCompiler) -> CompilationState:
        return compiler._compile_cache(self)
