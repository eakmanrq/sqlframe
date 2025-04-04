# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import typing as t

from sqlframe.base.operations import Operation, group_operation, operation

if t.TYPE_CHECKING:
    from sqlframe.base.column import Column
    from sqlframe.base.session import DF
else:
    DF = t.TypeVar("DF")


# https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-groupby.html
# https://stackoverflow.com/questions/37975227/what-is-the-difference-between-cube-rollup-and-groupby-operators
class _BaseGroupedData(t.Generic[DF]):
    last_op: Operation

    def __init__(
        self,
        df: DF,
        group_by_cols: t.Union[t.List[Column], t.List[t.List[Column]]],
        last_op: Operation,
    ):
        self._df = df.copy()
        self.session = df.session
        self.last_op = last_op
        self.group_by_cols = group_by_cols

    def _get_function_applied_columns(
        self, func_name: str, cols: t.Tuple[str, ...]
    ) -> t.List[Column]:
        from sqlframe.base import functions as F

        func_name = func_name.lower()
        return [
            getattr(F, func_name)(name).alias(
                self.session._sanitize_column_name(f"{func_name}({name})")
            )
            for name in cols
        ]

    @group_operation(Operation.SELECT)
    def agg(self, *exprs: t.Union[Column, t.Dict[str, str]]) -> DF:
        from sqlframe.base.column import Column

        columns = (
            [
                self._get_function_applied_columns(agg_func, (column_name,))[0]
                for column_name, agg_func in exprs[0].items()
            ]
            if isinstance(exprs[0], dict)
            else exprs
        )
        cols = self._df._ensure_and_normalize_cols(columns)

        if not self.group_by_cols or not isinstance(self.group_by_cols[0], (list, tuple, set)):
            expression = self._df.expression.group_by(
                # User column_expression for group by to avoid alias in group by
                *[x.column_expression for x in self.group_by_cols]  # type: ignore
            ).select(*[x.expression for x in self.group_by_cols + cols], append=False)  # type: ignore
            group_by_cols = self.group_by_cols
        else:
            from sqlglot import exp

            expression = self._df.expression
            all_grouping_sets = []
            group_by_cols = []
            for grouping_set in self.group_by_cols:
                all_grouping_sets.append(
                    exp.Tuple(expressions=[x.column_expression for x in grouping_set])  # type: ignore
                )
                group_by_cols.extend(grouping_set)  # type: ignore
            group_by_cols = list(dict.fromkeys(group_by_cols))
            group_by = exp.Group(grouping_sets=[exp.GroupingSets(expressions=all_grouping_sets)])
            expression.set("group", group_by)
        for col in cols:
            # Spark supports having an empty grouping_id which means all of the columns but other dialects
            # like duckdb don't support this so we expand the grouping_id to include all of the columns
            if col.column_expression.this == "GROUPING_ID":
                col.column_expression.set("expressions", [x.expression for x in group_by_cols])  # type: ignore
        expression = expression.select(*[x.expression for x in group_by_cols + cols], append=False)  # type: ignore
        return self._df.copy(expression=expression)

    def count(self) -> DF:
        from sqlframe.base import functions as F

        return self.agg(F.count("*").alias("count"))

    def mean(self, *cols: str) -> DF:
        return self.avg(*cols)

    def avg(self, *cols: str) -> DF:
        return self.agg(*self._get_function_applied_columns("avg", cols))

    def max(self, *cols: str) -> DF:
        return self.agg(*self._get_function_applied_columns("max", cols))

    def min(self, *cols: str) -> DF:
        return self.agg(*self._get_function_applied_columns("min", cols))

    def sum(self, *cols: str) -> DF:
        return self.agg(*self._get_function_applied_columns("sum", cols))

    def pivot(self, *cols: str) -> DF:
        raise NotImplementedError("Sum distinct is not currently implemented")
