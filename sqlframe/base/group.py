# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import sys
import typing as t

from sqlframe.base.operations import Operation

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

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
        group_by_cols: t.List[Column],
        last_op: Operation,
        is_cube: bool = False,
    ):
        self._df = df
        self.session = df.session
        self.last_op = last_op
        self.group_by_cols = group_by_cols
        self.is_cube = is_cube
        self.pivot_col: t.Optional[str] = None
        self.pivot_values: t.Optional[t.List[t.Any]] = None

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

    def agg(self, *exprs: t.Union[Column, t.Dict[str, str]]) -> DF:
        from sqlframe.base.plan import (
            GroupAggNode,
            GroupByNode,
            PivotAggNode,
            PivotNode,
        )

        columns = (
            [
                self._get_function_applied_columns(agg_func, (column_name,))[0]
                for column_name, agg_func in exprs[0].items()
            ]
            if isinstance(exprs[0], dict)
            else list(exprs)
        )

        if self.pivot_col is not None and self.pivot_values is not None:
            return self._df._with_plan(
                PivotAggNode(
                    parent=PivotNode(
                        parent=GroupByNode(parent=self._df._plan, cols=tuple(self.group_by_cols)),
                        pivot_col=self.pivot_col,
                        pivot_values=self.pivot_values,
                    ),
                    group_by_cols=list(self.group_by_cols),
                    agg_exprs=tuple(columns),
                )
            )

        return self._df._with_plan(
            GroupAggNode(
                parent=GroupByNode(parent=self._df._plan, cols=tuple(self.group_by_cols)),
                group_by_cols=list(self.group_by_cols),
                agg_exprs=tuple(columns),
                is_cube=self.is_cube,
            )
        )

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

    def pivot(self, pivot_col: str, values: t.Optional[t.List[t.Any]] = None) -> Self:
        """
        Pivots a column of the current DataFrame and perform the specified aggregation.

        There are two versions of the pivot function: one that requires the caller
        to specify the list of distinct values to pivot on, and one that does not.
        The latter is more concise but less efficient, because Spark needs to first
        compute the list of distinct values internally.

        Parameters
        ----------
        pivot_col : str
            Name of the column to pivot.
        values : list, optional
            List of values that will be translated to columns in the output DataFrame.

        Returns
        -------
        GroupedData
            Returns self to allow chaining with aggregation methods.
        """
        if self.session._is_postgres:
            raise NotImplementedError(
                "Pivot operation is not supported in Postgres. Please create an issue if you would like a workaround implemented."
            )

        self.pivot_col = pivot_col

        if values is None:
            # Eagerly compute distinct values
            from sqlframe.base.column import Column

            distinct_df = self._df.select(pivot_col).distinct()
            distinct_rows = distinct_df.collect()
            # Sort to make the results deterministic
            self.pivot_values = sorted([row[0] for row in distinct_rows])
        else:
            self.pivot_values = values

        return self
