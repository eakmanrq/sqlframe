# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import sys
import typing as t

from sqlframe.base.operations import Operation, group_operation, operation

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
        group_by_cols: t.Union[t.List[Column], t.List[t.List[Column]]],
        last_op: Operation,
    ):
        self._df = df.copy()
        self.session = df.session
        self.last_op = last_op
        self.group_by_cols = group_by_cols
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

        # Handle pivot transformation
        if self.pivot_col is not None and self.pivot_values is not None:
            from sqlglot import exp

            from sqlframe.base import functions as F

            if self.session._is_snowflake and len(cols) > 1:
                raise ValueError(
                    "Snowflake does not support multiple aggregation functions in a single group by operation."
                )

            # Build the pivot expression
            # First, we need to convert the DataFrame to include the pivot logic
            df = self._df.copy()

            # Create the base query with group by columns, pivot column, and aggregation columns
            select_cols = []
            # Add group by columns
            for col in self.group_by_cols:
                select_cols.append(col.expression)  # type: ignore
            # Add pivot column
            select_cols.append(Column.ensure_col(self.pivot_col).expression)
            # Add the value columns that will be aggregated
            for agg_col in cols:
                # Extract the column being aggregated from the aggregation function
                # For example, from SUM(earnings), we want to extract 'earnings'
                if (
                    isinstance(agg_col.column_expression, exp.AggFunc)
                    and agg_col.column_expression.this
                ):
                    if agg_col.column_expression.this not in select_cols:
                        select_cols.append(agg_col.column_expression.this)

            # Create the base query
            base_query = df.expression.select(*select_cols, append=False)

            # Build pivot expression
            pivot_expressions = []
            for agg_col in cols:
                if isinstance(agg_col.column_expression, exp.AggFunc):
                    # Clone the aggregation function
                    # Snowflake doesn't support alias in the pivot, so we need to use the column_expression
                    agg_func = (
                        agg_col.column_expression.copy()
                        if self.session._is_snowflake
                        else agg_col.expression.copy()
                    )
                    pivot_expressions.append(agg_func)

            # Create the IN clause with pivot values
            in_values = []
            for v in self.pivot_values:
                if isinstance(v, str):
                    in_values.append(exp.Literal.string(v))
                else:
                    in_values.append(exp.Literal.number(v))

            # Build the pivot node with the fields parameter
            pivot = exp.Pivot(
                expressions=pivot_expressions,
                fields=[
                    exp.In(
                        this=Column.ensure_col(self.pivot_col).column_expression,
                        expressions=in_values,
                    )
                ],
            )

            # Create a subquery with the pivot attached
            subquery = base_query.subquery()
            subquery.set("pivots", [pivot])

            # Create the final select from the pivoted subquery
            final_select_in_values = []
            for col in in_values:  # type: ignore
                for agg_col in cols:
                    original_name = col.alias_or_name  # type: ignore
                    if self.session._is_snowflake:
                        # Snowflake takes the provided values, like 'Java', and creates the column as "'Java'"
                        # Therefore the user to select the column would need to use "'Java'"
                        # This does not conform to the PySpark API, nor is it very user-friendly.
                        # Therefore, we select the column as expected, and tell SQLFrame it is case-sensitive, but then
                        # alias is to case-insensitive "Java" so that the user can select it without quotes.
                        # This has a downside that if a user really needed case-sensitive column names then it wouldn't work.
                        new_col = exp.to_column(
                            col.alias_or_name,  # type: ignore
                            quoted=True,
                            dialect=self.session.execution_dialect,
                        )
                        new_col.this.set("this", f"'{new_col.this.this}'")
                        new_col = exp.alias_(new_col, original_name)
                        new_col.unalias()._meta = {"case_sensitive": True}
                    elif self.session._is_bigquery:
                        # BigQuery flips the alias order to <alias>_<value> instead of <value>_<alias>
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
                        # DuckDB always respects the alias if if num_cols == 1
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
                *[x.column_expression for x in self.group_by_cols] + final_select_in_values  # type: ignore
            ).from_(subquery)

            return self._df.copy(expression=expression)

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
