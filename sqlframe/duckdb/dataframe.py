from __future__ import annotations

import logging
import typing as t

from sqlframe.base.dataframe import (
    BaseDataFrame,
    _BaseDataFrameNaFunctions,
    _BaseDataFrameStatFunctions,
)
from sqlframe.base.mixins.dataframe_mixins import (
    NoCachePersistSupportMixin,
    TypedColumnsFromTempViewMixin,
)
from sqlframe.duckdb.group import DuckDBGroupedData

if t.TYPE_CHECKING:
    from sqlframe.duckdb.session import DuckDBSession  # noqa
    from sqlframe.duckdb.readwriter import DuckDBDataFrameWriter  # noqa
    from sqlframe.duckdb.group import DuckDBGroupedData  # noqa
    from pyarrow import Table as ArrowTable, RecordBatchReader

logger = logging.getLogger(__name__)


class DuckDBDataFrameNaFunctions(_BaseDataFrameNaFunctions["DuckDBDataFrame"]):
    pass


class DuckDBDataFrameStatFunctions(_BaseDataFrameStatFunctions["DuckDBDataFrame"]):
    pass


class DuckDBDataFrame(
    NoCachePersistSupportMixin,
    TypedColumnsFromTempViewMixin,
    BaseDataFrame[
        "DuckDBSession",
        "DuckDBDataFrameWriter",
        "DuckDBDataFrameNaFunctions",
        "DuckDBDataFrameStatFunctions",
        "DuckDBGroupedData",
    ],
):
    _na = DuckDBDataFrameNaFunctions
    _stat = DuckDBDataFrameStatFunctions
    _group_data = DuckDBGroupedData

    def _validate_plan(self) -> None:
        if not self.session.eager_validation:
            return

        import duckdb
        from sqlglot import exp

        expressions = self._get_expressions(optimize=False)
        if len(expressions) != 1 or not isinstance(expressions[0], exp.Select):
            return
        sql = "EXPLAIN " + self.session._to_sql(expressions[0])
        # DuckDB releases without ResourceInUseException, such as 1.2.x, run a new
        # statement while a result is unfinished instead of refusing it.
        resource_in_use = getattr(duckdb, "ResourceInUseException", None)
        in_use_errors = (resource_in_use,) if resource_in_use else ()
        try:
            self.session._conn.sql(sql)
        except in_use_errors:
            # A separate DuckDB cursor keeps the caller's unfinished result intact.
            cursor = self.session._conn.cursor()
            try:
                cursor.sql(sql)
            finally:
                cursor.close()

    def explain(
        self, extended: t.Optional[t.Union[bool, str]] = None, mode: t.Optional[str] = None
    ) -> None:
        results = self._get_explain_plan_rows()
        print(results[0][1])

    @t.overload
    def toArrow(self) -> ArrowTable: ...

    @t.overload
    def toArrow(self, batch_size: int) -> RecordBatchReader: ...

    def toArrow(self, batch_size: t.Optional[int] = None) -> t.Union[ArrowTable, RecordBatchReader]:
        if hasattr(self.session._cur, "fetch_arrow_table"):
            self._collect(skip_rows=True)
            assert self.session._last_result is not None
            if not batch_size:
                return getattr(self.session._last_result, "fetch_arrow_table")()
            return getattr(self.session._last_result, "fetch_record_batch")(batch_size)

        expressions = self._get_expressions(optimize=False)
        for expression in expressions[:-1]:
            self.session._execute(self.session._to_sql(expression))
        relation = self.session._cur.sql(self.session._to_sql(expressions[-1]))
        if not batch_size:
            return relation.to_arrow_table()
        return relation.to_arrow_reader(batch_size)
