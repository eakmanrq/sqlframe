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
        self._collect(skip_rows=True)
        if not batch_size:
            return self.session._last_result.arrow()
        return self.session._last_result.fetch_record_batch(batch_size)
