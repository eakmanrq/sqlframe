from __future__ import annotations

import logging
import sys
import typing as t

from sqlframe.base.dataframe import (
    _BaseDataFrame,
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
    from pyarrow import Table as ArrowTable


logger = logging.getLogger(__name__)


class DuckDBDataFrameNaFunctions(_BaseDataFrameNaFunctions["DuckDBDataFrame"]):
    pass


class DuckDBDataFrameStatFunctions(_BaseDataFrameStatFunctions["DuckDBDataFrame"]):
    pass


class DuckDBDataFrame(
    NoCachePersistSupportMixin,
    TypedColumnsFromTempViewMixin,
    _BaseDataFrame[
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

    def toArrow(self) -> ArrowTable:
        self._collect(skip_rows=True)
        return self.session._last_result.arrow()
