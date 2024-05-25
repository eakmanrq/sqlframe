from __future__ import annotations

import logging
import sys
import typing as t

from sqlframe.base.dataframe import (
    _BaseDataFrame,
    _BaseDataFrameNaFunctions,
    _BaseDataFrameStatFunctions,
)
from sqlframe.base.mixins.dataframe_mixins import PrintSchemaFromTempObjectsMixin
from sqlframe.duckdb.group import DuckDBGroupedData

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if t.TYPE_CHECKING:
    from sqlframe.duckdb.session import DuckDBSession  # noqa
    from sqlframe.duckdb.readwriter import DuckDBDataFrameWriter  # noqa
    from sqlframe.duckdb.group import DuckDBGroupedData  # noqa


logger = logging.getLogger(__name__)


class DuckDBDataFrameNaFunctions(_BaseDataFrameNaFunctions["DuckDBDataFrame"]):
    pass


class DuckDBDataFrameStatFunctions(_BaseDataFrameStatFunctions["DuckDBDataFrame"]):
    pass


class DuckDBDataFrame(
    PrintSchemaFromTempObjectsMixin,
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

    def cache(self) -> Self:
        logger.warning("DuckDB does not support caching. Ignoring cache() call.")
        return self

    def persist(self) -> Self:
        logger.warning("DuckDB does not support persist. Ignoring persist() call.")
        return self
