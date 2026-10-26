from __future__ import annotations

import typing as t

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.base.readerwriter import _BaseDataFrameReader, _BaseDataFrameWriter

if t.TYPE_CHECKING:
    from sqlframe.clickhouse.dataframe import ClickHouseDataFrame
    from sqlframe.clickhouse.session import ClickHouseSession
    from sqlframe.clickhouse.table import ClickHouseTable


class ClickHouseDataFrameReader(
    _BaseDataFrameReader["ClickHouseSession", "ClickHouseDataFrame", "ClickHouseTable"]
):
    pass


class ClickHouseDataFrameWriter(_BaseDataFrameWriter["ClickHouseSession", "ClickHouseDataFrame"]):
    def insertInto(self, tableName: str, overwrite: t.Optional[bool] = None):
        raise UnsupportedOperationError(
            "ClickHouse table writes are unsupported; use the ClickHouse client for inserts."
        )

    def saveAsTable(self, name: str, format: t.Optional[str] = None, mode: t.Optional[str] = None):
        raise UnsupportedOperationError(
            "ClickHouse table writes are unsupported; create tables with an explicit engine "
            "using the ClickHouse client."
        )
