from __future__ import annotations

import typing as t

from sqlframe.base.session import _BaseSession
from sqlframe.clickhouse.catalog import ClickHouseCatalog
from sqlframe.clickhouse.dataframe import ClickHouseDataFrame
from sqlframe.clickhouse.readwriter import ClickHouseDataFrameReader, ClickHouseDataFrameWriter
from sqlframe.clickhouse.table import ClickHouseTable
from sqlframe.clickhouse.udf import ClickHouseUDFRegistration

if t.TYPE_CHECKING:
    from clickhouse_connect.dbapi.connection import Connection
else:
    Connection = t.Any


class ClickHouseSession(
    _BaseSession[
        ClickHouseCatalog,
        ClickHouseDataFrameReader,
        ClickHouseDataFrameWriter,
        ClickHouseDataFrame,
        ClickHouseTable,
        Connection,  # type: ignore[type-var]
        ClickHouseUDFRegistration,
    ]
):
    _catalog = ClickHouseCatalog
    _reader = ClickHouseDataFrameReader
    _writer = ClickHouseDataFrameWriter
    _df = ClickHouseDataFrame
    _table = ClickHouseTable
    _udf_registration = ClickHouseUDFRegistration

    class Builder(_BaseSession.Builder):
        DEFAULT_EXECUTION_DIALECT = "clickhouse"

        @property
        def session(self) -> ClickHouseSession:
            return ClickHouseSession(**self._session_kwargs)

        def getOrCreate(self) -> ClickHouseSession:
            return super().getOrCreate()  # type: ignore

    builder = Builder()
