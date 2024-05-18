from __future__ import annotations

import typing as t
import warnings

from sqlframe.base.session import _BaseSession
from sqlframe.snowflake.catalog import SnowflakeCatalog
from sqlframe.snowflake.dataframe import SnowflakeDataFrame
from sqlframe.snowflake.readwriter import (
    SnowflakeDataFrameReader,
    SnowflakeDataFrameWriter,
)

if t.TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection
else:
    SnowflakeConnection = t.Any


class SnowflakeSession(
    _BaseSession[  # type: ignore
        SnowflakeCatalog,
        SnowflakeDataFrameReader,
        SnowflakeDataFrameWriter,
        SnowflakeDataFrame,
        SnowflakeConnection,
    ],
):
    _catalog = SnowflakeCatalog
    _reader = SnowflakeDataFrameReader
    _writer = SnowflakeDataFrameWriter
    _df = SnowflakeDataFrame

    def __init__(self, conn: t.Optional[SnowflakeConnection] = None):
        warnings.warn(
            "SnowflakeSession is still in active development. Functions may not work as expected."
        )
        if not hasattr(self, "_conn"):
            super().__init__(conn)

    class Builder(_BaseSession.Builder):
        DEFAULT_INPUT_DIALECT = "snowflake"
        DEFAULT_OUTPUT_DIALECT = "snowflake"

        @property
        def session(self) -> SnowflakeSession:
            return SnowflakeSession(**self._session_kwargs)

        def getOrCreate(self) -> SnowflakeSession:
            self._set_session_properties()
            return self.session

    builder = Builder()
