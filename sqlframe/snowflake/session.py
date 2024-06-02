from __future__ import annotations

import json
import typing as t
import warnings

try:
    from snowflake.connector.converter import SnowflakeConverter
except ImportError:
    SnowflakeConverter = object  # type: ignore

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


class JsonLoadsSnowflakeConverter(SnowflakeConverter):
    # This might not be needed once proper arrow types are supported.
    # Checkout this PR: https://github.com/snowflakedb/snowflake-connector-python/pull/1853/files
    # Specifically see if `alter session set enable_structured_types_in_client_response=true` and
    # `alter session set force_enable_structured_types_native_arrow_format=true` are supported then it might work.
    # At the time of writing these were not supported parameters on my version on Snowflake.
    def _json_loads(self, ctx: dict[str, t.Any]) -> t.Callable:
        def conv(value: str) -> t.List:
            # Snowflake returns "undefined" for null values when inside an array
            # We check if we replaced "'undefined'" string and if so we switch it back
            # this is a lazy approach compared to writing a proper regex replace
            return json.loads(value.replace("undefined", "null").replace("'null'", "undefined"))

        return conv

    _OBJECT_to_python = _json_loads  # type: ignore
    _VARIANT_to_python = _json_loads  # type: ignore
    _ARRAY_to_python = _json_loads  # type: ignore


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

    DEFAULT_TIME_FORMAT = "YYYY-MM-DD HH:MI:SS"

    def __init__(self, conn: t.Optional[SnowflakeConnection] = None):
        import snowflake

        snowflake.connector.cursor.CAN_USE_ARROW_RESULT_FORMAT = False

        if not hasattr(self, "_conn"):
            super().__init__(conn)
            if self._conn.converter and not isinstance(
                self._conn.converter, JsonLoadsSnowflakeConverter
            ):
                self._conn.converter = JsonLoadsSnowflakeConverter(
                    use_numpy=self._conn._numpy,
                    support_negative_year=self._conn._support_negative_year,
                )
            else:
                self._conn._converter_class = JsonLoadsSnowflakeConverter  # type: ignore

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
