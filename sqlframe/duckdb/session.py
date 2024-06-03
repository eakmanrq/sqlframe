from __future__ import annotations

import typing as t
from functools import cached_property

from sqlframe.base.session import _BaseSession
from sqlframe.base.util import soundex, verify_numpy_installed
from sqlframe.duckdb.catalog import DuckDBCatalog
from sqlframe.duckdb.dataframe import DuckDBDataFrame
from sqlframe.duckdb.readwriter import (
    DuckDBDataFrameReader,
    DuckDBDataFrameWriter,
)

if t.TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

else:
    DuckDBPyConnection = t.Any


class DuckDBSession(
    _BaseSession[  # type: ignore
        DuckDBCatalog,
        DuckDBDataFrameReader,
        DuckDBDataFrameWriter,
        DuckDBDataFrame,
        DuckDBPyConnection,
    ]
):
    _catalog = DuckDBCatalog
    _reader = DuckDBDataFrameReader
    _writer = DuckDBDataFrameWriter
    _df = DuckDBDataFrame

    DEFAULT_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self, conn: t.Optional[DuckDBPyConnection] = None, *args, **kwargs):
        import duckdb
        from duckdb.typing import VARCHAR

        if not hasattr(self, "_conn"):
            conn = conn or duckdb.connect()
            try:
                # Creating a function requires numpy to be installed so if they don't have it, we'll just skip it
                verify_numpy_installed()
                conn.create_function("SOUNDEX", lambda x: soundex(x), return_type=VARCHAR)
            except ImportError:
                pass

            super().__init__(conn, *args, **kwargs)

    @classmethod
    def _try_get_map(cls, value: t.Any) -> t.Optional[t.Dict[str, t.Any]]:
        if value and isinstance(value, dict) and "key" in value and "value" in value:
            return dict(zip(value["key"], value["value"]))
        return None

    class Builder(_BaseSession.Builder):
        DEFAULT_INPUT_DIALECT = "duckdb"
        DEFAULT_OUTPUT_DIALECT = "duckdb"

        @cached_property
        def session(self) -> DuckDBSession:
            return DuckDBSession(**self._session_kwargs)

        def getOrCreate(self) -> DuckDBSession:
            self._set_session_properties()
            return self.session

    builder = Builder()
