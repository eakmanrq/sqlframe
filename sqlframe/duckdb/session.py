from __future__ import annotations

import typing as t
from functools import cached_property

from sqlframe.base.session import _BaseSession
from sqlframe.base.util import soundex
from sqlframe.duckdb.catalog import DuckDBCatalog
from sqlframe.duckdb.dataframe import DuckDBDataFrame
from sqlframe.duckdb.readwriter import (
    DuckDBDataFrameReader,
    DuckDBDataFrameWriter,
)
from sqlframe.duckdb.table import DuckDBTable
from sqlframe.duckdb.udf import DuckDBUDFRegistration

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
        DuckDBTable,
        DuckDBPyConnection,  # type: ignore
        DuckDBUDFRegistration,
    ]
):
    _catalog = DuckDBCatalog
    _reader = DuckDBDataFrameReader
    _writer = DuckDBDataFrameWriter
    _df = DuckDBDataFrame
    _table = DuckDBTable
    _udf_registration = DuckDBUDFRegistration

    def __init__(self, conn: t.Optional[DuckDBPyConnection] = None, *args, **kwargs):
        import duckdb
        from duckdb.typing import VARCHAR

        if not hasattr(self, "_conn"):
            conn = conn or duckdb.connect()
            try:
                conn.create_function("SOUNDEX", lambda x: soundex(x), return_type=VARCHAR)
            except ImportError:
                pass

            super().__init__(conn, *args, **kwargs)
            self._last_result = None

    @cached_property
    def _cur(self) -> DuckDBPyConnection:  # type: ignore
        return self._conn

    @classmethod
    def _try_get_map(cls, value: t.Any) -> t.Optional[t.Dict[str, t.Any]]:
        if value and isinstance(value, dict):
            # DuckDB < 1.1.0 support
            if "key" in value and "value" in value:
                return dict(zip(value["key"], value["value"]))
            # DuckDB >= 1.1.0 support
            # If a key is not a string then it must not represent a column and therefore must be a map
            if len([k for k in value if not isinstance(k, str)]) > 0:
                return value
        return None

    def _execute(self, sql: str) -> None:
        self._last_result = self._cur.execute(sql)  # type: ignore

    @property
    def _is_duckdb(self) -> bool:
        return True

    class Builder(_BaseSession.Builder):
        DEFAULT_EXECUTION_DIALECT = "duckdb"

        @cached_property
        def session(self) -> DuckDBSession:
            return DuckDBSession(**self._session_kwargs)

        def getOrCreate(self) -> DuckDBSession:
            return super().getOrCreate()  # type: ignore

    builder = Builder()
