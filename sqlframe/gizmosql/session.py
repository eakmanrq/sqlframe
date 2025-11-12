from __future__ import annotations

import typing as t
from functools import cached_property

from sqlframe.base.session import _BaseSession
from sqlframe.gizmosql.catalog import GizmoSQLCatalog
from sqlframe.gizmosql.dataframe import GizmoSQLDataFrame
from sqlframe.gizmosql.readwriter import (
    GizmoSQLDataFrameReader,
    GizmoSQLDataFrameWriter,
)
from sqlframe.gizmosql.table import GizmoSQLTable
from sqlframe.gizmosql.udf import GizmoSQLUDFRegistration

if t.TYPE_CHECKING:
    from sqlframe.gizmosql.connect import GizmoSQLConnection, GizmoSQLAdbcCursor

else:
    GizmoSQLConnection = t.Any
    GizmoSQLAdbcCursor = t.Any


class GizmoSQLSession(
    _BaseSession[  # type: ignore
        GizmoSQLCatalog,
        GizmoSQLDataFrameReader,
        GizmoSQLDataFrameWriter,
        GizmoSQLDataFrame,
        GizmoSQLTable,
        GizmoSQLConnection,  # type: ignore
        GizmoSQLUDFRegistration,
    ]
):
    _catalog = GizmoSQLCatalog
    _reader = GizmoSQLDataFrameReader
    _writer = GizmoSQLDataFrameWriter
    _df = GizmoSQLDataFrame
    _table = GizmoSQLTable
    _udf_registration = GizmoSQLUDFRegistration

    def __init__(self, conn: t.Optional[GizmoSQLConnection] = None):
        if not hasattr(self, "_conn"):
            super().__init__(conn)
            self._last_result = None

    @cached_property
    def _cur(self) -> GizmoSQLAdbcCursor:  # type: ignore
        return self._conn.cursor()

    @classmethod
    def _try_get_map(cls, value: t.Any) -> t.Optional[t.Dict[str, t.Any]]:
        if value and isinstance(value, dict):
            # GizmoSQL < 1.1.0 support
            if "key" in value and "value" in value:
                return dict(zip(value["key"], value["value"]))
            # GizmoSQL >= 1.1.0 support
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
        def session(self) -> GizmoSQLSession:
            return GizmoSQLSession(**self._session_kwargs)

        def getOrCreate(self) -> GizmoSQLSession:
            return super().getOrCreate()  # type: ignore

    builder = Builder()
