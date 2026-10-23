from __future__ import annotations

import typing as t
from functools import cached_property

from sqlglot import exp, parse_one
from sqlglot.dialects import DuckDB

from sqlframe.base.session import _BaseSession
from sqlframe.base.util import soundex
from sqlframe.duckdb.catalog import DuckDBCatalog
from sqlframe.duckdb.dataframe import DuckDBDataFrame
from sqlframe.duckdb.readwriter import DuckDBDataFrameReader, DuckDBDataFrameWriter
from sqlframe.duckdb.table import DuckDBTable
from sqlframe.duckdb.udf import DuckDBUDFRegistration

if t.TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

else:
    DuckDBPyConnection = t.Any


class _DuckDBV2(DuckDB):
    class Generator(DuckDB.Generator):
        def lambda_sql(self, expression: exp.Lambda, *args, **kwargs) -> str:
            expression = expression.copy()
            expression.set("colon", True)
            return super().lambda_sql(expression, *args, **kwargs)


class DuckDBSession(
    _BaseSession[
        DuckDBCatalog,
        DuckDBDataFrameReader,
        DuckDBDataFrameWriter,
        DuckDBDataFrame,
        DuckDBTable,
        DuckDBPyConnection,  # type: ignore[type-var]
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
        from duckdb import InvalidInputException

        try:  # Available from duckdb 1.4.1
            from duckdb.sqltypes import VARCHAR
        except ImportError:
            from duckdb.typing import VARCHAR

        if not hasattr(self, "_conn"):
            conn = conn or duckdb.connect()
            try:
                self._udf_conn = conn.cursor()
                self._udf_conn.create_function("SOUNDEX", lambda x: soundex(x), return_type=VARCHAR)
            except (ImportError, InvalidInputException):
                pass

            super().__init__(conn, *args, **kwargs)
            self._last_result = None

    @cached_property
    def _cur(self) -> DuckDBPyConnection:
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
        self._last_result = self._cur.execute(sql)

    def _to_sql(self, sql: t.Union[str, exp.Expr], **kwargs) -> str:
        import duckdb

        duckdb_v2 = tuple(map(int, duckdb.__version__.split(".")[:2])) >= (2, 0)
        if duckdb_v2:
            kwargs.setdefault("dialect", _DuckDBV2())
        generated_sql = super()._to_sql(sql, **kwargs)
        if duckdb_v2 and " -> " in generated_sql:
            # Older SQLGlot generators insert some lambdas after lambda_sql has run.
            parsed = parse_one(generated_sql, dialect="duckdb")
            if parsed.find(exp.Lambda):
                return parsed.sql(dialect=_DuckDBV2())
        return generated_sql

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
