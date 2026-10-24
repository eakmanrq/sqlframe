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


_SPARK_TIMEZONE_KEY = "spark.sql.session.timeZone"


class _DuckDBRuntimeConfig:
    """The Spark session timezone backed by DuckDB's connection setting."""

    def __init__(self, session: DuckDBSession):
        self._session = session

    def get(self, key: str) -> str:
        if key != _SPARK_TIMEZONE_KEY:
            raise NotImplementedError(f"Unsupported Spark session config: {key}")
        return self._session._conn.execute("SELECT current_setting('TimeZone')").fetchone()[0]

    def set(self, key: str, value: str) -> None:
        if key != _SPARK_TIMEZONE_KEY:
            raise NotImplementedError(f"Unsupported Spark session config: {key}")
        self._session._conn.execute("SET SESSION TimeZone = ?", [value])


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

    def __init__(
        self,
        conn: t.Optional[DuckDBPyConnection] = None,
        *args,
        eager_validation: t.Optional[bool] = None,
        **kwargs,
    ):
        import duckdb
        from duckdb import InvalidInputException

        if eager_validation is not None and not isinstance(eager_validation, bool):
            raise ValueError("sqlframe.eager_validation must be a boolean")
        if eager_validation is not None or not hasattr(self, "eager_validation"):
            self.eager_validation = bool(eager_validation)

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

    @cached_property
    def conf(self) -> _DuckDBRuntimeConfig:
        return _DuckDBRuntimeConfig(self)

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
        EAGER_VALIDATION_KEY = "sqlframe.eager_validation"

        def _set_eager_validation(self, value: t.Any) -> None:
            if not isinstance(value, bool):
                raise ValueError("sqlframe.eager_validation must be a boolean")
            self._session_kwargs["eager_validation"] = value
            if "session" in self.__dict__:
                self.session.eager_validation = value

        def __init__(self):
            super().__init__()
            self._time_zone: t.Optional[str] = None

        def _set_config(
            self,
            key: t.Optional[str] = None,
            value: t.Optional[t.Any] = None,
            *,
            map: t.Optional[t.Dict[str, t.Any]] = None,
        ) -> None:
            if key == self.EAGER_VALIDATION_KEY:
                self._set_eager_validation(value)
                key, value = None, None
            if map and self.EAGER_VALIDATION_KEY in map:
                self._set_eager_validation(map[self.EAGER_VALIDATION_KEY])
                map = {k: v for k, v in map.items() if k != self.EAGER_VALIDATION_KEY}
            if key == _SPARK_TIMEZONE_KEY:
                if value is not None:
                    self._time_zone = value
                key = value = None
            if map and _SPARK_TIMEZONE_KEY in map:
                self._time_zone = map[_SPARK_TIMEZONE_KEY]
            super()._set_config(key, value, map=map)

        @cached_property
        def session(self) -> DuckDBSession:
            return DuckDBSession(**self._session_kwargs)

        def getOrCreate(self) -> DuckDBSession:
            session = t.cast(DuckDBSession, super().getOrCreate())
            if self._time_zone is not None:
                session.conf.set(_SPARK_TIMEZONE_KEY, self._time_zone)
            return session

    builder = Builder()
