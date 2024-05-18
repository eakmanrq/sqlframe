from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlframe.base.session import _BaseSession
from sqlframe.postgres.catalog import PostgresCatalog
from sqlframe.postgres.dataframe import PostgresDataFrame
from sqlframe.postgres.readwriter import (
    PostgresDataFrameReader,
    PostgresDataFrameWriter,
)

if t.TYPE_CHECKING:
    from psycopg2.extensions import connection as psycopg2_connection

    from sqlframe.base.types import Row
else:
    psycopg2_connection = t.Any


class PostgresSession(
    _BaseSession[  # type: ignore
        PostgresCatalog,
        PostgresDataFrameReader,
        PostgresDataFrameWriter,
        PostgresDataFrame,
        psycopg2_connection,
    ],
):
    _catalog = PostgresCatalog
    _reader = PostgresDataFrameReader
    _writer = PostgresDataFrameWriter
    _df = PostgresDataFrame

    DEFAULT_TIME_FORMAT = "yyyy-MM-dd HH:MI:SS"

    def __init__(self, conn: t.Optional[psycopg2_connection] = None):
        if not hasattr(self, "_conn"):
            super().__init__(conn)
            self._execute("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch")

    def _fetch_rows(
        self, sql: t.Union[str, exp.Expression], *, quote_identifiers: bool = True
    ) -> t.List[Row]:
        from psycopg2 import ProgrammingError

        try:
            return super()._fetch_rows(sql, quote_identifiers=quote_identifiers)
        except ProgrammingError as e:
            if "no results to fetch" in str(e):
                return []
            raise e

    class Builder(_BaseSession.Builder):
        DEFAULT_INPUT_DIALECT = "postgres"
        DEFAULT_OUTPUT_DIALECT = "postgres"

        @property
        def session(self) -> PostgresSession:
            return PostgresSession(**self._session_kwargs)

        def getOrCreate(self) -> PostgresSession:
            self._set_session_properties()
            return self.session

    builder = Builder()
