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
from sqlframe.postgres.table import PostgresTable
from sqlframe.postgres.udf import PostgresUDFRegistration

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
        PostgresTable,
        psycopg2_connection,  # type: ignore
        PostgresUDFRegistration,
    ],
):
    _catalog = PostgresCatalog
    _reader = PostgresDataFrameReader
    _writer = PostgresDataFrameWriter
    _df = PostgresDataFrame
    _table = PostgresTable
    _udf_registration = PostgresUDFRegistration

    def __init__(self, conn: t.Optional[psycopg2_connection] = None):
        if not hasattr(self, "_conn"):
            super().__init__(conn)
            self._execute("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch")
            self._execute("""CREATE OR REPLACE FUNCTION pg_temp.try_to_timestamp(input_text TEXT, format TEXT)
RETURNS TIMESTAMP AS $$
BEGIN
    RETURN TO_TIMESTAMP(input_text, format);
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;""")

    def _collect(
        self,
        expressions: t.Union[str, exp.Expression, t.List[str], t.List[exp.Expression]],
        *,
        quote_identifiers: bool = True,
        skip_normalization: bool = False,
        skip_rows: bool = False,
    ) -> t.List[Row]:
        from psycopg2 import ProgrammingError

        try:
            return super()._collect(
                expressions,
                quote_identifiers=quote_identifiers,
                skip_normalization=skip_normalization,
            )
        except ProgrammingError as e:
            if "no results to fetch" in str(e):
                return []
            raise e

    @property
    def _is_postgres(self) -> bool:
        return True

    class Builder(_BaseSession.Builder):
        DEFAULT_EXECUTION_DIALECT = "postgres"

        @property
        def session(self) -> PostgresSession:
            return PostgresSession(**self._session_kwargs)

        def getOrCreate(self) -> PostgresSession:
            return super().getOrCreate()  # type: ignore

    builder = Builder()
