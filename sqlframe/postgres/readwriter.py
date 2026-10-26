# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlframe.base.mixins.readwriter_mixins import PandasLoaderMixin, PandasWriterMixin
from sqlframe.base.readerwriter import (
    _BaseDataFrameReader,
    _BaseDataFrameWriter,
)
from sqlframe.base.util import build_drop, normalize_string

if t.TYPE_CHECKING:
    from sqlframe.postgres.session import PostgresSession  # noqa
    from sqlframe.postgres.dataframe import PostgresDataFrame  # noqa
    from sqlframe.postgres.table import PostgresTable  # noqa


class PostgresDataFrameReader(
    PandasLoaderMixin["PostgresSession", "PostgresDataFrame"],
    _BaseDataFrameReader["PostgresSession", "PostgresDataFrame", "PostgresTable"],
):
    pass


class PostgresDataFrameWriter(
    PandasWriterMixin["PostgresSession", "PostgresDataFrame"],
    _BaseDataFrameWriter["PostgresSession", "PostgresDataFrame"],
):
    def saveAsTable(
        self, name: str, format: str | None = None, mode: str | None = None
    ) -> PostgresDataFrameWriter:
        if (mode or self._mode) != "overwrite":
            return super().saveAsTable(name, format=format, mode=mode)
        if format is not None:
            raise NotImplementedError("Providing Format in the save as table is not supported")

        table = exp.to_table(
            normalize_string(name, from_dialect="input", is_table=True),
            dialect=self._session.input_dialect,
        )
        df = self._df.copy(output_expression_container=exp.Create(this=table.copy(), kind="TABLE"))
        if self._session._has_connection:
            from psycopg2.extensions import TRANSACTION_STATUS_IDLE

            conn = self._session._conn
            owns_transaction = (
                conn.autocommit and conn.get_transaction_status() == TRANSACTION_STATUS_IDLE
            )
            if owns_transaction:
                self._session._execute("BEGIN")
            else:
                self._session._execute("SAVEPOINT sqlframe_overwrite")
            try:
                self._session._execute(
                    self._session._to_sql(build_drop(table, kind="TABLE", exists=True))
                )
                df.collect()
            except Exception:
                if owns_transaction:
                    self._session._execute("ROLLBACK")
                else:
                    self._session._execute("ROLLBACK TO SAVEPOINT sqlframe_overwrite")
                    self._session._execute("RELEASE SAVEPOINT sqlframe_overwrite")
                raise
            if owns_transaction:
                self._session._execute("COMMIT")
            else:
                self._session._execute("RELEASE SAVEPOINT sqlframe_overwrite")
        return self.copy(_df=df)
