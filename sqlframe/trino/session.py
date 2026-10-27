from __future__ import annotations

import typing as t

import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.scope import traverse_scope

from sqlframe.standalone.session import StandaloneSession
from sqlframe.trino.catalog import TrinoCatalog
from sqlframe.trino.dataframe import TrinoDataFrame
from sqlframe.trino.readwriter import TrinoDataFrameWriter
from sqlframe.trino.table import TrinoTable


class TrinoSession(StandaloneSession):
    """Plan with SQLFrame and execute Trino SQL using a supplied DB-API connection.

    Connected sessions can discover table columns from Trino. Offline sessions
    need schemas registered with ``catalog.add_table`` before reading tables.
    """

    _catalog = TrinoCatalog
    _df = TrinoDataFrame
    _table = TrinoTable
    _writer = TrinoDataFrameWriter

    def sql(
        self,
        sqlQuery: t.Union[str, exp.Expression],
        dialect: DialectType = None,
        qualify: bool = True,
    ):
        if qualify and self._has_connection:
            expression = (
                sqlglot.parse_one(sqlQuery, read=dialect or self.input_dialect)
                if isinstance(sqlQuery, str)
                else sqlQuery
            )
            if isinstance(expression, exp.Query):
                normalized = normalize_identifiers(
                    expression.copy(), dialect=dialect or self.input_dialect
                )
                for scope in traverse_scope(normalized):
                    for _, source in scope.selected_sources.values():
                        if isinstance(source, exp.Table):
                            source_table = source.copy()
                            source_table.set("alias", None)
                            self.catalog.add_table(source_table)
        return super().sql(sqlQuery, dialect=dialect, qualify=qualify)

    def _collect(self, *args, **kwargs):
        if not self._has_connection:
            raise ValueError("Trino collection requires a DB-API connection")
        return super()._collect(*args, **kwargs)

    class Builder(StandaloneSession.Builder):
        DEFAULT_OUTPUT_DIALECT = "trino"
        DEFAULT_EXECUTION_DIALECT = "trino"

        @property
        def session(self) -> TrinoSession:
            return TrinoSession(**self._session_kwargs)

        def getOrCreate(self) -> TrinoSession:
            return super().getOrCreate()  # type: ignore[return-value]

    builder = Builder()
