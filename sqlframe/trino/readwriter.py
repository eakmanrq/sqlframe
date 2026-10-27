from __future__ import annotations

import re

from sqlglot import exp
from sqlglot.errors import ParseError, TokenError
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.standalone.readwriter import (
    StandaloneDataFrameReader as TrinoDataFrameReader,
)
from sqlframe.standalone.readwriter import StandaloneDataFrameWriter


class TrinoDataFrameWriter(StandaloneDataFrameWriter):
    def _table(self, name: str) -> exp.Table:
        message = "Trino writes require table, schema.table, or catalog.schema.table"
        if not isinstance(name, str) or ";" in name:
            raise UnsupportedOperationError(message)
        try:
            tables = self._session.input_dialect.parse_into(exp.Table, name)
        except (ParseError, TokenError) as error:
            raise UnsupportedOperationError(message) from error
        if len(tables) != 1 or not isinstance(tables[0], exp.Table):
            raise UnsupportedOperationError(message)
        table = tables[0]
        if any(value for key, value in table.args.items() if key not in {"this", "db", "catalog"}):
            raise UnsupportedOperationError(message)
        if (
            not isinstance(table.this, exp.Identifier)
            or not 1 <= len(table.parts) <= 3
            or any(not isinstance(part, exp.Identifier) or not part.name for part in table.parts)
        ):
            raise UnsupportedOperationError(message)
        normalize_identifiers(table, dialect=self._session.input_dialect)
        return normalize_identifiers(table, dialect=self._session.execution_dialect)

    def _options(self, format: str | None = None) -> None:
        if format is not None or self._state_format_to_write is not None or self.state_options:
            raise UnsupportedOperationError("Trino table write options and formats are unsupported")

    def _resolve(self, table: exp.Table) -> exp.Table:
        if not self._session._has_connection:
            raise ValueError("Trino table writes require a DB-API connection")
        if not table.catalog or not table.db:
            catalog, schema = self._session._collect("SELECT current_catalog, current_schema")[0]
            if not table.catalog:
                if not catalog:
                    raise UnsupportedOperationError("Trino writes require a current catalog")
                table.set("catalog", exp.to_identifier(catalog, quoted=True))
            if not table.db:
                if not schema:
                    raise UnsupportedOperationError("Trino writes require a current schema")
                table.set("db", exp.to_identifier(schema, quoted=True))
        return table

    def _table_sql(self, table: exp.Table) -> str:
        return table.sql(dialect=self._session.execution_dialect, identify=True)

    def _exists(self, table: exp.Table) -> bool:
        catalog = table.args["catalog"].sql(dialect=self._session.execution_dialect, identify=True)
        schema = exp.Literal.string(table.db).sql(dialect=self._session.execution_dialect)
        name = exp.Literal.string(table.name).sql(dialect=self._session.execution_dialect)
        rows = self._session._collect(
            f"SELECT table_type FROM {catalog}.information_schema.tables "
            f"WHERE table_schema = {schema} AND table_name = {name}"
        )
        if not rows:
            return False
        if len(rows) != 1 or rows[0][0] != "BASE TABLE":
            raise UnsupportedOperationError("Trino writes require a base table, not a view")
        return True

    @staticmethod
    def _validate_names(names: list[str]) -> None:
        names = [name.lower() for name in names]
        if not names or any(not name for name in names) or len(names) != len(set(names)):
            raise UnsupportedOperationError("Trino writes require unique, nonempty columns")

    def _validate_columns(self, columns: list[tuple[str, str]]) -> None:
        self._validate_names([name for name, _ in columns])
        if any(
            not dtype
            or re.search(r"\bunknown\b", re.sub(r'"(?:[^"]|"")*"', "", dtype), re.IGNORECASE)
            for _, dtype in columns
        ):
            raise UnsupportedOperationError(
                "Trino writes do not support unknown column types; cast nulls to explicit types"
            )

    def _source(self) -> tuple[str, list[tuple[str, str]]]:
        self._validate_names(self._df.columns)
        queries = self._df.sql(
            dialect=self._session.execution_dialect, optimize=False, pretty=False, as_list=True
        )
        if len(queries) != 1:
            raise UnsupportedOperationError("Trino table writes require one SELECT query")
        query = queries[0]
        expressions = self._session.execution_dialect.parse(query)
        if len(expressions) != 1 or not isinstance(expressions[0], exp.Query):
            raise UnsupportedOperationError("Trino table writes require one SELECT query")
        # Server analysis retains native widths and precision that Spark schemas lose.
        columns = [(row[0], row[4]) for row in self._session._collect(f"DESCRIBE OUTPUT ({query})")]
        self._validate_columns(columns)
        return query, columns

    @staticmethod
    def _compatible(source: str, target: str) -> bool:
        source, target = source.lower(), target.lower()
        if source == target:
            return True
        integers = ["tinyint", "smallint", "integer", "bigint"]
        if source in integers and target in integers:
            return integers.index(source) <= integers.index(target)
        src_varchar = re.fullmatch(r"varchar(?:\((\d+)\))?", source)
        dst_varchar = re.fullmatch(r"varchar(?:\((\d+)\))?", target)
        if src_varchar and dst_varchar:
            return dst_varchar[1] is None or (
                src_varchar[1] is not None and int(src_varchar[1]) <= int(dst_varchar[1])
            )
        return False

    def _append(self, table: exp.Table, *, by_name: bool) -> None:
        table_sql = self._table_sql(table)
        target = [
            (row[0], row[1]) for row in self._session._collect(f"SHOW COLUMNS FROM {table_sql}")
        ]
        self._validate_columns(target)
        query, source = self._source()
        if len(source) != len(target):
            raise UnsupportedOperationError("Trino column counts do not match target table")
        if by_name:
            target_by_name = {name.lower(): (name, dtype) for name, dtype in target}
            if {name.lower() for name, _ in source} != set(target_by_name):
                raise UnsupportedOperationError("Trino column names do not match target table")
            destinations = [target_by_name[name.lower()] for name, _ in source]
        else:
            destinations = target
        for (_, src_type), (name, dst_type) in zip(source, destinations):
            if not self._compatible(src_type, dst_type):
                raise UnsupportedOperationError(
                    f"Trino type mismatch for {name!r}: source {src_type}, target {dst_type}. "
                    "Cast source columns to compatible target types explicitly."
                )
        names = ", ".join(
            exp.to_identifier(name, quoted=True).sql(dialect=self._session.execution_dialect)
            for name, _ in destinations
        )
        self._session._collect(f"INSERT INTO {table_sql} ({names}) {query}")

    def insertInto(self, tableName: str, overwrite: bool | None = None):
        if overwrite or self._mode not in {None, "append"}:
            raise UnsupportedOperationError(
                "Trino insertInto overwrite and other modes are unsupported"
            )
        self._options()
        table = self._resolve(self._table(tableName))
        if not self._exists(table):
            raise UnsupportedOperationError("Trino insertInto requires an existing table")
        self._append(table, by_name=self._by_name)
        return self.copy()

    def saveAsTable(self, name: str, format: str | None = None, mode: str | None = None):
        effective_mode = mode if mode is not None else self._mode
        if effective_mode is None:
            effective_mode = "error"
        if effective_mode not in {"append", "error", "errorifexists"}:
            raise UnsupportedOperationError(
                f"Trino saveAsTable {effective_mode!r} mode is unsupported"
            )
        self._options(format)
        table = self._resolve(self._table(name))
        if self._exists(table):
            if effective_mode != "append":
                raise FileExistsError(f"Trino table already exists: {name}")
            self._append(table, by_name=True)
        else:
            query, _ = self._source()
            self._session._collect(f"CREATE TABLE {self._table_sql(table)} AS {query}")
        return self.copy()


__all__ = ["TrinoDataFrameReader", "TrinoDataFrameWriter"]
