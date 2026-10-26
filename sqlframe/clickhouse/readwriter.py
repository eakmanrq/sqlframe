from __future__ import annotations

import re
import typing as t

from sqlglot import exp
from sqlglot.errors import ParseError

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.base.readerwriter import _BaseDataFrameReader, _BaseDataFrameWriter

if t.TYPE_CHECKING:
    from sqlframe.clickhouse.dataframe import ClickHouseDataFrame
    from sqlframe.clickhouse.session import ClickHouseSession
    from sqlframe.clickhouse.table import ClickHouseTable


class ClickHouseDataFrameReader(
    _BaseDataFrameReader["ClickHouseSession", "ClickHouseDataFrame", "ClickHouseTable"]
):
    def table(self, tableName: str) -> ClickHouseTable:
        table = exp.to_table(tableName, dialect=self.session.input_dialect)
        for identifier in table.find_all(exp.Identifier):
            normalized = self.session.input_dialect.normalize_identifier(identifier.copy())
            if normalized.name != identifier.name:
                raise UnsupportedOperationError(
                    f"ClickHouse table {tableName!r} loses case during Spark identifier "
                    "normalization. Case-sensitive table names are unsupported; use "
                    "lowercase table and database names, or mark the table case-sensitive "
                    "in raw SQL: spark.sql('SELECT * FROM `Events` "
                    "/* sqlglot.meta case_sensitive */', qualify=False). "
                    "See docs/clickhouse.md."
                )
        return super().table(tableName)


class ClickHouseDataFrameWriter(_BaseDataFrameWriter["ClickHouseSession", "ClickHouseDataFrame"]):
    # Creation is deliberately limited to scalar types whose ClickHouse type can
    # be carried unchanged from DESCRIBE (SELECT ...) into CREATE TABLE.
    _CREATE_TYPE = re.compile(
        r"(?:Nullable\()?"
        r"(?:String|Bool|Int(?:8|16|32|64)|UInt(?:8|16|32|64)|Float(?:32|64)|"
        r"Date|Date32|DateTime(?:\('(?:UTC)'\))?|DateTime64\(\d+(?:, '(?:UTC)')?\)|"
        r"Decimal(?:32|64|128|256)?\(\d+(?:, \d+)?\))"
        r"\)?\Z"
    )

    def _table_sql(self, name: str) -> str:
        if ";" in name:
            raise UnsupportedOperationError("ClickHouse writes require a table or database.table")
        try:
            tables = self._session.input_dialect.parse_into(exp.Table, name)
        except ParseError as error:
            raise UnsupportedOperationError(
                "ClickHouse writes require a table or database.table"
            ) from error
        if len(tables) != 1 or not isinstance(tables[0], exp.Table):
            raise UnsupportedOperationError("ClickHouse writes require a table or database.table")
        table = tables[0]
        if (
            table.catalog
            or not table.name
            or not isinstance(table.this, exp.Identifier)
            or any(value for key, value in table.args.items() if key not in {"this", "db"})
        ):
            raise UnsupportedOperationError("ClickHouse writes require a table or database.table")
        for identifier in table.find_all(exp.Identifier):
            normalized = self._session.input_dialect.normalize_identifier(identifier.copy())
            if normalized.name != identifier.name:
                raise UnsupportedOperationError(
                    "ClickHouse writes require lowercase table and database names"
                )
        return table.sql(dialect=self._session.execution_dialect, identify=True)

    def _options(self) -> tuple[str | None, str | None]:
        options = dict(self.state_options)
        engine = options.pop("clickhouse.engine", None)
        order_by = options.pop("clickhouse.order_by", None)
        if options or self._state_format_to_write:
            raise UnsupportedOperationError("Unsupported ClickHouse table write option or format")
        if engine is not None and (
            not isinstance(engine, str) or engine not in {"Memory", "MergeTree"}
        ):
            raise UnsupportedOperationError("clickhouse.engine must be Memory or MergeTree")
        if order_by is not None and (not isinstance(order_by, str) or not order_by.strip()):
            raise UnsupportedOperationError("clickhouse.order_by must name columns")
        return engine, order_by

    def _source(self) -> tuple[str, list[dict[str, t.Any]]]:
        queries = self._df.sql(
            dialect=self._session.execution_dialect, optimize=False, pretty=False, as_list=True
        )
        if len(queries) != 1:
            raise UnsupportedOperationError("ClickHouse table writes require one SELECT query")
        query = queries[0]
        columns = [r.asDict() for r in self._session._collect(f"DESCRIBE ({query})")]
        self._validate_columns(columns)
        return query, columns

    def _validate_columns(self, columns: list[dict[str, t.Any]]) -> None:
        names = [column["name"] for column in columns]
        if not names or len(names) != len(set(names)):
            raise UnsupportedOperationError("ClickHouse writes require unique, nonempty columns")
        for column in columns:
            self._session.catalog._column_from_describe(column)

    def _exists(self, table_sql: str) -> bool:
        return bool(self._session._collect(f"EXISTS TABLE {table_sql}")[0][0])

    def _append(self, table_sql: str, *, by_name: bool) -> None:
        if not self._exists(table_sql):
            raise UnsupportedOperationError("ClickHouse append requires an existing table")
        target = [r.asDict() for r in self._session._collect(f"DESCRIBE TABLE {table_sql}")]
        self._validate_columns(target)
        if any(column.get("default_type") for column in target):
            raise UnsupportedOperationError(
                "ClickHouse writes do not support computed/default columns"
            )
        query, source = self._source()
        if by_name:
            source_by_name = {c["name"]: c for c in source}
            if set(source_by_name) != {c["name"] for c in target}:
                raise UnsupportedOperationError("ClickHouse column names do not match target table")
            pairs = [(source_by_name[c["name"]], c) for c in target]
            insert_columns = source
        else:
            if len(source) != len(target):
                raise UnsupportedOperationError(
                    "ClickHouse column counts do not match target table"
                )
            pairs = list(zip(source, target))
            insert_columns = target
        for src, dst in pairs:
            if src["type"] != dst["type"]:
                raise UnsupportedOperationError(
                    f"ClickHouse type mismatch for {dst['name']!r}: "
                    f"source {src['type']}, target {dst['type']}"
                )
        names = ", ".join(
            exp.to_identifier(c["name"], quoted=True).sql(dialect=self._session.execution_dialect)
            for c in insert_columns
        )
        self._session._execute(f"INSERT INTO {table_sql} ({names}) {query}")

    def insertInto(self, tableName: str, overwrite: t.Optional[bool] = None):
        if overwrite or self._mode not in {None, "append"}:
            raise UnsupportedOperationError(
                "ClickHouse overwrite and other insert modes are unsupported"
            )
        engine, order_by = self._options()
        if engine is not None or order_by is not None:
            raise UnsupportedOperationError(
                "ClickHouse engine options only apply to table creation"
            )
        self._append(self._table_sql(tableName), by_name=self._by_name)
        return self.copy()

    def saveAsTable(self, name: str, format: t.Optional[str] = None, mode: t.Optional[str] = None):
        if format is not None:
            raise UnsupportedOperationError("ClickHouse table write format is unsupported")
        effective_mode = mode if mode is not None else self._mode
        if effective_mode is None:
            effective_mode = "error"
        if effective_mode not in {"append", "error", "errorifexists"}:
            raise UnsupportedOperationError(f"ClickHouse {effective_mode} mode is unsupported")
        engine, order_by = self._options()
        table_sql = self._table_sql(name)
        exists = self._exists(table_sql)
        if exists:
            if effective_mode != "append":
                raise FileExistsError(f"ClickHouse table already exists: {name}")
            if engine is not None or order_by is not None:
                raise UnsupportedOperationError(
                    "ClickHouse engine options only apply to table creation"
                )
            self._append(table_sql, by_name=True)
            return self.copy()
        if engine is None:
            raise UnsupportedOperationError(
                "Table creation requires option('clickhouse.engine', ...)"
            )
        if engine == "MergeTree" and not order_by:
            raise UnsupportedOperationError("MergeTree requires clickhouse.order_by")
        if engine == "Memory" and order_by is not None:
            raise UnsupportedOperationError("Memory does not support clickhouse.order_by")
        query, columns = self._source()
        if any(not self._CREATE_TYPE.fullmatch(c["type"]) for c in columns):
            raise UnsupportedOperationError("Unsupported ClickHouse type for table creation")
        declarations = ", ".join(
            f"{exp.to_identifier(c['name'], quoted=True).sql(dialect=self._session.execution_dialect)} "
            f"{c['type']}"
            for c in columns
        )
        engine_sql = engine
        if engine == "MergeTree":
            keys = [part.strip() for part in order_by.split(",")]  # type: ignore[union-attr]
            by_name = {c["name"]: c for c in columns}
            if not keys or any(key not in by_name for key in keys):
                raise UnsupportedOperationError("clickhouse.order_by must name source columns")
            if any(by_name[key]["type"].startswith("Nullable(") for key in keys):
                raise UnsupportedOperationError("MergeTree order columns cannot be nullable")
            quoted = ", ".join(
                exp.to_identifier(key, quoted=True).sql(dialect=self._session.execution_dialect)
                for key in keys
            )
            engine_sql += f" ORDER BY ({quoted})"
        self._session._execute(f"CREATE TABLE {table_sql} ({declarations}) ENGINE = {engine_sql}")
        names = ", ".join(
            exp.to_identifier(c["name"], quoted=True).sql(dialect=self._session.execution_dialect)
            for c in columns
        )
        self._session._execute(f"INSERT INTO {table_sql} ({names}) {query}")
        return self.copy()
