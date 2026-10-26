from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.errors import ParseError

from sqlframe.base.catalog import Column, _BaseCatalog
from sqlframe.base.exceptions import UnsupportedOperationError

if t.TYPE_CHECKING:
    from sqlframe.clickhouse.dataframe import ClickHouseDataFrame
    from sqlframe.clickhouse.session import ClickHouseSession
    from sqlframe.clickhouse.table import ClickHouseTable


class ClickHouseCatalog(
    _BaseCatalog["ClickHouseSession", "ClickHouseDataFrame", "ClickHouseTable"]
):
    def get_columns(self, table: exp.Table | str) -> t.Dict[str, exp.DataType]:
        # A dot in a discovered name belongs to the identifier, not a table qualifier.
        return {
            (
                exp.column(name, quoted=True).sql(dialect=self.session.input_dialect)
                if "." in name
                else name
            ): data_type
            for name, data_type in super().get_columns(table).items()
        }

    def _column_from_describe(self, row) -> Column:
        name = row["name"]
        normalized = self.session.input_dialect.normalize_identifier(exp.to_identifier(name))
        if normalized.name != name:
            raise UnsupportedOperationError(
                f"ClickHouse column {name!r} loses case during Spark identifier normalization. "
                "Case-sensitive column names are unsupported; use lowercase column names, or "
                "mark the column case-sensitive in raw SQL and alias it to a lowercase name: "
                "spark.sql('SELECT `userId` /* sqlglot.meta case_sensitive */ AS user_id "
                "FROM events', qualify=False). See docs/clickhouse.md."
            )
        error = f"Unsupported ClickHouse type {row['type']} for column {row['name']!r}"
        try:
            data_type = exp.DataType.build(row["type"], dialect=self.session.execution_dialect)
        except ParseError as exc:
            raise UnsupportedOperationError(error) from exc
        # ClickHouse nests Nullable inside LowCardinality, never the reverse.
        value_type = (
            data_type.expressions[0]
            if data_type.this == exp.DataType.Type.LOWCARDINALITY
            else data_type
        )
        nullable = bool(value_type.args.get("nullable"))

        def spark_type(node):
            if not isinstance(node, exp.DataType):
                return node
            # LowCardinality changes storage, not the value's Spark type. transform skips
            # a replacement's children, so map the wrapped type here.
            if node.this == exp.DataType.Type.LOWCARDINALITY:
                return node.expressions[0].transform(spark_type)
            # Map ClickHouse value types to types Spark can represent.
            mapping = {
                exp.DataType.Type.DATETIME64: "TIMESTAMP",
                exp.DataType.Type.DATE32: "DATE",
                exp.DataType.Type.ENUM8: "STRING",
                exp.DataType.Type.ENUM16: "STRING",
                exp.DataType.Type.FIXEDSTRING: "STRING",
                exp.DataType.Type.IPV4: "STRING",
                exp.DataType.Type.IPV6: "STRING",
                exp.DataType.Type.UUID: "STRING",
                exp.DataType.Type.UTINYINT: "SMALLINT",
                exp.DataType.Type.USMALLINT: "INT",
                exp.DataType.Type.UINT: "BIGINT",
                exp.DataType.Type.UBIGINT: "DECIMAL(20, 0)",
            }
            if node.this in mapping:
                return exp.DataType.build(mapping[node.this], dialect=self.session.output_dialect)
            if node.this in {
                exp.DataType.Type.INT128,
                exp.DataType.Type.INT256,
                exp.DataType.Type.UINT128,
                exp.DataType.Type.UINT256,
            }:
                raise UnsupportedOperationError(error)
            return node

        data_type = data_type.transform(spark_type)
        spark_sql = data_type.sql(dialect=self.session.output_dialect)
        try:
            exp.DataType.build(spark_sql, dialect=self.session.output_dialect)
        except ParseError as exc:
            raise UnsupportedOperationError(error) from exc
        return Column(
            name=row["name"],
            description=None,
            dataType=spark_sql,
            nullable=nullable,
            isPartition=False,
            isBucket=False,
        )

    def listColumns(
        self, tableName: str, dbName: t.Optional[str] = None, include_temp: bool = False
    ) -> t.List[Column]:
        table = exp.to_table(tableName, dialect=self.session.input_dialect)
        if dbName:
            table.set("db", exp.to_identifier(dbName))
        rows = self.session._collect(
            f"DESCRIBE TABLE {table.sql(dialect=self.session.execution_dialect, identify=True)}"
        )
        return [self._column_from_describe(row) for row in rows]
