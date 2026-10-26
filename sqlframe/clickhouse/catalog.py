from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlframe.base.catalog import Column, _BaseCatalog
from sqlframe.base.exceptions import UnsupportedOperationError

if t.TYPE_CHECKING:
    from sqlframe.clickhouse.dataframe import ClickHouseDataFrame
    from sqlframe.clickhouse.session import ClickHouseSession
    from sqlframe.clickhouse.table import ClickHouseTable


class ClickHouseCatalog(
    _BaseCatalog["ClickHouseSession", "ClickHouseDataFrame", "ClickHouseTable"]
):
    def _column_from_describe(self, row) -> Column:
        data_type = exp.DataType.build(row["type"], dialect=self.session.execution_dialect)
        nullable = bool(data_type.args.get("nullable"))

        def spark_type(node):
            if not isinstance(node, exp.DataType):
                return node
            # LowCardinality changes storage, not the value's Spark type.
            if node.this == exp.DataType.Type.LOWCARDINALITY:
                return node.expressions[0].copy()
            # Spark lacks unsigned types. Widen them without losing values.
            widening = {
                exp.DataType.Type.USMALLINT: "INT",
                exp.DataType.Type.UINT: "BIGINT",
                exp.DataType.Type.UBIGINT: "DECIMAL(20, 0)",
            }
            if node.this in widening:
                return exp.DataType.build(widening[node.this], dialect=self.session.output_dialect)
            if node.this in {exp.DataType.Type.UINT128, exp.DataType.Type.UINT256}:
                raise UnsupportedOperationError(
                    f"Unsupported ClickHouse column type: {row['type']}"
                )
            return node

        data_type = data_type.transform(spark_type)
        return Column(
            name=row["name"],
            description=None,
            dataType=data_type.sql(dialect=self.session.output_dialect),
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
