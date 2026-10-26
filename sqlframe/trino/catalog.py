from __future__ import annotations

import fnmatch
import typing as t

from sqlglot import exp

from sqlframe.base.catalog import Column, Table
from sqlframe.base.util import to_schema
from sqlframe.standalone.catalog import StandaloneCatalog


class TrinoCatalog(StandaloneCatalog):
    def currentCatalog(self) -> str:
        return self.session._collect("SELECT current_catalog")[0][0]

    def currentDatabase(self) -> str:
        schema = self.session._collect("SELECT current_schema")[0][0]
        if schema is None:
            raise ValueError("No current Trino schema is set")
        return schema

    def listTables(
        self, dbName: t.Optional[str] = None, pattern: t.Optional[str] = None
    ) -> t.List[Table]:
        schema = to_schema(dbName, dialect=self.session.input_dialect) if dbName else None
        catalog_name = schema.catalog if schema and schema.catalog else self.currentCatalog()
        schema_name = schema.db if schema else self.currentDatabase()
        catalog_sql = exp.to_identifier(catalog_name).sql(
            dialect=self.session.execution_dialect, identify=True
        )
        schema_sql = exp.Literal.string(schema_name).sql(dialect=self.session.execution_dialect)
        cursor = self.session._cur
        cursor.execute(
            f"SELECT table_name, table_type FROM {catalog_sql}.information_schema.tables "
            f"WHERE table_schema = {schema_sql}"
        )
        return [
            Table(
                name=name,
                catalog=catalog_name,
                namespace=[schema_name],
                description=None,
                tableType="VIEW" if table_type == "VIEW" else "TABLE",
                isTemporary=False,
            )
            for name, table_type in cursor.fetchall()
            if pattern is None or fnmatch.fnmatch(name, pattern)
        ]

    def listColumns(
        self, tableName: str, dbName: t.Optional[str] = None, include_temp: bool = False
    ) -> t.List[Column]:
        if not self.session._has_connection:
            raise NotImplementedError("Trino catalog lookup requires a DB-API connection")

        table = exp.to_table(tableName, dialect=self.session.input_dialect)
        if dbName:
            schema = to_schema(dbName, dialect=self.session.input_dialect)
            if not table.db:
                table.set("db", schema.args.get("db"))
            if not table.catalog:
                table.set("catalog", schema.args.get("catalog"))

        sql = (
            f"SHOW COLUMNS FROM {table.sql(dialect=self.session.execution_dialect, identify=True)}"
        )
        cursor = self.session._cur
        cursor.execute(sql)
        return [
            Column(
                name=row[0],
                description=row[3] if len(row) > 3 else None,
                dataType=row[1],
                nullable=True,
                isPartition=False,
                isBucket=False,
            )
            for row in cursor.fetchall()
        ]
