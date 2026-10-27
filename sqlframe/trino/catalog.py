from __future__ import annotations

import fnmatch
import typing as t

from sqlglot import exp
from sqlglot.errors import ParseError
from sqlglot.schema import MappingSchema, flatten_schema

from sqlframe.base.catalog import Column, Table
from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.base.util import normalize_string, safe_parse_identifier, to_schema
from sqlframe.standalone.catalog import StandaloneCatalog

if t.TYPE_CHECKING:
    from sqlglot.schema import ColumnMapping


class _TrinoMappingSchema(MappingSchema):
    def depth(self) -> int:
        # A local view may be registered before a three-part server table.
        return 3


class TrinoCatalog(StandaloneCatalog):
    def _to_spark_type(self, trino_type: str, column_name: str) -> exp.DataType:
        error = f"Unsupported Trino type {trino_type} for column {column_name!r}"

        def spark_type(node):
            if not isinstance(node, exp.DataType):
                return node
            mapping = {
                exp.DataType.Type.VARCHAR: "STRING",
                exp.DataType.Type.CHAR: "STRING",
                exp.DataType.Type.JSON: "STRING",
                exp.DataType.Type.UUID: "STRING",
                exp.DataType.Type.VARBINARY: "BINARY",
                exp.DataType.Type.TIMESTAMP: "TIMESTAMP_NTZ",
                exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
            }
            if node.this in {exp.DataType.Type.TIMESTAMP, exp.DataType.Type.TIMESTAMPTZ}:
                # Spark timestamps have microsecond precision; Trino allows up to 12 digits.
                if node.expressions and int(node.expressions[0].this.this) > 6:
                    raise UnsupportedOperationError(error)
            if node.this in mapping:
                return exp.DataType.build(mapping[node.this], dialect="spark")
            if node.this == exp.DataType.Type.STRUCT and any(
                not isinstance(field, exp.ColumnDef) for field in node.expressions
            ):
                # Spark StructFields require names; do not invent names for anonymous ROW fields.
                raise UnsupportedOperationError(error)
            if node.this not in {
                exp.DataType.Type.BOOLEAN,
                exp.DataType.Type.TINYINT,
                exp.DataType.Type.SMALLINT,
                exp.DataType.Type.INT,
                exp.DataType.Type.BIGINT,
                exp.DataType.Type.FLOAT,
                exp.DataType.Type.DOUBLE,
                exp.DataType.Type.DECIMAL,
                exp.DataType.Type.DATE,
                exp.DataType.Type.ARRAY,
                exp.DataType.Type.MAP,
                exp.DataType.Type.STRUCT,
            }:
                raise UnsupportedOperationError(error)
            return node

        try:
            data_type = exp.DataType.build(trino_type, dialect=self.session.execution_dialect)
        except ParseError as exc:
            raise UnsupportedOperationError(error) from exc
        return data_type.transform(spark_type)

    def _parse_native_type(self, trino_type: str, column_name: str) -> exp.DataType:
        try:
            return exp.DataType.build(trino_type, dialect=self.session.execution_dialect)
        except ParseError as exc:
            raise UnsupportedOperationError(
                f"Unsupported Trino type {trino_type} for column {column_name!r}"
            ) from exc

    def get_columns(self, table: exp.Table | str) -> t.Dict[str, exp.DataType]:
        table = self.ensure_table(table)
        return {
            column.name: self._parse_native_type(column.dataType, column.name)
            for column in self.listColumns(table.sql(dialect=self.session.input_dialect))
        }

    def __init__(self, sparkSession, schema: t.Optional[MappingSchema] = None):
        if schema is None:
            schema = _TrinoMappingSchema()
        elif not isinstance(schema, _TrinoMappingSchema):
            supplied_schema = schema
            schema = _TrinoMappingSchema(
                visible=schema.visible,
                dialect=schema.dialect,
                normalize=schema.normalize,
                udf_mapping=schema.udf_mapping,
            )
            for path in flatten_schema(supplied_schema.mapping, depth=supplied_schema.depth()):
                table = exp.table_(
                    path[-1],
                    db=path[-2] if len(path) > 1 else None,
                    catalog=path[-3] if len(path) > 2 else None,
                    quoted=True,
                )
                schema.add_table(
                    table,
                    supplied_schema.find(table),
                    normalize=False,
                    match_depth=False,
                )
        super().__init__(sparkSession, schema)

    def add_table(
        self,
        table: exp.Table | str,
        column_mapping: t.Optional[ColumnMapping] = None,
        **kwargs: t.Any,
    ) -> None:
        # Trino queries can mix fully-qualified server tables with local temp views.
        kwargs.setdefault("match_depth", False)
        if column_mapping is None and self.session._has_connection:
            table = self.ensure_table(table)
            if not self._schema.find(table):
                # Keep native AST types for optimizer inference, including timestamp
                # precision and quoted ROW names. Spark mappings are report-only.
                column_mapping = {
                    normalize_string(
                        name, from_dialect="output", to_dialect="input", is_column=True
                    ): data_type
                    for name, data_type in self.get_columns(table).items()
                }
        return super().add_table(table, column_mapping=column_mapping, **kwargs)

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
        schema_name = (
            normalize_string(
                schema.db,
                from_dialect="input",
                to_dialect="execution",
                to_string_literal=True,
            )
            if schema
            else self.currentDatabase()
        )
        catalog_sql = exp.to_identifier(catalog_name).sql(
            dialect=self.session.execution_dialect, identify=True
        )
        schema_sql = exp.Literal.string(schema_name).sql(dialect=self.session.execution_dialect)
        cursor = self.session._cur
        cursor.execute(
            f"SELECT table_name, table_type FROM {catalog_sql}.information_schema.tables "
            f"WHERE table_schema = {schema_sql}"
        )
        tables = [
            Table(
                name=name,
                catalog=catalog_name,
                namespace=[schema_name],
                description=None,
                tableType="VIEW" if table_type == "VIEW" else "TABLE",
                isTemporary=False,
            )
            for name, table_type in cursor.fetchall()
        ]
        tables.extend(
            Table(
                name=name,
                catalog=None,
                namespace=[],
                description=None,
                tableType="VIEW",
                isTemporary=True,
            )
            for name in self.session.temp_views
        )
        normalized_pattern = (
            normalize_string(pattern, from_dialect="input", to_dialect="output", is_pattern=True)
            if pattern is not None
            else None
        )
        return [
            table
            for table in tables
            if normalized_pattern is None or fnmatch.fnmatch(table.name, normalized_pattern)
        ]

    def listColumns(
        self, tableName: str, dbName: t.Optional[str] = None, include_temp: bool = False
    ) -> t.List[Column]:
        table = exp.to_table(tableName, dialect=self.session.input_dialect)
        if dbName is None and not table.db and not table.catalog:
            table_name = normalize_string(tableName, from_dialect="input", is_table=True)
            if df := self.session.temp_views.get(table_name):
                return [
                    Column(
                        name=name,
                        description=None,
                        dataType="",
                        nullable=True,
                        isPartition=False,
                        isBucket=False,
                    )
                    for name in df.columns
                ]
        if not self.session._has_connection:
            raise NotImplementedError("Trino catalog lookup requires a DB-API connection")

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
                name=safe_parse_identifier(row[0], dialect=self.session.execution_dialect).sql(
                    dialect=self.session.output_dialect
                ),
                description=row[3] if len(row) > 3 else None,
                dataType=row[1],
                nullable=True,
                isPartition=False,
                isBucket=False,
            )
            for row in cursor.fetchall()
        ]
