from __future__ import annotations

import fnmatch
import typing as t

from sqlglot import exp

from sqlframe.base.catalog import CatalogMetadata, Column, Function
from sqlframe.base.decorators import normalize
from sqlframe.base.mixins.catalog_mixins import (
    ListDatabasesFromInfoSchemaMixin,
    ListTablesFromInfoSchemaMixin,
    _BaseInfoSchemaMixin,
)
from sqlframe.base.util import schema_, to_schema

if t.TYPE_CHECKING:
    from google.cloud.bigquery import StandardSqlDataType

    from sqlframe.bigquery.dataframe import BigQueryDataFrame  # noqa
    from sqlframe.bigquery.session import BigQuerySession  # noqa


class BigQueryCatalog(
    ListDatabasesFromInfoSchemaMixin["BigQuerySession", "BigQueryDataFrame"],
    ListTablesFromInfoSchemaMixin["BigQuerySession", "BigQueryDataFrame"],
    _BaseInfoSchemaMixin["BigQuerySession", "BigQueryDataFrame"],
):
    QUALIFY_INFO_SCHEMA_WITH_DATABASE = True
    UPPERCASE_INFO_SCHEMA = True

    def setCurrentCatalog(self, catalogName: str) -> None:
        self.session.default_project = catalogName

    def currentCatalog(self) -> str:
        return self.session.default_project

    def setCurrentDatabase(self, dbName: str) -> None:
        self.session.default_dataset = dbName

    def currentDatabase(self) -> str:
        if not self.session.default_dataset:
            raise ValueError(
                "No default dataset set. Define `default_dataset` when creating `BigQuerySession`."
            )
        return to_schema(self.session.default_dataset).db

    @normalize(["tableName", "dbName"])
    def listColumns(
        self, tableName: str, dbName: t.Optional[str] = None, include_temp: bool = False
    ) -> t.List[Column]:
        """Returns a t.List of columns for the given table/view in the specified database.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        tableName : str
            name of the table to t.List columns.

            .. versionchanged:: 3.4.0
               Allow ``tableName`` to be qualified with catalog name when ``dbName`` is None.

        dbName : str, t.Optional
            name of the database to find the table to t.List columns.

        Returns
        -------
        t.List
            A t.List of :class:`Column`.

        Notes
        -----
        The order of arguments here is different from that of its JVM counterpart
        because Python does not support method overloading.

        If no database is specified, the current database and catalog
        are used. This API includes all temporary views.

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tblA (name STRING, age INT) USING parquet")
        >>> spark.catalog.t.listColumns("tblA")
        [Column(name='name', description=None, dataType='string', nullable=True, ...
        >>> _ = spark.sql("DROP TABLE tblA")
        """

        # Source: https://github.com/TobikoData/sqlmesh/blob/4bf5e7aa9302e877273812842eba0b457e28af9e/sqlmesh/core/engine_adapter/bigquery.py#L186-L205
        def dtype_to_sql(dtype: t.Optional[StandardSqlDataType]) -> str:
            assert dtype

            kind = dtype.type_kind
            assert kind

            # Not using the enum value to preserve compatibility with older versions
            # of the BigQuery library.
            if kind.name == "ARRAY":
                return f"ARRAY<{dtype_to_sql(dtype.array_element_type)}>"
            if kind.name == "STRUCT":
                struct_type = dtype.struct_type
                assert struct_type
                fields = ", ".join(
                    f"{field.name} {dtype_to_sql(field.type)}" for field in struct_type.fields
                )
                return f"STRUCT<{fields}>"
            if kind.name == "TYPE_KIND_UNSPECIFIED":
                return "JSON"
            return kind.name

        if df := self.session.temp_views.get(tableName):
            return [
                Column(
                    name=x,
                    description=None,
                    dataType="",
                    nullable=True,
                    isPartition=False,
                    isBucket=False,
                )
                for x in df.columns
            ]

        table = exp.to_table(tableName, dialect=self.session.input_dialect)
        schema = to_schema(dbName, dialect=self.session.input_dialect) if dbName else None
        if not table.db:
            if schema and schema.db:
                table.set("db", schema.args["db"])
            else:
                table.set(
                    "db",
                    exp.parse_identifier(
                        self.currentDatabase(), dialect=self.session.input_dialect
                    ),
                )
        if not table.catalog:
            if schema and schema.catalog:
                table.set("catalog", schema.args["catalog"])
            else:
                table.set(
                    "catalog",
                    exp.parse_identifier(self.currentCatalog(), dialect=self.session.input_dialect),
                )
        bq_table = self.session._client.get_table(table=".".join(part.name for part in table.parts))
        columns = [
            Column(
                name=field.name,
                description=field.description,
                dataType=exp.DataType.build(
                    dtype_to_sql(field.to_standard_sql().type), dialect=self.session.input_dialect
                ).sql(dialect=self.session.input_dialect),
                nullable=field.is_nullable,
                isPartition=False,
                isBucket=False,
            )
            for field in bq_table.schema
        ]
        if bq_table.time_partitioning and not bq_table.time_partitioning.field:
            columns.append(
                Column(
                    name="_PARTITIONTIME",
                    description=None,
                    dataType=exp.DataType.build("TIMESTAMP").sql(
                        dialect=self.session.input_dialect
                    ),
                    nullable=False,
                    isPartition=True,
                    isBucket=False,
                )
            )
            if bq_table.time_partitioning.type_ == "DAY":
                columns.append(
                    Column(
                        name="_PARTITIONDATE",
                        description=None,
                        dataType=exp.DataType.build("DATE").sql(dialect=self.session.input_dialect),
                        nullable=False,
                        isPartition=True,
                        isBucket=False,
                    )
                )
        return columns

    def listCatalogs(self, pattern: t.Optional[str] = None) -> t.List[CatalogMetadata]:
        return [CatalogMetadata(name=self.session.default_project, description=None)]

    def listFunctions(
        self, dbName: t.Optional[str] = None, pattern: t.Optional[str] = None
    ) -> t.List[Function]:
        """
        Returns a t.List of functions registered in the specified database.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        dbName : str
            name of the database to t.List the functions.
            ``dbName`` can be qualified with catalog name.
        pattern : str
            The pattern that the function name needs to match.

            .. versionchanged: 3.5.0
                Adds ``pattern`` argument.

        Returns
        -------
        t.List
            A t.List of :class:`Function`.

        Notes
        -----
        If no database is specified, the current database and catalog
        are used. This API includes all temporary functions.

        Examples
        --------
        >>> spark.catalog.t.listFunctions()
        [Function(name=...

        >>> spark.catalog.t.listFunctions(pattern="to_*")
        [Function(name=...

        >>> spark.catalog.t.listFunctions(pattern="*not_existing_func*")
        []
        """
        if not dbName:
            schema = schema_(
                db=exp.parse_identifier(self.currentDatabase(), dialect=self.session.input_dialect),
                catalog=exp.parse_identifier(
                    self.currentCatalog(), dialect=self.session.input_dialect
                ),
            )
        else:
            schema = to_schema(dbName, dialect=self.session.input_dialect)
        table = self._get_info_schema_table("routines", database=schema.db)
        select = (
            exp.select("routine_name", "specific_schema", "specific_catalog")
            .from_(table)
            .where(exp.column("specific_schema").eq(schema.db))
        )
        if schema.catalog:
            select = select.where(exp.column("specific_catalog").eq(schema.catalog))
        functions = self.session._fetch_rows(select)
        if pattern:
            functions = [x for x in functions if fnmatch.fnmatch(x["routine_name"], pattern)]
        return [
            Function(
                name=x["routine_name"],
                catalog=x["specific_catalog"],
                namespace=[x["specific_schema"]],
                description=None,
                className="",
                isTemporary=False,
            )
            for x in functions
        ]
