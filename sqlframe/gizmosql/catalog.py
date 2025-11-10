# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import fnmatch
import typing as t

from sqlglot import exp, parse_one

from sqlframe.base.catalog import Column
from sqlframe.base.catalog import Function, _BaseCatalog
from sqlframe.base.mixins.catalog_mixins import (
    CreateTableFromFunctionMixin,
    GetCurrentCatalogFromFunctionMixin,
    GetCurrentDatabaseFromFunctionMixin,
    ListCatalogsFromInfoSchemaMixin,
    ListDatabasesFromInfoSchemaMixin,
    ListTablesFromInfoSchemaMixin,
    SetCurrentCatalogFromUseMixin,
    SetCurrentDatabaseFromUseMixin,
)
from sqlframe.base.util import normalize_string, schema_, to_schema

if t.TYPE_CHECKING:
    from sqlframe.gizmosql.session import GizmoSQLSession  # noqa
    from sqlframe.gizmosql.dataframe import GizmoSQLDataFrame  # noqa
    from sqlframe.gizmosql.table import GizmoSQLTable  # noqa


class GizmoSQLCatalog(
    GetCurrentCatalogFromFunctionMixin["GizmoSQLSession", "GizmoSQLDataFrame", "GizmoSQLTable"],
    SetCurrentCatalogFromUseMixin["GizmoSQLSession", "GizmoSQLDataFrame", "GizmoSQLTable"],
    GetCurrentDatabaseFromFunctionMixin["GizmoSQLSession", "GizmoSQLDataFrame", "GizmoSQLTable"],
    CreateTableFromFunctionMixin["GizmoSQLSession", "GizmoSQLDataFrame", "GizmoSQLTable"],
    ListDatabasesFromInfoSchemaMixin["GizmoSQLSession", "GizmoSQLDataFrame", "GizmoSQLTable"],
    ListCatalogsFromInfoSchemaMixin["GizmoSQLSession", "GizmoSQLDataFrame", "GizmoSQLTable"],
    SetCurrentDatabaseFromUseMixin["GizmoSQLSession", "GizmoSQLDataFrame", "GizmoSQLTable"],
    ListTablesFromInfoSchemaMixin["GizmoSQLSession", "GizmoSQLDataFrame", "GizmoSQLTable"],
    _BaseCatalog["GizmoSQLSession", "GizmoSQLDataFrame", "GizmoSQLTable"],
):
    TEMP_CATALOG_FILTER = exp.column("table_catalog").eq("temp")

    def currentCatalog(self) -> str:
        return self.session._conn.adbc_current_catalog

    CURRENT_CATALOG_EXPRESSION: exp.Expression = exp.column("current_catalog")

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
        tableName = normalize_string(tableName, from_dialect="input", is_table=True)
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
        dbName = normalize_string(dbName, from_dialect="input", is_schema=True) if dbName else None
        schema = to_schema(dbName, dialect=self.session.input_dialect) if dbName else None
        if not table.db:
            if schema and schema.db:
                table.set("db", schema.args["db"])
            else:
                current_database = normalize_string(
                    self.currentDatabase(), from_dialect="output", to_dialect="input"
                )
                table.set(
                    "db",
                    exp.parse_identifier(current_database, dialect=self.session.input_dialect),
                )
        if not table.catalog:
            if schema and schema.catalog:
                table.set("catalog", schema.args["catalog"])
            else:
                current_catalog = self.currentCatalog()
                table.set(
                    "catalog",
                    current_catalog,
                )
        select = parse_one(
            f"""
        SELECT
    column_name,
    data_type,
    is_nullable
FROM
    information_schema.columns
WHERE table_name = '{table.name}'
ORDER BY
    ordinal_position;
        """,
            dialect="duckdb",
        )
        if table.db:
            schema_filter: exp.Expression = exp.column("table_schema").eq(table.db)
            if include_temp and self.TEMP_SCHEMA_FILTER:
                schema_filter = exp.Or(this=schema_filter, expression=self.TEMP_SCHEMA_FILTER)
            select = select.where(schema_filter)  # type: ignore
        if table.catalog:
            catalog_filter: exp.Expression = exp.column("table_catalog").eq(table.catalog)
            if include_temp and self.TEMP_CATALOG_FILTER:
                catalog_filter = exp.Or(this=catalog_filter, expression=self.TEMP_CATALOG_FILTER)
            select = select.where(catalog_filter)  # type: ignore
        results = self.session._collect(select)
        return [
            Column(
                name=normalize_string(
                    x["column_name"], from_dialect="execution", to_dialect="output"
                ),
                description=None,
                dataType=normalize_string(
                    x["data_type"], from_dialect="execution", to_dialect="output", is_datatype=True
                ),
                nullable=x["is_nullable"] == "YES",
                isPartition=False,
                isBucket=False,
            )
            for x in results
        ]

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
            current_database = normalize_string(
                self.currentDatabase(), from_dialect="output", to_dialect="input"
            )
            current_catalog = normalize_string(
                self.currentCatalog(), from_dialect="output", to_dialect="input"
            )
            schema = schema_(
                db=exp.parse_identifier(current_database, dialect=self.session.input_dialect),
                catalog=exp.parse_identifier(current_catalog, dialect=self.session.input_dialect),
            )
        else:
            dbName = normalize_string(dbName, from_dialect="input", is_schema=True)
            schema = to_schema(dbName, dialect=self.session.input_dialect)
        select = (
            exp.select("function_name", "schema_name", "database_name")
            .from_("duckdb_functions()")
            .where(exp.column("schema_name").eq(schema.db))
        )
        if schema.catalog:
            select = select.where(exp.column("database_name").eq(schema.catalog))
        functions = [
            Function(
                name=normalize_string(
                    x["function_name"], from_dialect="execution", to_dialect="output"
                ),
                catalog=normalize_string(
                    x["database_name"], from_dialect="execution", to_dialect="output"
                ),
                namespace=[
                    normalize_string(
                        x["schema_name"], from_dialect="execution", to_dialect="output"
                    )
                ],
                description=None,
                className="",
                isTemporary=False,
            )
            for x in self.session._collect(select)
            if x["function_name"] not in ["@>", "<@", "&&"]
        ]
        if pattern:
            normalized_pattern = normalize_string(
                pattern, from_dialect="input", to_dialect="output", is_pattern=True
            )
            functions = [x for x in functions if fnmatch.fnmatch(x.name, normalized_pattern)]
        return functions
