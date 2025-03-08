# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import fnmatch
import typing as t

from sqlglot import exp

from sqlframe.base.catalog import Function, _BaseCatalog
from sqlframe.base.mixins.catalog_mixins import (
    CreateTableFromFunctionMixin,
    GetCurrentCatalogFromFunctionMixin,
    GetCurrentDatabaseFromFunctionMixin,
    ListCatalogsFromInfoSchemaMixin,
    ListColumnsFromInfoSchemaMixin,
    ListDatabasesFromInfoSchemaMixin,
    ListTablesFromInfoSchemaMixin,
    SetCurrentCatalogFromUseMixin,
    SetCurrentDatabaseFromUseMixin,
)
from sqlframe.base.util import normalize_string, schema_, to_schema

if t.TYPE_CHECKING:
    from sqlframe.duckdb.session import DuckDBSession  # noqa
    from sqlframe.duckdb.dataframe import DuckDBDataFrame  # noqa
    from sqlframe.duckdb.table import DuckDBTable  # noqa


class DuckDBCatalog(
    GetCurrentCatalogFromFunctionMixin["DuckDBSession", "DuckDBDataFrame", "DuckDBTable"],
    SetCurrentCatalogFromUseMixin["DuckDBSession", "DuckDBDataFrame", "DuckDBTable"],
    GetCurrentDatabaseFromFunctionMixin["DuckDBSession", "DuckDBDataFrame", "DuckDBTable"],
    CreateTableFromFunctionMixin["DuckDBSession", "DuckDBDataFrame", "DuckDBTable"],
    ListDatabasesFromInfoSchemaMixin["DuckDBSession", "DuckDBDataFrame", "DuckDBTable"],
    ListCatalogsFromInfoSchemaMixin["DuckDBSession", "DuckDBDataFrame", "DuckDBTable"],
    SetCurrentDatabaseFromUseMixin["DuckDBSession", "DuckDBDataFrame", "DuckDBTable"],
    ListTablesFromInfoSchemaMixin["DuckDBSession", "DuckDBDataFrame", "DuckDBTable"],
    ListColumnsFromInfoSchemaMixin["DuckDBSession", "DuckDBDataFrame", "DuckDBTable"],
    _BaseCatalog["DuckDBSession", "DuckDBDataFrame", "DuckDBTable"],
):
    TEMP_CATALOG_FILTER = exp.column("table_catalog").eq("temp")

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
