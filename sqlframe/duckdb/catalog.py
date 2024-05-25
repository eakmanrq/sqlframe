# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import fnmatch
import typing as t

from sqlglot import exp

from sqlframe.base.catalog import Function, _BaseCatalog
from sqlframe.base.mixins.catalog_mixins import (
    GetCurrentCatalogFromFunctionMixin,
    GetCurrentDatabaseFromFunctionMixin,
    ListCatalogsFromInfoSchemaMixin,
    ListColumnsFromInfoSchemaMixin,
    ListDatabasesFromInfoSchemaMixin,
    ListTablesFromInfoSchemaMixin,
    SetCurrentCatalogFromUseMixin,
    SetCurrentDatabaseFromUseMixin,
)
from sqlframe.base.util import schema_, to_schema

if t.TYPE_CHECKING:
    from sqlframe.duckdb.session import DuckDBSession  # noqa
    from sqlframe.duckdb.dataframe import DuckDBDataFrame  # noqa


class DuckDBCatalog(
    GetCurrentCatalogFromFunctionMixin["DuckDBSession", "DuckDBDataFrame"],
    SetCurrentCatalogFromUseMixin["DuckDBSession", "DuckDBDataFrame"],
    GetCurrentDatabaseFromFunctionMixin["DuckDBSession", "DuckDBDataFrame"],
    ListDatabasesFromInfoSchemaMixin["DuckDBSession", "DuckDBDataFrame"],
    ListCatalogsFromInfoSchemaMixin["DuckDBSession", "DuckDBDataFrame"],
    SetCurrentDatabaseFromUseMixin["DuckDBSession", "DuckDBDataFrame"],
    ListTablesFromInfoSchemaMixin["DuckDBSession", "DuckDBDataFrame"],
    ListColumnsFromInfoSchemaMixin["DuckDBSession", "DuckDBDataFrame"],
    _BaseCatalog["DuckDBSession", "DuckDBDataFrame"],
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
            schema = schema_(
                db=exp.parse_identifier(self.currentDatabase(), dialect=self.session.input_dialect),
                catalog=exp.parse_identifier(
                    self.currentCatalog(), dialect=self.session.input_dialect
                ),
            )
        else:
            schema = to_schema(dbName, dialect=self.session.input_dialect)
        select = (
            exp.select("function_name", "schema_name", "database_name")
            .from_("duckdb_functions()")
            .where(exp.column("schema_name").eq(schema.db))
        )
        if schema.catalog:
            select = select.where(exp.column("database_name").eq(schema.catalog))
        functions = self.session._fetch_rows(select)
        if pattern:
            functions = [x for x in functions if fnmatch.fnmatch(x["function_name"], pattern)]
        return [
            Function(
                name=x["function_name"],
                catalog=x["database_name"],
                namespace=[x["schema_name"]],
                description=None,
                className="",
                isTemporary=False,
            )
            for x in functions
        ]
