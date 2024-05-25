# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import fnmatch
import typing as t

from sqlglot import exp, parse_one

from sqlframe.base.catalog import Function, _BaseCatalog
from sqlframe.base.mixins.catalog_mixins import (
    GetCurrentCatalogFromFunctionMixin,
    GetCurrentDatabaseFromFunctionMixin,
    ListCatalogsFromInfoSchemaMixin,
    ListColumnsFromInfoSchemaMixin,
    ListDatabasesFromInfoSchemaMixin,
    ListTablesFromInfoSchemaMixin,
    SetCurrentDatabaseFromSearchPathMixin,
)

if t.TYPE_CHECKING:
    from sqlframe.postgres.session import PostgresSession  # noqa
    from sqlframe.postgres.dataframe import PostgresDataFrame  # noqa


class PostgresCatalog(
    GetCurrentCatalogFromFunctionMixin["PostgresSession", "PostgresDataFrame"],
    GetCurrentDatabaseFromFunctionMixin["PostgresSession", "PostgresDataFrame"],
    ListDatabasesFromInfoSchemaMixin["PostgresSession", "PostgresDataFrame"],
    ListCatalogsFromInfoSchemaMixin["PostgresSession", "PostgresDataFrame"],
    SetCurrentDatabaseFromSearchPathMixin["PostgresSession", "PostgresDataFrame"],
    ListTablesFromInfoSchemaMixin["PostgresSession", "PostgresDataFrame"],
    ListColumnsFromInfoSchemaMixin["PostgresSession", "PostgresDataFrame"],
    _BaseCatalog["PostgresSession", "PostgresDataFrame"],
):
    CURRENT_CATALOG_EXPRESSION: exp.Expression = exp.column("current_catalog")
    TEMP_SCHEMA_FILTER = exp.column("table_schema").like("pg_temp_%")

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
        # SO: https://stackoverflow.com/questions/44143816/any-way-to-list-all-user-defined-postgresql-functions
        query = parse_one(
            """SELECT n.nspname as "namespace",
  p.proname as "name"
FROM pg_catalog.pg_proc p
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE pg_catalog.pg_function_is_visible(p.oid)
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
ORDER BY 1, 2;
        """,
            dialect=self.session.input_dialect,
        )
        functions = self.session._fetch_rows(query)
        catalog = self.currentCatalog()
        results = [
            Function(
                name=x["name"],
                catalog=catalog,
                namespace=[x["namespace"]],
                description=None,
                className="",
                isTemporary=False,
            )
            for x in functions
        ]
        if pattern:
            results = [x for x in results if fnmatch.fnmatch(x.name, pattern)]
        return results
