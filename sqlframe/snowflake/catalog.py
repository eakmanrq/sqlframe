# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import fnmatch
import json
import typing as t

from sqlglot import exp, parse_one

from sqlframe.base.catalog import Function, _BaseCatalog
from sqlframe.base.decorators import normalize
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
    from sqlframe.snowflake.session import SnowflakeSession  # noqa
    from sqlframe.snowflake.dataframe import SnowflakeDataFrame  # noqa


class SnowflakeCatalog(
    SetCurrentCatalogFromUseMixin["SnowflakeSession", "SnowflakeDataFrame"],
    GetCurrentCatalogFromFunctionMixin["SnowflakeSession", "SnowflakeDataFrame"],
    GetCurrentDatabaseFromFunctionMixin["SnowflakeSession", "SnowflakeDataFrame"],
    ListDatabasesFromInfoSchemaMixin["SnowflakeSession", "SnowflakeDataFrame"],
    ListCatalogsFromInfoSchemaMixin["SnowflakeSession", "SnowflakeDataFrame"],
    SetCurrentDatabaseFromUseMixin["SnowflakeSession", "SnowflakeDataFrame"],
    ListTablesFromInfoSchemaMixin["SnowflakeSession", "SnowflakeDataFrame"],
    ListColumnsFromInfoSchemaMixin["SnowflakeSession", "SnowflakeDataFrame"],
    _BaseCatalog["SnowflakeSession", "SnowflakeDataFrame"],
):
    CURRENT_CATALOG_EXPRESSION: exp.Expression = exp.func("current_database")

    @normalize(["dbName"])
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
        if dbName is None:
            schema = schema_(
                db=exp.parse_identifier(self.currentDatabase(), dialect=self.session.input_dialect),
                catalog=exp.parse_identifier(
                    self.currentCatalog(), dialect=self.session.input_dialect
                ),
            )
        else:
            schema = to_schema(dbName, dialect=self.session.input_dialect)
            if not schema.catalog:
                schema.set("catalog", exp.parse_identifier(self.currentCatalog()))
        query = parse_one(
            f"""SHOW USER FUNCTIONS IN {schema.sql(dialect=self.session.input_dialect)}""",
            dialect=self.session.input_dialect,
        )
        functions = self.session._fetch_rows(query)
        if pattern:
            functions = [x for x in functions if fnmatch.fnmatch(x["name"], pattern)]
        return [
            Function(
                name=x["name"],
                catalog=x["catalog_name"],
                namespace=[x["schema_name"]],
                description=None,
                className="",
                isTemporary=False,
            )
            for x in functions
        ]

    @normalize(["table_name"])
    def get_columns(self, table_name: str) -> t.Dict[str, exp.DataType]:
        table = exp.to_table(table_name)
        if not table.catalog:
            table.set(
                "catalog",
                exp.parse_identifier(self.currentCatalog(), dialect=self.session.input_dialect),
            )
        if not table.db:
            table.set(
                "db",
                exp.parse_identifier(self.currentDatabase(), dialect=self.session.input_dialect),
            )
        sql = f"SHOW COLUMNS IN TABLE {table.sql(dialect=self.session.input_dialect)}"
        results = self.session._fetch_rows(sql)
        return {
            exp.column(row["column_name"], quoted=True).sql(
                dialect=self.session.input_dialect
            ): exp.DataType.build(
                json.loads(row["data_type"])["type"], dialect=self.session.input_dialect, udt=True
            )
            for row in results
        }
