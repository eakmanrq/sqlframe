# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import fnmatch
import json
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
    SetCurrentCatalogFromUseMixin,
    SetCurrentDatabaseFromUseMixin,
)
from sqlframe.base.util import normalize_string, schema_, to_schema

if t.TYPE_CHECKING:
    from sqlframe.databricks.session import DatabricksSession  # noqa
    from sqlframe.databricks.dataframe import DatabricksDataFrame  # noqa


class DatabricksCatalog(
    SetCurrentCatalogFromUseMixin["DatabricksSession", "DatabricksDataFrame"],
    GetCurrentCatalogFromFunctionMixin["DatabricksSession", "DatabricksDataFrame"],
    GetCurrentDatabaseFromFunctionMixin["DatabricksSession", "DatabricksDataFrame"],
    ListDatabasesFromInfoSchemaMixin["DatabricksSession", "DatabricksDataFrame"],
    ListCatalogsFromInfoSchemaMixin["DatabricksSession", "DatabricksDataFrame"],
    SetCurrentDatabaseFromUseMixin["DatabricksSession", "DatabricksDataFrame"],
    ListTablesFromInfoSchemaMixin["DatabricksSession", "DatabricksDataFrame"],
    ListColumnsFromInfoSchemaMixin["DatabricksSession", "DatabricksDataFrame"],
    _BaseCatalog["DatabricksSession", "DatabricksDataFrame"],
):
    CURRENT_CATALOG_EXPRESSION: exp.Expression = exp.func("current_catalog")
    UPPERCASE_INFO_SCHEMA = True

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
                db=exp.parse_identifier(
                    self.currentDatabase(), dialect=self.session.output_dialect
                ),
                catalog=exp.parse_identifier(
                    self.currentCatalog(), dialect=self.session.output_dialect
                ),
            )
        else:
            dbName = normalize_string(dbName, from_dialect="input", is_schema=True)
            schema = to_schema(dbName, dialect=self.session.input_dialect)
            if not schema.catalog:
                schema.set(
                    "catalog",
                    exp.parse_identifier(
                        self.currentCatalog(), dialect=self.session.output_dialect
                    ),
                )
        current_catalog = self.currentCatalog()
        self.session._collect(f"USE CATALOG {schema.catalog}")

        query = parse_one(
            f"""SHOW USER FUNCTIONS IN {schema.sql(dialect=self.session.input_dialect)}""",
            dialect=self.session.input_dialect,
        )
        functions = [
            Function(
                name=normalize_string(x["function"], from_dialect="execution", to_dialect="output"),
                catalog=normalize_string(
                    schema.catalog, from_dialect="execution", to_dialect="output"
                ),
                namespace=[
                    normalize_string(
                        schema.db, from_dialect="execution", to_dialect="output"
                    )
                ],
                description=None,
                className="",
                isTemporary=False,
            )
            for x in self.session._collect(query)
        ]
        if pattern:
            normalized_pattern = normalize_string(
                pattern, from_dialect="input", to_dialect="output", is_pattern=True
            )
            functions = [x for x in functions if fnmatch.fnmatch(x.name, normalized_pattern)]
        self.session._collect(f"USE CATALOG {current_catalog}")
        return functions

    def get_columns(self, table: exp.Table | str) -> t.Dict[str, exp.DataType]:
        table = (
            normalize_string(table, from_dialect="input", is_table=True)
            if isinstance(table, str)
            else table
        )
        table = exp.to_table(table, dialect=self.session.input_dialect)
        if not table.catalog:
            table.set(
                "catalog",
                exp.parse_identifier(
                    normalize_string(
                        self.currentCatalog(), from_dialect="output", to_dialect="input"
                    ),
                    dialect=self.session.input_dialect,
                ),
            )
        if not table.db:
            table.set(
                "db",
                exp.parse_identifier(
                    normalize_string(
                        self.currentDatabase(), from_dialect="output", to_dialect="input"
                    ),
                    dialect=self.session.input_dialect,
                ),
            )
        sql = f"DESCRIBE TABLE {table.sql(dialect=self.session.input_dialect)}"
        results = self.session._collect(sql)
        return {
            normalize_string(
                row["col_name"],
                from_dialect="execution",
                to_dialect="output",
                is_column=True,
            ): exp.DataType.build(
                normalize_string(
                    row["data_type"],
                    from_dialect="execution",
                    to_dialect="output",
                    is_datatype=True,
                ),
                dialect=self.session.output_dialect,
                udt=True,
            )
            for row in results if row["data_type"] != '' and row["data_type"] != "data_type"
        }
