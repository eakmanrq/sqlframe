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
from sqlframe.base.util import schema_, to_schema

if t.TYPE_CHECKING:
    from sqlframe.redshift.dataframe import RedshiftDataFrame
    from sqlframe.redshift.session import RedshiftSession


class RedshiftCatalog(
    GetCurrentCatalogFromFunctionMixin["RedshiftSession", "RedshiftDataFrame"],
    GetCurrentDatabaseFromFunctionMixin["RedshiftSession", "RedshiftDataFrame"],
    ListDatabasesFromInfoSchemaMixin["RedshiftSession", "RedshiftDataFrame"],
    ListCatalogsFromInfoSchemaMixin["RedshiftSession", "RedshiftDataFrame"],
    SetCurrentDatabaseFromSearchPathMixin["RedshiftSession", "RedshiftDataFrame"],
    ListTablesFromInfoSchemaMixin["RedshiftSession", "RedshiftDataFrame"],
    ListColumnsFromInfoSchemaMixin["RedshiftSession", "RedshiftDataFrame"],
    _BaseCatalog["RedshiftSession", "RedshiftDataFrame"],
):
    CURRENT_CATALOG_EXPRESSION: exp.Expression = exp.func("current_database")

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
            f"""SELECT database_name as catalog, schema_name as namespace, function_name as name
FROM svv_redshift_functions
WHERE database_name = '{schema.catalog}'
and schema_name = '{schema.db}'
ORDER BY function_name;
        """,
            dialect=self.session.input_dialect,
        )
        functions = self.session._fetch_rows(query)
        if pattern:
            functions = [x for x in functions if fnmatch.fnmatch(x["name"], pattern)]
        return [
            Function(
                name=x["name"],
                catalog=x["catalog"],
                namespace=[x["namespace"]],
                description=None,
                className="",
                isTemporary=False,
            )
            for x in functions
        ]

    # def get_columns(self, table_name: t.Union[exp.Table, str]) -> t.Dict[str, exp.DataType]:
    #     table = self.ensure_table(table_name)
    #     if not table.catalog:
    #         table.set("catalog", exp.parse_identifier(self.currentCatalog()))
    #     if not table.db:
    #         table.set("db", exp.parse_identifier(self.currentDatabase()))
    #     sql = f"SHOW COLUMNS FROM TABLE {table.sql(dialect=self.session.input_dialect)}"
    #     results = sorted(self.session._fetch_rows(sql), key=lambda x: x["ordinal_position"])
    #     return {
    #         row["column_name"]: exp.DataType.build(
    #             row["data_type"], dialect=self.session.input_dialect, udt=True
    #         )
    #         for row in results
    #     }
