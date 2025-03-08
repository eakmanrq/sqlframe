# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import fnmatch
import typing as t

from sqlglot import exp, parse_one

from sqlframe.base.catalog import Function, _BaseCatalog
from sqlframe.base.mixins.catalog_mixins import (
    CreateTableFromFunctionMixin,
    GetCurrentCatalogFromFunctionMixin,
    GetCurrentDatabaseFromFunctionMixin,
    ListCatalogsFromInfoSchemaMixin,
    ListColumnsFromInfoSchemaMixin,
    ListDatabasesFromInfoSchemaMixin,
    ListTablesFromInfoSchemaMixin,
    SetCurrentDatabaseFromSearchPathMixin,
)
from sqlframe.base.util import normalize_string, schema_, to_schema

if t.TYPE_CHECKING:
    from sqlframe.redshift.dataframe import RedshiftDataFrame
    from sqlframe.redshift.session import RedshiftSession
    from sqlframe.redshift.table import RedshiftTable


class RedshiftCatalog(
    GetCurrentCatalogFromFunctionMixin["RedshiftSession", "RedshiftDataFrame", "RedshiftTable"],
    GetCurrentDatabaseFromFunctionMixin["RedshiftSession", "RedshiftDataFrame", "RedshiftTable"],
    CreateTableFromFunctionMixin["RedshiftSession", "RedshiftDataFrame", "RedshiftTable"],
    ListDatabasesFromInfoSchemaMixin["RedshiftSession", "RedshiftDataFrame", "RedshiftTable"],
    ListCatalogsFromInfoSchemaMixin["RedshiftSession", "RedshiftDataFrame", "RedshiftTable"],
    SetCurrentDatabaseFromSearchPathMixin["RedshiftSession", "RedshiftDataFrame", "RedshiftTable"],
    ListTablesFromInfoSchemaMixin["RedshiftSession", "RedshiftDataFrame", "RedshiftTable"],
    ListColumnsFromInfoSchemaMixin["RedshiftSession", "RedshiftDataFrame", "RedshiftTable"],
    _BaseCatalog["RedshiftSession", "RedshiftDataFrame", "RedshiftTable"],
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
        query = parse_one(
            f"""SELECT database_name as catalog, schema_name as namespace, function_name as name
FROM svv_redshift_functions
WHERE database_name = '{schema.catalog}'
and schema_name = '{schema.db}'
ORDER BY function_name;
        """,
            dialect="redshift",
        )
        functions = [
            Function(
                name=normalize_string(x["name"], from_dialect="execution", to_dialect="output"),
                catalog=normalize_string(
                    x["catalog"], from_dialect="execution", to_dialect="output"
                ),
                namespace=[
                    normalize_string(x["namespace"], from_dialect="execution", to_dialect="output")
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
        return functions

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
