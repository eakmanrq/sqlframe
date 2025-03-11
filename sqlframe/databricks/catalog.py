# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import fnmatch
import typing as t

import sqlglot as sg
from sqlglot import exp, parse_one

from sqlframe.base.catalog import TABLE, Column, Function, _BaseCatalog
from sqlframe.base.mixins.catalog_mixins import (
    CreateTableFromFunctionMixin,
    GetCurrentCatalogFromFunctionMixin,
    GetCurrentDatabaseFromFunctionMixin,
    ListCatalogsFromInfoSchemaMixin,
    ListDatabasesFromInfoSchemaMixin,
    ListTablesFromInfoSchemaMixin,
    SetCurrentDatabaseFromUseMixin,
)
from sqlframe.base.types import StructType
from sqlframe.base.util import (
    get_column_mapping_from_schema_input,
    normalize_string,
    schema_,
    to_csv,
    to_schema,
)

if t.TYPE_CHECKING:
    from sqlframe.databricks.session import DatabricksSession  # noqa
    from sqlframe.databricks.dataframe import DatabricksDataFrame  # noqa
    from sqlframe.databricks.table import DatabricksTable  # noqa


class DatabricksCatalog(
    GetCurrentCatalogFromFunctionMixin[
        "DatabricksSession", "DatabricksDataFrame", "DatabricksTable"
    ],
    GetCurrentDatabaseFromFunctionMixin[
        "DatabricksSession", "DatabricksDataFrame", "DatabricksTable"
    ],
    CreateTableFromFunctionMixin["DatabricksSession", "DatabricksDataFrame", "DatabricksTable"],
    ListDatabasesFromInfoSchemaMixin["DatabricksSession", "DatabricksDataFrame", "DatabricksTable"],
    ListCatalogsFromInfoSchemaMixin["DatabricksSession", "DatabricksDataFrame", "DatabricksTable"],
    SetCurrentDatabaseFromUseMixin["DatabricksSession", "DatabricksDataFrame", "DatabricksTable"],
    ListTablesFromInfoSchemaMixin["DatabricksSession", "DatabricksDataFrame", "DatabricksTable"],
    _BaseCatalog["DatabricksSession", "DatabricksDataFrame", "DatabricksTable"],
):
    CURRENT_CATALOG_EXPRESSION: exp.Expression = exp.func("current_catalog")
    UPPERCASE_INFO_SCHEMA = True

    def setCurrentCatalog(self, catalogName: str) -> None:
        self.session._collect(
            exp.Use(
                kind=exp.Var(this=exp.to_identifier("CATALOG")),
                this=exp.parse_identifier(catalogName, dialect=self.session.input_dialect),
            ),
            quote_identifiers=False,
        )

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
                name=normalize_string(
                    x["function"].split(".")[-1], from_dialect="execution", to_dialect="output"
                ),
                catalog=normalize_string(
                    schema.catalog, from_dialect="execution", to_dialect="output"
                ),
                namespace=[
                    normalize_string(schema.db, from_dialect="execution", to_dialect="output")
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
            for row in results
            if row["data_type"] != "" and row["data_type"] != "data_type"
        }

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
        tableName = normalize_string(
            tableName, from_dialect=self.session.input_dialect, is_table=True
        )
        dbName = (
            normalize_string(dbName, from_dialect=self.session.input_dialect, is_schema=True)
            if dbName
            else None
        )
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
                current_database = normalize_string(
                    self.currentDatabase(),
                    from_dialect=self.session.output_dialect,
                    to_dialect=self.session.input_dialect,
                )
                table.set(
                    "db",
                    exp.parse_identifier(current_database, dialect=self.session.input_dialect),
                )
        if not table.catalog:
            if schema and schema.catalog:
                table.set("catalog", schema.args["catalog"])
            else:
                current_catalog = normalize_string(
                    self.currentCatalog(),
                    from_dialect=self.session.output_dialect,
                    to_dialect=self.session.input_dialect,
                )
                table.set(
                    "catalog",
                    exp.parse_identifier(current_catalog, dialect=self.session.input_dialect),
                )
        sql = f"DESCRIBE TABLE {'.'.join(part.name for part in table.parts)}"
        results = self.session._collect(sql)

        is_partition = False
        partitions = set([])
        for row in results:
            if row["col_name"] == "# Partition Information":
                is_partition = True
            if is_partition and row["data_type"] != "" and row["data_type"] != "data_type":
                partitions.add(row["col_name"])

        columns = []
        for row in results:
            if row["data_type"] == "" or row["data_type"] == "data_type":
                break
            columns.append(
                Column(
                    name=normalize_string(
                        row["col_name"],
                        from_dialect=self.session.execution_dialect,
                        to_dialect=self.session.output_dialect,
                    ),
                    description=row["comment"],
                    dataType=normalize_string(
                        row["data_type"],
                        from_dialect=self.session.execution_dialect,
                        to_dialect=self.session.output_dialect,
                        is_datatype=True,
                    ),
                    nullable=True,
                    isPartition=True if row["col_name"] in partitions else False,
                    isBucket=False,
                )
            )

        return columns

    def createTable(
        self,
        tableName: str,
        path: t.Optional[str] = None,
        source: t.Optional[str] = None,
        schema: t.Optional[StructType] = None,
        description: t.Optional[str] = None,
        **options: str,
    ) -> DatabricksTable:
        """Creates a table based on the dataset in a data source.

        .. versionadded:: 2.2.0

        Parameters
        ----------
        tableName : str
            name of the table to create.

            .. versionchanged:: 3.4.0
               Allow ``tableName`` to be qualified with catalog name.

        path : str, t.Optional
            the path in which the data for this table exists.
            When ``path`` is specified, an external table is
            created from the data at the given path. Otherwise a managed table is created.
        source : str, t.Optional
            the source of this table such as 'parquet, 'orc', etc.
            If ``source`` is not specified, the default data source configured by
            ``spark.sql.sources.default`` will be used.
        schema : class:`StructType`, t.Optional
            the schema for this table.
        description : str, t.Optional
            the description of this table.

            .. versionchanged:: 3.1.0
                Added the ``description`` parameter.

        **options : dict, t.Optional
            extra options to specify in the table.

        Returns
        -------
        :class:`DataFrame`
            The DataFrame associated with the table.

        Examples
        --------
        Creating a managed table.

        >>> _ = spark.catalog.createTable("tbl1", schema=spark.range(1).schema, source='parquet')
        >>> _ = spark.sql("DROP TABLE tbl1")

        Creating an external table

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     _ = spark.catalog.createTable(
        ...         "tbl2", schema=spark.range(1).schema, path=d, source='parquet')
        >>> _ = spark.sql("DROP TABLE tbl2")
        """
        if not isinstance(tableName, str):
            raise TypeError("tableName must be a string")
        if path is not None and not isinstance(path, str):
            raise TypeError("path must be a string")
        if source is not None and not isinstance(source, str):
            raise TypeError("source must be a string")
        if schema is not None and not isinstance(schema, StructType):
            raise TypeError("schema must be a StructType")
        if description is not None and not isinstance(description, str):
            raise TypeError("description must be a string")

        source = (source or "delta").lower()
        replace: t.Union[str, bool, None] = options.pop("replace", None)
        exists: t.Union[str, bool, None] = options.pop("exists", None)
        table_properties: t.Union[str, t.Dict[str, str]] = options.pop("properties", {})
        partitionBy: t.Union[t.List[str], str, None] = options.pop("partitionBy", None)
        clusterBy: t.Union[t.List[str], str, None] = options.pop("clusterBy", None)

        if isinstance(replace, str) and replace.lower() == "true":
            replace = True
        if isinstance(exists, str) and exists.lower() == "true":
            exists = True

        schema_expressions: t.List[exp.Expression] = []
        if schema is not None and isinstance(schema, StructType):
            column_mapping = get_column_mapping_from_schema_input(
                schema, dialect=self.session.input_dialect
            )
            schema_expressions = [
                exp.ColumnDef(
                    this=exp.parse_identifier(k, dialect=self.session.input_dialect), kind=v
                )
                for k, v in column_mapping.items()
            ]

        name = normalize_string(tableName, from_dialect="input", is_table=True)
        properties: t.List[exp.Expression] = []
        if source is not None:
            properties.append(exp.FileFormatProperty(this=exp.Var(this=source.upper())))
        if path is not None:
            properties.append(exp.LocationProperty(this=exp.convert(path)))
            if replace and source != "delta":
                replace = None
                drop_expression = exp.Drop(
                    this=exp.to_table(name, dialect=self.session.input_dialect),
                    kind="TABLE",
                    exists=True,
                )
                if self.session._has_connection:
                    self.session._collect(drop_expression)
        if description is not None:
            properties.append(exp.SchemaCommentProperty(this=exp.convert(description)))
        if partitionBy is not None:
            if isinstance(partitionBy, str):
                partition_by = [partitionBy]
            else:
                partition_by = partitionBy
            properties.append(
                exp.PartitionedByProperty(
                    this=exp.Tuple(expressions=list(map(sg.to_identifier, partition_by)))
                )
            )
        if clusterBy is not None:
            if isinstance(clusterBy, str):
                cluster_by = [clusterBy]
            else:
                cluster_by = clusterBy
            properties.append(
                exp.Cluster(
                    expressions=[exp.Tuple(expressions=list(map(sg.to_identifier, cluster_by)))]
                )
            )

        properties.extend(
            exp.Property(this=sg.to_identifier(name=k), value=exp.convert(value=v))
            for k, v in (table_properties if isinstance(table_properties, dict) else {}).items()
        )

        format_options: dict[str, t.Union[bool, float, int, str, None]] = {
            key: f"'{val}'" for key, val in options.items() if val is not None
        }
        format_options_str = to_csv(format_options, " ")

        output_expression_container = exp.Create(
            this=exp.Schema(
                this=exp.to_table(name, dialect=self.session.input_dialect),
                expressions=schema_expressions,
            ),
            kind="TABLE",
            exists=exists,
            replace=replace,
            properties=exp.Properties(expressions=properties),
        )
        if self.session._has_connection:
            sql = self.session._to_sql(output_expression_container, quote_identifiers=True)
            sql += f" OPTIONS ({format_options_str})" if format_options_str else ""
            self.session._collect(sql)

        df = self.session.table(name)
        return df
