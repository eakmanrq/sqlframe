import fnmatch
import typing as t

from sqlglot import exp

from sqlframe.base.catalog import (
    DF,
    SESSION,
    TABLE,
    CatalogMetadata,
    Column,
    Database,
    Table,
    _BaseCatalog,
)
from sqlframe.base.types import StructType
from sqlframe.base.util import (
    get_column_mapping_from_schema_input,
    normalize_string,
    schema_,
    to_schema,
)


class _BaseInfoSchemaMixin(_BaseCatalog, t.Generic[SESSION, DF, TABLE]):
    QUALIFY_INFO_SCHEMA_WITH_DATABASE = False
    UPPERCASE_INFO_SCHEMA = False

    def _get_info_schema_table(
        self,
        table_name: str,
        database: t.Optional[str] = None,
        qualify_override: t.Optional[bool] = None,
    ) -> exp.Table:
        table = table_name
        db = "information_schema"
        if self.UPPERCASE_INFO_SCHEMA:
            table = table.upper()
            db = db.upper()
        qualify = (
            qualify_override
            if qualify_override is not None
            else self.QUALIFY_INFO_SCHEMA_WITH_DATABASE
        )
        if qualify:
            if database:
                catalog = normalize_string(database, from_dialect="input", to_dialect="output")
            else:
                catalog = self.currentDatabase()
            if not db:
                raise ValueError("Table name must be qualified with a database.")
            return exp.table_(
                catalog=exp.to_identifier(catalog, quoted=True),
                db=exp.to_identifier(db, quoted=True),
                table=exp.to_identifier(table, quoted=True),
            )
        return exp.table_(
            db=exp.to_identifier(db, quoted=True), table=exp.to_identifier(table, quoted=True)
        )


class GetCurrentCatalogFromFunctionMixin(_BaseCatalog, t.Generic[SESSION, DF, TABLE]):
    CURRENT_CATALOG_EXPRESSION: exp.Expression = exp.func("current_catalog")

    def currentCatalog(self) -> str:
        """Returns the current default catalog in this session.

        .. versionadded:: 3.4.0

        Examples
        --------
        >>> spark.catalog.currentCatalog()
        'spark_catalog'
        """
        return normalize_string(
            self.session._collect(
                exp.select(self.CURRENT_CATALOG_EXPRESSION), quote_identifiers=False
            )[0][0],
            from_dialect="execution",
            to_dialect="output",
        )


class GetCurrentDatabaseFromFunctionMixin(_BaseCatalog, t.Generic[SESSION, DF, TABLE]):
    CURRENT_DATABASE_EXPRESSION: exp.Expression = exp.func("current_schema")

    def currentDatabase(self) -> str:
        """Returns the current default schema in this session.

        .. versionadded:: 3.4.0

        Examples
        --------
        >>> spark.catalog.currentDatabase()
        'default'
        """
        return normalize_string(
            self.session._collect(exp.select(self.CURRENT_DATABASE_EXPRESSION))[0][0],
            from_dialect="execution",
            to_dialect="output",
        )


class SetCurrentCatalogFromUseMixin(_BaseCatalog, t.Generic[SESSION, DF, TABLE]):
    def setCurrentCatalog(self, catalogName: str) -> None:
        """Sets the current default catalog in this session.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        catalogName : str
            name of the catalog to set

        Examples
        --------
        >>> spark.catalog.setCurrentCatalog("spark_catalog")
        """
        self.session._collect(
            exp.Use(this=exp.parse_identifier(catalogName, dialect=self.session.input_dialect))
        )


class CreateTableFromFunctionMixin(_BaseCatalog, t.Generic[SESSION, DF, TABLE]):
    def createTable(
        self,
        tableName: str,
        path: t.Optional[str] = None,
        source: t.Optional[str] = None,
        schema: t.Optional[StructType] = None,
        description: t.Optional[str] = None,
        **options: str,
    ) -> TABLE:
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
        if source is not None:
            raise NotImplementedError("Providing source to create table is not supported")
        if path is not None:
            raise NotImplementedError("Creating a external table is not supported")

        replace: t.Union[str, bool, None] = options.pop("replace", None)
        exists: t.Union[str, bool, None] = options.pop("exists", None)

        if isinstance(replace, str) and replace.lower() == "true":
            replace = True
        if isinstance(exists, str) and exists.lower() == "true":
            exists = True

        if schema is None:
            raise ValueError("schema must be specified.")

        column_mapping = get_column_mapping_from_schema_input(
            schema, dialect=self.session.input_dialect
        )
        expressions = [
            exp.ColumnDef(this=exp.parse_identifier(k, dialect=self.session.input_dialect), kind=v)
            for k, v in column_mapping.items()
        ]

        name = normalize_string(tableName, from_dialect="input", is_table=True)
        output_expression_container = exp.Create(
            this=exp.Schema(
                this=exp.to_table(name, dialect=self.session.input_dialect),
                expressions=expressions,
            ),
            kind="TABLE",
            exists=exists,
            replace=replace,
        )
        if self.session._has_connection:
            self.session._collect(output_expression_container)

        df = self.session.table(name)
        return df

    def createExternalTable(
        self,
        tableName: str,
        path: t.Optional[str] = None,
        source: t.Optional[str] = None,
        schema: t.Optional[StructType] = None,
        **options: str,
    ) -> TABLE:
        """Creates a table based on the dataset in a data source.

        It returns the DataFrame associated with the external table.

        The data source is specified by the ``source`` and a set of ``options``.
        If ``source`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        t.Optionally, a schema can be provided as the schema of the returned :class:`DataFrame` and
        created external table.

        .. versionadded:: 2.0.0

        Returns
        -------
        :class:`DataFrame`
        """
        return self.createTable(tableName, path=path, source=source, schema=schema, **options)


class ListDatabasesFromInfoSchemaMixin(_BaseInfoSchemaMixin, t.Generic[SESSION, DF, TABLE]):
    def listDatabases(self, pattern: t.Optional[str] = None) -> t.List[Database]:
        """
        Returns a t.List of databases available across all sessions.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        pattern : str
            The pattern that the database name needs to match.

            .. versionchanged: 3.5.0
                Adds ``pattern`` argument.

        Returns
        -------
        t.List
            A t.List of :class:`Database`.

        Examples
        --------
        >>> spark.catalog.t.listDatabases()
        [Database(name='default', catalog='spark_catalog', description='default database', ...

        >>> spark.catalog.t.listDatabases("def*")
        [Database(name='default', catalog='spark_catalog', description='default database', ...

        >>> spark.catalog.t.listDatabases("def2*")
        []
        """
        table = self._get_info_schema_table("schemata", qualify_override=False)
        results = self.session._collect(
            # The table name can be specifically formatted in uppercase to run properly and we don't want to
            # normalize that away
            exp.Select().select("schema_name", "catalog_name").from_(table),
            skip_normalization=True,
        )
        databases = [
            Database(
                name=normalize_string(x[0], from_dialect="execution", to_dialect="output"),
                catalog=normalize_string(x[1], from_dialect="execution", to_dialect="output"),
                description=None,
                locationUri="",
            )
            for x in results
        ]
        if pattern:
            normalized_pattern = normalize_string(
                pattern, from_dialect="input", to_dialect="output", is_pattern=True
            )
            databases = [db for db in databases if fnmatch.fnmatch(db.name, normalized_pattern)]
        return databases


class ListCatalogsFromInfoSchemaMixin(_BaseInfoSchemaMixin, t.Generic[SESSION, DF, TABLE]):
    def listCatalogs(self, pattern: t.Optional[str] = None) -> t.List[CatalogMetadata]:
        """
        Returns a t.List of databases available across all sessions.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        pattern : str
            The pattern that the database name needs to match.

            .. versionchanged: 3.5.0
                Adds ``pattern`` argument.

        Returns
        -------
        t.List
            A t.List of :class:`Database`.

        Examples
        --------
        >>> spark.catalog.t.listDatabases()
        [Database(name='default', catalog='spark_catalog', description='default database', ...

        >>> spark.catalog.t.listDatabases("def*")
        [Database(name='default', catalog='spark_catalog', description='default database', ...

        >>> spark.catalog.t.listDatabases("def2*")
        []
        """
        table = self._get_info_schema_table("schemata")
        results = self.session._collect(
            exp.Select().select("catalog_name").from_(table).distinct(), skip_normalization=True
        )
        catalogs = [
            CatalogMetadata(
                name=normalize_string(x[0], from_dialect="execution", to_dialect="output"),
                description=None,
            )
            for x in results
        ]
        if pattern:
            normalized_pattern = normalize_string(
                pattern, from_dialect="input", to_dialect="output", is_pattern=True
            )
            catalogs = [
                catalog for catalog in catalogs if fnmatch.fnmatch(catalog.name, normalized_pattern)
            ]
        return catalogs


class SetCurrentDatabaseFromSearchPathMixin(_BaseCatalog, t.Generic[SESSION, DF, TABLE]):
    def setCurrentDatabase(self, dbName: str) -> None:
        """
        Sets the current default database in this session.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> spark.catalog.setCurrentDatabase("default")
        """
        self.session._execute(f'SET search_path TO "{dbName}"')


class SetCurrentDatabaseFromUseMixin(_BaseCatalog, t.Generic[SESSION, DF, TABLE]):
    def setCurrentDatabase(self, dbName: str) -> None:
        """
        Sets the current default database in this session.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> spark.catalog.setCurrentDatabase("default")
        """
        dbName = normalize_string(dbName, from_dialect="input", to_dialect="output", is_schema=True)
        schema = to_schema(dbName, dialect=self.session.output_dialect)

        if not schema.catalog:
            schema.set(
                "catalog",
                exp.parse_identifier(self.currentCatalog(), dialect=self.session.output_dialect),
            )
        self.session._collect(exp.Use(this=schema))


class ListTablesFromInfoSchemaMixin(_BaseInfoSchemaMixin, t.Generic[SESSION, DF, TABLE]):
    def listTables(
        self, dbName: t.Optional[str] = None, pattern: t.Optional[str] = None
    ) -> t.List[Table]:
        """Returns a t.List of tables/views in the specified database.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        dbName : str
            name of the database to t.List the tables.

            .. versionchanged:: 3.4.0
               Allow ``dbName`` to be qualified with catalog name.

        pattern : str
            The pattern that the database name needs to match.

            .. versionchanged: 3.5.0
                Adds ``pattern`` argument.

        Returns
        -------
        t.List
            A t.List of :class:`Table`.

        Notes
        -----
        If no database is specified, the current database and catalog
        are used. This API includes all temporary views.

        Examples
        --------
        >>> spark.range(1).createTempView("test_view")
        >>> spark.catalog.t.listTables()
        [Table(name='test_view', catalog=None, namespace=[], description=None, ...

        >>> spark.catalog.t.listTables(pattern="test*")
        [Table(name='test_view', catalog=None, namespace=[], description=None, ...

        >>> spark.catalog.t.listTables(pattern="table*")
        []

        >>> _ = spark.catalog.dropTempView("test_view")
        >>> spark.catalog.t.listTables()
        []
        """
        if dbName is None and pattern is None:
            database = normalize_string(
                self.currentDatabase(), from_dialect="output", to_dialect="input"
            )
            catalog = normalize_string(
                self.currentCatalog(), from_dialect="output", to_dialect="input"
            )
            schema = schema_(
                db=exp.parse_identifier(database, dialect=self.session.input_dialect),
                catalog=exp.parse_identifier(catalog, dialect=self.session.input_dialect),
            )
        elif dbName:
            dbName = normalize_string(dbName, from_dialect="input", is_schema=True)
            schema = to_schema(dbName, dialect=self.session.input_dialect)
        else:
            schema = None
        table = self._get_info_schema_table("tables", database=schema.db if schema else None)
        select = exp.select(
            'table_name AS "table_name"',
            'table_schema AS "table_schema"',
            'table_catalog AS "table_catalog"',
            'table_type AS "table_type"',
        ).from_(table)
        if schema and schema.db:
            select = select.where(
                exp.column("table_schema").eq(
                    normalize_string(
                        schema.db,
                        from_dialect="input",
                        to_dialect="execution",
                        to_string_literal=True,
                    )
                )
            )
        if schema and schema.catalog:
            select = select.where(
                exp.column("table_catalog").eq(
                    normalize_string(
                        schema.catalog,
                        from_dialect="input",
                        to_dialect="execution",
                        to_string_literal=True,
                    )
                )
            )
        results = self.session._collect(select, skip_normalization=True)
        tables = [
            Table(
                name=normalize_string(
                    x["table_name"], from_dialect="execution", to_dialect="output"
                ),
                catalog=normalize_string(
                    x["table_catalog"], from_dialect="execution", to_dialect="output"
                ),
                namespace=[
                    normalize_string(
                        x["table_schema"], from_dialect="execution", to_dialect="output"
                    )
                ],
                description=None,
                tableType="VIEW" if x["table_type"] == "VIEW" else "MANAGED",
                isTemporary=False,
            )
            for x in results
        ]
        for table in self.session.temp_views.keys():
            tables.append(
                Table(
                    name=table,  # type: ignore
                    catalog=None,
                    namespace=[],
                    description=None,
                    tableType="VIEW",
                    isTemporary=True,
                )
            )
        if pattern:
            tables = [
                x
                for x in tables
                if fnmatch.fnmatch(
                    x.name,
                    normalize_string(
                        pattern, from_dialect="input", to_dialect="output", is_pattern=True
                    ),
                )
            ]
        return tables


class ListColumnsFromInfoSchemaMixin(_BaseInfoSchemaMixin, t.Generic[SESSION, DF, TABLE]):
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
        dbName = normalize_string(dbName, from_dialect="input", is_schema=True) if dbName else None
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
                current_catalog = normalize_string(
                    self.currentCatalog(), from_dialect="output", to_dialect="input"
                )
                table.set(
                    "catalog",
                    exp.parse_identifier(current_catalog, dialect=self.session.input_dialect),
                )
        source_table = self._get_info_schema_table("columns", database=table.db)
        select = (
            exp.select(
                'column_name AS "column_name"',
                'data_type AS "data_type"',
                'is_nullable AS "is_nullable"',
            )
            .from_(source_table)
            .where(
                exp.column("table_name").eq(
                    normalize_string(
                        table.name,
                        from_dialect="input",
                        to_dialect="execution",
                        to_string_literal=True,
                    )
                )
            )
        )
        if table.db:
            schema_filter: exp.Expression = exp.column("table_schema").eq(
                normalize_string(
                    table.db, from_dialect="input", to_dialect="execution", to_string_literal=True
                )
            )
            if include_temp and self.TEMP_SCHEMA_FILTER:
                schema_filter = exp.Or(this=schema_filter, expression=self.TEMP_SCHEMA_FILTER)
            select = select.where(schema_filter)
        if table.catalog:
            catalog_filter: exp.Expression = exp.column("table_catalog").eq(
                normalize_string(
                    table.catalog,
                    from_dialect="input",
                    to_dialect="execution",
                    to_string_literal=True,
                )
            )
            if include_temp and self.TEMP_CATALOG_FILTER:
                catalog_filter = exp.Or(this=catalog_filter, expression=self.TEMP_CATALOG_FILTER)
            select = select.where(catalog_filter)
        results = self.session._collect(select, skip_normalization=True)
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
