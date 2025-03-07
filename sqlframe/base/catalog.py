# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import typing as t
from collections import defaultdict

from sqlglot import MappingSchema, exp

from sqlframe.base.exceptions import TableSchemaError
from sqlframe.base.util import ensure_column_mapping, normalize_string, to_schema

if t.TYPE_CHECKING:
    from sqlglot.schema import ColumnMapping

    from sqlframe.base._typing import StorageLevel, UserDefinedFunctionLike
    from sqlframe.base.session import DF, TABLE, _BaseSession
    from sqlframe.base.types import DataType, StructType

    SESSION = t.TypeVar("SESSION", bound=_BaseSession)
else:
    DF = t.TypeVar("DF")
    TABLE = t.TypeVar("TABLE")
    SESSION = t.TypeVar("SESSION")


class _BaseCatalog(t.Generic[SESSION, DF, TABLE]):
    """User-facing catalog API, accessible through `SparkSession.catalog`."""

    TEMP_CATALOG_FILTER: t.Optional[exp.Expression] = None
    TEMP_SCHEMA_FILTER: t.Optional[exp.Expression] = None

    def __init__(self, sparkSession: SESSION, schema: t.Optional[MappingSchema] = None) -> None:
        """Create a new Catalog that wraps the underlying JVM object."""
        self.session = sparkSession
        self._schema = schema or MappingSchema()
        self._quoted_columns: t.Dict[exp.Table, t.List[str]] = defaultdict(list)

    @property
    def spark(self) -> SESSION:
        return self.session

    def ensure_table(self, table_name: exp.Table | str) -> exp.Table:
        return (
            (
                exp.to_table(table_name, dialect=self.session.input_dialect)
                .transform(self.session.input_dialect.normalize_identifier)
                .assert_is(exp.Table)
            )
            if isinstance(table_name, str)
            else table_name
        )

    def get_columns_from_schema(self, table: exp.Table | str) -> t.Dict[str, exp.DataType]:
        table = self.ensure_table(table)
        return {
            exp.column(name, quoted=name in self._quoted_columns[table]).sql(
                dialect=self.session.input_dialect
            ): exp.DataType.build(dtype, dialect=self.session.input_dialect)
            for name, dtype in self._schema.find(table, raise_on_missing=True).items()  # type: ignore
        }

    def get_columns(self, table: exp.Table | str) -> t.Dict[str, exp.DataType]:
        table = self.ensure_table(table)
        columns = self.listColumns(table.sql(dialect=self.session.input_dialect))
        if not columns:
            return {}
        return {
            c.name: exp.DataType.build(c.dataType, dialect=self.session.output_dialect)
            for c in columns
        }

    def add_table(
        self,
        table: exp.Table | str,
        column_mapping: t.Optional[ColumnMapping] = None,
        **kwargs: t.Any,
    ) -> None:
        # TODO: Making this an update or add
        table = self.ensure_table(table)
        if self._schema.find(table):
            return
        if column_mapping is None:
            try:
                column_mapping = {
                    normalize_string(
                        k, from_dialect="output", to_dialect="input", is_column=True
                    ): normalize_string(
                        v.sql(dialect=self.session.output_dialect),
                        from_dialect="output",
                        to_dialect="input",
                        is_datatype=True,
                    )
                    for k, v in self.get_columns(table).items()
                }
            except NotImplementedError:
                # TODO: Add doc link
                raise TableSchemaError(
                    "This session does not have access to a catalog that can lookup column information. See docs for explicitly defining columns or using a session that can automatically determine this."
                )
        column_mapping = ensure_column_mapping(column_mapping)  # type: ignore
        for column_name in column_mapping:
            column = exp.to_column(column_name, dialect=self.session.input_dialect)
            if column.this.quoted:
                self._quoted_columns[table].append(column.this.name)

        self._schema.add_table(table, column_mapping, dialect=self.session.input_dialect, **kwargs)

    def getDatabase(self, dbName: str) -> Database:
        """Get the database with the specified name.
        This throws an :class:`AnalysisException` when the database cannot be found.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        dbName : str
             name of the database to get.

        Returns
        -------
        :class:`Database`
            The database found by the name.

        Examples
        --------
        >>> spark.catalog.getDatabase("default")
        Database(name='default', catalog='spark_catalog', description='default database', ...

        Using the fully qualified name with the catalog name.

        >>> spark.catalog.getDatabase("spark_catalog.default")
        Database(name='default', catalog='spark_catalog', description='default database', ...
        """
        dbName = normalize_string(dbName, from_dialect="input", is_schema=True)
        schema = to_schema(dbName, dialect=self.session.input_dialect)
        database_name = schema.db
        databases = self.listDatabases(pattern=database_name)
        if len(databases) == 0:
            raise ValueError(f"Database '{dbName}' not found")
        if len(databases) > 1:
            if schema.catalog is not None:
                filtered_databases = [
                    db
                    for db in databases
                    if normalize_string(db.catalog, from_dialect="output", to_dialect="input")  # type: ignore
                    == schema.catalog
                ]
                if filtered_databases:
                    return filtered_databases[0]
        return databases[0]

    def databaseExists(self, dbName: str) -> bool:
        """Check if the database with the specified name exists.

        .. versionadded:: 3.3.0

        Parameters
        ----------
        dbName : str
            name of the database to check existence

            .. versionchanged:: 3.4.0
               Allow ``dbName`` to be qualified with catalog name.

        Returns
        -------
        bool
            Indicating whether the database exists

        Examples
        --------
        Check if 'test_new_database' database exists

        >>> spark.catalog.databaseExists("test_new_database")
        False
        >>> _ = spark.sql("CREATE DATABASE test_new_database")
        >>> spark.catalog.databaseExists("test_new_database")
        True

        Using the fully qualified name with the catalog name.

        >>> spark.catalog.databaseExists("spark_catalog.test_new_database")
        True
        >>> _ = spark.sql("DROP DATABASE test_new_database")
        """
        try:
            self.getDatabase(dbName)
            return True
        except ValueError:
            return False

    def getTable(self, tableName: str) -> Table:
        """Get the table or view with the specified name. This table can be a temporary view or a
        table/view. This throws an :class:`AnalysisException` when no Table can be found.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        tableName : str
            name of the table to get.

            .. versionchanged:: 3.4.0
               Allow `tableName` to be qualified with catalog name.

        Returns
        -------
        :class:`Table`
            The table found by the name.

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tbl1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.getTable("tbl1")
        Table(name='tbl1', catalog='spark_catalog', namespace=['default'], ...

        Using the fully qualified name with the catalog name.

        >>> spark.catalog.getTable("default.tbl1")
        Table(name='tbl1', catalog='spark_catalog', namespace=['default'], ...
        >>> spark.catalog.getTable("spark_catalog.default.tbl1")
        Table(name='tbl1', catalog='spark_catalog', namespace=['default'], ...
        >>> _ = spark.sql("DROP TABLE tbl1")

        Throw an analysis exception when the table does not exist.

        >>> spark.catalog.getTable("tbl1")
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        tableName = normalize_string(tableName, from_dialect="input", is_table=True)
        table = exp.to_table(tableName, dialect=self.session.input_dialect)
        schema = table.copy()
        schema.set("this", None)
        tables = self.listTables(
            schema.sql(dialect=self.session.input_dialect) if schema.db else None
        )
        matching_tables = [
            t
            for t in tables
            if normalize_string(t.name, from_dialect="output", to_dialect="input") == table.name
        ]
        if not matching_tables:
            raise ValueError(f"Table '{tableName}' not found")
        return matching_tables[0]

    def functionExists(self, functionName: str, dbName: t.Optional[str] = None) -> bool:
        """Check if the function with the specified name exists.
        This can either be a temporary function or a function.

        .. versionadded:: 3.3.0

        Parameters
        ----------
        functionName : str
            name of the function to check existence

            .. versionchanged:: 3.4.0
               Allow ``functionName`` to be qualified with catalog name

        dbName : str, t.Optional
            name of the database to check function existence in.

        Returns
        -------
        bool
            Indicating whether the function exists

        Notes
        -----
        If no database is specified, the current database and catalog
        are used. This API includes all temporary functions.

        Examples
        --------
        >>> spark.catalog.functionExists("count")
        True

        Using the fully qualified name for function name.

        >>> spark.catalog.functionExists("default.unexisting_function")
        False
        >>> spark.catalog.functionExists("spark_catalog.default.unexisting_function")
        False
        """
        functions = self.listFunctions(dbName)
        return any([f.name == functionName for f in functions])

    def getFunction(self, functionName: str) -> Function:
        """Get the function with the specified name. This function can be a temporary function or a
        function. This throws an :class:`AnalysisException` when the function cannot be found.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        functionName : str
            name of the function to check existence.

        Returns
        -------
        :class:`Function`
            The function found by the name.

        Examples
        --------
        >>> _ = spark.sql(
        ...     "CREATE FUNCTION my_func1 AS 'test.org.apache.spark.sql.MyDoubleAvg'")
        >>> spark.catalog.getFunction("my_func1")
        Function(name='my_func1', catalog='spark_catalog', namespace=['default'], ...

        Using the fully qualified name for function name.

        >>> spark.catalog.getFunction("default.my_func1")
        Function(name='my_func1', catalog='spark_catalog', namespace=['default'], ...
        >>> spark.catalog.getFunction("spark_catalog.default.my_func1")
        Function(name='my_func1', catalog='spark_catalog', namespace=['default'], ...

        Throw an analysis exception when the function does not exists.

        >>> spark.catalog.getFunction("my_func2")
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        table = exp.to_table(functionName, dialect=self.session.input_dialect)
        if table.catalog or table.db:
            schema = table.copy()
            schema.set("this", None)
            db_name = schema.sql(dialect=self.session.input_dialect)
            function_name = table.name
        else:
            db_name = None
            function_name = functionName
        functions = self.listFunctions(dbName=db_name, pattern=function_name)
        matching_functions = [f for f in functions if f.name == function_name]
        if not matching_functions:
            raise ValueError(f"Function '{functionName}' not found")
        return matching_functions[0]

    def tableExists(self, tableName: str, dbName: t.Optional[str] = None) -> bool:
        """Check if the table or view with the specified name exists.
        This can either be a temporary view or a table/view.

        .. versionadded:: 3.3.0

        Parameters
        ----------
        tableName : str
            name of the table to check existence.
            If no database is specified, first try to treat ``tableName`` as a
            multi-layer-namespace identifier, then try ``tableName`` as a normal table
            name in the current database if necessary.

            .. versionchanged:: 3.4.0
               Allow ``tableName`` to be qualified with catalog name when ``dbName`` is None.

        dbName : str, t.Optional
            name of the database to check table existence in.

        Returns
        -------
        bool
            Indicating whether the table/view exists

        Examples
        --------
        This function can check if a table is defined or not:

        >>> spark.catalog.tableExists("unexisting_table")
        False
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tbl1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.tableExists("tbl1")
        True

        Using the fully qualified names for tables.

        >>> spark.catalog.tableExists("default.tbl1")
        True
        >>> spark.catalog.tableExists("spark_catalog.default.tbl1")
        True
        >>> spark.catalog.tableExists("tbl1", "default")
        True
        >>> _ = spark.sql("DROP TABLE tbl1")

        Check if views exist:

        >>> spark.catalog.tableExists("view1")
        False
        >>> _ = spark.sql("CREATE VIEW view1 AS SELECT 1")
        >>> spark.catalog.tableExists("view1")
        True

        Using the fully qualified names for views.

        >>> spark.catalog.tableExists("default.view1")
        True
        >>> spark.catalog.tableExists("spark_catalog.default.view1")
        True
        >>> spark.catalog.tableExists("view1", "default")
        True
        >>> _ = spark.sql("DROP VIEW view1")

        Check if temporary views exist:

        >>> _ = spark.sql("CREATE TEMPORARY VIEW view1 AS SELECT 1")
        >>> spark.catalog.tableExists("view1")
        True
        >>> df = spark.sql("DROP VIEW view1")
        >>> spark.catalog.tableExists("view1")
        False
        """
        tableName = normalize_string(tableName, from_dialect="input", is_table=True)
        dbName = normalize_string(dbName, from_dialect="input", is_schema=True) if dbName else None
        table = exp.to_table(tableName, dialect=self.session.input_dialect)
        schema_arg = to_schema(dbName, dialect=self.session.input_dialect) if dbName else None
        if not table.db:
            if schema_arg and schema_arg.db:
                table.set("db", schema_arg.args["db"])
            else:
                table.set("db", exp.parse_identifier(self.currentDatabase(), dialect="duckdb"))
        if not table.catalog:
            if schema_arg and schema_arg.catalog:
                table.set("catalog", schema_arg.args["catalog"])
            else:
                table.set("catalog", exp.parse_identifier(self.currentCatalog(), dialect="duckdb"))
        table_name = table.name
        schema = table.copy()
        schema.set("this", None)
        tables = self.listTables(schema.sql(dialect=self.session.input_dialect))
        return any([x for x in tables if x.name == table_name])

    def currentCatalog(self) -> str:
        """Returns the current default catalog in this session.

        .. versionadded:: 3.4.0

        Examples
        --------
        >>> spark.catalog.currentCatalog()
        'spark_catalog'
        """
        raise NotImplementedError

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
        raise NotImplementedError

    def currentDatabase(self) -> str:
        """Returns the current default schema in this session.

        .. versionadded:: 3.4.0

        Examples
        --------
        >>> spark.catalog.currentDatabase()
        'default'
        """
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

    def setCurrentDatabase(self, dbName: str) -> None:
        """
        Sets the current default database in this session.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> spark.catalog.setCurrentDatabase("default")
        """
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError()

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
        raise NotImplementedError()

    def dropTempView(self, viewName: str) -> bool:
        """Drops the local temporary view with the given view name in the catalog.
        If the view has been cached before, then it will also be uncached.
        Returns true if this view is dropped successfully, false otherwise.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        viewName : str
            name of the temporary view to drop.

        Returns
        -------
        bool
            If the temporary view was successfully dropped or not.

            .. versionadded:: 2.1.0
                The return type of this method was ``None`` in Spark 2.0, but changed to ``bool``
                in Spark 2.1.

        Examples
        --------
        >>> spark.createDataFrame([(1, 1)]).createTempView("my_table")

        Dropping the temporary view.

        >>> spark.catalog.dropTempView("my_table")
        True

        Throw an exception if the temporary view does not exists.

        >>> spark.table("my_table")
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        raise NotImplementedError()

    def dropGlobalTempView(self, viewName: str) -> bool:
        """Drops the global temporary view with the given view name in the catalog.

        .. versionadded:: 2.1.0

        Parameters
        ----------
        viewName : str
            name of the global view to drop.

        Returns
        -------
        bool
            If the global view was successfully dropped or not.

        Notes
        -----
        If the view has been cached before, then it will also be uncached.

        Examples
        --------
        >>> spark.createDataFrame([(1, 1)]).createGlobalTempView("my_table")

        Dropping the global view.

        >>> spark.catalog.dropGlobalTempView("my_table")
        True

        Throw an exception if the global view does not exists.

        >>> spark.table("global_temp.my_table")
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        raise NotImplementedError()

    def registerFunction(
        self, name: str, f: t.Callable[..., t.Any], returnType: t.Optional[DataType] = None
    ) -> UserDefinedFunctionLike:
        """An alias for :func:`spark.udf.register`.
        See :meth:`pyspark.sql.UDFRegistration.register`.

        .. versionadded:: 2.0.0

        .. deprecated:: 2.3.0
            Use :func:`spark.udf.register` instead.

        .. versionchanged:: 3.4.0
            Supports Spark Connect.
        """
        raise NotImplementedError()

    def isCached(self, tableName: str) -> bool:
        """
        Returns true if the table is currently cached in-memory.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        tableName : str
            name of the table to get.

            .. versionchanged:: 3.4.0
                Allow ``tableName`` to be qualified with catalog name.

        Returns
        -------
        bool

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tbl1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.cacheTable("tbl1")
        >>> spark.catalog.isCached("tbl1")
        True

        Throw an analysis exception when the table does not exist.

        >>> spark.catalog.isCached("not_existing_table")
        Traceback (most recent call last):
            ...
        AnalysisException: ...

        Using the fully qualified name for the table.

        >>> spark.catalog.isCached("spark_catalog.default.tbl1")
        True
        >>> spark.catalog.uncacheTable("tbl1")
        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        raise NotImplementedError()

    def cacheTable(self, tableName: str, storageLevel: t.Optional[StorageLevel] = None) -> None:
        """Caches the specified table in-memory or with given storage level.
        Default MEMORY_AND_DISK.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        tableName : str
            name of the table to get.

            .. versionchanged:: 3.4.0
                Allow ``tableName`` to be qualified with catalog name.

        storageLevel : :class:`StorageLevel`
            storage level to set for persistence.

            .. versionchanged:: 3.5.0
                Allow to specify storage level.

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tbl1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.cacheTable("tbl1")

        or

        >>> spark.catalog.cacheTable("tbl1", StorageLevel.OFF_HEAP)

        Throw an analysis exception when the table does not exist.

        >>> spark.catalog.cacheTable("not_existing_table")
        Traceback (most recent call last):
            ...
        AnalysisException: ...

        Using the fully qualified name for the table.

        >>> spark.catalog.cacheTable("spark_catalog.default.tbl1")
        >>> spark.catalog.uncacheTable("tbl1")
        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        raise NotImplementedError()

    def uncacheTable(self, tableName: str) -> None:
        """Removes the specified table from the in-memory cache.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        tableName : str
            name of the table to get.

            .. versionchanged:: 3.4.0
                Allow ``tableName`` to be qualified with catalog name.

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tbl1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.cacheTable("tbl1")
        >>> spark.catalog.uncacheTable("tbl1")
        >>> spark.catalog.isCached("tbl1")
        False

        Throw an analysis exception when the table does not exist.

        >>> spark.catalog.uncacheTable("not_existing_table")
        Traceback (most recent call last):
            ...
        AnalysisException: ...

        Using the fully qualified name for the table.

        >>> spark.catalog.uncacheTable("spark_catalog.default.tbl1")
        >>> spark.catalog.isCached("tbl1")
        False
        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        raise NotImplementedError()

    def clearCache(self) -> None:
        """Removes all cached tables from the in-memory cache.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tbl1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.clearCache()
        >>> spark.catalog.isCached("tbl1")
        False
        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        raise NotImplementedError()

    def refreshTable(self, tableName: str) -> None:
        """Invalidates and refreshes all the cached data and metadata of the given table.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        tableName : str
            name of the table to get.

            .. versionchanged:: 3.4.0
                Allow ``tableName`` to be qualified with catalog name.

        Examples
        --------
        The example below caches a table, and then removes the data.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        ...     _ = spark.sql(
        ...         "CREATE TABLE tbl1 (col STRING) USING TEXT LOCATION '{}'".format(d))
        ...     _ = spark.sql("INSERT INTO tbl1 SELECT 'abc'")
        ...     spark.catalog.cacheTable("tbl1")
        ...     spark.table("tbl1").show()
        +---+
        |col|
        +---+
        |abc|
        +---+

        Because the table is cached, it computes from the cached data as below.

        >>> spark.table("tbl1").count()
        1

        After refreshing the table, it shows 0 because the data does not exist t.Anymore.

        >>> spark.catalog.refreshTable("tbl1")
        >>> spark.table("tbl1").count()
        0

        Using the fully qualified name for the table.

        >>> spark.catalog.refreshTable("spark_catalog.default.tbl1")
        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        raise NotImplementedError()

    def recoverPartitions(self, tableName: str) -> None:
        """Recovers all the partitions of the given table and updates the catalog.

        .. versionadded:: 2.1.1

        Parameters
        ----------
        tableName : str
            name of the table to get.

        Notes
        -----
        Only works with a partitioned table, and not a view.

        Examples
        --------
        The example below creates a partitioned table against the existing directory of
        the partitioned table. After that, it recovers the partitions.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        ...     spark.range(1).selectExpr(
        ...         "id as key", "id as value").write.partitionBy("key").mode("overwrite").save(d)
        ...     _ = spark.sql(
        ...          "CREATE TABLE tbl1 (key LONG, value LONG)"
        ...          "USING parquet OPTIONS (path '{}') PARTITIONED BY (key)".format(d))
        ...     spark.table("tbl1").show()
        ...     spark.catalog.recoverPartitions("tbl1")
        ...     spark.table("tbl1").show()
        +-----+---+
        |value|key|
        +-----+---+
        +-----+---+
        +-----+---+
        |value|key|
        +-----+---+
        |    0|  0|
        +-----+---+
        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        raise NotImplementedError()

    def refreshByPath(self, path: str) -> None:
        """Invalidates and refreshes all the cached data (and the associated metadata) for t.Any
        DataFrame that contains the given data source path.

        .. versionadded:: 2.2.0

        Parameters
        ----------
        path : str
            the path to refresh the cache.

        Examples
        --------
        The example below caches a table, and then removes the data.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        ...     _ = spark.sql(
        ...         "CREATE TABLE tbl1 (col STRING) USING TEXT LOCATION '{}'".format(d))
        ...     _ = spark.sql("INSERT INTO tbl1 SELECT 'abc'")
        ...     spark.catalog.cacheTable("tbl1")
        ...     spark.table("tbl1").show()
        +---+
        |col|
        +---+
        |abc|
        +---+

        Because the table is cached, it computes from the cached data as below.

        >>> spark.table("tbl1").count()
        1

        After refreshing the table by path, it shows 0 because the data does not exist t.Anymore.

        >>> spark.catalog.refreshByPath(d)
        >>> spark.table("tbl1").count()
        0

        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        raise NotImplementedError()

    def _reset(self) -> None:
        """(Internal use only) Drop all existing databases (except "default"), tables,
        partitions and functions, and set the current database to "default".

        This is mainly used for tests.
        """
        raise NotImplementedError()


class CatalogMetadata(t.NamedTuple):
    name: str
    description: t.Optional[str]


class Database(t.NamedTuple):
    name: str
    catalog: t.Optional[str]
    description: t.Optional[str]
    locationUri: str


class Table(t.NamedTuple):
    name: str
    catalog: t.Optional[str]
    namespace: t.Optional[t.List[str]]
    description: t.Optional[str]
    tableType: str
    isTemporary: bool

    @property
    def database(self) -> t.Optional[str]:
        if self.namespace is not None and len(self.namespace) == 1:
            return self.namespace[0]
        else:
            return None


class Column(t.NamedTuple):
    name: str
    description: t.Optional[str]
    dataType: str
    nullable: bool
    isPartition: bool
    isBucket: bool


class Function(t.NamedTuple):
    name: str
    catalog: t.Optional[str]
    namespace: t.Optional[t.List[str]]
    description: t.Optional[str]
    className: str
    isTemporary: bool
