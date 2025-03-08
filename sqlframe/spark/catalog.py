# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import fnmatch
import typing as t

from sqlframe.base.catalog import (
    CatalogMetadata,
    Column,
    Database,
    Function,
    Table,
    _BaseCatalog,
)
from sqlframe.base.types import DataType, StructType
from sqlframe.base.util import normalize_string

if t.TYPE_CHECKING:
    from sqlframe.base._typing import StorageLevel, UserDefinedFunctionLike
    from sqlframe.spark.dataframe import SparkDataFrame
    from sqlframe.spark.session import SparkSession  # noqa
    from sqlframe.spark.table import SparkTable  # noqa


class SparkCatalog(
    _BaseCatalog["SparkSession", "SparkDataFrame", "SparkTable"],
):
    @property
    def _spark_catalog(self):
        return self.session.spark_session.catalog

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
        dbName = normalize_string(
            dbName, from_dialect="input", to_dialect="execution", is_schema=True
        )
        resp = self._spark_catalog.getDatabase(dbName)
        return Database(
            name=normalize_string(resp.name, from_dialect="execution", to_dialect="output"),
            catalog=normalize_string(resp.catalog, from_dialect="execution", to_dialect="output"),
            description=resp.description,
            locationUri=resp.locationUri,
        )

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
        dbName = normalize_string(
            dbName, from_dialect="input", to_dialect="execution", is_schema=True
        )
        return self._spark_catalog.databaseExists(dbName)

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
        tableName = normalize_string(
            tableName, from_dialect="input", to_dialect="execution", is_table=True
        )
        resp = self._spark_catalog.getTable(tableName)
        return Table(
            name=normalize_string(resp.name, from_dialect="execution", to_dialect="output"),
            catalog=normalize_string(resp.catalog, from_dialect="execution", to_dialect="output"),
            namespace=[
                normalize_string(x, from_dialect="execution", to_dialect="output")
                for x in resp.namespace
            ],
            description=resp.description,
            tableType=resp.tableType,
            isTemporary=resp.isTemporary,
        )

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
        dbName = (
            normalize_string(dbName, from_dialect="input", to_dialect="execution")
            if dbName
            else None
        )
        functionName = normalize_string(functionName, from_dialect="input", to_dialect="execution")
        return self._spark_catalog.functionExists(functionName, dbName)

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
        functionName = normalize_string(functionName, from_dialect="input", to_dialect="execution")
        resp = self._spark_catalog.getFunction(functionName)
        return Function(
            name=normalize_string(resp.name, from_dialect="execution", to_dialect="output"),
            catalog=normalize_string(resp.catalog, from_dialect="execution", to_dialect="output")
            if resp.catalog
            else None,
            namespace=[
                normalize_string(x, from_dialect="execution", to_dialect="output")
                for x in resp.namespace
            ]
            if resp.namespace
            else resp.namespace,
            description=resp.description,
            className=resp.className,
            isTemporary=resp.isTemporary,
        )

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
        tableName = normalize_string(
            tableName, from_dialect="input", to_dialect="execution", is_table=True
        )
        dbName = (
            normalize_string(dbName, from_dialect="input", to_dialect="execution")
            if dbName
            else None
        )
        return self._spark_catalog.tableExists(tableName, dbName)

    def currentCatalog(self) -> str:
        """Returns the current default catalog in this session.

        .. versionadded:: 3.4.0

        Examples
        --------
        >>> spark.catalog.currentCatalog()
        'spark_catalog'
        """
        return self._spark_catalog.currentCatalog()

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
        catalogName = normalize_string(catalogName, from_dialect="input", to_dialect="execution")
        return self._spark_catalog.setCurrentCatalog(catalogName)

    def currentDatabase(self) -> str:
        """Returns the current default schema in this session.

        .. versionadded:: 3.4.0

        Examples
        --------
        >>> spark.catalog.currentDatabase()
        'default'
        """
        return normalize_string(
            self._spark_catalog.currentDatabase(), from_dialect="execution", to_dialect="output"
        )

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
        pattern = (
            normalize_string(pattern, from_dialect="input", to_dialect="execution", is_pattern=True)
            if pattern
            else None
        )
        resp = self._spark_catalog.listDatabases(pattern)
        return [
            Database(
                name=normalize_string(x.name, from_dialect="execution", to_dialect="output"),
                catalog=normalize_string(x.catalog, from_dialect="execution", to_dialect="output"),
                description=x.description,
                locationUri=x.locationUri,
            )
            for x in resp
        ]

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
        pattern = (
            normalize_string(pattern, from_dialect="input", to_dialect="execution", is_pattern=True)
            if pattern
            else None
        )
        resp = self._spark_catalog.listCatalogs(pattern)
        return [
            CatalogMetadata(
                name=normalize_string(x.name, from_dialect="execution", to_dialect="output"),
                description=x.description,
            )
            for x in resp
        ]

    def setCurrentDatabase(self, dbName: str) -> None:
        """
        Sets the current default database in this session.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> spark.catalog.setCurrentDatabase("default")
        """
        dbName = normalize_string(dbName, from_dialect="input", to_dialect="execution")
        return self._spark_catalog.setCurrentDatabase(dbName)

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
        dbName = (
            normalize_string(dbName, from_dialect="input", to_dialect="execution", is_schema=True)
            if dbName
            else None
        )
        pattern = (
            normalize_string(pattern, from_dialect="input", to_dialect="execution", is_pattern=True)
            if pattern
            else None
        )
        tables = self._spark_catalog.listTables(dbName, pattern)
        for table_name in self.spark.temp_views:
            table_name = normalize_string(
                table_name, from_dialect="input", to_dialect="execution", is_table=True
            )
            if not pattern or (pattern and fnmatch.fnmatch(table_name, pattern)):
                tables.append(
                    Table(
                        name=table_name,
                        catalog=None,
                        namespace=[],
                        description=None,
                        tableType="VIEW",
                        isTemporary=True,
                    )
                )
        return [
            Table(
                name=normalize_string(x.name, from_dialect="execution", to_dialect="output"),
                catalog=normalize_string(x.catalog, from_dialect="execution", to_dialect="output")
                if x.catalog
                else None,
                namespace=[
                    normalize_string(y, from_dialect="execution", to_dialect="output")
                    for y in x.namespace
                ],
                description=x.description,
                tableType=x.tableType,
                isTemporary=x.isTemporary,
            )
            for x in tables
        ]

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
            tableName, from_dialect="input", to_dialect="execution", is_table=True
        )
        if df := self.spark.temp_views.get(tableName):
            return [
                Column(
                    name=normalize_string(col, from_dialect="execution", to_dialect="output"),
                    description=None,
                    dataType="",
                    nullable=True,
                    isPartition=False,
                    isBucket=False,
                )
                for col in df.columns
            ]
        return [
            Column(
                name=normalize_string(x.name, from_dialect="execution", to_dialect="output"),
                description=x.description,
                dataType=normalize_string(
                    x.dataType, from_dialect="execution", to_dialect="output", is_datatype=True
                ),
                nullable=x.nullable,
                isPartition=x.isPartition,
                isBucket=x.isBucket,
            )
            for x in self._spark_catalog.listColumns(tableName, dbName)
        ]

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
        dbName = (
            normalize_string(dbName, from_dialect="input", to_dialect="execution")
            if dbName
            else None
        )
        pattern = (
            normalize_string(pattern, from_dialect="input", to_dialect="execution", is_pattern=True)
            if pattern
            else None
        )
        return [
            Function(
                name=normalize_string(x.name, from_dialect="execution", to_dialect="output"),
                catalog=normalize_string(x.catalog, from_dialect="execution", to_dialect="output")
                if x.catalog
                else None,
                namespace=[
                    normalize_string(y, from_dialect="execution", to_dialect="output")
                    for y in x.namespace
                ]
                if x.namespace
                else x.namespace,
                description=x.description,
                className=x.className,
                isTemporary=x.isTemporary,
            )
            for x in self._spark_catalog.listFunctions(dbName, pattern)
        ]

    def createExternalTable(
        self,
        tableName: str,
        path: t.Optional[str] = None,
        source: t.Optional[str] = None,
        schema: t.Optional[StructType] = None,
        **options: str,
    ) -> SparkTable:
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
        tableName = normalize_string(tableName, from_dialect="input", to_dialect="execution")
        return self._spark_catalog.createExternalTable(
            tableName,
            path,
            source,
            schema,
            **options,
        )

    def createTable(
        self,
        tableName: str,
        path: t.Optional[str] = None,
        source: t.Optional[str] = None,
        schema: t.Optional[StructType] = None,
        description: t.Optional[str] = None,
        **options: str,
    ) -> SparkTable:
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
        tableName = normalize_string(tableName, from_dialect="input", to_dialect="execution")
        return self._spark_catalog.createTable(
            tableName,
            path,
            source,
            schema,
            description,
            **options,
        )

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
        viewName = normalize_string(viewName, from_dialect="input", to_dialect="execution")
        return self._spark_catalog.dropTempView(viewName)

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
        viewName = normalize_string(viewName, from_dialect="input", to_dialect="execution")
        return self._spark_catalog.dropGlobalTempView(viewName)

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
        name = normalize_string(name, from_dialect="input", to_dialect="execution")
        return self._spark_catalog.registerFunction(name, f, returnType)

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
        tableName = normalize_string(
            tableName, from_dialect="input", to_dialect="execution", is_table=True
        )
        return self._spark_catalog.isCached(tableName)

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
        tableName = normalize_string(
            tableName, from_dialect="input", to_dialect="execution", is_table=True
        )
        return self._spark_catalog.cacheTable(tableName, storageLevel)

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
        tableName = normalize_string(
            tableName, from_dialect="input", to_dialect="execution", is_table=True
        )
        return self._spark_catalog.uncacheTable(tableName)

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
        return self._spark_catalog.clearCache()

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
        tableName = normalize_string(
            tableName, from_dialect="input", to_dialect="execution", is_table=True
        )
        return self._spark_catalog.refreshTable(tableName)

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
        tableName = normalize_string(
            tableName, from_dialect="input", to_dialect="execution", is_table=True
        )
        return self._spark_catalog.recoverPartitions(tableName)

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
        return self._spark_catalog.refreshByPath(path)

    def _reset(self) -> None:
        """(Internal use only) Drop all existing databases (except "default"), tables,
        partitions and functions, and set the current database to "default".

        This is mainly used for tests.
        """
        return self._spark_catalog._reset()
