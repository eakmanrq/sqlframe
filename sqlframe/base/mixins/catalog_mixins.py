import fnmatch
import typing as t

from sqlglot import exp

from sqlframe.base.catalog import (
    DF,
    SESSION,
    CatalogMetadata,
    Column,
    Database,
    Table,
    _BaseCatalog,
)
from sqlframe.base.decorators import normalize
from sqlframe.base.util import schema_, to_schema


class _BaseInfoSchemaMixin(_BaseCatalog, t.Generic[SESSION, DF]):
    QUALIFY_INFO_SCHEMA_WITH_DATABASE = False
    UPPERCASE_INFO_SCHEMA = False

    def _get_info_schema_table(
        self,
        table_name: str,
        database: t.Optional[str] = None,
        qualify_override: t.Optional[bool] = None,
    ) -> exp.Table:
        table = f"information_schema.{table_name}"
        if self.UPPERCASE_INFO_SCHEMA:
            table = table.upper()
        qualify = (
            qualify_override
            if qualify_override is not None
            else self.QUALIFY_INFO_SCHEMA_WITH_DATABASE
        )
        if qualify:
            db = database or self.currentDatabase()
            if not db:
                raise ValueError("Table name must be qualified with a database.")
            table = f"{db}.{table}"
        return exp.to_table(table)


class GetCurrentCatalogFromFunctionMixin(_BaseCatalog, t.Generic[SESSION, DF]):
    CURRENT_CATALOG_EXPRESSION: exp.Expression = exp.func("current_catalog")

    def currentCatalog(self) -> str:
        """Returns the current default catalog in this session.

        .. versionadded:: 3.4.0

        Examples
        --------
        >>> spark.catalog.currentCatalog()
        'spark_catalog'
        """
        return self.session._fetch_rows(
            exp.select(self.CURRENT_CATALOG_EXPRESSION), quote_identifiers=False
        )[0][0]


class GetCurrentDatabaseFromFunctionMixin(_BaseCatalog, t.Generic[SESSION, DF]):
    CURRENT_DATABASE_EXPRESSION: exp.Expression = exp.func("current_schema")

    def currentDatabase(self) -> str:
        """Returns the current default schema in this session.

        .. versionadded:: 3.4.0

        Examples
        --------
        >>> spark.catalog.currentDatabase()
        'default'
        """
        return self.session._fetch_rows(exp.select(self.CURRENT_DATABASE_EXPRESSION))[0][0]


class SetCurrentCatalogFromUseMixin(_BaseCatalog, t.Generic[SESSION, DF]):
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
        self.session._execute(
            exp.Use(this=exp.parse_identifier(catalogName, dialect=self.session.input_dialect))
        )


class ListDatabasesFromInfoSchemaMixin(_BaseInfoSchemaMixin, t.Generic[SESSION, DF]):
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
        results = self.session._fetch_rows(
            exp.Select().select("schema_name", "catalog_name").from_(table)
        )
        databases = [
            Database(name=x[0], catalog=x[1], description=None, locationUri="") for x in results
        ]
        if pattern:
            databases = [db for db in databases if fnmatch.fnmatch(db.name, pattern)]
        return databases


class ListCatalogsFromInfoSchemaMixin(_BaseInfoSchemaMixin, t.Generic[SESSION, DF]):
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
        results = self.session._fetch_rows(
            exp.Select().select("catalog_name").from_(table).distinct()
        )
        catalogs = [CatalogMetadata(name=x[0], description=None) for x in results]
        if pattern:
            catalogs = [catalog for catalog in catalogs if fnmatch.fnmatch(catalog.name, pattern)]
        return catalogs


class SetCurrentDatabaseFromSearchPathMixin(_BaseCatalog, t.Generic[SESSION, DF]):
    def setCurrentDatabase(self, dbName: str) -> None:
        """
        Sets the current default database in this session.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> spark.catalog.setCurrentDatabase("default")
        """
        self.session._execute(f'SET search_path TO "{dbName}"')


class SetCurrentDatabaseFromUseMixin(_BaseCatalog, t.Generic[SESSION, DF]):
    def setCurrentDatabase(self, dbName: str) -> None:
        """
        Sets the current default database in this session.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> spark.catalog.setCurrentDatabase("default")
        """
        schema = to_schema(dbName, dialect=self.session.input_dialect)
        if not schema.catalog:
            schema.set(
                "catalog",
                exp.parse_identifier(self.currentCatalog(), dialect=self.session.input_dialect),
            )
        self.session._execute(exp.Use(this=schema))


class ListTablesFromInfoSchemaMixin(_BaseInfoSchemaMixin, t.Generic[SESSION, DF]):
    @normalize(["dbName"])
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
            schema = schema_(
                db=exp.parse_identifier(self.currentDatabase(), dialect=self.session.input_dialect),
                catalog=exp.parse_identifier(
                    self.currentCatalog(), dialect=self.session.input_dialect
                ),
            )
        elif dbName:
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
            select = select.where(exp.column("table_schema").eq(schema.db))
        if schema and schema.catalog:
            select = select.where(exp.column("table_catalog").eq(schema.catalog))
        results = self.session._fetch_rows(select)
        tables = [
            Table(
                name=x["table_name"],
                catalog=x["table_catalog"],
                namespace=[x["table_schema"]],
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
            tables = [x for x in tables if fnmatch.fnmatch(x.name, pattern)]
        return tables


class ListColumnsFromInfoSchemaMixin(_BaseInfoSchemaMixin, t.Generic[SESSION, DF]):
    @normalize(["tableName", "dbName"])
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
                table.set(
                    "db",
                    exp.parse_identifier(
                        self.currentDatabase(), dialect=self.session.input_dialect
                    ),
                )
        if not table.catalog:
            if schema and schema.catalog:
                table.set("catalog", schema.args["catalog"])
            else:
                table.set(
                    "catalog",
                    exp.parse_identifier(self.currentCatalog(), dialect=self.session.input_dialect),
                )
        source_table = self._get_info_schema_table("columns", database=table.db)
        select = (
            exp.select(
                'column_name AS "column_name"',
                'data_type AS "data_type"',
                'is_nullable AS "is_nullable"',
            )
            .from_(source_table)
            .where(exp.column("table_name").eq(table.name))
        )
        if table.db:
            schema_filter: exp.Expression = exp.column("table_schema").eq(table.db)
            if include_temp and self.TEMP_SCHEMA_FILTER:
                schema_filter = exp.Or(this=schema_filter, expression=self.TEMP_SCHEMA_FILTER)
            select = select.where(schema_filter)
        if table.catalog:
            catalog_filter: exp.Expression = exp.column("table_catalog").eq(table.catalog)
            if include_temp and self.TEMP_CATALOG_FILTER:
                catalog_filter = exp.Or(this=catalog_filter, expression=self.TEMP_CATALOG_FILTER)
            select = select.where(catalog_filter)
        results = self.session._fetch_rows(select)
        return [
            Column(
                name=x["column_name"],
                description=None,
                dataType=x["data_type"],
                nullable=x["is_nullable"] == "YES",
                isPartition=False,
                isBucket=False,
            )
            for x in results
        ]
