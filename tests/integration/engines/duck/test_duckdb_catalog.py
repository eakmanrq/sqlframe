import pytest

from sqlframe.base.catalog import CatalogMetadata, Column, Database, Function, Table
from sqlframe.duckdb.session import DuckDBSession


@pytest.fixture
def duckdb_session() -> DuckDBSession:
    import duckdb

    connector = duckdb.connect()
    connector.execute("set TimeZone = 'UTC'")
    connector.execute("SELECT * FROM duckdb_settings() WHERE name = 'TimeZone'")
    assert connector.fetchone()[1] == "UTC"  # type: ignore
    session = DuckDBSession(connector)
    session._execute('''ATTACH ':memory:' AS "default"''')
    session._execute('''ATTACH ':memory:' AS "catalog1"''')
    session._execute('''ATTACH ':memory:' AS "catalog2"''')
    session._execute('''USE "default"''')
    session._execute('CREATE SCHEMA "default"."db1"')
    session._execute('CREATE TABLE "default"."main"."table1" (id INTEGER, name VARCHAR(100))')
    session._execute("CREATE MACRO main.testing(a, b) AS a + b")
    return session


def test_current_catalog(duckdb_session: DuckDBSession):
    # Default is considered a word that must be quoted in DuckDB
    assert duckdb_session.catalog.currentCatalog() == "default"


def test_set_current_catalog(duckdb_session: DuckDBSession):
    assert duckdb_session.catalog.currentCatalog() == "default"
    duckdb_session.catalog.setCurrentCatalog("catalog1")
    assert duckdb_session.catalog.currentCatalog() == "catalog1"


def test_list_catalogs(duckdb_session: DuckDBSession):
    assert sorted(duckdb_session.catalog.listCatalogs(), key=lambda x: x.name) == [
        CatalogMetadata(name="catalog1", description=None),
        CatalogMetadata(name="catalog2", description=None),
        CatalogMetadata(name="default", description=None),
        CatalogMetadata(name="memory", description=None),
        CatalogMetadata(name="system", description=None),
        CatalogMetadata(name="temp", description=None),
    ]


def test_current_database(duckdb_session: DuckDBSession):
    assert duckdb_session.catalog.currentDatabase() == "main"


def test_set_current_database(duckdb_session: DuckDBSession):
    assert duckdb_session.catalog.currentDatabase() == "main"
    duckdb_session.catalog.setCurrentDatabase("db1")
    assert duckdb_session.catalog.currentDatabase() == "db1"


def test_list_databases(duckdb_session: DuckDBSession):
    assert sorted(duckdb_session.catalog.listDatabases(), key=lambda x: (x.catalog, x.name)) == [
        Database(name="main", catalog="catalog1", description=None, locationUri=""),
        Database(name="main", catalog="catalog2", description=None, locationUri=""),
        Database(name="db1", catalog="default", description=None, locationUri=""),
        Database(name="main", catalog="default", description=None, locationUri=""),
        Database(name="main", catalog="memory", description=None, locationUri=""),
        Database(name="information_schema", catalog="system", description=None, locationUri=""),
        Database(name="main", catalog="system", description=None, locationUri=""),
        Database(name="pg_catalog", catalog="system", description=None, locationUri=""),
        Database(name="main", catalog="temp", description=None, locationUri=""),
    ]


def test_list_databases_pattern(duckdb_session: DuckDBSession):
    assert sorted(
        duckdb_session.catalog.listDatabases("db*"), key=lambda x: (x.catalog, x.name)
    ) == [
        Database(name="db1", catalog="default", description=None, locationUri=""),
    ]


def test_get_database_no_match(duckdb_session: DuckDBSession):
    with pytest.raises(ValueError):
        assert duckdb_session.catalog.getDatabase("nonexistent")


def test_get_database_name_only(duckdb_session: DuckDBSession):
    assert duckdb_session.catalog.getDatabase("db1") == Database(
        name="db1", catalog="default", description=None, locationUri=""
    )


def test_get_database_name_and_catalog(duckdb_session: DuckDBSession):
    assert duckdb_session.catalog.getDatabase("system.information_schema") == Database(
        name="information_schema", catalog="system", description=None, locationUri=""
    )


def test_database_exists_does_exist(duckdb_session: DuckDBSession):
    assert duckdb_session.catalog.databaseExists("db1") is True


def test_database_exists_does_not_exist(duckdb_session: DuckDBSession):
    assert duckdb_session.catalog.databaseExists("nonexistent") is False


def test_list_tables_no_args(duckdb_session: DuckDBSession):
    assert sorted(
        duckdb_session.catalog.listTables(), key=lambda x: (x.catalog, x.database, x.name)
    ) == [
        Table(
            name="table1",
            catalog="default",
            namespace=["main"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_list_tables_db_no_catalog(duckdb_session: DuckDBSession):
    assert sorted(
        duckdb_session.catalog.listTables("main"), key=lambda x: (x.catalog, x.database, x.name)
    ) == [
        Table(
            name="table1",
            catalog="default",
            namespace=["main"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_list_tables_db_and_catalog(duckdb_session: DuckDBSession):
    assert sorted(
        duckdb_session.catalog.listTables("default.main"),
        key=lambda x: (x.catalog, x.database, x.name),
    ) == [
        Table(
            name="table1",
            catalog="default",
            namespace=["main"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_list_tables_pattern(duckdb_session: DuckDBSession):
    assert sorted(
        duckdb_session.catalog.listTables(pattern="tab*"),
        key=lambda x: (x.catalog, x.database, x.name),
    ) == [
        Table(
            name="table1",
            catalog="default",
            namespace=["main"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_get_table(duckdb_session: DuckDBSession):
    assert duckdb_session.catalog.getTable("default.main.table1") == Table(
        name="table1",
        catalog="default",
        namespace=["main"],
        description=None,
        tableType="MANAGED",
        isTemporary=False,
    )


def test_get_table_not_exists(duckdb_session: DuckDBSession):
    with pytest.raises(ValueError):
        assert duckdb_session.catalog.getTable("default.main.nonexistent")


def test_list_functions(duckdb_session: DuckDBSession):
    assert (
        Function(
            name="testing",
            catalog="default",
            namespace=["main"],
            description=None,
            className="",
            isTemporary=False,
        )
        in duckdb_session.catalog.listFunctions()
    )


def test_list_functions_pattern(duckdb_session: DuckDBSession):
    assert duckdb_session.catalog.listFunctions(pattern="test*") == [
        Function(
            name="testing",
            catalog="default",
            namespace=["main"],
            description=None,
            className="",
            isTemporary=False,
        )
    ]


def test_function_exists_does_exist(duckdb_session: DuckDBSession):
    assert duckdb_session.catalog.functionExists("testing") is True


def test_function_exists_does_not_exist(duckdb_session: DuckDBSession):
    assert duckdb_session.catalog.functionExists("nonexistent") is False


def test_get_function_exists(duckdb_session: DuckDBSession):
    assert duckdb_session.catalog.getFunction("default.main.testing") == Function(
        name="testing",
        catalog="default",
        namespace=["main"],
        description=None,
        className="",
        isTemporary=False,
    )


def test_get_function_not_exists(duckdb_session: DuckDBSession):
    with pytest.raises(ValueError):
        assert duckdb_session.catalog.getFunction("default.main.nonexistent")


def test_list_columns(duckdb_session: DuckDBSession):
    assert sorted(
        duckdb_session.catalog.listColumns("default.main.table1"), key=lambda x: x.name
    ) == [
        Column(
            name="id",
            description=None,
            dataType="INT",
            nullable=True,
            isPartition=False,
            isBucket=False,
        ),
        Column(
            name="name",
            description=None,
            dataType="STRING",
            nullable=True,
            isPartition=False,
            isBucket=False,
        ),
    ]


def test_list_columns_use_db_name(duckdb_session: DuckDBSession):
    assert sorted(
        duckdb_session.catalog.listColumns("table1", dbName="default.main"), key=lambda x: x.name
    ) == [
        Column(
            name="id",
            description=None,
            dataType="INT",
            nullable=True,
            isPartition=False,
            isBucket=False,
        ),
        Column(
            name="name",
            description=None,
            dataType="STRING",
            nullable=True,
            isPartition=False,
            isBucket=False,
        ),
    ]


def test_table_exists_table_name_only(duckdb_session: DuckDBSession):
    assert duckdb_session.catalog.tableExists("default.main.table1") is True


def test_table_exists_table_name_and_db_name(duckdb_session: DuckDBSession):
    assert duckdb_session.catalog.tableExists("table1", dbName="default.main") is True


def test_table_not_exists(duckdb_session: DuckDBSession):
    assert duckdb_session.catalog.tableExists("nonexistent") is False


def test_create_external_table(duckdb_session: DuckDBSession):
    with pytest.raises(NotImplementedError):
        duckdb_session.catalog.createExternalTable("table1", "default.main", "path/to/table")


def test_create_table(duckdb_session: DuckDBSession):
    with pytest.raises(NotImplementedError):
        duckdb_session.catalog.createTable("table1", "default.main")


def test_drop_temp_view(duckdb_session: DuckDBSession):
    with pytest.raises(NotImplementedError):
        duckdb_session.catalog.dropTempView("view1")


def test_drop_global_temp_view(duckdb_session: DuckDBSession):
    with pytest.raises(NotImplementedError):
        duckdb_session.catalog.dropGlobalTempView("view1")


def test_register_function(duckdb_session: DuckDBSession):
    with pytest.raises(NotImplementedError):
        duckdb_session.catalog.registerFunction("function1", lambda x: x)


def test_is_cached(duckdb_session: DuckDBSession):
    with pytest.raises(NotImplementedError):
        duckdb_session.catalog.isCached("table1")


def test_cache_table(duckdb_session: DuckDBSession):
    with pytest.raises(NotImplementedError):
        duckdb_session.catalog.cacheTable("table1")


def test_uncache_table(duckdb_session: DuckDBSession):
    with pytest.raises(NotImplementedError):
        duckdb_session.catalog.uncacheTable("table1")


def test_clear_cache(duckdb_session: DuckDBSession):
    with pytest.raises(NotImplementedError):
        duckdb_session.catalog.clearCache()


def test_refresh_table(duckdb_session: DuckDBSession):
    with pytest.raises(NotImplementedError):
        duckdb_session.catalog.refreshTable("table1")


def test_recover_partitions(duckdb_session: DuckDBSession):
    with pytest.raises(NotImplementedError):
        duckdb_session.catalog.recoverPartitions("table1")


def test_refresh_by_path(duckdb_session: DuckDBSession):
    with pytest.raises(NotImplementedError):
        duckdb_session.catalog.refreshByPath("path/to/table")
