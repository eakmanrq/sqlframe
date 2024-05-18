import typing as t

import pytest

from sqlframe.base.catalog import CatalogMetadata, Column, Database, Function, Table
from sqlframe.snowflake.session import SnowflakeSession

pytest_plugins = ["tests.integration.fixtures"]
pytestmark = [
    pytest.mark.snowflake,
    pytest.mark.xdist_group("snowflake_tests"),
]


@pytest.fixture
def reset_catalog(snowflake_session: SnowflakeSession) -> t.Iterator[None]:
    yield
    snowflake_session.catalog.setCurrentCatalog("sqlframe")
    snowflake_session.catalog.setCurrentDatabase("db1")


@pytest.fixture
def reset_database(snowflake_session: SnowflakeSession) -> t.Iterator[None]:
    yield
    snowflake_session.catalog.setCurrentDatabase("db1")


def test_current_catalog(snowflake_session: SnowflakeSession):
    assert snowflake_session.catalog.currentCatalog() == "SQLFRAME"


def test_set_current_catalog(snowflake_session: SnowflakeSession, reset_catalog):
    assert snowflake_session.catalog.currentCatalog() == "SQLFRAME"
    snowflake_session.catalog.setCurrentCatalog("catalog1")
    assert snowflake_session.catalog.currentCatalog() == "CATALOG1"


def test_list_catalogs(snowflake_session: SnowflakeSession):
    assert sorted(snowflake_session.catalog.listCatalogs(), key=lambda x: x.name) == [
        CatalogMetadata(name="SQLFRAME", description=None)
    ]


def test_current_database(snowflake_session: SnowflakeSession):
    assert snowflake_session.catalog.currentDatabase() == "DB1"


def test_set_current_database(snowflake_session: SnowflakeSession, reset_database):
    assert snowflake_session.catalog.currentDatabase() == "DB1"
    snowflake_session.catalog.setCurrentDatabase("public")
    assert snowflake_session.catalog.currentDatabase() == "PUBLIC"


def test_list_databases(snowflake_session: SnowflakeSession):
    assert sorted(snowflake_session.catalog.listDatabases(), key=lambda x: (x.catalog, x.name)) == [
        Database(name="DB1", catalog="SQLFRAME", description=None, locationUri=""),
        Database(name="INFORMATION_SCHEMA", catalog="SQLFRAME", description=None, locationUri=""),
        Database(name="PUBLIC", catalog="SQLFRAME", description=None, locationUri=""),
    ]


def test_list_databases_pattern(snowflake_session: SnowflakeSession):
    assert sorted(
        snowflake_session.catalog.listDatabases("DB*"), key=lambda x: (x.catalog, x.name)
    ) == [
        Database(name="DB1", catalog="SQLFRAME", description=None, locationUri=""),
    ]


def test_get_database_no_match(snowflake_session: SnowflakeSession):
    with pytest.raises(ValueError):
        assert snowflake_session.catalog.getDatabase("nonexistent")


def test_get_database_name_only(snowflake_session: SnowflakeSession):
    assert snowflake_session.catalog.getDatabase("db1") == Database(
        name="DB1", catalog="SQLFRAME", description=None, locationUri=""
    )


def test_get_database_name_and_catalog(snowflake_session: SnowflakeSession):
    assert snowflake_session.catalog.getDatabase("sqlframe.db1") == Database(
        name="DB1", catalog="SQLFRAME", description=None, locationUri=""
    )


def test_database_exists_does_exist(snowflake_session: SnowflakeSession):
    assert snowflake_session.catalog.databaseExists("db1") is True


def test_database_exists_does_not_exist(snowflake_session: SnowflakeSession):
    assert snowflake_session.catalog.databaseExists("nonexistent") is False


def test_list_tables_no_args(snowflake_session: SnowflakeSession):
    assert sorted(
        snowflake_session.catalog.listTables(), key=lambda x: (x.catalog, x.database, x.name)
    ) == [
        Table(
            name="TABLE1",
            catalog="SQLFRAME",
            namespace=["DB1"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_list_tables_db_no_catalog(snowflake_session: SnowflakeSession):
    assert sorted(
        snowflake_session.catalog.listTables("db1"), key=lambda x: (x.catalog, x.database, x.name)
    ) == [
        Table(
            name="TABLE1",
            catalog="SQLFRAME",
            namespace=["DB1"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_list_tables_db_and_catalog(snowflake_session: SnowflakeSession):
    assert sorted(
        snowflake_session.catalog.listTables("sqlframe.db1"),
        key=lambda x: (x.catalog, x.database, x.name),
    ) == [
        Table(
            name="TABLE1",
            catalog="SQLFRAME",
            namespace=["DB1"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_list_tables_pattern(snowflake_session: SnowflakeSession):
    assert Table(
        name="TABLE1",
        catalog="SQLFRAME",
        namespace=["DB1"],
        description=None,
        tableType="MANAGED",
        isTemporary=False,
    ) in snowflake_session.catalog.listTables(pattern="TAB*")


def test_get_table(snowflake_session: SnowflakeSession):
    assert snowflake_session.catalog.getTable("sqlframe.db1.table1") == Table(
        name="TABLE1",
        catalog="SQLFRAME",
        namespace=["DB1"],
        description=None,
        tableType="MANAGED",
        isTemporary=False,
    )


def test_get_table_not_exists(snowflake_session: SnowflakeSession):
    with pytest.raises(ValueError):
        assert snowflake_session.catalog.getTable("dev.db1.nonexistent")


def test_list_functions(snowflake_session: SnowflakeSession):
    assert snowflake_session.catalog.listFunctions() == [
        Function(
            name="ADD",
            catalog="SQLFRAME",
            namespace=["DB1"],
            description=None,
            className="",
            isTemporary=False,
        )
    ]


def test_list_functions_pattern(snowflake_session: SnowflakeSession):
    assert snowflake_session.catalog.listFunctions(dbName="DB1", pattern="AD*") == [
        Function(
            name="ADD",
            catalog="SQLFRAME",
            namespace=["DB1"],
            description=None,
            className="",
            isTemporary=False,
        )
    ]


def test_function_exists_does_exist(snowflake_session: SnowflakeSession):
    assert snowflake_session.catalog.functionExists("ADD", dbName="SQLFRAME.DB1") is True


def test_function_exists_does_not_exist(snowflake_session: SnowflakeSession):
    assert snowflake_session.catalog.functionExists("nonexistent") is False


def test_get_function_exists(snowflake_session: SnowflakeSession):
    assert snowflake_session.catalog.getFunction("SQLFRAME.DB1.ADD") == Function(
        name="ADD",
        catalog="SQLFRAME",
        namespace=["DB1"],
        description=None,
        className="",
        isTemporary=False,
    )


def test_get_function_not_exists(snowflake_session: SnowflakeSession):
    with pytest.raises(ValueError):
        assert snowflake_session.catalog.getFunction("sqlframe.db1.nonexistent")


def test_list_columns(snowflake_session: SnowflakeSession):
    assert sorted(
        snowflake_session.catalog.listColumns("sqlframe.db1.table1"), key=lambda x: x.name
    ) == [
        Column(
            name="ID",
            description=None,
            dataType="NUMBER",
            nullable=True,
            isPartition=False,
            isBucket=False,
        ),
        Column(
            name="NAME",
            description=None,
            dataType="TEXT",
            nullable=True,
            isPartition=False,
            isBucket=False,
        ),
    ]


def test_list_columns_use_db_name(snowflake_session: SnowflakeSession):
    assert sorted(
        snowflake_session.catalog.listColumns("table1", dbName="sqlframe.db1"), key=lambda x: x.name
    ) == [
        Column(
            name="ID",
            description=None,
            dataType="NUMBER",
            nullable=True,
            isPartition=False,
            isBucket=False,
        ),
        Column(
            name="NAME",
            description=None,
            dataType="TEXT",
            nullable=True,
            isPartition=False,
            isBucket=False,
        ),
    ]


def test_table_exists_table_name_only(snowflake_session: SnowflakeSession):
    assert snowflake_session.catalog.tableExists("sqlframe.db1.table1") is True


def test_table_exists_table_name_and_db_name(snowflake_session: SnowflakeSession):
    assert snowflake_session.catalog.tableExists("table1", dbName="sqlframe.db1") is True


def test_table_not_exists(snowflake_session: SnowflakeSession):
    assert snowflake_session.catalog.tableExists("nonexistent") is False


def test_create_external_table(snowflake_session: SnowflakeSession):
    with pytest.raises(NotImplementedError):
        snowflake_session.catalog.createExternalTable("table1", "tests.public", "path/to/table")


def test_create_table(snowflake_session: SnowflakeSession):
    with pytest.raises(NotImplementedError):
        snowflake_session.catalog.createTable("table1", "tests.public")


def test_drop_temp_view(snowflake_session: SnowflakeSession):
    with pytest.raises(NotImplementedError):
        snowflake_session.catalog.dropTempView("view1")


def test_drop_global_temp_view(snowflake_session: SnowflakeSession):
    with pytest.raises(NotImplementedError):
        snowflake_session.catalog.dropGlobalTempView("view1")


def test_register_function(snowflake_session: SnowflakeSession):
    with pytest.raises(NotImplementedError):
        snowflake_session.catalog.registerFunction("function1", lambda x: x)


def test_is_cached(snowflake_session: SnowflakeSession):
    with pytest.raises(NotImplementedError):
        snowflake_session.catalog.isCached("table1")


def test_cache_table(snowflake_session: SnowflakeSession):
    with pytest.raises(NotImplementedError):
        snowflake_session.catalog.cacheTable("table1")


def test_uncache_table(snowflake_session: SnowflakeSession):
    with pytest.raises(NotImplementedError):
        snowflake_session.catalog.uncacheTable("table1")


def test_clear_cache(snowflake_session: SnowflakeSession):
    with pytest.raises(NotImplementedError):
        snowflake_session.catalog.clearCache()


def test_refresh_table(snowflake_session: SnowflakeSession):
    with pytest.raises(NotImplementedError):
        snowflake_session.catalog.refreshTable("table1")


def test_recover_partitions(snowflake_session: SnowflakeSession):
    with pytest.raises(NotImplementedError):
        snowflake_session.catalog.recoverPartitions("table1")


def test_refresh_by_path(snowflake_session: SnowflakeSession):
    with pytest.raises(NotImplementedError):
        snowflake_session.catalog.refreshByPath("path/to/table")
