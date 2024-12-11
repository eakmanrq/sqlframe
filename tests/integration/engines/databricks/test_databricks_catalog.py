import typing as t

import pytest

from sqlframe.base.catalog import CatalogMetadata, Column, Database, Function, Table
from sqlframe.databricks.session import DatabricksSession

pytest_plugins = ["tests.integration.fixtures"]
pytestmark = [
    pytest.mark.databricks,
    pytest.mark.xdist_group("databricks_tests"),
]


@pytest.fixture
def reset_catalog(databricks_session: DatabricksSession) -> t.Iterator[None]:
    yield
    databricks_session.catalog.setCurrentCatalog("sqlframe")
    databricks_session.catalog.setCurrentDatabase("db1")


@pytest.fixture
def reset_database(databricks_session: DatabricksSession) -> t.Iterator[None]:
    yield
    databricks_session.catalog.setCurrentDatabase("db1")


def test_current_catalog(databricks_session: DatabricksSession):
    assert databricks_session.catalog.currentCatalog() == "sqlframe"


def test_set_current_catalog(databricks_session: DatabricksSession, reset_catalog):
    assert databricks_session.catalog.currentCatalog() == "sqlframe"
    databricks_session.catalog.setCurrentCatalog("catalog1")
    assert databricks_session.catalog.currentCatalog() == "catalog1"


def test_list_catalogs(databricks_session: DatabricksSession):
    assert sorted(databricks_session.catalog.listCatalogs(), key=lambda x: x.name) == [
        CatalogMetadata(name="sqlframe", description=None)
    ]


def test_current_database(databricks_session: DatabricksSession):
    assert databricks_session.catalog.currentDatabase() == "db1"


def test_set_current_database(databricks_session: DatabricksSession, reset_database):
    assert databricks_session.catalog.currentDatabase() == "db1"
    databricks_session.catalog.setCurrentDatabase("default")
    assert databricks_session.catalog.currentDatabase() == "default"


def test_list_databases(databricks_session: DatabricksSession):
    assert sorted(
        databricks_session.catalog.listDatabases(), key=lambda x: (x.catalog, x.name)
    ) == [
        Database(name="db1", catalog="sqlframe", description=None, locationUri=""),
        Database(name="default", catalog="sqlframe", description=None, locationUri=""),
        Database(name="information_schema", catalog="sqlframe", description=None, locationUri=""),
    ]


def test_list_databases_pattern(databricks_session: DatabricksSession):
    assert sorted(
        databricks_session.catalog.listDatabases("db*"), key=lambda x: (x.catalog, x.name)
    ) == [
        Database(name="db1", catalog="sqlframe", description=None, locationUri=""),
    ]


def test_get_database_no_match(databricks_session: DatabricksSession):
    with pytest.raises(ValueError):
        assert databricks_session.catalog.getDatabase("nonexistent")


def test_get_database_name_only(databricks_session: DatabricksSession):
    assert databricks_session.catalog.getDatabase("db1") == Database(
        name="db1", catalog="sqlframe", description=None, locationUri=""
    )


def test_get_database_name_and_catalog(databricks_session: DatabricksSession):
    assert databricks_session.catalog.getDatabase("sqlframe.db1") == Database(
        name="db1", catalog="sqlframe", description=None, locationUri=""
    )


def test_database_exists_does_exist(databricks_session: DatabricksSession):
    assert databricks_session.catalog.databaseExists("db1") is True


def test_database_exists_does_not_exist(databricks_session: DatabricksSession):
    assert databricks_session.catalog.databaseExists("nonexistent") is False


def test_list_tables_no_args(databricks_session: DatabricksSession):
    assert sorted(
        databricks_session.catalog.listTables(), key=lambda x: (x.catalog, x.database, x.name)
    ) == [
        Table(
            name="table1",
            catalog="sqlframe",
            namespace=["db1"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_list_tables_db_no_catalog(databricks_session: DatabricksSession):
    assert sorted(
        databricks_session.catalog.listTables("db1"), key=lambda x: (x.catalog, x.database, x.name)
    ) == [
        Table(
            name="table1",
            catalog="sqlframe",
            namespace=["db1"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_list_tables_db_and_catalog(databricks_session: DatabricksSession):
    assert sorted(
        databricks_session.catalog.listTables("sqlframe.db1"),
        key=lambda x: (x.catalog, x.database, x.name),
    ) == [
        Table(
            name="table1",
            catalog="sqlframe",
            namespace=["db1"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_list_tables_pattern(databricks_session: DatabricksSession):
    assert Table(
        name="table1",
        catalog="sqlframe",
        namespace=["db1"],
        description=None,
        tableType="MANAGED",
        isTemporary=False,
    ) in databricks_session.catalog.listTables(pattern="tab*")


def test_get_table(databricks_session: DatabricksSession):
    assert databricks_session.catalog.getTable("sqlframe.db1.table1") == Table(
        name="table1",
        catalog="sqlframe",
        namespace=["db1"],
        description=None,
        tableType="MANAGED",
        isTemporary=False,
    )


def test_get_table_not_exists(databricks_session: DatabricksSession):
    with pytest.raises(ValueError):
        assert databricks_session.catalog.getTable("dev.db1.nonexistent")


def test_list_functions(databricks_session: DatabricksSession):
    assert databricks_session.catalog.listFunctions() == [
        Function(
            name="add",
            catalog="sqlframe",
            namespace=["db1"],
            description=None,
            className="",
            isTemporary=False,
        )
    ]


def test_list_functions_pattern(databricks_session: DatabricksSession):
    assert databricks_session.catalog.listFunctions(dbName="db1", pattern="ad*") == [
        Function(
            name="add",
            catalog="sqlframe",
            namespace=["db1"],
            description=None,
            className="",
            isTemporary=False,
        )
    ]


def test_function_exists_does_exist(databricks_session: DatabricksSession):
    assert databricks_session.catalog.functionExists("add", dbName="sqlframe.db1") is True


def test_function_exists_does_not_exist(databricks_session: DatabricksSession):
    assert databricks_session.catalog.functionExists("nonexistent") is False


def test_get_function_exists(databricks_session: DatabricksSession):
    assert databricks_session.catalog.getFunction("sqlframe.db1.add") == Function(
        name="add",
        catalog="sqlframe",
        namespace=["db1"],
        description=None,
        className="",
        isTemporary=False,
    )


def test_get_function_not_exists(databricks_session: DatabricksSession):
    with pytest.raises(ValueError):
        assert databricks_session.catalog.getFunction("sqlframe.db1.nonexistent")


def test_list_columns(databricks_session: DatabricksSession):
    assert sorted(
        databricks_session.catalog.listColumns("sqlframe.db1.table1"), key=lambda x: x.name
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
            dataType="VARCHAR(100)",
            nullable=True,
            isPartition=False,
            isBucket=False,
        ),
    ]


def test_list_columns_use_db_name(databricks_session: DatabricksSession):
    assert sorted(
        databricks_session.catalog.listColumns("table1", dbName="sqlframe.db1"),
        key=lambda x: x.name,
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
            dataType="VARCHAR(100)",
            nullable=True,
            isPartition=False,
            isBucket=False,
        ),
    ]


def test_table_exists_table_name_only(databricks_session: DatabricksSession):
    assert databricks_session.catalog.tableExists("sqlframe.db1.table1") is True


def test_table_exists_table_name_and_db_name(databricks_session: DatabricksSession):
    assert databricks_session.catalog.tableExists("table1", dbName="sqlframe.db1") is True


def test_table_not_exists(databricks_session: DatabricksSession):
    assert databricks_session.catalog.tableExists("nonexistent") is False


def test_create_external_table(databricks_session: DatabricksSession):
    with pytest.raises(NotImplementedError):
        databricks_session.catalog.createExternalTable(
            "table1", "sqlframe.default", "path/to/table"
        )


def test_create_table(databricks_session: DatabricksSession):
    with pytest.raises(NotImplementedError):
        databricks_session.catalog.createTable("table1", "sqlframe.default")


def test_drop_temp_view(databricks_session: DatabricksSession):
    with pytest.raises(NotImplementedError):
        databricks_session.catalog.dropTempView("view1")


def test_drop_global_temp_view(databricks_session: DatabricksSession):
    with pytest.raises(NotImplementedError):
        databricks_session.catalog.dropGlobalTempView("view1")


def test_register_function(databricks_session: DatabricksSession):
    with pytest.raises(NotImplementedError):
        databricks_session.catalog.registerFunction("function1", lambda x: x)


def test_is_cached(databricks_session: DatabricksSession):
    with pytest.raises(NotImplementedError):
        databricks_session.catalog.isCached("table1")


def test_cache_table(databricks_session: DatabricksSession):
    with pytest.raises(NotImplementedError):
        databricks_session.catalog.cacheTable("table1")


def test_uncache_table(databricks_session: DatabricksSession):
    with pytest.raises(NotImplementedError):
        databricks_session.catalog.uncacheTable("table1")


def test_clear_cache(databricks_session: DatabricksSession):
    with pytest.raises(NotImplementedError):
        databricks_session.catalog.clearCache()


def test_refresh_table(databricks_session: DatabricksSession):
    with pytest.raises(NotImplementedError):
        databricks_session.catalog.refreshTable("table1")


def test_recover_partitions(databricks_session: DatabricksSession):
    with pytest.raises(NotImplementedError):
        databricks_session.catalog.recoverPartitions("table1")


def test_refresh_by_path(databricks_session: DatabricksSession):
    with pytest.raises(NotImplementedError):
        databricks_session.catalog.refreshByPath("path/to/table")
