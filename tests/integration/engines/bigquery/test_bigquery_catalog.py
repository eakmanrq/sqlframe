import typing as t

import pytest

from sqlframe.base.catalog import CatalogMetadata, Column, Database, Function, Table
from sqlframe.bigquery.session import BigQuerySession

pytest_plugins = ["tests.integration.fixtures"]
pytestmark = [
    pytest.mark.bigquery,
    pytest.mark.xdist_group("bigquery_tests"),
]


@pytest.fixture
def reset_catalog(bigquery_session: BigQuerySession) -> t.Iterator[None]:
    yield
    bigquery_session.catalog.setCurrentCatalog("sqlframe")


@pytest.fixture
def reset_database(bigquery_session: BigQuerySession) -> t.Iterator[None]:
    yield
    bigquery_session.catalog.setCurrentDatabase("sqlframe.db1")


def test_current_catalog(bigquery_session: BigQuerySession):
    assert bigquery_session.catalog.currentCatalog() == "sqlframe"


def test_set_current_catalog(bigquery_session: BigQuerySession, reset_catalog: t.Iterator[None]):
    assert bigquery_session.catalog.currentCatalog() == "sqlframe"
    bigquery_session.catalog.setCurrentCatalog("catalog1")
    assert bigquery_session.catalog.currentCatalog() == "catalog1"


def test_list_catalogs(bigquery_session: BigQuerySession):
    assert bigquery_session.catalog.listCatalogs() == [
        CatalogMetadata(name="sqlframe", description=None)
    ]


def test_current_database(bigquery_session: BigQuerySession):
    assert bigquery_session.catalog.currentDatabase() == "db1"


def test_set_current_database(bigquery_session: BigQuerySession, reset_database: t.Iterator[None]):
    assert bigquery_session.catalog.currentDatabase() == "db1"
    bigquery_session.catalog.setCurrentDatabase("sqlframe.db2")
    assert bigquery_session.catalog.currentDatabase() == "db2"


def test_list_databases(bigquery_session: BigQuerySession):
    assert sorted(bigquery_session.catalog.listDatabases(), key=lambda x: (x.catalog, x.name)) == [
        Database(name="db1", catalog="sqlframe", description=None, locationUri=""),
    ]


def test_list_databases_pattern(bigquery_session: BigQuerySession):
    assert sorted(
        bigquery_session.catalog.listDatabases("db*"), key=lambda x: (x.catalog, x.name)
    ) == [
        Database(name="db1", catalog="sqlframe", description=None, locationUri=""),
    ]


def test_get_database_no_match(bigquery_session: BigQuerySession):
    with pytest.raises(ValueError):
        assert bigquery_session.catalog.getDatabase("nonexistent")


def test_get_database_name_only(bigquery_session: BigQuerySession):
    assert bigquery_session.catalog.getDatabase("db1") == Database(
        name="db1", catalog="sqlframe", description=None, locationUri=""
    )


def test_get_database_name_and_catalog(bigquery_session: BigQuerySession):
    assert bigquery_session.catalog.getDatabase("db1") == Database(
        name="db1", catalog="sqlframe", description=None, locationUri=""
    )


def test_database_exists_does_exist(bigquery_session: BigQuerySession):
    assert bigquery_session.catalog.databaseExists("db1") is True


def test_database_exists_does_not_exist(bigquery_session: BigQuerySession):
    assert bigquery_session.catalog.databaseExists("nonexistent") is False


def test_list_tables_no_args(bigquery_session: BigQuerySession):
    assert sorted(
        bigquery_session.catalog.listTables(), key=lambda x: (x.catalog, x.database, x.name)
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


def test_list_tables_db_no_catalog(bigquery_session: BigQuerySession):
    assert sorted(
        bigquery_session.catalog.listTables("db1"), key=lambda x: (x.catalog, x.database, x.name)
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


def test_list_tables_db_and_catalog(bigquery_session: BigQuerySession):
    assert sorted(
        bigquery_session.catalog.listTables("sqlframe.db1"),
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


def test_list_tables_pattern(bigquery_session: BigQuerySession):
    assert bigquery_session.catalog.listTables(pattern="tab*") == [
        Table(
            name="table1",
            catalog="sqlframe",
            namespace=["db1"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_get_table(bigquery_session: BigQuerySession):
    assert bigquery_session.catalog.getTable("table1") == Table(
        name="table1",
        catalog="sqlframe",
        namespace=["db1"],
        description=None,
        tableType="MANAGED",
        isTemporary=False,
    )


def test_get_table_not_exists(bigquery_session: BigQuerySession):
    with pytest.raises(ValueError):
        assert bigquery_session.catalog.getTable("nonexistent")


def test_list_functions(bigquery_session: BigQuerySession):
    assert bigquery_session.catalog.listFunctions() == [
        Function(
            name="add",
            catalog="sqlframe",
            namespace=["db1"],
            description=None,
            className="",
            isTemporary=False,
        )
    ]


def test_list_functions_pattern(bigquery_session: BigQuerySession):
    assert bigquery_session.catalog.listFunctions(pattern="a*") == [
        Function(
            name="add",
            catalog="sqlframe",
            namespace=["db1"],
            description=None,
            className="",
            isTemporary=False,
        )
    ]


def test_list_functions_db_name_and_pattern(bigquery_session: BigQuerySession):
    assert bigquery_session.catalog.listFunctions("db1", pattern="a*") == [
        Function(
            name="add",
            catalog="sqlframe",
            namespace=["db1"],
            description=None,
            className="",
            isTemporary=False,
        )
    ]


def test_function_exists_does_exist(bigquery_session: BigQuerySession):
    assert bigquery_session.catalog.functionExists("add") is True


def test_function_exists_does_not_exist(bigquery_session: BigQuerySession):
    assert bigquery_session.catalog.functionExists("nonexistent") is False


def test_get_function_exists(bigquery_session: BigQuerySession):
    assert bigquery_session.catalog.getFunction("sqlframe.db1.add") == Function(
        name="add",
        catalog="sqlframe",
        namespace=["db1"],
        description=None,
        className="",
        isTemporary=False,
    )


def test_get_function_not_exists(bigquery_session: BigQuerySession):
    with pytest.raises(ValueError):
        assert bigquery_session.catalog.getFunction("sqlframe.db1.nonexistent")


def test_list_columns(bigquery_session: BigQuerySession):
    assert sorted(bigquery_session.catalog.listColumns("table1"), key=lambda x: x.name) == [
        Column(
            name="id",
            description=None,
            dataType="BIGINT",
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


def test_list_columns_use_db_name(bigquery_session: BigQuerySession):
    assert sorted(
        bigquery_session.catalog.listColumns("table1", dbName="sqlframe.db1"), key=lambda x: x.name
    ) == [
        Column(
            name="id",
            description=None,
            dataType="BIGINT",
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


def test_table_exists_table_name_only(bigquery_session: BigQuerySession):
    assert bigquery_session.catalog.tableExists("sqlframe.db1.table1") is True


def test_table_exists_table_name_and_db_name(bigquery_session: BigQuerySession):
    assert bigquery_session.catalog.tableExists("table1", dbName="sqlframe.db1") is True


def test_table_not_exists(bigquery_session: BigQuerySession):
    assert bigquery_session.catalog.tableExists("nonexistent", dbName="sqlframe.db1") is False


def test_create_external_table(bigquery_session: BigQuerySession):
    with pytest.raises(NotImplementedError):
        bigquery_session.catalog.createExternalTable("table1", "sqlframe.db1", "path/to/table")


def test_create_table(bigquery_session: BigQuerySession):
    with pytest.raises(NotImplementedError):
        bigquery_session.catalog.createTable("table1", "sqlframe.db1")


def test_drop_temp_view(bigquery_session: BigQuerySession):
    with pytest.raises(NotImplementedError):
        bigquery_session.catalog.dropTempView("view1")


def test_drop_global_temp_view(bigquery_session: BigQuerySession):
    with pytest.raises(NotImplementedError):
        bigquery_session.catalog.dropGlobalTempView("view1")


def test_register_function(bigquery_session: BigQuerySession):
    with pytest.raises(NotImplementedError):
        bigquery_session.catalog.registerFunction("function1", lambda x: x)


def test_is_cached(bigquery_session: BigQuerySession):
    with pytest.raises(NotImplementedError):
        bigquery_session.catalog.isCached("table1")


def test_cache_table(bigquery_session: BigQuerySession):
    with pytest.raises(NotImplementedError):
        bigquery_session.catalog.cacheTable("table1")


def test_uncache_table(bigquery_session: BigQuerySession):
    with pytest.raises(NotImplementedError):
        bigquery_session.catalog.uncacheTable("table1")


def test_clear_cache(bigquery_session: BigQuerySession):
    with pytest.raises(NotImplementedError):
        bigquery_session.catalog.clearCache()


def test_refresh_table(bigquery_session: BigQuerySession):
    with pytest.raises(NotImplementedError):
        bigquery_session.catalog.refreshTable("table1")


def test_recover_partitions(bigquery_session: BigQuerySession):
    with pytest.raises(NotImplementedError):
        bigquery_session.catalog.recoverPartitions("table1")


def test_refresh_by_path(bigquery_session: BigQuerySession):
    with pytest.raises(NotImplementedError):
        bigquery_session.catalog.refreshByPath("path/to/table")
