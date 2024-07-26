import typing as t

import pytest

from sqlframe.base.catalog import CatalogMetadata, Column, Database, Function, Table
from sqlframe.redshift.session import RedshiftSession

pytest_plugins = ["tests.integration.fixtures"]
pytestmark = [
    pytest.mark.redshift,
    pytest.mark.xdist_group("redshift_tests"),
]


@pytest.fixture
def reset_database(redshift_session: RedshiftSession) -> t.Iterator[None]:
    yield
    redshift_session.catalog.setCurrentDatabase("public")


def test_current_catalog(redshift_session: RedshiftSession):
    assert redshift_session.catalog.currentCatalog() == "dev"


def test_set_current_catalog(redshift_session: RedshiftSession):
    with pytest.raises(NotImplementedError):
        redshift_session.catalog.setCurrentCatalog("catalog1")


def test_list_catalogs(redshift_session: RedshiftSession):
    assert sorted(redshift_session.catalog.listCatalogs(), key=lambda x: x.name) == [
        CatalogMetadata(name="dev", description=None)
    ]


def test_current_database(redshift_session: RedshiftSession):
    assert redshift_session.catalog.currentDatabase() == "public"


def test_set_current_database(redshift_session: RedshiftSession, reset_database):
    assert redshift_session.catalog.currentDatabase() == "public"
    redshift_session.catalog.setCurrentDatabase("db1")
    assert redshift_session.catalog.currentDatabase() == "db1"


def test_list_databases(redshift_session: RedshiftSession):
    assert sorted(redshift_session.catalog.listDatabases(), key=lambda x: (x.catalog, x.name)) == [
        Database(name="db1", catalog="dev", description=None, locationUri=""),
    ]


def test_list_databases_pattern(redshift_session: RedshiftSession):
    assert sorted(
        redshift_session.catalog.listDatabases("db*"), key=lambda x: (x.catalog, x.name)
    ) == [
        Database(name="db1", catalog="dev", description=None, locationUri=""),
    ]


def test_get_database_no_match(redshift_session: RedshiftSession):
    with pytest.raises(ValueError):
        assert redshift_session.catalog.getDatabase("nonexistent")


def test_get_database_name_only(redshift_session: RedshiftSession):
    assert redshift_session.catalog.getDatabase("db1") == Database(
        name="db1", catalog="dev", description=None, locationUri=""
    )


def test_get_database_name_and_catalog(redshift_session: RedshiftSession):
    assert redshift_session.catalog.getDatabase("catalog1.db1") == Database(
        name="db1", catalog="dev", description=None, locationUri=""
    )


def test_database_exists_does_exist(redshift_session: RedshiftSession):
    assert redshift_session.catalog.databaseExists("db1") is True


def test_database_exists_does_not_exist(redshift_session: RedshiftSession):
    assert redshift_session.catalog.databaseExists("nonexistent") is False


def test_list_tables_no_args(redshift_session: RedshiftSession):
    # no tables are returns since default schema is public
    assert (
        sorted(redshift_session.catalog.listTables(), key=lambda x: (x.catalog, x.database, x.name))
        == []
    )


def test_list_tables_db_no_catalog(redshift_session: RedshiftSession):
    assert sorted(
        redshift_session.catalog.listTables("db1"), key=lambda x: (x.catalog, x.database, x.name)
    ) == [
        Table(
            name="table1",
            catalog="dev",
            namespace=["db1"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_list_tables_db_and_catalog(redshift_session: RedshiftSession):
    assert sorted(
        redshift_session.catalog.listTables("dev.db1"),
        key=lambda x: (x.catalog, x.database, x.name),
    ) == [
        Table(
            name="table1",
            catalog="dev",
            namespace=["db1"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_list_tables_pattern(redshift_session: RedshiftSession):
    assert Table(
        name="table1",
        catalog="dev",
        namespace=["db1"],
        description=None,
        tableType="MANAGED",
        isTemporary=False,
    ) in redshift_session.catalog.listTables(pattern="tab*")


def test_get_table(redshift_session: RedshiftSession):
    assert redshift_session.catalog.getTable("dev.db1.table1") == Table(
        name="table1",
        catalog="dev",
        namespace=["db1"],
        description=None,
        tableType="MANAGED",
        isTemporary=False,
    )


def test_get_table_not_exists(redshift_session: RedshiftSession):
    with pytest.raises(ValueError):
        assert redshift_session.catalog.getTable("dev.db1.nonexistent")


def test_list_functions(redshift_session: RedshiftSession):
    # no functions are returned since default schema is public
    assert redshift_session.catalog.listFunctions() == []


def test_list_functions_pattern(redshift_session: RedshiftSession):
    assert redshift_session.catalog.listFunctions(dbName="db1", pattern="ad*") == [
        Function(
            name="add",
            catalog="dev",
            namespace=["db1"],
            description=None,
            className="",
            isTemporary=False,
        )
    ]


def test_function_exists_does_exist(redshift_session: RedshiftSession):
    assert redshift_session.catalog.functionExists("add", dbName="dev.db1") is True


def test_function_exists_does_not_exist(redshift_session: RedshiftSession):
    assert redshift_session.catalog.functionExists("nonexistent") is False


def test_get_function_exists(redshift_session: RedshiftSession):
    assert redshift_session.catalog.getFunction("dev.db1.add") == Function(
        name="add",
        catalog="dev",
        namespace=["db1"],
        description=None,
        className="",
        isTemporary=False,
    )


def test_get_function_not_exists(redshift_session: RedshiftSession):
    with pytest.raises(ValueError):
        assert redshift_session.catalog.getFunction("default.main.nonexistent")


def test_list_columns(redshift_session: RedshiftSession):
    assert sorted(redshift_session.catalog.listColumns("dev.db1.table1"), key=lambda x: x.name) == [
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


def test_list_columns_use_db_name(redshift_session: RedshiftSession):
    assert sorted(
        redshift_session.catalog.listColumns("table1", dbName="dev.db1"), key=lambda x: x.name
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


def test_table_exists_table_name_only(redshift_session: RedshiftSession):
    assert redshift_session.catalog.tableExists("dev.db1.table1") is True


def test_table_exists_table_name_and_db_name(redshift_session: RedshiftSession):
    assert redshift_session.catalog.tableExists("table1", dbName="dev.db1") is True


def test_table_not_exists(redshift_session: RedshiftSession):
    assert redshift_session.catalog.tableExists("nonexistent") is False


def test_create_external_table(redshift_session: RedshiftSession):
    with pytest.raises(NotImplementedError):
        redshift_session.catalog.createExternalTable("table1", "tests.public", "path/to/table")


def test_create_table(redshift_session: RedshiftSession):
    with pytest.raises(NotImplementedError):
        redshift_session.catalog.createTable("table1", "tests.public")


def test_drop_temp_view(redshift_session: RedshiftSession):
    with pytest.raises(NotImplementedError):
        redshift_session.catalog.dropTempView("view1")


def test_drop_global_temp_view(redshift_session: RedshiftSession):
    with pytest.raises(NotImplementedError):
        redshift_session.catalog.dropGlobalTempView("view1")


def test_register_function(redshift_session: RedshiftSession):
    with pytest.raises(NotImplementedError):
        redshift_session.catalog.registerFunction("function1", lambda x: x)


def test_is_cached(redshift_session: RedshiftSession):
    with pytest.raises(NotImplementedError):
        redshift_session.catalog.isCached("table1")


def test_cache_table(redshift_session: RedshiftSession):
    with pytest.raises(NotImplementedError):
        redshift_session.catalog.cacheTable("table1")


def test_uncache_table(redshift_session: RedshiftSession):
    with pytest.raises(NotImplementedError):
        redshift_session.catalog.uncacheTable("table1")


def test_clear_cache(redshift_session: RedshiftSession):
    with pytest.raises(NotImplementedError):
        redshift_session.catalog.clearCache()


def test_refresh_table(redshift_session: RedshiftSession):
    with pytest.raises(NotImplementedError):
        redshift_session.catalog.refreshTable("table1")


def test_recover_partitions(redshift_session: RedshiftSession):
    with pytest.raises(NotImplementedError):
        redshift_session.catalog.recoverPartitions("table1")


def test_refresh_by_path(redshift_session: RedshiftSession):
    with pytest.raises(NotImplementedError):
        redshift_session.catalog.refreshByPath("path/to/table")
