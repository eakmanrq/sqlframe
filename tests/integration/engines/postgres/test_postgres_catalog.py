import pytest

from sqlframe.base.catalog import CatalogMetadata, Column, Database, Function, Table
from sqlframe.postgres.session import PostgresSession

pytest_plugins = ["tests.integration.fixtures"]


def test_current_catalog(postgres_session: PostgresSession):
    assert postgres_session.catalog.currentCatalog() == "tests"


def test_set_current_catalog(postgres_session: PostgresSession):
    with pytest.raises(NotImplementedError):
        postgres_session.catalog.setCurrentCatalog("catalog1")


def test_list_catalogs(postgres_session: PostgresSession):
    assert sorted(postgres_session.catalog.listCatalogs(), key=lambda x: x.name) == [
        CatalogMetadata(name="tests", description=None)
    ]


def test_current_database(postgres_session: PostgresSession):
    assert postgres_session.catalog.currentDatabase() == "public"


def test_set_current_database(postgres_session: PostgresSession):
    assert postgres_session.catalog.currentDatabase() == "public"
    postgres_session.catalog.setCurrentDatabase("db1")
    assert postgres_session.catalog.currentDatabase() == "db1"


def test_list_databases_pattern(postgres_session: PostgresSession):
    assert sorted(
        postgres_session.catalog.listDatabases("db*"), key=lambda x: (x.catalog, x.name)
    ) == [
        Database(name="db1", catalog="tests", description=None, locationUri=""),
    ]


def test_get_database_no_match(postgres_session: PostgresSession):
    with pytest.raises(ValueError):
        assert postgres_session.catalog.getDatabase("nonexistent")


def test_get_database_name_only(postgres_session: PostgresSession):
    assert postgres_session.catalog.getDatabase("db1") == Database(
        name="db1", catalog="tests", description=None, locationUri=""
    )


def test_get_database_name_and_catalog(postgres_session: PostgresSession):
    # catalog1 is ignored
    assert postgres_session.catalog.getDatabase("catalog1.information_schema") == Database(
        name="information_schema", catalog="tests", description=None, locationUri=""
    )


def test_database_exists_does_exist(postgres_session: PostgresSession):
    assert postgres_session.catalog.databaseExists("db1") is True


def test_database_exists_does_not_exist(postgres_session: PostgresSession):
    assert postgres_session.catalog.databaseExists("nonexistent") is False


def test_list_tables_no_args(postgres_session: PostgresSession):
    assert sorted(
        postgres_session.catalog.listTables(), key=lambda x: (x.catalog, x.database, x.name)
    ) == [
        Table(
            name="table1",
            catalog="tests",
            namespace=["public"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_list_tables_db_no_catalog(postgres_session: PostgresSession):
    assert sorted(
        postgres_session.catalog.listTables("public"), key=lambda x: (x.catalog, x.database, x.name)
    ) == [
        Table(
            name="table1",
            catalog="tests",
            namespace=["public"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_list_tables_db_and_catalog(postgres_session: PostgresSession):
    assert sorted(
        postgres_session.catalog.listTables("tests.public"),
        key=lambda x: (x.catalog, x.database, x.name),
    ) == [
        Table(
            name="table1",
            catalog="tests",
            namespace=["public"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_list_tables_pattern(postgres_session: PostgresSession):
    assert Table(
        name="table1",
        catalog="tests",
        namespace=["public"],
        description=None,
        tableType="MANAGED",
        isTemporary=False,
    ) in postgres_session.catalog.listTables(pattern="tab*")


def test_get_table(postgres_session: PostgresSession):
    assert postgres_session.catalog.getTable("tests.public.table1") == Table(
        name="table1",
        catalog="tests",
        namespace=["public"],
        description=None,
        tableType="MANAGED",
        isTemporary=False,
    )


def test_get_table_not_exists(postgres_session: PostgresSession):
    with pytest.raises(ValueError):
        assert postgres_session.catalog.getTable("tests.public.nonexistent")


def test_list_functions(postgres_session: PostgresSession):
    assert (
        Function(
            name="testing",
            catalog="tests",
            namespace=["public"],
            description=None,
            className="",
            isTemporary=False,
        )
        in postgres_session.catalog.listFunctions()
    )


def test_list_functions_pattern(postgres_session: PostgresSession):
    assert postgres_session.catalog.listFunctions(pattern="test*") == [
        Function(
            name="testing",
            catalog="tests",
            namespace=["public"],
            description=None,
            className="",
            isTemporary=False,
        )
    ]


def test_function_exists_does_exist(postgres_session: PostgresSession):
    assert postgres_session.catalog.functionExists("testing") is True


def test_function_exists_does_not_exist(postgres_session: PostgresSession):
    assert postgres_session.catalog.functionExists("nonexistent") is False


def test_get_function_exists(postgres_session: PostgresSession):
    assert postgres_session.catalog.getFunction("tests.public.testing") == Function(
        name="testing",
        catalog="tests",
        namespace=["public"],
        description=None,
        className="",
        isTemporary=False,
    )


def test_get_function_not_exists(postgres_session: PostgresSession):
    with pytest.raises(ValueError):
        assert postgres_session.catalog.getFunction("default.main.nonexistent")


def test_list_columns(postgres_session: PostgresSession):
    assert sorted(
        postgres_session.catalog.listColumns("tests.public.table1"), key=lambda x: x.name
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


def test_list_columns_use_db_name(postgres_session: PostgresSession):
    assert sorted(
        postgres_session.catalog.listColumns("table1", dbName="tests.public"), key=lambda x: x.name
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


def test_table_exists_table_name_only(postgres_session: PostgresSession):
    assert postgres_session.catalog.tableExists("tests.public.table1") is True


def test_table_exists_table_name_and_db_name(postgres_session: PostgresSession):
    assert postgres_session.catalog.tableExists("table1", dbName="tests.public") is True


def test_table_not_exists(postgres_session: PostgresSession):
    assert postgres_session.catalog.tableExists("nonexistent") is False


def test_create_external_table(postgres_session: PostgresSession):
    with pytest.raises(NotImplementedError):
        postgres_session.catalog.createExternalTable("table1", "tests.public", "path/to/table")


def test_create_table(postgres_session: PostgresSession):
    with pytest.raises(NotImplementedError):
        postgres_session.catalog.createTable("table1", "tests.public")


def test_drop_temp_view(postgres_session: PostgresSession):
    with pytest.raises(NotImplementedError):
        postgres_session.catalog.dropTempView("view1")


def test_drop_global_temp_view(postgres_session: PostgresSession):
    with pytest.raises(NotImplementedError):
        postgres_session.catalog.dropGlobalTempView("view1")


def test_register_function(postgres_session: PostgresSession):
    with pytest.raises(NotImplementedError):
        postgres_session.catalog.registerFunction("function1", lambda x: x)


def test_is_cached(postgres_session: PostgresSession):
    with pytest.raises(NotImplementedError):
        postgres_session.catalog.isCached("table1")


def test_cache_table(postgres_session: PostgresSession):
    with pytest.raises(NotImplementedError):
        postgres_session.catalog.cacheTable("table1")


def test_uncache_table(postgres_session: PostgresSession):
    with pytest.raises(NotImplementedError):
        postgres_session.catalog.uncacheTable("table1")


def test_clear_cache(postgres_session: PostgresSession):
    with pytest.raises(NotImplementedError):
        postgres_session.catalog.clearCache()


def test_refresh_table(postgres_session: PostgresSession):
    with pytest.raises(NotImplementedError):
        postgres_session.catalog.refreshTable("table1")


def test_recover_partitions(postgres_session: PostgresSession):
    with pytest.raises(NotImplementedError):
        postgres_session.catalog.recoverPartitions("table1")


def test_refresh_by_path(postgres_session: PostgresSession):
    with pytest.raises(NotImplementedError):
        postgres_session.catalog.refreshByPath("path/to/table")
