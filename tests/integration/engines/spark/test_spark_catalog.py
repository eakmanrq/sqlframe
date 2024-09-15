import typing as t

import pytest
from pyspark.errors import AnalysisException

from sqlframe.spark.catalog import CatalogMetadata, Column, Database, Function, Table
from sqlframe.spark.session import SparkSession

pytest_plugins = ["tests.integration.fixtures"]


@pytest.fixture
def reset_database(spark_session: SparkSession) -> t.Iterator[None]:
    yield
    spark_session.catalog.setCurrentDatabase("db1")


def test_current_catalog(spark_session: SparkSession):
    assert spark_session.catalog.currentCatalog() == "spark_catalog"


def test_list_catalogs(spark_session: SparkSession):
    assert sorted(spark_session.catalog.listCatalogs(), key=lambda x: x.name) == [
        CatalogMetadata(name="spark_catalog", description=None)
    ]


def test_current_database(spark_session: SparkSession):
    assert spark_session.catalog.currentDatabase() == "db1"


def test_set_current_database(spark_session: SparkSession, reset_database):
    assert spark_session.catalog.currentDatabase() == "db1"
    spark_session.catalog.setCurrentDatabase("default")
    assert spark_session.catalog.currentDatabase() == "default"


def test_list_databases(spark_session: SparkSession):
    assert sorted(
        [
            Database(name=x.name, catalog=x.catalog, description="", locationUri="")
            for x in spark_session.catalog.listDatabases()
        ],
        key=lambda x: (x.catalog, x.name),
    ) == [
        Database(name="db1", catalog="spark_catalog", description="", locationUri=""),
        Database(name="default", catalog="spark_catalog", description="", locationUri=""),
    ]


def test_list_databases_pattern(spark_session: SparkSession):
    assert sorted(
        [
            Database(name=x.name, catalog=x.catalog, description="", locationUri="")
            for x in spark_session.catalog.listDatabases("db*")
        ],
        key=lambda x: (x.catalog, x.name),
    ) == [
        Database(name="db1", catalog="spark_catalog", description="", locationUri=""),
    ]


def test_get_database_no_match(spark_session: SparkSession):
    with pytest.raises(AnalysisException):
        assert spark_session.catalog.getDatabase("nonexistent")


def test_get_database_name_only(spark_session: SparkSession):
    database = spark_session.catalog.getDatabase("db1")
    assert database.name == "db1"
    assert database.catalog == "spark_catalog"


def test_database_exists_does_exist(spark_session: SparkSession):
    assert spark_session.catalog.databaseExists("db1") is True


def test_database_exists_does_not_exist(spark_session: SparkSession):
    assert spark_session.catalog.databaseExists("nonexistent") is False


def test_list_tables_no_args(spark_session: SparkSession):
    assert sorted(
        # Filter out temporary tables from TPCDS test
        [x for x in spark_session.catalog.listTables() if x.tableType != "TEMPORARY"],
        key=lambda x: (x.catalog, x.database, x.name),
    ) == [
        Table(
            name="table1",
            catalog="spark_catalog",
            namespace=["db1"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_list_tables_db_no_catalog(spark_session: SparkSession):
    assert sorted(
        # Filter out temporary tables from TPCDS test
        [x for x in spark_session.catalog.listTables("db1") if x.tableType != "TEMPORARY"],
        key=lambda x: (x.catalog or "", x.database, x.name),
    ) == [
        Table(
            name="table1",
            catalog="spark_catalog",
            namespace=["db1"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_list_tables_db_and_catalog(spark_session: SparkSession):
    assert sorted(
        # Filter out temporary tables from TPCDS test
        [
            x
            for x in spark_session.catalog.listTables("spark_catalog.db1")
            if x.tableType != "TEMPORARY"
        ],
        key=lambda x: (x.catalog or "", x.database, x.name),
    ) == [
        Table(
            name="table1",
            catalog="spark_catalog",
            namespace=["db1"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_list_tables_pattern(spark_session: SparkSession):
    assert sorted(
        spark_session.catalog.listTables(pattern="tab*"),
        key=lambda x: (x.catalog, x.database, x.name),
    ) == [
        Table(
            name="table1",
            catalog="spark_catalog",
            namespace=["db1"],
            description=None,
            tableType="MANAGED",
            isTemporary=False,
        )
    ]


def test_get_table(spark_session: SparkSession):
    assert spark_session.catalog.getTable("spark_catalog.db1.table1") == Table(
        name="table1",
        catalog="spark_catalog",
        namespace=["db1"],
        description=None,
        tableType="MANAGED",
        isTemporary=False,
    )


def test_get_table_not_exists(spark_session: SparkSession):
    with pytest.raises(AnalysisException):
        assert spark_session.catalog.getTable("spark_catalog.db1.nonexistent")


def test_list_functions(spark_session: SparkSession):
    assert "add" in [x.name for x in spark_session.catalog.listFunctions()]


def test_list_functions_pattern(spark_session: SparkSession):
    assert "add" in [x.name for x in spark_session.catalog.listFunctions(pattern="ad*")]


def test_function_exists_does_exist(spark_session: SparkSession):
    assert spark_session.catalog.functionExists("add") is True


def test_function_exists_does_not_exist(spark_session: SparkSession):
    assert spark_session.catalog.functionExists("nonexistent") is False


def test_get_function_exists(spark_session: SparkSession):
    function = spark_session.catalog.getFunction("add")
    assert function.name == "add"
    assert function.catalog is None
    assert function.namespace is None
    assert function.isTemporary is True


def test_get_function_not_exists(spark_session: SparkSession):
    with pytest.raises(AnalysisException):
        assert spark_session.catalog.getFunction("default.main.nonexistent")


def test_list_columns(spark_session: SparkSession):
    assert sorted(
        spark_session.catalog.listColumns("spark_catalog.db1.table1"), key=lambda x: x.name
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


def test_list_columns_use_db_name(spark_session: SparkSession):
    assert sorted(
        spark_session.catalog.listColumns("table1", dbName="db1"), key=lambda x: x.name
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


def test_table_exists_table_name_only(spark_session: SparkSession):
    assert spark_session.catalog.tableExists("spark_catalog.db1.table1") is True


def test_table_exists_table_name_and_db_name(spark_session: SparkSession):
    assert spark_session.catalog.tableExists("table1", dbName="db1") is True


def test_table_not_exists(spark_session: SparkSession):
    assert spark_session.catalog.tableExists("nonexistent") is False
