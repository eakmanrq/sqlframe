from __future__ import annotations

import os
import typing as t
from pathlib import Path

import duckdb
import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession as PySparkSession
from pytest_postgresql.janitor import DatabaseJanitor

from sqlframe.base.session import _BaseSession
from sqlframe.bigquery.session import BigQuerySession
from sqlframe.databricks.session import DatabricksSession
from sqlframe.duckdb.session import DuckDBSession
from sqlframe.postgres.session import PostgresSession
from sqlframe.redshift.session import RedshiftSession
from sqlframe.snowflake.session import SnowflakeSession
from sqlframe.spark.session import SparkSession
from sqlframe.standalone import types as StandaloneTypes
from sqlframe.standalone.dataframe import StandaloneDataFrame
from sqlframe.standalone.session import StandaloneSession

if t.TYPE_CHECKING:
    from databricks.sql import Connection as DatabricksConnection
    from google.cloud.bigquery.dbapi.connection import (
        Connection as BigQueryConnection,
    )
    from redshift_connector.core import Connection as RedshiftConnection
    from snowflake.connector import SnowflakeConnection

    from tests.types import EmployeeData


def load_tpcds(paths: t.List[Path], session: t.Union[_BaseSession, PySparkSession]):
    for path in paths:
        table_name = path.name
        session.read.parquet(str(path / "*.parquet")).createOrReplaceTempView(table_name)


@pytest.fixture(scope="session")
def gen_tpcds(tmp_path_factory) -> t.List[Path]:
    path_root = tmp_path_factory.mktemp("tpcds")
    results = []
    tables = [
        "web_site",
        "web_sales",
        "web_returns",
        "web_page",
        "warehouse",
        "time_dim",
        "store_sales",
        "store_returns",
        "store",
        "ship_mode",
        "reason",
        "promotion",
        "item",
        "inventory",
        "income_band",
        "household_demographics",
        "date_dim",
        "customer_demographics",
        "customer_address",
        "customer",
        "catalog_sales",
        "catalog_returns",
        "catalog_page",
        "call_center",
    ]
    con = duckdb.connect()
    con.sql("CALL dsdgen(sf=0.01)")
    for table in tables:
        path = path_root / table
        path.mkdir(parents=True)
        con.sql(
            f"COPY (SELECT * FROM {table}) TO '{path}' (FORMAT PARQUET, PER_THREAD_OUTPUT TRUE)"
        )
        results.append(path)
    con.close()
    return results


@pytest.fixture(scope="session")
def pyspark_session(tmp_path_factory, gen_tpcds: t.List[Path]) -> PySparkSession:
    data_dir = tmp_path_factory.mktemp("spark_connection")
    derby_dir = tmp_path_factory.mktemp("derby")
    spark = (
        PySparkSession.builder.enableHiveSupport()
        .config("spark.sql.analyzer.failAmbiguousSelfJoin", "false")
        .config("spark.sql.warehouse.dir", data_dir)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby_dir}")
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.sql.session.timeZone", "UTC")
        .master("local[1]")
        .appName("Unit-tests")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("DROP DATABASE IF EXISTS db1 CASCADE")
    spark.sql("CREATE DATABASE db1")
    spark.sql("CREATE TABLE db1.table1 (id INTEGER, name STRING)")
    spark.catalog.registerFunction("add", lambda x, y: x + y)
    spark.catalog.setCurrentDatabase("db1")
    load_tpcds(gen_tpcds, spark)
    return spark


@pytest.fixture(scope="function")
def standalone_session() -> StandaloneSession:
    return StandaloneSession()


@pytest.fixture()
def spark_session(pyspark_session: PySparkSession) -> SparkSession:
    return SparkSession()


@pytest.fixture(scope="function")
def duckdb_session() -> DuckDBSession:
    connector = duckdb.connect()
    connector.execute("set TimeZone = 'UTC'")
    connector.execute("SELECT * FROM duckdb_settings() WHERE name = 'TimeZone'")
    assert connector.fetchone()[1] == "UTC"  # type: ignore
    connector.execute("INSTALL tpcds")
    return DuckDBSession(conn=connector)


@pytest.fixture
def function_scoped_postgres(postgresql_proc):
    import psycopg2

    janitor = DatabaseJanitor(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        dbname=postgresql_proc.dbname,
        version=postgresql_proc.version,
        password=postgresql_proc.password,
    )
    with janitor:
        conn = psycopg2.connect(
            dbname=postgresql_proc.dbname,
            user=postgresql_proc.user,
            password=postgresql_proc.password,
            host=postgresql_proc.host,
            port=postgresql_proc.port,
        )
        yield conn


@pytest.fixture
def postgres_session(function_scoped_postgres) -> PostgresSession:
    session = PostgresSession(function_scoped_postgres)
    session._execute("SET TIME ZONE 'UTC'")
    session._execute('CREATE SCHEMA "db1"')
    session._execute('CREATE TABLE "table1" (id INTEGER, name VARCHAR(100))')
    session._execute(
        "CREATE FUNCTION testing(integer, integer) RETURNS integer AS 'select $1 + $2;' LANGUAGE SQL"
    )
    return session


@pytest.fixture(scope="session")
def bigquery_connection() -> BigQueryConnection:
    from google.cloud.bigquery.dbapi import connect

    connection = connect()
    cursor = connection.cursor()
    cursor.execute("DROP SCHEMA IF EXISTS sqlframe.db1 CASCADE")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS sqlframe.db1")
    cursor.execute("CREATE TABLE IF NOT EXISTS sqlframe.db1.table1 (id INTEGER, name STRING)")
    cursor.execute(
        "CREATE FUNCTION IF NOT EXISTS sqlframe.db1.add(x INT64, y INT64) RETURNS INT64 AS (x + y);"
    )
    return connection


@pytest.fixture
def bigquery_session(bigquery_connection: BigQueryConnection) -> BigQuerySession:
    return BigQuerySession(bigquery_connection, default_dataset="sqlframe.db1")


@pytest.fixture(scope="session")
def redshift_connection() -> RedshiftConnection:
    from redshift_connector import connect

    conn = connect(
        user=os.environ["SQLFRAME_REDSHIFT_USER"],
        password=os.environ["SQLFRAME_REDSHIFT_PASSWORD"],
        database=os.environ["SQLFRAME_REDSHIFT_DATABASE"],
        host=os.environ["SQLFRAME_REDSHIFT_HOST"],
        port=int(os.environ["SQLFRAME_REDSHIFT_PORT"]),
    )
    conn.autocommit = True
    return conn


@pytest.fixture
def redshift_session(redshift_connection: RedshiftConnection) -> RedshiftSession:
    session = RedshiftSession(redshift_connection)
    session._execute("CREATE SCHEMA IF NOT EXISTS db1")
    session._execute("CREATE TABLE IF NOT EXISTS db1.table1 (id INTEGER, name VARCHAR(100))")
    session._execute(
        "CREATE OR REPLACE FUNCTION db1.add(x INT, y INT) RETURNS INT IMMUTABLE AS $$ select $1 + $2 $$ LANGUAGE SQL"
    )
    return session


@pytest.fixture(scope="session")
def snowflake_connection() -> SnowflakeConnection:
    from snowflake.connector import connect

    conn = connect(
        account=os.environ["SQLFRAME_SNOWFLAKE_ACCOUNT"],
        user=os.environ["SQLFRAME_SNOWFLAKE_USER"],
        password=os.environ["SQLFRAME_SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SQLFRAME_SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SQLFRAME_SNOWFLAKE_DATABASE"],
        schema=os.environ["SQLFRAME_SNOWFLAKE_SCHEMA"],
    )
    return conn


@pytest.fixture
def snowflake_session(snowflake_connection: SnowflakeConnection) -> SnowflakeSession:
    session = SnowflakeSession(snowflake_connection)
    session._execute("ALTER SESSION SET TIMEZONE = 'UTC'")
    session._execute("CREATE SCHEMA IF NOT EXISTS db1")
    session._execute("CREATE TABLE IF NOT EXISTS db1.table1 (id INTEGER, name VARCHAR(100))")
    session._execute(
        "CREATE OR REPLACE FUNCTION db1.add(x INT, y INT) RETURNS INT IMMUTABLE AS $$ select x + y $$"
    )
    return session


@pytest.fixture(scope="session")
def databricks_connection() -> DatabricksConnection:
    from databricks.sql import connect

    conn = connect(
        server_hostname=os.environ["SQLFRAME_DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["SQLFRAME_DATABRICKS_HTTP_PATH"],
        access_token=os.environ["SQLFRAME_DATABRICKS_ACCESS_TOKEN"],
        auth_type="access_token",
        catalog=os.environ["SQLFRAME_DATABRICKS_CATALOG"],
        schema=os.environ["SQLFRAME_DATABRICKS_SCHEMA"],
        _disable_pandas=True,
    )
    return conn


@pytest.fixture
def databricks_session(databricks_connection: DatabricksConnection) -> DatabricksSession:
    session = DatabricksSession(databricks_connection)
    session._execute("CREATE SCHEMA IF NOT EXISTS db1")
    session._execute("CREATE TABLE IF NOT EXISTS db1.table1 (id INTEGER, name VARCHAR(100))")
    session._execute("CREATE OR REPLACE FUNCTION db1.add(x INT, y INT) RETURNS INT RETURN x + y")
    return session


@pytest.fixture(scope="module")
def _employee_data() -> EmployeeData:
    return [
        (1, "Jack", "Shephard", 37, 1),
        (2, "John", "Locke", 65, 1),
        (3, "Kate", "Austen", 37, 2),
        (4, "Claire", "Littleton", 27, 2),
        (5, "Hugo", "Reyes", 29, 100),
    ]


@pytest.fixture(scope="function")
def standalone_employee(
    standalone_session: StandaloneSession, _employee_data: EmployeeData
) -> StandaloneDataFrame:
    sqlf_employee_schema = StandaloneTypes.StructType(
        [
            StandaloneTypes.StructField("employee_id", StandaloneTypes.IntegerType(), False),
            StandaloneTypes.StructField("fname", StandaloneTypes.StringType(), False),
            StandaloneTypes.StructField("lname", StandaloneTypes.StringType(), False),
            StandaloneTypes.StructField("age", StandaloneTypes.IntegerType(), False),
            StandaloneTypes.StructField("store_id", StandaloneTypes.IntegerType(), False),
        ]
    )
    df = standalone_session.createDataFrame(data=_employee_data, schema=sqlf_employee_schema)
    standalone_session.catalog.add_table("employee", sqlf_employee_schema)  # type: ignore
    return df
