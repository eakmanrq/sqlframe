from __future__ import annotations

import os
import typing as t

import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession as PySparkSession
from pytest_postgresql.janitor import DatabaseJanitor

from sqlframe.bigquery.session import BigQuerySession
from sqlframe.duckdb.session import DuckDBSession
from sqlframe.postgres.session import PostgresSession
from sqlframe.redshift.session import RedshiftSession
from sqlframe.snowflake.session import SnowflakeSession
from sqlframe.spark.session import SparkSession
from sqlframe.standalone import types as StandaloneTypes
from sqlframe.standalone.dataframe import StandaloneDataFrame
from sqlframe.standalone.session import StandaloneSession

if t.TYPE_CHECKING:
    from google.cloud.bigquery.dbapi.connection import (
        Connection as BigQueryConnection,
    )
    from redshift_connector.core import Connection as RedshiftConnection
    from snowflake.connector import SnowflakeConnection

    from tests.types import EmployeeData


@pytest.fixture(scope="session")
def pyspark_session(tmp_path_factory) -> PySparkSession:
    data_dir = tmp_path_factory.mktemp("spark_connection")
    derby_dir = tmp_path_factory.mktemp("derby")
    spark = (
        PySparkSession.builder.enableHiveSupport()
        .config("spark.sql.analyzer.failAmbiguousSelfJoin", "false")
        .config("spark.sql.warehouse.dir", data_dir)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby_dir}")
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.sql.session.timeZone", "America/Los_Angeles")
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
    return spark


@pytest.fixture(scope="function")
def standalone_session() -> StandaloneSession:
    return StandaloneSession()


@pytest.fixture()
def spark_session(pyspark_session: PySparkSession) -> SparkSession:
    return SparkSession()


@pytest.fixture(scope="function")
def duckdb_session() -> DuckDBSession:
    from duckdb import connect

    # https://github.com/duckdb/duckdb/issues/11404
    connection = connect()
    connection.sql("set TimeZone = 'UTC'")
    return DuckDBSession(conn=connection)


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
    session._execute("CREATE SCHEMA IF NOT EXISTS db1")
    session._execute("CREATE TABLE IF NOT EXISTS db1.table1 (id INTEGER, name VARCHAR(100))")
    session._execute(
        "CREATE OR REPLACE FUNCTION db1.add(x INT, y INT) RETURNS INT IMMUTABLE AS $$ select x + y $$"
    )
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
