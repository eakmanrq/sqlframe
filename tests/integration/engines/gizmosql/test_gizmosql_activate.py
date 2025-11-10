import pytest

from sqlframe import activate


def test_activate_with_connection(gizmosql_adbc_connection):
    conn = gizmosql_adbc_connection
    with conn.cursor() as cursor:
        cursor.execute('CREATE SCHEMA "activate_test_1"').fetchall()
        cursor.execute('CREATE TABLE "activate_test_1"."test" (a INT)').fetchall()
        cursor.execute('INSERT INTO "activate_test_1"."test" VALUES (1)').fetchall()

    activate("gizmosql", conn=gizmosql_adbc_connection)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("test").getOrCreate()

    df = spark.table("activate_test_1.test").select("`a`")
    assert df.collect() == [(1,)]


def test_activate_with_connection_and_input_dialect(gizmosql_adbc_connection):
    conn = gizmosql_adbc_connection
    with conn.cursor() as cursor:
        cursor.execute('CREATE SCHEMA "activate_test_2"').fetchall()
        cursor.execute('CREATE TABLE "activate_test_2"."test" (a INT)').fetchall()
        cursor.execute('INSERT INTO "activate_test_2"."test" VALUES (1)').fetchall()
    activate("gizmosql", conn=conn, config={"sqlframe.input.dialect": "duckdb"})
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("test").getOrCreate()

    df = spark.table("activate_test_2.test").select('"a"')
    assert df.collect() == [(1,)]
