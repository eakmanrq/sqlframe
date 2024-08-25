import pytest

from sqlframe import activate

pytest_plugins = ["tests.common_fixtures"]


@pytest.mark.forked
def test_activate_with_connection(function_scoped_postgres):
    cursor = function_scoped_postgres.cursor()
    cursor.execute('CREATE SCHEMA "activate_test"')
    cursor.execute('CREATE TABLE "activate_test"."test" (a INT)')
    cursor.execute('INSERT INTO "activate_test"."test" VALUES (1)')
    activate("postgres", conn=function_scoped_postgres)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("test").getOrCreate()

    df = spark.table("activate_test.test").select("`a`")
    assert df.collect() == [(1,)]


@pytest.mark.forked
def test_activate_with_connection_and_input_dialect(function_scoped_postgres):
    cursor = function_scoped_postgres.cursor()
    cursor.execute('CREATE SCHEMA "activate_test"')
    cursor.execute('CREATE TABLE "activate_test"."test" (a INT)')
    cursor.execute('INSERT INTO "activate_test"."test" VALUES (1)')
    activate(
        "postgres", conn=function_scoped_postgres, config={"sqlframe.input.dialect": "postgres"}
    )
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("test").getOrCreate()

    df = spark.table("activate_test.test").select('"a"')
    assert df.collect() == [(1,)]
