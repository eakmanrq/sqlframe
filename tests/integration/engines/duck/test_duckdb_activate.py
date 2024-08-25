import pytest

from sqlframe import activate


@pytest.mark.forked
def test_activate_with_connection():
    import duckdb

    connector = duckdb.connect()
    connector.execute('CREATE SCHEMA "memory"."activate_test"')
    connector.execute('CREATE TABLE "memory"."activate_test"."test" (a INT)')
    connector.execute('INSERT INTO "memory"."activate_test"."test" VALUES (1)')
    activate("duckdb", conn=connector)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("test").getOrCreate()

    df = spark.table("memory.activate_test.test").select("`a`")
    assert df.collect() == [(1,)]


@pytest.mark.forked
def test_activate_with_connection_and_input_dialect():
    import duckdb

    connector = duckdb.connect()
    connector.execute('CREATE SCHEMA "memory"."activate_test"')
    connector.execute('CREATE TABLE "memory"."activate_test"."test" (a INT)')
    connector.execute('INSERT INTO "memory"."activate_test"."test" VALUES (1)')
    activate("duckdb", conn=connector, config={"sqlframe.input.dialect": "duckdb"})
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("test").getOrCreate()

    df = spark.table("memory.activate_test.test").select('"a"')
    assert df.collect() == [(1,)]
