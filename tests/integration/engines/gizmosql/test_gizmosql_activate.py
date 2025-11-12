import os

import pytest

from sqlframe import activate
from sqlframe.gizmosql.connect import GizmoSQLConnection


@pytest.mark.forked
def test_activate_with_connection():
    # We need to grab a thread-safe connection (we can't use a session fixture b/c we are in a fork)
    conn = GizmoSQLConnection(uri="grpc+tcp://localhost:31337",
                              db_kwargs={"username": "gizmosql_username",
                                         "password": "gizmosql_password"
                                         },
                              autocommit=True
                              )
    with conn.cursor() as cursor:
        cursor.execute('DROP SCHEMA IF EXISTS "activate_test_1" CASCADE').fetchall()
        cursor.execute('CREATE SCHEMA "activate_test_1"').fetchall()
        cursor.execute('CREATE TABLE "activate_test_1"."test" (a INT)').fetchall()
        cursor.execute('INSERT INTO "activate_test_1"."test" VALUES (1)').fetchall()

    activate("gizmosql", conn=conn)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("test").getOrCreate()

    df = spark.table("activate_test_1.test").select("`a`")
    assert df.collect() == [(1,)]


@pytest.mark.forked
def test_activate_with_connection_and_input_dialect():
    # We need to grab a thread-safe connection (we can't use a session fixture b/c we are in a fork)
    conn = GizmoSQLConnection(uri="grpc+tcp://localhost:31337",
                              db_kwargs={"username": "gizmosql_username",
                                         "password": "gizmosql_password"
                                         },
                              autocommit=True
                              )
    with conn.cursor() as cursor:
        cursor.execute('DROP SCHEMA IF EXISTS "activate_test_2" CASCADE').fetchall()
        cursor.execute('CREATE OR REPLACE SCHEMA "activate_test_2"').fetchall()
        cursor.execute('CREATE OR REPLACE TABLE "activate_test_2"."test" (a INT)').fetchall()
        cursor.execute('INSERT INTO "activate_test_2"."test" VALUES (1)').fetchall()

    activate("gizmosql", conn=conn, config={"sqlframe.input.dialect": "duckdb"})
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("test").getOrCreate()

    df = spark.table("activate_test_2.test").select('"a"')
    assert df.collect() == [(1,)]
