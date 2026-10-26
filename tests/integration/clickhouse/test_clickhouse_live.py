"""Live connector checks; scripts/test-clickhouse.sh sets SQLFRAME_CLICKHOUSE_HOST/PORT."""

import os
import uuid

import pytest

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.base.session import _BaseSession
from sqlframe.clickhouse import ClickHouseSession
from sqlframe.clickhouse import functions as F

pytestmark = pytest.mark.clickhouse


@pytest.fixture
def clickhouse_session():
    if not (host := os.getenv("SQLFRAME_CLICKHOUSE_HOST")):
        pytest.skip("Set SQLFRAME_CLICKHOUSE_HOST to run against ClickHouse")
    from clickhouse_connect import dbapi

    connection = dbapi.connect(
        host=host,
        port=int(os.getenv("SQLFRAME_CLICKHOUSE_PORT", "18123")),
        username=os.getenv("SQLFRAME_CLICKHOUSE_USER", "default"),
        password=os.getenv("SQLFRAME_CLICKHOUSE_PASSWORD", "sqlframe_test"),
    )
    table = f"sqlframe_events_{uuid.uuid4().hex[:12]}"
    cursor = connection.cursor()
    cursor.execute(f"CREATE TABLE {table} (name String, value Int64, u64 UInt64) ENGINE = Memory")
    cursor.execute(f"INSERT INTO {table} VALUES ('alpha', 2, 18446744073709551615), ('beta', 1, 0)")
    session = ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()
    try:
        yield session, table
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        session.stop()


def test_query_existing_table(clickhouse_session):
    session, table = clickhouse_session
    assert _BaseSession._instance is session

    rows = session.table(table).filter(F.col("value") > 1).select("name", "value").collect()

    assert [row.asDict() for row in rows] == [{"name": "alpha", "value": 2}]


def test_raw_sql_and_catalog_types(clickhouse_session):
    session, table = clickhouse_session
    assert _BaseSession._instance is session

    rows = session.sql(f"SELECT name, u64 FROM {table} WHERE value = 2", qualify=False).collect()
    columns = session.catalog.listColumns(table)
    schema = session.table(table).schema

    assert [row.asDict() for row in rows] == [{"name": "alpha", "u64": 18446744073709551615}]
    assert [(column.name, column.dataType) for column in columns] == [
        ("name", "STRING"),
        ("value", "BIGINT"),
        ("u64", "DECIMAL(20, 0)"),
    ]
    assert schema.simpleString() == "struct<name:string, value:bigint, u64:decimal(20, 0)>"


def test_values_and_aggregation(clickhouse_session):
    session, _ = clickhouse_session
    assert _BaseSession._instance is session

    rows = (
        session.createDataFrame([(1, "a"), (2, "a"), (3, "b")], ["amount", "category"])
        .groupBy("category")
        .agg(F.sum("amount").alias("total"))
        .orderBy("category")
        .collect()
    )

    assert [row.asDict() for row in rows] == [
        {"category": "a", "total": 3},
        {"category": "b", "total": 3},
    ]


def test_unsupported_write_fails(clickhouse_session):
    session, table = clickhouse_session
    assert _BaseSession._instance is session

    with pytest.raises(UnsupportedOperationError, match="ClickHouse table writes"):
        session.table(table).write.saveAsTable(f"{table}_copy")


def test_outer_joins_return_nulls_and_full_join_keys(clickhouse_session):
    session, _ = clickhouse_session
    assert _BaseSession._instance is session
    suffix = uuid.uuid4().hex[:12]
    left_table = f"sqlframe_left_{suffix}"
    right_table = f"sqlframe_right_{suffix}"
    cursor = session._conn.cursor()
    cursor.execute(f"CREATE TABLE {left_table} (id Int64, left_value String) ENGINE = Memory")
    cursor.execute(f"CREATE TABLE {right_table} (id Int64, right_value String) ENGINE = Memory")
    try:
        cursor.execute(f"INSERT INTO {left_table} VALUES (1, 'left'), (2, 'left only')")
        cursor.execute(f"INSERT INTO {right_table} VALUES (1, 'right'), (3, 'right only')")
        left = session.table(left_table)
        right = session.table(right_table)

        left_rows = left.join(right, on="id", how="left").collect()
        right_rows = left.join(right, on="id", how="right").collect()
        full_rows = left.join(right, on="id", how="full").collect()

        assert sorted((row.asDict() for row in left_rows), key=lambda row: row["id"]) == [
            {"id": 1, "left_value": "left", "right_value": "right"},
            {"id": 2, "left_value": "left only", "right_value": None},
        ]
        assert sorted((row.asDict() for row in right_rows), key=lambda row: row["id"]) == [
            {"id": 1, "left_value": "left", "right_value": "right"},
            {"id": 3, "left_value": None, "right_value": "right only"},
        ]
        assert sorted((row.asDict() for row in full_rows), key=lambda row: row["id"]) == [
            {"id": 1, "left_value": "left", "right_value": "right"},
            {"id": 2, "left_value": "left only", "right_value": None},
            {"id": 3, "left_value": None, "right_value": "right only"},
        ]
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS {left_table}")
        cursor.execute(f"DROP TABLE IF EXISTS {right_table}")


def test_dotted_column_name_is_read_as_one_identifier(clickhouse_session):
    session, _ = clickhouse_session
    assert _BaseSession._instance is session
    table = f"sqlframe_dotted_{uuid.uuid4().hex[:12]}"
    cursor = session._conn.cursor()
    cursor.execute(f"CREATE TABLE {table} (`a.b` String, value Int64) ENGINE = Memory")
    try:
        cursor.execute(f"INSERT INTO {table} VALUES ('alpha', 2)")

        columns = session.catalog.listColumns(table)
        frame = session.table(table)
        sql = frame.sql()
        rows = frame.collect()

        assert [column.name for column in columns] == ["a.b", "value"]
        assert "`a.b`" in sql
        assert frame.schema.simpleString() == "struct<a.b:string, value:bigint>"
        assert [row.asDict() for row in rows] == [{"a.b": "alpha", "value": 2}]
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
