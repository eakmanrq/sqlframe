"""Live connector checks; set SQLFRAME_CLICKHOUSE_HOST after starting compose.yml."""

import os
import uuid

import pytest

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.clickhouse import ClickHouseSession
from sqlframe.clickhouse import functions as F

pytestmark = pytest.mark.clickhouse


@pytest.fixture(scope="module")
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

    rows = session.table(table).filter(F.col("value") > 1).select("name", "value").collect()

    assert [row.asDict() for row in rows] == [{"name": "alpha", "value": 2}]


def test_raw_sql_and_catalog_types(clickhouse_session):
    session, table = clickhouse_session

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

    with pytest.raises(UnsupportedOperationError, match="ClickHouse table writes"):
        session.table(table).write.saveAsTable(f"{table}_copy")
