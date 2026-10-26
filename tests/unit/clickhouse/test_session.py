from __future__ import annotations

from unittest.mock import Mock

import pytest

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.clickhouse import ClickHouseSession


@pytest.fixture
def connection():
    cursor = Mock()
    cursor.description = [("name",), ("value",)]
    cursor.fetchall.return_value = [("alpha", 2)]
    conn = Mock()
    conn.cursor.return_value = cursor
    return conn


def test_collect_uses_clickhouse_sql_and_dbapi_cursor(connection):
    session = ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()

    rows = session.sql("SELECT name, value FROM events WHERE value > 1", qualify=False).collect()

    assert rows[0].asDict() == {"name": "alpha", "value": 2}
    sql = connection.cursor.return_value.execute.call_args.args[0]
    assert 'FROM "events"' in sql
    assert '"value" > 1' in sql


def test_table_uses_clickhouse_describe_for_schema(connection):
    cursor = connection.cursor.return_value

    def execute(sql):
        if sql.startswith("DESCRIBE"):
            cursor.description = [("name",), ("type",)]
            cursor.fetchall.return_value = [("name", "String"), ("value", "Nullable(Int64)")]
        else:
            cursor.description = [("name",), ("value",)]
            cursor.fetchall.return_value = [("alpha", 2)]

    cursor.execute.side_effect = execute
    session = ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()

    rows = session.table("events").select("name", "value").collect()

    assert rows[0].asDict() == {"name": "alpha", "value": 2}
    assert cursor.execute.call_args_list[0].args[0] == 'DESCRIBE TABLE "events"'


def test_table_write_fails_before_executing_sql(connection):
    session = ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()
    df = session.sql("SELECT 1 AS value")

    with pytest.raises(UnsupportedOperationError, match="ClickHouse table writes"):
        df.write.saveAsTable("events")
    with pytest.raises(UnsupportedOperationError, match="ClickHouse table writes"):
        df.write.insertInto("events")

    connection.cursor.assert_not_called()


def test_embedded_clickhouse_query_and_values():
    chdb = pytest.importorskip("chdb")
    conn = chdb.dbapi.connect()
    try:
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE events (name String, value Int64) ENGINE = Memory")
        cursor.execute("INSERT INTO events VALUES ('alpha', 2), ('beta', 1)")
        cursor.execute(
            "CREATE TABLE tags (name LowCardinality(String), value UInt64) ENGINE = Memory"
        )
        cursor.execute("INSERT INTO tags VALUES ('green', 3)")
        session = ClickHouseSession.builder.config("sqlframe.conn", conn).getOrCreate()

        rows = session.table("events").filter("value > 1").select("name", "value").collect()
        values = session.createDataFrame([{"id": 1, "name": "Ada"}]).collect()
        tags = session.table("tags").collect()

        assert [row.asDict() for row in rows] == [{"name": "alpha", "value": 2}]
        assert [row.asDict() for row in values] == [{"id": 1, "name": "Ada"}]
        assert [row.asDict() for row in tags] == [{"name": "green", "value": 3}]
    finally:
        conn.close()


@pytest.mark.parametrize(
    "clickhouse_type,spark_type",
    [
        ("DateTime64(3)", "TIMESTAMP"),
        ("Nullable(DateTime64(6, 'UTC'))", "TIMESTAMP"),
        ("Date32", "DATE"),
        ("Enum8('a' = 1, 'b' = 2)", "STRING"),
        ("Enum16('a' = 1, 'b' = 2)", "STRING"),
        ("FixedString(3)", "STRING"),
        ("IPv4", "STRING"),
        ("IPv6", "STRING"),
        ("UUID", "STRING"),
        ("Array(DateTime64(3))", "ARRAY<TIMESTAMP>"),
    ],
)
def test_describe_maps_clickhouse_types(connection, clickhouse_type, spark_type):
    from sqlglot import exp

    session = ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()
    column = session.catalog._column_from_describe({"name": "value", "type": clickhouse_type})

    assert column.dataType == spark_type
    assert exp.DataType.build(column.dataType, dialect=session.output_dialect)
    assert column.nullable == clickhouse_type.startswith("Nullable")


@pytest.mark.parametrize(
    "clickhouse_type", ["Int128", "Int256", "UInt128", "UInt256", "Point", "Object('json')"]
)
def test_describe_rejects_unsupported_types_with_column_name(connection, clickhouse_type):
    session = ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()

    with pytest.raises(UnsupportedOperationError, match="payload"):
        session.catalog._column_from_describe({"name": "payload", "type": clickhouse_type})
