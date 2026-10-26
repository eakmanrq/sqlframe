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
