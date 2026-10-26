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


def test_unsupported_table_write_modes_fail_before_executing_sql(connection):
    session = ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()
    df = session.sql("SELECT 1 AS value")

    with pytest.raises(UnsupportedOperationError, match="overwrite"):
        df.write.mode("overwrite").saveAsTable("events")
    with pytest.raises(UnsupportedOperationError, match="overwrite"):
        df.write.insertInto("events", overwrite=True)

    connection.cursor.assert_not_called()


@pytest.mark.parametrize(
    "name",
    [
        "db.events x",
        "events as e",
        "events final",
        "events; drop table x",
        "events; archive",
        "events;",
        " ",
        "",
    ],
)
@pytest.mark.parametrize("method", ["saveAsTable", "insertInto"])
def test_malformed_table_write_names_fail_before_cursor_call(connection, name, method):
    session = ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()
    writer = session.sql("SELECT 1 AS value").write
    if method == "saveAsTable":
        writer = writer.option("clickhouse.engine", "Memory")

    with pytest.raises(UnsupportedOperationError, match="table or database.table"):
        getattr(writer, method)(name)

    connection.cursor.assert_not_called()


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO events SELECT 1 AS value",
        "CREATE TABLE events AS SELECT 1 AS value",
    ],
)
def test_raw_sql_write_fails_before_executing_sql(connection, sql):
    session = ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()

    with pytest.raises(UnsupportedOperationError, match="ClickHouse table writes"):
        session.sql(sql)

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
        cursor.execute(
            "CREATE TABLE limits (label LowCardinality(Nullable(String)), u16 UInt16, "
            "u32 UInt32, u64 UInt64) ENGINE = Memory"
        )
        cursor.execute("INSERT INTO limits VALUES (NULL, 65535, 4294967295, 18446744073709551615)")
        session = ClickHouseSession.builder.config("sqlframe.conn", conn).getOrCreate()

        rows = session.table("events").filter("value > 1").select("name", "value").collect()
        values = session.createDataFrame([{"id": 1, "name": "Ada"}]).collect()
        tags = session.table("tags").collect()
        limits = session.table("limits").collect()
        columns = session.catalog.listColumns("limits")

        assert [row.asDict() for row in rows] == [{"name": "alpha", "value": 2}]
        assert [row.asDict() for row in values] == [{"id": 1, "name": "Ada"}]
        assert [row.asDict() for row in tags] == [{"name": "green", "value": 3}]
        assert [(c.name, c.dataType, c.nullable) for c in columns] == [
            ("label", "STRING", True),
            ("u16", "INT", False),
            ("u32", "BIGINT", False),
            ("u64", "DECIMAL(20, 0)", False),
        ]
        assert [row.asDict() for row in limits] == [
            {"label": None, "u16": 65535, "u32": 4294967295, "u64": 18446744073709551615}
        ]
    finally:
        conn.close()


@pytest.mark.parametrize(
    "clickhouse_type,spark_type,nullable",
    [
        ("DateTime64(3)", "TIMESTAMP", False),
        ("Nullable(DateTime64(6, 'UTC'))", "TIMESTAMP", True),
        ("Date32", "DATE", False),
        ("Enum8('a' = 1, 'b' = 2)", "STRING", False),
        ("Enum16('a' = 1, 'b' = 2)", "STRING", False),
        ("FixedString(3)", "STRING", False),
        ("IPv4", "STRING", False),
        ("IPv6", "STRING", False),
        ("UUID", "STRING", False),
        ("Array(DateTime64(3))", "ARRAY<TIMESTAMP>", False),
        ("Nullable(String)", "STRING", True),
        ("LowCardinality(String)", "STRING", False),
        ("LowCardinality(Nullable(String))", "STRING", True),
        ("Array(LowCardinality(String))", "ARRAY<STRING>", False),
        ("UInt8", "SMALLINT", False),
        ("Nullable(UInt8)", "SMALLINT", True),
        ("Array(UInt8)", "ARRAY<SMALLINT>", False),
        ("LowCardinality(UInt8)", "SMALLINT", False),
        ("UInt16", "INT", False),
        ("UInt32", "BIGINT", False),
        ("UInt64", "DECIMAL(20, 0)", False),
        ("Nullable(UInt64)", "DECIMAL(20, 0)", True),
        ("LowCardinality(UInt16)", "INT", False),
        ("LowCardinality(UInt32)", "BIGINT", False),
        ("LowCardinality(UInt64)", "DECIMAL(20, 0)", False),
        ("LowCardinality(Nullable(UInt64))", "DECIMAL(20, 0)", True),
        ("Array(LowCardinality(UInt64))", "ARRAY<DECIMAL(20, 0)>", False),
        ("Map(String, UInt32)", "MAP<STRING, BIGINT>", False),
    ],
)
def test_describe_maps_clickhouse_types(connection, clickhouse_type, spark_type, nullable):
    from sqlglot import exp

    session = ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()
    column = session.catalog._column_from_describe({"name": "value", "type": clickhouse_type})

    assert column.dataType == spark_type
    assert exp.DataType.build(column.dataType, dialect=session.output_dialect)
    assert column.nullable == nullable


@pytest.mark.parametrize(
    "clickhouse_type", ["Int128", "Int256", "UInt128", "UInt256", "Point", "Object('json')"]
)
def test_describe_rejects_unsupported_types_with_column_name(connection, clickhouse_type):
    session = ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()

    with pytest.raises(UnsupportedOperationError, match="payload"):
        session.catalog._column_from_describe({"name": "payload", "type": clickhouse_type})
