from unittest.mock import Mock

from sqlframe.base import types
from sqlframe.clickhouse import ClickHouseSession


def test_schema_and_print_schema_describe_query(capsys):
    cursor = Mock()
    cursor.description = [("name",), ("type",)]
    cursor.fetchall.return_value = [("name", "String"), ("value", "Nullable(UInt32)")]
    connection = Mock()
    connection.cursor.return_value = cursor
    session = ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()
    df = session.sql("SELECT name, value + 1 AS value FROM events", qualify=False)

    assert df.schema == types.StructType(
        [
            types.StructField("name", types.StringType(), True),
            types.StructField("value", types.LongType(), True),
        ]
    )
    df.printSchema()

    assert capsys.readouterr().out == (
        "root\n |-- name: string (nullable = false)\n |-- value: bigint (nullable = true)\n"
    )
    assert cursor.execute.call_count == 2
    for call in cursor.execute.call_args_list:
        assert call.args[0] == f"DESCRIBE ({session._to_sql(df.expression)})"


def test_embedded_clickhouse_schema(capsys):
    import pytest

    chdb = pytest.importorskip("chdb")
    connection = chdb.dbapi.connect()
    try:
        session = ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()
        df = session.sql(
            "SELECT 'alpha' AS name, CAST(2 AS Nullable(UInt32)) AS value",
            dialect="clickhouse",
            qualify=False,
        )

        assert df.schema.simpleString() == "struct<name:string, value:bigint>"
        df.printSchema()
        assert capsys.readouterr().out == (
            "root\n |-- name: string (nullable = false)\n |-- value: bigint (nullable = true)\n"
        )
    finally:
        connection.close()
