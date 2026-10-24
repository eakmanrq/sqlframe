import json
import typing as t

from snowflake.connector import SnowflakeConnection

from sqlframe.snowflake.session import JsonLoadsSnowflakeConverter, SnowflakeSession


class FakeCursor:
    description = [
        ("array_col", 10, None, None, None, None, True),
        ("object_col", 9, None, None, None, None, True),
        ("variant_col", 5, None, None, None, None, True),
        ("text_col", 2, None, None, None, None, True),
    ]

    def execute(self, sql):
        self.sql = sql

    def fetchall(self):
        array_json = json.dumps([1, None])
        return [
            (array_json, json.dumps({"a": 2}), json.dumps("value"), array_json),
            ([3], {"b": 4}, None, "ordinary text"),
        ]


class FakeV5Connection:
    def __init__(self):
        self.result_cursor = FakeCursor()

    def cursor(self):
        return self.result_cursor


def test_v5_session_decodes_semi_structured_results():
    connection = FakeV5Connection()
    session = SnowflakeSession(t.cast(SnowflakeConnection, connection))

    rows = session._collect("SELECT 1")

    assert connection.result_cursor.sql == "SELECT 1"
    assert rows[0] == ([1, None], {"a": 2}, "value", "[1, null]")
    assert rows[1] == ([3], {"b": 4}, None, "ordinary text")


def test_v4_session_preserves_decoded_string_variants():
    class V4Cursor(FakeCursor):
        description = [("variant_col", 5, None, None, None, None, True)]

        def fetchall(self):
            convert = connection.converter._VARIANT_to_python({})
            return [(convert(json.dumps(value)),) for value in ("value", "123", "null")]

    class V4Connection(FakeV5Connection):
        def __init__(self):
            self.converter = JsonLoadsSnowflakeConverter()
            self.result_cursor = V4Cursor()

    connection = V4Connection()
    session = SnowflakeSession(t.cast(SnowflakeConnection, connection))

    assert [row[0] for row in session._collect("SELECT 1")] == ["value", "123", "null"]
