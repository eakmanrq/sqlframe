import typing as t

from snowflake.connector import SnowflakeConnection

from sqlframe.snowflake.session import SnowflakeSession


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
        return [
            ('[1, null]', '{"a": 2}', '"value"', '[1, null]'),
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
