from unittest.mock import Mock

import pytest

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.clickhouse import ClickHouseSession


@pytest.mark.parametrize("column_name", ["userId", "USERID", "UserID"])
def test_table_rejects_case_sensitive_columns_before_select(column_name):
    cursor = Mock()
    cursor.description = [("name",), ("type",)]
    cursor.fetchall.return_value = [(column_name, "String"), ("value", "Int64")]
    connection = Mock()
    connection.cursor.return_value = cursor
    session = ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()

    with pytest.raises(UnsupportedOperationError, match=column_name):
        session.table("events").collect()

    cursor.execute.assert_called_once_with('DESCRIBE TABLE "events"')


def test_mixed_case_column_raw_sql_workaround():
    chdb = pytest.importorskip("chdb")
    connection = chdb.dbapi.connect()
    try:
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE events (userId String, value Int64) ENGINE = Memory")
        cursor.execute("INSERT INTO events VALUES ('a', 1)")
        spark = ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()

        with pytest.raises(UnsupportedOperationError, match="sqlglot.meta case_sensitive"):
            spark.table("events")
        # The raw SQL workaround documented in docs/clickhouse.md, verbatim.
        rows = spark.sql(
            "SELECT `userId` /* sqlglot.meta case_sensitive */ AS user_id, value FROM events",
            qualify=False,
        ).collect()

        assert [row.asDict() for row in rows] == [{"user_id": "a", "value": 1}]
    finally:
        connection.close()
