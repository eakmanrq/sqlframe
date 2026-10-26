from unittest.mock import Mock

import pytest

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.clickhouse import ClickHouseSession


@pytest.mark.parametrize(
    "table_name", ["Events", "`Events`", "default.Events", "`Analytics`.events"]
)
@pytest.mark.parametrize("reader", [False, True])
def test_table_rejects_names_that_lose_case_before_querying(table_name, reader):
    cursor = Mock()
    cursor.description = [("name",), ("type",)]
    cursor.fetchall.return_value = [("value", "Int64")]
    connection = Mock()
    connection.cursor.return_value = cursor
    session = ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()

    with pytest.raises(UnsupportedOperationError, match="Case-sensitive table names"):
        (session.read if reader else session).table(table_name)

    cursor.execute.assert_not_called()


def test_mixed_case_table_raw_sql_workaround():
    chdb = pytest.importorskip("chdb")
    connection = chdb.dbapi.connect()
    try:
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE "Events" (value Int64) ENGINE = Memory')
        cursor.execute('INSERT INTO "Events" VALUES (42)')
        session = ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()

        with pytest.raises(UnsupportedOperationError, match="Case-sensitive table names"):
            session.table("`Events`")
        rows = session.sql(
            "SELECT value FROM `Events` /* sqlglot.meta case_sensitive */", qualify=False
        ).collect()

        assert [row.asDict() for row in rows] == [{"value": 42}]
    finally:
        connection.close()
