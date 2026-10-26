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
