from unittest.mock import Mock

import pytest

from sqlframe.base import functions as base_functions
from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.clickhouse import ClickHouseSession
from sqlframe.clickhouse import functions as F


@pytest.mark.parametrize(
    "name,args",
    [
        ("translate", ("value", "a", "b")),
        ("dayofweek", ("value",)),
        ("datediff", ("value", "value")),
        ("date_diff", ("value", "value")),
        ("to_timestamp", ("value",)),
        ("unix_timestamp", ("value",)),
        ("from_unixtime", ("value",)),
        ("sort_array", ("value",)),
        ("hash", ("value",)),
        ("format_number", ("value", 2)),
        ("monotonically_increasing_id", ()),
        ("struct", ("value",)),
        ("to_json", ("value",)),
        ("sha2", ("value", 256)),
    ],
)
def test_known_unsupported_functions_fail_before_query(name, args):
    connection = Mock()
    ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()
    assert "clickhouse" in getattr(base_functions, name).unsupported_engines

    with pytest.raises(UnsupportedOperationError, match=rf"{name}.*ClickHouse"):
        getattr(F, name)(*args)

    connection.cursor.assert_not_called()
    assert name not in vars(F)


def test_supported_functions_remain_available():
    ClickHouseSession.builder.getOrCreate()
    assert F.upper is base_functions.upper
    assert F.upper("value").sql(dialect="clickhouse")
    assert "ARRAY_SORT" in F.array_sort("value").sql(dialect="spark")


def test_array_sort_comparator_is_unsupported():
    ClickHouseSession.builder.getOrCreate()
    with pytest.raises(UnsupportedOperationError, match="array_sort comparator"):
        F.array_sort("value", lambda left, right: left - right)


def test_array_sort_comparator_in_raw_expression_is_unsupported():
    connection = Mock()
    session = ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()
    frame = session.createDataFrame([([3, 1, 2],)], ["items"])

    with pytest.raises(UnsupportedOperationError, match="array_sort comparator"):
        frame.select(F.expr("array_sort(items, (l, r) -> r - l)")).sql(dialect="clickhouse")
    with pytest.raises(UnsupportedOperationError, match="array_sort comparator"):
        session.sql("SELECT array_sort(items, (l, r) -> r - l) FROM events", qualify=False).sql(
            dialect="clickhouse", optimize=False
        )

    connection.cursor.assert_not_called()


def test_unknown_function_is_an_attribute_error():
    with pytest.raises(AttributeError):
        getattr(F, "not_a_sqlframe_function")
