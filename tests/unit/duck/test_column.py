import pytest

from sqlframe.duckdb import DuckDBSession
from sqlframe.duckdb import functions as F


@pytest.mark.parametrize("accessor", ["getItem", "getitem", "element_at"])
@pytest.mark.parametrize("key, expected", [(10, 1), (0, 7), (-1, 9), (11, None)])
@pytest.mark.parametrize("column_key", [False, True])
def test_integer_map_key_lookup(accessor, key, expected, column_key):
    session = DuckDBSession()
    session._conn.execute("CREATE TABLE maps AS SELECT MAP {10: 1, 0: 7, -1: 9} AS m")
    df = session.table("maps").withColumn("key", F.lit(key))
    lookup_key = F.col("key") if column_key else key
    if accessor == "element_at":
        value = F.element_at("m", lookup_key)
    elif accessor == "getitem":
        value = F.col("m")[lookup_key]
    else:
        value = F.col("m").getItem(lookup_key)

    assert df.select(value.alias("v")).collect() == [(expected,)]


def test_nested_integer_map_key_lookup():
    session = DuckDBSession()
    session._conn.execute("CREATE TABLE maps AS SELECT MAP {10: MAP {0: [4, 5]}} AS m")
    df = session.table("maps")

    assert df.select(F.col("m")[10][0][1].alias("v")).collect() == [(5,)]


def test_array_indexing_preserves_api_offsets():
    session = DuckDBSession()
    df = session.range(1).select(F.lit([4, 5, 6]).alias("a"))

    assert df.select(
        F.col("a")[0], F.col("a").getItem(1), F.element_at("a", 1), F.element_at("a", -1)
    ).collect() == [(4, 5, 4, 6)]
