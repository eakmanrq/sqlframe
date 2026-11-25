import datetime
from decimal import Decimal

import duckdb
import pytest

from sqlframe.duckdb import DuckDBSession
from sqlframe.duckdb import functions as F
from sqlframe.duckdb import types as T

pytest_plugins = ["tests.integration.fixtures"]
pytestmark = [pytest.mark.duckdb, pytest.mark.local]


@pytest.mark.parametrize("annotated", [True, False])
@pytest.mark.parametrize("kwargs", [{}, {"returnType": None}])
def test_udf_register_defaults_to_string(duckdb_session: DuckDBSession, annotated, kwargs):
    def multi(x: int, y: int) -> int:
        return x * y

    f = multi if annotated else lambda x, y: x * y
    duckdb_session.udf.register("multi", f, **kwargs)

    result = duckdb_session.sql("SELECT multi(1, 2) AS value")
    assert result.collect() == [("2",)]
    assert result.schema.fields[0].dataType == T.StringType()


def test_udf_register_sqlframe_string_type(duckdb_session: DuckDBSession):
    def shout(s: str) -> str:
        return s.upper() + "!"

    duckdb_session.udf.register("shout", shout, T.StringType())
    df = duckdb_session.createDataFrame([(1, "alice"), (2, "bob"), (3, "carol")], ["id", "name"])

    result = df.select("id", F.call_function("shout", F.col("name")).alias("shouted"))

    assert result.orderBy("id").collect() == [(1, "ALICE!"), (2, "BOB!"), (3, "CAROL!")]


@pytest.mark.parametrize(
    "return_type,value,expected",
    [
        (T.IntegerType(), 42, 42),
        (T.LongType(), 2**40, 2**40),
        (T.BooleanType(), True, True),
        (T.DoubleType(), 1.5, 1.5),
        (T.DecimalType(10, 2), Decimal("12.34"), 12.34),
        (T.DateType(), datetime.date(2026, 1, 2), datetime.date(2026, 1, 2)),
        (T.ArrayType(T.StringType()), ["a", "b"], ["a", "b"]),
        (T.MapType(T.IntegerType(), T.StringType()), {1: "a"}, {1: "a"}),
        (
            T.StructType(
                [T.StructField("name", T.StringType()), T.StructField("n", T.IntegerType())]
            ),
            {"name": "alice", "n": 2},
            T.Row(name="alice", n=2),
        ),
    ],
)
def test_udf_register_sqlframe_return_types(
    duckdb_session: DuckDBSession, return_type, value, expected
):
    duckdb_session.udf.register("constant_value", lambda: value, return_type)

    result = duckdb_session.sql("SELECT constant_value() AS value")
    assert result.collect() == [(expected,)]
    assert result.schema.fields[0].dataType == return_type


@pytest.mark.parametrize("return_type", [T.IntegerType(), "INTEGER", duckdb.sqltype("INTEGER")])
def test_udf_register_explicit_return_type(duckdb_session: DuckDBSession, return_type):
    duckdb_session.udf.register("multi", lambda x, y: x * y, return_type)

    assert duckdb_session.sql("SELECT multi(1, 2)").collect() == [(2,)]
