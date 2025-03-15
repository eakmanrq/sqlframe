from __future__ import annotations

import datetime
import math
import typing as t
from collections import Counter
from decimal import Decimal

import pytest
import pytz
from pyspark.sql import SparkSession as PySparkSession
from sqlglot import exp

from sqlframe.base.session import _BaseSession
from sqlframe.base.types import Row
from sqlframe.base.util import dialect_to_string
from sqlframe.base.util import (
    get_func_from_session as get_func_from_session_without_fallback,
)
from sqlframe.bigquery import BigQuerySession
from sqlframe.databricks import DatabricksSession
from sqlframe.duckdb import DuckDBCatalog, DuckDBSession
from sqlframe.postgres import PostgresDataFrame, PostgresSession
from sqlframe.snowflake import SnowflakeSession
from sqlframe.spark.session import SparkSession

if t.TYPE_CHECKING:
    from sqlframe.base.dataframe import BaseDataFrame

pytest_plugins = ["tests.integration.fixtures"]


class GetDfAndFuncCallable(t.Protocol):
    def __call__(
        self, name: str, limit: t.Optional[int] = None
    ) -> t.Tuple[BaseDataFrame, t.Callable]: ...


def get_func_from_session(name: str, session: t.Union[PySparkSession, _BaseSession]) -> t.Callable:
    return get_func_from_session_without_fallback(name, session, fallback=False)


@pytest.fixture
def get_session_and_func(
    get_engine_session_and_spark,
) -> t.Callable[[str], t.Tuple[_BaseSession, t.Callable]]:
    def _get_session_and_func(name: str) -> t.Tuple[_BaseSession, t.Callable]:
        session = get_engine_session_and_spark()
        try:
            return session, get_func_from_session(name, session)
        except AttributeError:
            dialect = (
                "pyspark"
                if isinstance(session, PySparkSession)
                else dialect_to_string(session.input_dialect)
            )
            pytest.skip(f"{dialect} does not support {name}")

    return _get_session_and_func


@pytest.fixture
def get_func() -> t.Callable[[str, t.Union[PySparkSession, _BaseSession]], t.Callable]:
    def _get_func(name: str, session: t.Union[PySparkSession, _BaseSession]) -> t.Callable:
        try:
            return get_func_from_session(name, session)
        except AttributeError:
            dialect = (
                "pyspark"
                if isinstance(session, PySparkSession)
                else dialect_to_string(session.input_dialect)
            )
            pytest.skip(f"{dialect} does not support {name}")

    return _get_func


@pytest.fixture
def get_window() -> t.Callable:
    def _get_window(session: t.Union[PySparkSession, _BaseSession]) -> t.Callable:
        if isinstance(session, PySparkSession):
            from pyspark.sql import Window

            return Window
        from sqlframe.base.window import Window  # type: ignore

        return Window

    return _get_window


@pytest.fixture
def get_types() -> t.Callable:
    def _get_types(session: t.Union[PySparkSession, _BaseSession]) -> t.Any:
        if isinstance(session, PySparkSession):
            from pyspark.sql import types

            return types
        from sqlframe.base import types  # type: ignore

        return types

    return _get_types


@pytest.mark.parametrize(
    "arg, expected",
    [
        ("test", "test"),
        (30, 30),
        (10.10, 10.1),
        (False, False),
        (None, None),
        ([1, 2, 3], [1, 2, 3]),
        (datetime.date(2022, 1, 1), datetime.date(2022, 1, 1)),
        (datetime.datetime(2022, 1, 1, 1, 1, 1), datetime.datetime(2022, 1, 1, 1, 1, 1)),
        (
            datetime.datetime(2022, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
            datetime.datetime(2022, 1, 1, 1, 1, 1),
        ),
        ({"cola": 1}, {"cola": 1}),
        (Row(**{"cola": 1, "colb": "test"}), Row(**{"cola": 1, "colb": "test"})),
    ],
)
def test_lit(get_session_and_func, arg, expected):
    session, lit = get_session_and_func("lit")
    if isinstance(session, PostgresSession):
        if isinstance(arg, dict):
            pytest.skip("Postgres doesn't support map types")
        if isinstance(arg, Row):
            pytest.skip("Postgres doesn't support struct types")
    if isinstance(session, PySparkSession):
        if isinstance(arg, dict):
            pytest.skip("PySpark doesn't literal dict types")
        if isinstance(arg, Row):
            pytest.skip("PySpark doesn't support literal row types")
    if isinstance(session, BigQuerySession):
        if isinstance(arg, dict):
            pytest.skip("BigQuery doesn't support map types")
    if isinstance(session, SnowflakeSession):
        if isinstance(arg, Row):
            pytest.skip("Snowflake doesn't support literal row types")
    if isinstance(session, DuckDBSession):
        if isinstance(arg, dict):
            expected = Row(**expected)
    assert session.range(1).select(lit(arg).alias("test")).collect() == [Row(test=expected)]


@pytest.mark.parametrize(
    "input, output",
    [
        ("employee_id", "employee_id"),
        ("employee id", "employee id"),
    ],
)
def test_col(get_session_and_func, input, output):
    session, col = get_session_and_func("col")
    df = session.createDataFrame([(1,)], schema=[input])
    result = df.select(col(input)).first()
    assert result[0] == 1
    assert result.__fields__[0] == output


@pytest.mark.parametrize(
    "arg, expected",
    [
        (1, "bigint"),
        (2.0, "double"),
        ("foo", "string"),
        ({"a": 1}, "map<string,bigint>"),
        ([1, 2, 3], "array<bigint>"),
        (Row(a=1), "struct<a:bigint>"),
        (datetime.date(2022, 1, 1), "date"),
        (datetime.datetime(2022, 1, 1, 0, 0, 0), "timestamptz"),
        (datetime.datetime(2022, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc), "timestamptz"),
        (True, "boolean"),
        (bytes("test", "utf-8"), "binary"),
    ],
)
def test_typeof(get_session_and_func, get_types, arg, expected):
    session, typeof = get_session_and_func("typeof")
    # If we just pass a struct in for values then Spark will automatically explode the struct into columns
    # it won't do this though if there is another column so that is why we include an ignore column
    df = session.createDataFrame([(1, arg)], schema=["ignore_col", "col"])
    dialect = (
        "spark"
        if isinstance(session, PySparkSession)
        else dialect_to_string(session.execution_dialect)
    )
    if isinstance(session, (SparkSession, PySparkSession, DatabricksSession)):
        if expected == "timestamptz":
            expected = "timestamp"
    if isinstance(session, DuckDBSession):
        if expected == "binary":
            pytest.skip("DuckDB doesn't support binary")
        expected = expected.replace("string", "varchar").replace("struct<a:", "struct<a ")
    if isinstance(session, BigQuerySession):
        if expected.startswith("map"):
            pytest.skip("BigQuery doesn't support map types")
        if "<" in expected:
            expected = expected.split("<")[0]
        if expected == "binary":
            pytest.skip("BigQuery doesn't support binary")
    if isinstance(session, PostgresSession):
        if expected.startswith("map"):
            pytest.skip("Postgres doesn't support map types")
        elif expected.startswith("struct"):
            pytest.skip("Postgres doesn't support struct types")
        elif expected == "binary":
            pytest.skip("Postgres doesn't support binary")
    if isinstance(session, SnowflakeSession):
        if expected == "bigint":
            expected = "int"
        elif expected == "string":
            expected = "varchar"
        elif expected.startswith("map") or expected.startswith("struct"):
            expected = "object"
        elif expected.startswith("array"):
            pytest.skip("Snowflake doesn't handle arrays properly in values clause")
    result = df.select(typeof("col").alias("test")).first()[0]
    assert exp.DataType.build(result, dialect=dialect) == exp.DataType.build(
        expected, dialect=dialect
    )


def test_alias(get_session_and_func):
    session, col = get_session_and_func("col")
    df = session.createDataFrame([(1,)], schema=["employee_id"])
    assert df.select(col("employee_id").alias("test")).first().__fields__[0] == "test"
    space_result = df.select(col("employee_id").alias("A Space In New Name")).first().__fields__[0]
    assert space_result == "A Space In New Name"


def test_asc(get_session_and_func):
    session, asc = get_session_and_func("asc")
    df = session.range(5)
    assert df.orderBy(asc("id")).collect() == [
        Row(id=0),
        Row(id=1),
        Row(id=2),
        Row(id=3),
        Row(id=4),
    ]


def test_desc(get_session_and_func):
    session, desc = get_session_and_func("desc")
    df = session.range(5)
    assert df.sort(desc("id")).collect() == [
        Row(id=4),
        Row(id=3),
        Row(id=2),
        Row(id=1),
        Row(id=0),
    ]


def test_abs(get_session_and_func, get_func):
    session, abs = get_session_and_func("abs")
    lit = get_func("lit", session)
    df = session.range(1)
    assert df.select(abs(lit(-1))).first()[0] == 1


def test_max(get_session_and_func, get_func):
    session, max = get_session_and_func("max")
    col = get_func("col", session)
    df = session.range(10)
    assert df.select(max(col("id"))).first()[0] == 9


def test_min(get_session_and_func):
    session, min = get_session_and_func("min")
    df = session.range(10)
    assert df.select(min(df.id)).first()[0] == 0


def test_max_by(get_session_and_func):
    session, max_by = get_session_and_func("max_by")
    df = session.createDataFrame(
        [
            ("Java", 2012, 20000),
            ("dotNET", 2012, 5000),
            ("dotNET", 2013, 48000),
            ("Java", 2013, 30000),
        ],
        schema=("course", "year", "earnings"),
    )
    assert df.groupby("course").agg(max_by("year", "earnings")).orderBy("course").collect() == [
        Row(value="Java", value2=2013),
        Row(value="dotNET", value2=2013),
    ]


def test_min_by(get_session_and_func):
    session, min_by = get_session_and_func("min_by")
    df = session.createDataFrame(
        [
            ("Java", 2012, 20000),
            ("dotNET", 2012, 5000),
            ("dotNET", 2013, 48000),
            ("Java", 2013, 30000),
        ],
        schema=("course", "year", "earnings"),
    )
    assert df.groupby("course").agg(min_by("year", "earnings")).orderBy("course").collect() == [
        Row(value="Java", value2=2012),
        Row(value="dotNET", value2=2012),
    ]


def test_count(get_session_and_func, get_func):
    session, count = get_session_and_func("count")
    expr = get_func("expr", session)
    df_example = session.createDataFrame([(None,), (1,), (2,), (3,)], schema=["numbers"])
    assert df_example.select(
        count(expr("*")), count(df_example.numbers).alias("value2")
    ).collect() == [Row(first_count=4, second_count=3)]


def test_sum(get_session_and_func):
    session, sum = get_session_and_func("sum")
    df = session.range(10)
    assert df.select(sum(df["id"])).collect() == [Row(sum_id=45)]


def test_avg(get_session_and_func):
    session, avg = get_session_and_func("avg")
    df = session.range(10)
    assert df.select(avg(df["id"])).collect() == [Row(avg_id=4.5)]


def test_mean(get_session_and_func):
    session, mean = get_session_and_func("mean")
    df = session.range(10)
    assert df.select(mean(df["id"])).collect() == [Row(mean_id=4.5)]


def test_sum_distinct(get_session_and_func):
    session, sum_distinct = get_session_and_func("sum_distinct")
    df = session.createDataFrame([(None,), (1,), (1,), (2,)], schema=["numbers"])
    assert df.select(sum_distinct(df["numbers"])).collect() == [Row(result=3)]


def test_acos(get_session_and_func):
    session, acos = get_session_and_func("acos")
    df = session.range(-1, 2)
    results = df.select(acos(df.id)).collect()
    assert math.isclose(results[0][0], 3.141592653589793)
    assert math.isclose(results[1][0], 1.5707963267948966)
    assert results[2][0] == 0.0


def test_acosh(get_session_and_func):
    session, acosh = get_session_and_func("acosh")
    df = session.range(1, 2)
    assert df.select(acosh(df.id)).collect() == [Row(acosh=0.0)]


def test_asin(get_session_and_func):
    session, asin = get_session_and_func("asin")
    # Original: df.select(asin(df.schema.fieldNames()[0]))
    df = session.createDataFrame([(0,), (1,)], schema=["numbers"])
    results = df.select(asin(df.numbers)).collect()
    assert results[0][0] == 0.0
    assert math.isclose(results[1][0], 1.5707963267948966)


def test_asinh(get_session_and_func):
    session, asinh = get_session_and_func("asinh")
    df = session.range(1)
    assert df.select(asinh("id")).collect() == [
        Row(id=0.0),
    ]


def test_atan(get_session_and_func):
    session, atan = get_session_and_func("atan")
    df = session.range(1)
    assert df.select(atan("id")).collect() == [
        Row(id=0.0),
    ]


def test_atan2(get_session_and_func, get_func):
    session, atan2 = get_session_and_func("atan2")
    lit = get_func("lit", session)
    df = session.range(1)
    assert math.isclose(df.select(atan2(lit(1), lit(2))).first()[0], 0.4636476090008061)


def test_atanh(get_session_and_func):
    session, atanh = get_session_and_func("atanh")
    df = session.createDataFrame([(0,)], schema=["numbers"])
    assert df.select(atanh(df["numbers"])).collect() == [
        Row(value=0.0),
    ]


def test_cbrt(get_session_and_func, get_func):
    session, cbrt = get_session_and_func("cbrt")
    lit = get_func("lit", session)
    df = session.range(1)
    assert math.isclose(df.select(cbrt(lit(27))).first()[0], 3.0)


def test_ceil(get_session_and_func, get_func):
    session, ceil = get_session_and_func("ceil")
    lit = get_func("lit", session)
    df = session.range(1)
    assert df.select(ceil(lit(-0.1))).collect() == [
        Row(value=0.0),
    ]


def test_ceiling(get_session_and_func, get_func):
    session, ceiling = get_session_and_func("ceiling")
    lit = get_func("lit", session)
    df = session.range(1)
    assert df.select(ceiling(lit(-0.1))).collect() == [
        Row(value=0.0),
    ]


def test_cos(get_session_and_func, get_func):
    session, cos = get_session_and_func("cos")
    lit = get_func("lit", session)
    df = session.range(1)
    assert df.select(cos(lit(math.pi))).first() == Row(value=-1.0)


def test_cosh(get_session_and_func, get_func):
    session, cosh = get_session_and_func("cosh")
    lit = get_func("lit", session)
    df = session.range(1)
    assert math.isclose(df.select(cosh(lit(1))).first()[0], Row(value=1.543080634815244)[0])


def test_cot(get_session_and_func, get_func):
    session, cot = get_session_and_func("cot")
    lit = get_func("lit", session)
    df = session.range(1)
    assert math.isclose(df.select(cot(lit(math.radians(45)))).first()[0], Row(value=1.0)[0])


def test_csc(get_session_and_func, get_func):
    session, csc = get_session_and_func("csc")
    lit = get_func("lit", session)
    df = session.range(1)
    assert df.select(csc(lit(math.radians(90)))).first() == Row(value=1.0)


def test_e(get_session_and_func):
    session, e = get_session_and_func("e")
    df = session.range(1)
    assert df.select(e()).collect() == [
        Row(value=2.718281828459045),
    ]


def test_exp(get_session_and_func, get_func):
    session, exp = get_session_and_func("exp")
    lit = get_func("lit", session)
    df = session.range(1)
    assert df.select(exp(lit(0))).collect() == [
        Row(value=1.0),
    ]


def test_expm1(get_session_and_func, get_func):
    session, expm1 = get_session_and_func("expm1")
    lit = get_func("lit", session)
    df = session.range(1)
    assert math.isclose(df.select(expm1(lit(1))).first()[0], 1.718281828459045)


def test_factorial(get_session_and_func):
    session, factorial = get_session_and_func("factorial")
    df = session.createDataFrame([(5,)], ["n"])
    assert df.select(factorial(df.n).alias("f")).collect() == [
        Row(value=120),
    ]


def test_floor(get_session_and_func, get_func):
    session, floor = get_session_and_func("floor")
    lit = get_func("lit", session)
    df = session.range(1)
    assert df.select(floor(lit(2.5))).collect() == [
        Row(value=2.0),
    ]


def test_log(get_session_and_func):
    session, log = get_session_and_func("log")
    df = session.createDataFrame([(1,), (2,), (4,)], schema=["value"])
    assert df.select(log(2.0, df.value).alias("log2_value")).collect() == [
        Row(log2_value=0.0),
        Row(log2_value=1.0),
        Row(log2_value=2.0),
    ]


def test_log10(get_session_and_func, get_func):
    session, log10 = get_session_and_func("log10")
    lit = get_func("lit", session)
    df = session.range(1)
    assert df.select(log10(lit(100))).collect() == [
        Row(value=2.0),
    ]


def test_log1p(get_session_and_func, get_func):
    session, log1p = get_session_and_func("log1p")
    e = get_func("e", session)
    df = session.range(1)
    assert math.isclose(df.select(log1p(e())).first()[0], 1.3132616875182228)


def test_log2(get_session_and_func):
    session, log2 = get_session_and_func("log2")
    df = session.createDataFrame([(4,)], schema=["a"])
    assert df.select(log2("a").alias("log2")).collect() == [
        Row(log2=2.0),
    ]


def test_rint(get_session_and_func, get_func):
    session, rint = get_session_and_func("rint")
    lit = get_func("lit", session)
    df = session.range(1)
    assert df.select(rint(lit(10.6))).collect() == [
        Row(value=11.0),
    ]
    assert df.select(rint(lit(10.3))).collect() == [
        Row(value=10.0),
    ]


def test_sec(get_session_and_func, get_func):
    session, sec = get_session_and_func("sec")
    lit = get_func("lit", session)
    df = session.range(1)
    assert math.isclose(df.select(sec(lit(1.5))).first()[0], 14.136832902969903)


def test_signum(get_session_and_func, get_func):
    session, signum = get_session_and_func("signum")
    lit = get_func("lit", session)
    df = session.range(1)
    assert df.select(signum(lit(-5)), signum(lit(6)).alias("value2")).collect() == [
        Row(value1=-1.0, value2=1.0),
    ]


def test_sin(get_session_and_func, get_func):
    session, sin = get_session_and_func("sin")
    lit = get_func("lit", session)
    df = session.range(1)
    assert df.select(sin(lit(math.radians(90)))).collect() == [
        Row(value=1.0),
    ]


def test_sinh(get_session_and_func, get_func):
    session, sinh = get_session_and_func("sinh")
    lit = get_func("lit", session)
    df = session.range(1)
    assert math.isclose(df.select(sinh(lit(1.1))).first()[0], 1.335647470124177)


def test_tan(get_session_and_func, get_func):
    session, tan = get_session_and_func("tan")
    lit = get_func("lit", session)
    df = session.range(1)
    assert math.isclose(df.select(tan(lit(math.radians(45)))).first()[0], 0.9999999999999999)


def test_tanh(get_session_and_func, get_func):
    session, tanh = get_session_and_func("tanh")
    lit = get_func("lit", session)
    df = session.range(1)
    assert math.isclose(df.select(tanh(lit(math.radians(90)))).first()[0], 0.9171523356672744)


def test_degrees(get_session_and_func, get_func):
    session, degrees = get_session_and_func("degrees")
    lit = get_func("lit", session)
    df = session.range(1)
    assert df.select(degrees(lit(math.pi))).collect() == [
        Row(value=180.0),
    ]


def test_radians(get_session_and_func, get_func):
    session, radians = get_session_and_func("radians")
    lit = get_func("lit", session)
    df = session.range(1)
    assert math.isclose(df.select(radians(lit(180))).first()[0], math.pi)


def test_bitwise_not(get_session_and_func, get_func):
    session, bitwise_not = get_session_and_func("bitwise_not")
    lit = get_func("lit", session)
    df = session.range(1)
    assert df.select(bitwise_not(lit(0))).collect() == [
        Row(value=-1),
    ]
    assert df.select(bitwise_not(lit(1))).collect() == [
        Row(value=-2),
    ]


def test_asc_nulls_first(get_session_and_func):
    session, asc_nulls_first = get_session_and_func("asc_nulls_first")
    df = session.createDataFrame([(1, "Bob"), (0, None), (2, "Alice")], ["age", "name"])
    assert df.sort(asc_nulls_first(df.name)).collect() == [
        Row(age=0, name=None),
        Row(age=2, name="Alice"),
        Row(age=1, name="Bob"),
    ]


def test_asc_nulls_last(get_session_and_func):
    session, asc_nulls_last = get_session_and_func("asc_nulls_last")
    df = session.createDataFrame([(1, "Bob"), (0, None), (2, "Alice")], ["age", "name"])
    assert df.sort(asc_nulls_last(df.name)).collect() == [
        Row(age=2, name="Alice"),
        Row(age=1, name="Bob"),
        Row(age=0, name=None),
    ]


def test_desc_nulls_first(get_session_and_func):
    session, desc_nulls_first = get_session_and_func("desc_nulls_first")
    df = session.createDataFrame([(1, "Bob"), (0, None), (2, "Alice")], ["age", "name"])
    assert df.sort(desc_nulls_first(df.name)).collect() == [
        Row(age=0, name=None),
        Row(age=1, name="Bob"),
        Row(age=2, name="Alice"),
    ]


def test_desc_nulls_last(get_session_and_func):
    session, desc_nulls_last = get_session_and_func("desc_nulls_last")
    df = session.createDataFrame([(1, "Bob"), (0, None), (2, "Alice")], ["age", "name"])
    assert df.sort(desc_nulls_last(df.name)).collect() == [
        Row(age=1, name="Bob"),
        Row(age=2, name="Alice"),
        Row(age=0, name=None),
    ]


def test_stddev(get_session_and_func):
    session, stddev = get_session_and_func("stddev")
    df = session.range(6)
    assert math.isclose(df.select(stddev("id")).first()[0], 1.8708286933869707)


def test_stddev_samp(get_session_and_func):
    session, stddev_samp = get_session_and_func("stddev_samp")
    df = session.range(6)
    assert math.isclose(df.select(stddev_samp("id")).first()[0], 1.8708286933869707)


def test_stddev_pop(get_session_and_func):
    session, stddev_pop = get_session_and_func("stddev_pop")
    df = session.range(6)
    assert round(df.select(stddev_pop("id")).first()[0], 4) == 1.7078


def test_variance(get_session_and_func):
    session, variance = get_session_and_func("variance")
    df = session.range(6)
    assert df.select(variance("id")).first() == Row(value=3.5)


def test_var_samp(get_session_and_func):
    session, var_samp = get_session_and_func("var_samp")
    df = session.range(6)
    assert df.select(var_samp(df.id)).first() == Row(value=3.5)


def test_var_pop(get_session_and_func):
    session, var_pop = get_session_and_func("var_pop")
    df = session.range(6)
    assert round(df.select(var_pop(df.id)).first()[0], 4) == 2.9167


def test_skewness(get_session_and_func):
    session, skewness = get_session_and_func("skewness")
    df = session.createDataFrame([[1], [1], [2]], ["c"])
    assert math.isclose(df.select(skewness(df.c)).first()[0], 0.7071067811865475, rel_tol=1e-5)


def test_kurtosis(get_session_and_func):
    session, kurtosis = get_session_and_func("kurtosis")
    if isinstance(session, SnowflakeSession):
        df = session.createDataFrame([[1], [1], [2], [3]], ["c"])
        assert math.isclose(df.select(kurtosis("c")).first()[0], -1.289265078884)
    else:
        df = session.createDataFrame([[1], [1], [2]], ["c"])
        assert math.isclose(df.select(kurtosis(df.c)).first()[0], -1.5)


def test_collect_list(get_session_and_func):
    session, collect_list = get_session_and_func("collect_list")
    df = session.createDataFrame([(2,), (5,), (5,)], ("age",))
    assert df.select(collect_list(df.age)).collect() == [Row(collect_list=[2, 5, 5])]


def test_collect_set(get_session_and_func):
    session, collect_set = get_session_and_func("collect_set")
    df = session.createDataFrame([(2,), (5,), (5,)], ("age",))
    assert sorted(df.select(collect_set(df.age)).first()[0]) == [2, 5]


def test_hypot(get_session_and_func, get_func):
    session, hypot = get_session_and_func("hypot")
    lit = get_func("lit", session)
    df = session.range(1)
    assert math.isclose(df.select(hypot(lit(1), lit(2))).first()[0], 2.23606797749979)


def test_pow(get_session_and_func, get_func):
    session, pow = get_session_and_func("pow")
    lit = get_func("lit", session)
    df = session.range(1)
    assert df.select(pow(lit(3), lit(2))).first()[0] == 9.0


def test_power(get_session_and_func, get_func):
    session, power = get_session_and_func("power")
    lit = get_func("lit", session)
    df = session.range(1)
    assert df.select(power(lit(3), lit(2))).first()[0] == 9.0


def test_row_number(get_session_and_func, get_window):
    session, row_number = get_session_and_func("row_number")
    df = session.range(3)
    Window = get_window(session)
    w = Window.orderBy(df.id.desc())
    assert df.withColumn("desc_order", row_number().over(w)).collect() == [
        Row(id=2, desc_order=1),
        Row(id=1, desc_order=2),
        Row(id=0, desc_order=3),
    ]


def test_dense_rank(get_session_and_func, get_window):
    session, dense_rank = get_session_and_func("dense_rank")
    df = session.createDataFrame([1, 1, 2, 3, 3, 4], "int")
    Window = get_window(session)
    w = Window.orderBy("value")
    assert df.withColumn("drank", dense_rank().over(w)).collect() == [
        Row(value=1, drank=1),
        Row(value=1, drank=1),
        Row(value=2, drank=2),
        Row(value=3, drank=3),
        Row(value=3, drank=3),
        Row(value=4, drank=4),
    ]


def test_rank(get_session_and_func, get_window):
    session, rank = get_session_and_func("rank")
    df = session.createDataFrame([1, 1, 2, 3, 3, 4], "int")
    Window = get_window(session)
    w = Window.orderBy("value")
    assert df.withColumn("drank", rank().over(w)).collect() == [
        Row(value=1, drank=1),
        Row(value=1, drank=1),
        Row(value=2, drank=3),
        Row(value=3, drank=4),
        Row(value=3, drank=4),
        Row(value=4, drank=6),
    ]


def test_cume_dist(get_session_and_func, get_window):
    session, cume_dist = get_session_and_func("cume_dist")
    df = session.createDataFrame([1, 2, 3, 3, 4], "int")
    Window = get_window(session)
    w = Window.orderBy("value")
    assert df.withColumn("cd", cume_dist().over(w)).collect() == [
        Row(value=1, drank=0.2),
        Row(value=2, drank=0.4),
        Row(value=3, drank=0.8),
        Row(value=3, drank=0.8),
        Row(value=4, drank=1.0),
    ]


def test_percent_rank(get_session_and_func, get_window):
    session, percent_rank = get_session_and_func("percent_rank")
    df = session.createDataFrame([1, 1, 2, 3, 3, 4], "int")
    Window = get_window(session)
    w = Window.orderBy("value")
    assert df.withColumn("pr", percent_rank().over(w)).collect() == [
        Row(value=1, drank=0.0),
        Row(value=1, drank=0.0),
        Row(value=2, drank=0.4),
        Row(value=3, drank=0.6),
        Row(value=3, drank=0.6),
        Row(value=4, drank=1.0),
    ]


def test_approx_count_distinct(get_session_and_func):
    session, approxCountDistinct = get_session_and_func("approxCountDistinct")
    df = session.createDataFrame([1, 2, 2, 3], "int")
    assert df.agg(approxCountDistinct("value").alias("distinct_values")).first()[0] == 3


def test_coalesce(get_session_and_func, get_func):
    session, coalesce = get_session_and_func("coalesce")
    lit = get_func("lit", session)
    df = session.createDataFrame([(None, None), (1, None), (None, 2)], ("a", "b"))
    assert df.select(coalesce(df["a"], df["b"])).collect() == [
        Row(value=None),
        Row(value=1),
        Row(value=2),
    ]
    assert df.select("*", coalesce(df["a"], lit(0.0))).collect() == [
        Row(a=None, b=None, value=0.0),
        Row(a=1, b=None, value=1),
        Row(a=None, b=2, value=0.0),
    ]


def test_corr(get_session_and_func):
    session, corr = get_session_and_func("corr")
    a = range(20)
    b = [2 * x for x in range(20)]
    df = session.createDataFrame(zip(a, b), ["a", "b"])
    assert math.isclose(df.agg(corr("a", "b").alias("c")).first()[0], 1.0)


def test_covar_pop(get_session_and_func):
    session, covar_pop = get_session_and_func("covar_pop")
    a = [1] * 10
    b = [1] * 10
    df = session.createDataFrame(zip(a, b), ["a", "b"])
    assert math.isclose(df.agg(covar_pop("a", "b").alias("c")).first()[0], 0.0)


def test_covar_samp(get_session_and_func):
    session, covar_samp = get_session_and_func("covar_samp")
    a = [1] * 10
    b = [1] * 10
    df = session.createDataFrame(zip(a, b), ["a", "b"])
    assert math.isclose(df.agg(covar_samp("a", "b").alias("c")).first()[0], 0.0)


def test_count_distinct(get_session_and_func):
    session, count_distinct = get_session_and_func("count_distinct")
    df1 = session.createDataFrame([1, 1, 3], "int")
    df2 = session.createDataFrame([1, 2], "int")
    df_joined = df1.join(df2)
    assert df_joined.select(count_distinct(df1.value, df2.value)).collect() == [Row(value=4)]


def test_first(get_session_and_func):
    session, first = get_session_and_func("first")
    df = session.createDataFrame([("Alice", 2), ("Bob", 5), ("Alice", None)], ("name", "age"))
    df = df.orderBy(df.age)
    if (
        isinstance(session, PySparkSession)
        or session.input_dialect.NULL_ORDERING == "nulls_are_small"
    ):
        assert df.groupby("name").agg(first("age")).orderBy("name").collect() == [
            Row(name="Alice", value=None),
            Row(name="Bob", value=5),
        ]
    elif session.input_dialect.NULL_ORDERING in ("nulls_are_last", "nulls_are_large"):
        assert df.groupby("name").agg(first("age")).orderBy("name").collect() == [
            Row(name="Alice", value=2),
            Row(name="Bob", value=5),
        ]
    else:
        raise RuntimeError(f"Got unexpected null_ordering: {session.input_dialect.NULL_ORDERING}")


def test_grouping_id(get_session_and_func, get_func):
    session, grouping_id = get_session_and_func("grouping_id")
    sum = get_func("sum", session)
    df = session.createDataFrame([(1, "a", "a"), (3, "a", "a"), (4, "b", "c")], ["c1", "c2", "c3"])
    result = df.cube("c2", "c3").agg(grouping_id(), sum("c1")).orderBy("c2", "c3").collect()
    if (
        isinstance(session, PySparkSession)
        or session.input_dialect.NULL_ORDERING == "nulls_are_small"
    ):
        assert result == [
            Row(c2=None, c3=None, value1=3, value2=8),
            Row(c2=None, c3="a", value1=2, value2=4),
            Row(c2=None, c3="c", value1=2, value2=4),
            Row(c2="a", c3=None, value1=1, value2=4),
            Row(c2="a", c3="a", value1=0, value2=4),
            Row(c2="b", c3=None, value1=1, value2=4),
            Row(c2="b", c3="c", value1=0, value2=4),
        ]
    elif session.input_dialect.NULL_ORDERING in ("nulls_are_last", "nulls_are_large"):
        assert result == [
            Row(c2="a", c3="a", value1=0, value2=4),
            Row(c2="a", c3=None, value1=1, value2=4),
            Row(c2="b", c3="c", value1=0, value2=4),
            Row(c2="b", c3=None, value1=1, value2=4),
            Row(c2=None, c3="a", value1=2, value2=4),
            Row(c2=None, c3="c", value1=2, value2=4),
            Row(c2=None, c3=None, value1=3, value2=8),
        ]
    else:
        raise RuntimeError(f"Got unexpected null_ordering: {session.input_dialect.NULL_ORDERING}")


def test_input_file_name(get_session_and_func, get_func):
    session, input_file_name = get_session_and_func("input_file_name")
    df = session.read.json("tests/fixtures/employee.json")
    assert df.select(input_file_name()).first()[0].endswith("tests/fixtures/employee.json")


def test_isnan(get_session_and_func):
    session, isnan = get_session_and_func("isnan")
    df = session.createDataFrame([(1.0, float("nan")), (float("nan"), 2.0)], ("a", "b"))
    assert df.select(isnan("a").alias("r1"), isnan(df.b).alias("r2")).collect() == [
        Row(r1=False, r2=True),
        Row(r1=True, r2=False),
    ]


def test_isnull(get_session_and_func):
    session, isnull = get_session_and_func("isnull")
    df = session.createDataFrame([(1, None), (None, 2)], ("a", "b"))
    assert df.select("a", "b", isnull("a").alias("r1"), isnull(df.b).alias("r2")).collect() == [
        Row(a=1, b=None, r1=False, r2=True),
        Row(a=None, b=2, r1=True, r2=False),
    ]


def test_last(get_session_and_func):
    session, last = get_session_and_func("last")
    df = session.createDataFrame([("Alice", 2), ("Bob", 5), ("Alice", None)], ("name", "age"))
    df = df.orderBy(df.age.desc())
    df.groupby("name").agg(last("age")).orderBy("name")
    assert df.groupby("name").agg(last("age")).orderBy("name").collect() == [
        Row(name="Alice", b=None),
        Row(name="Bob", b=5),
    ]


def test_nanvl(get_session_and_func):
    session, nanvl = get_session_and_func("nanvl")
    df = session.createDataFrame([(1.0, float("nan")), (float("nan"), 2.0)], ("a", "b"))
    assert df.select(nanvl("a", "b").alias("r1"), nanvl(df.a, df.b).alias("r2")).collect() == [
        Row(r1=1.0, r2=1.0),
        Row(r1=2.0, r2=2.0),
    ]


def test_randn(get_session_and_func):
    session, randn = get_session_and_func("randn")
    assert session.range(0, 2, 1, 1).withColumn("randn", randn(seed=42)).collect() == [
        Row(id=0, randn=2.384479054241165),
        Row(id=1, randn=0.1920934041293524),
    ]


def test_percentile_approx(get_session_and_func, get_func):
    session, percentile_approx = get_session_and_func("percentile_approx")
    col = get_func("sum", session)
    key = (col("id") % 3).alias("key")
    value = (42 + key * 10).alias("value")
    df = session.range(0, 1000, 1, 1).select(key, value)
    assert df.select(percentile_approx("value", 0.5, 1000000).alias("median")).collect() == [
        Row(value=42),
    ]
    assert df.select(
        percentile_approx("value", [0.25, 0.5, 0.75], 1000000).alias("median")
    ).collect() == [
        Row(value=[42, 42, 42]),
    ]


def test_percentile(get_session_and_func, get_func):
    session, percentile = get_session_and_func("percentile")
    col = get_func("sum", session)
    key = (col("id") % 3).alias("key")
    value = (42 + key * 10).alias("value")
    df = session.range(0, 1000, 1, 1).select(key, value)
    assert df.select(percentile("value", 0.5).alias("median")).collect() == [
        Row(value=42),
    ]
    if not isinstance(session, SnowflakeSession):
        assert df.select(percentile("value", [0.25, 0.5, 0.75]).alias("median")).collect() == [
            Row(value=[42, 42, 42]),
        ]


def test_rand(get_session_and_func):
    session, rand = get_session_and_func("rand")
    rows = session.range(0, 2, 1, 1).withColumn("rand", rand(seed=42) * 3).collect()
    assert len(rows) == 2
    for row in rows:
        assert len(row) == 2


def test_round(get_session_and_func):
    session, round = get_session_and_func("round")
    assert session.createDataFrame([(2.5,)], ["a"]).select(round("a", 0).alias("r")).collect() == [
        Row(r=3.0)
    ]


def test_bround(get_session_and_func, get_func):
    session, bround = get_session_and_func("bround")
    col = get_func("col", session)
    if isinstance(session, SnowflakeSession):
        # https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#label-data-types-for-fixed-point-numbers
        pytest.skip("Snowflake supports bround but the input must be a fixed-point number")
    assert session.createDataFrame([(2.5,)], ["a"]).select(bround("a", 0).alias("r")).collect() == [
        Row(r=2.0)
    ]


def test_shiftleft(get_session_and_func):
    session, shiftleft = get_session_and_func("shiftleft")
    assert session.createDataFrame([(21,)], ["a"]).select(
        shiftleft("a", 1).alias("r")
    ).collect() == [Row(r=42)]


def test_shiftright(get_session_and_func):
    session, shiftright = get_session_and_func("shiftright")
    assert session.createDataFrame([(42,)], ["a"]).select(
        shiftright("a", 1).alias("r")
    ).collect() == [Row(r=21)]


def test_shiftrightunsigned(get_session_and_func):
    session, shiftrightunsigned = get_session_and_func("shiftrightunsigned")
    df = session.createDataFrame([(-42,)], ["a"])
    assert df.select(shiftrightunsigned("a", 1).alias("r")).collect() == [
        Row(r=9223372036854775787)
    ]


def test_expr(get_session_and_func):
    session, expr = get_session_and_func("expr")
    df = session.createDataFrame([["Alice"], ["Bob"]], ["name"])
    assert df.select("name", expr("length(name)")).collect() == [
        Row(name="Alice", value=5),
        Row(name="Bob", value=3),
    ]


def test_struct(get_session_and_func):
    session, struct = get_session_and_func("struct")
    df = session.createDataFrame([("Alice", 2), ("Bob", 5)], ("name", "age"))
    expected = (
        [Row(value=Row(age=2, name="Alice")), Row(value=Row(age=5, name="Bob"))]
        if not isinstance(session, SnowflakeSession)
        else [Row(value={"age": 2, "name": "Alice"}), Row(value={"age": 5, "name": "Bob"})]
    )
    expected_field_names = ["age", "name"]
    result = df.select(struct("age", "name").alias("struct")).collect()
    assert result == expected
    if isinstance(session, (SparkSession, PySparkSession)):
        pass
    elif isinstance(session, SnowflakeSession):
        assert list(result[0][0]) == expected_field_names
    else:
        assert result[0][0]._unique_field_names == expected_field_names
    result = df.select(struct([df.age, df.name]).alias("struct")).collect()
    assert result == expected
    if isinstance(session, (SparkSession, PySparkSession)):
        pass
    elif isinstance(session, SnowflakeSession):
        assert list(result[0][0]) == expected_field_names
    else:
        assert result[0][0]._unique_field_names == expected_field_names


def test_greatest(get_session_and_func):
    session, greatest = get_session_and_func("greatest")
    df = session.createDataFrame([(1, 4, 3)], ["a", "b", "c"])
    assert df.select(greatest(df.a, df.b, df.c).alias("greatest")).collect() == [
        Row(greatest=4),
    ]


def test_least(get_session_and_func):
    session, least = get_session_and_func("least")
    df = session.createDataFrame([(1, 4, 3)], ["a", "b", "c"])
    assert df.select(least(df.a, df.b, df.c).alias("least")).collect() == [
        Row(least=1),
    ]


def test_when(get_session_and_func):
    session, when = get_session_and_func("when")
    df = session.range(3)
    assert df.select(when(df["id"] == 2, 3).otherwise(4).alias("age")).collect() == [
        Row(age=4),
        Row(age=4),
        Row(age=3),
    ]
    assert df.select(when(df.id == 2, df.id + 1).alias("age")).collect() == [
        Row(age=None),
        Row(age=None),
        Row(age=3),
    ]


def test_conv(get_session_and_func):
    session, conv = get_session_and_func("conv")
    df = session.createDataFrame([("010101",)], ["n"])
    assert df.select(conv(df.n, 2, 16).alias("hex")).collect() == [
        Row(hex="15"),
    ]


def test_lag(get_session_and_func, get_window):
    session, lag = get_session_and_func("lag")
    df = session.createDataFrame([("a", 1), ("a", 2), ("a", 3), ("b", 8), ("b", 2)], ["c1", "c2"])
    Window = get_window(session)
    w = Window.partitionBy("c1").orderBy("c2")
    assert df.withColumn("previous_value", lag("c2").over(w)).orderBy("c1", "c2").collect() == [
        Row(c1="a", c2=1, previous_value=None),
        Row(c1="a", c2=2, previous_value=1),
        Row(c1="a", c2=3, previous_value=2),
        Row(c1="b", c2=2, previous_value=None),
        Row(c1="b", c2=8, previous_value=2),
    ]
    assert df.withColumn("previous_value", lag("c2", 1, 0).over(w)).orderBy(
        "c1", "c2"
    ).collect() == [
        Row(c1="a", c2=1, previous_value=0),
        Row(c1="a", c2=2, previous_value=1),
        Row(c1="a", c2=3, previous_value=2),
        Row(c1="b", c2=2, previous_value=0),
        Row(c1="b", c2=8, previous_value=2),
    ]
    assert df.withColumn("previous_value", lag("c2", 2, -1).over(w)).orderBy(
        "c1", "c2"
    ).collect() == [
        Row(c1="a", c2=1, previous_value=-1),
        Row(c1="a", c2=2, previous_value=-1),
        Row(c1="a", c2=3, previous_value=1),
        Row(c1="b", c2=2, previous_value=-1),
        Row(c1="b", c2=8, previous_value=-1),
    ]


def test_lead(get_session_and_func, get_window):
    session, lead = get_session_and_func("lead")
    df = session.createDataFrame([("a", 1), ("a", 2), ("a", 3), ("b", 8), ("b", 2)], ["c1", "c2"])
    Window = get_window(session)
    w = Window.partitionBy("c1").orderBy("c2")
    assert df.withColumn("next_value", lead("c2").over(w)).orderBy("c1", "c2").collect() == [
        Row(c1="a", c2=1, next_value=2),
        Row(c1="a", c2=2, next_value=3),
        Row(c1="a", c2=3, next_value=None),
        Row(c1="b", c2=2, next_value=8),
        Row(c1="b", c2=8, next_value=None),
    ]
    assert df.withColumn("next_value", lead("c2", 1, 0).over(w)).orderBy("c1", "c2").collect() == [
        Row(c1="a", c2=1, next_value=2),
        Row(c1="a", c2=2, next_value=3),
        Row(c1="a", c2=3, next_value=0),
        Row(c1="b", c2=2, next_value=8),
        Row(c1="b", c2=8, next_value=0),
    ]
    assert df.withColumn("next_value", lead("c2", 2, -1).over(w)).orderBy("c1", "c2").collect() == [
        Row(c1="a", c2=1, next_value=3),
        Row(c1="a", c2=2, next_value=-1),
        Row(c1="a", c2=3, next_value=-1),
        Row(c1="b", c2=2, next_value=-1),
        Row(c1="b", c2=8, next_value=-1),
    ]


def test_nth_value(get_session_and_func, get_window):
    session, nth_value = get_session_and_func("nth_value")
    df = session.createDataFrame([("a", 1), ("a", 2), ("a", 3), ("b", 8), ("b", 2)], ["c1", "c2"])
    Window = get_window(session)
    w = Window.partitionBy("c1").orderBy("c2")
    assert df.withColumn("nth_value", nth_value("c2", 1).over(w)).orderBy("c1", "c2").collect() == [
        Row(c1="a", c2=1, nth_value=1),
        Row(c1="a", c2=2, nth_value=1),
        Row(c1="a", c2=3, nth_value=1),
        Row(c1="b", c2=2, nth_value=2),
        Row(c1="b", c2=8, nth_value=2),
    ]
    if isinstance(session, SnowflakeSession):
        assert df.withColumn("nth_value", nth_value("c2", 2).over(w)).orderBy(
            "c1", "c2"
        ).collect() == [
            # In spark since the 2nd value hasn't been seen yet (in the first row for example), then it returns Null
            # In Snowflake it will return the 2nd value regardless of the current row being processed
            Row(c1="a", c2=1, nth_value=2),
            Row(c1="a", c2=2, nth_value=2),
            Row(c1="a", c2=3, nth_value=2),
            Row(c1="b", c2=2, nth_value=8),
            Row(c1="b", c2=8, nth_value=8),
        ]
    else:
        assert df.withColumn("nth_value", nth_value("c2", 2).over(w)).orderBy(
            "c1", "c2"
        ).collect() == [
            Row(c1="a", c2=1, nth_value=None),
            Row(c1="a", c2=2, nth_value=2),
            Row(c1="a", c2=3, nth_value=2),
            Row(c1="b", c2=2, nth_value=None),
            Row(c1="b", c2=8, nth_value=8),
        ]


def test_ntile(get_session_and_func, get_window):
    session, ntile = get_session_and_func("ntile")
    df = session.createDataFrame([("a", 1), ("a", 2), ("a", 3), ("b", 8), ("b", 2)], ["c1", "c2"])
    Window = get_window(session)
    w = Window.partitionBy("c1").orderBy("c2")
    assert df.withColumn("ntile", ntile(2).over(w)).orderBy("c1", "c2").collect() == [
        Row(c1="a", c2=1, nth_value=1),
        Row(c1="a", c2=2, nth_value=1),
        Row(c1="a", c2=3, nth_value=2),
        Row(c1="b", c2=2, nth_value=1),
        Row(c1="b", c2=8, nth_value=2),
    ]


def test_current_date(get_session_and_func):
    session, current_date = get_session_and_func("current_date")
    df = session.range(1)
    # The current date can depend on how the connection is configured so we check for dates around today
    assert df.select(current_date()).first()[0] == datetime.date.today()


def test_current_timestamp(get_session_and_func):
    session, current_timestamp = get_session_and_func("current_timestamp")
    df = session.range(1)
    now = datetime.datetime.now(pytz.timezone("UTC")).replace(tzinfo=None)
    result = df.select(current_timestamp()).first()[0]
    assert isinstance(result, datetime.datetime)
    assert result >= now - datetime.timedelta(minutes=1) and result <= now + datetime.timedelta(
        minutes=1
    )


def test_date_format(get_session_and_func):
    session, date_format = get_session_and_func("date_format")
    df = session.createDataFrame([("2015-04-08",)], ["dt"])
    assert df.select(date_format("dt", "MM/dd/yyyy").alias("date")).first()[0] == "04/08/2015"


def test_year(get_session_and_func):
    session, year = get_session_and_func("year")
    df = session.createDataFrame([("2015-04-08",)], ["dt"])
    assert df.select(year("dt").alias("year")).first()[0] == 2015


def test_quarter(get_session_and_func):
    session, quarter = get_session_and_func("quarter")
    df = session.createDataFrame([("2015-04-08",)], ["dt"])
    assert df.select(quarter("dt").alias("quarter")).first()[0] == 2


def test_month(get_session_and_func):
    session, month = get_session_and_func("month")
    df = session.createDataFrame([("2015-04-08",)], ["dt"])
    assert df.select(month("dt").alias("month")).first()[0] == 4


def test_dayofweek(get_session_and_func):
    session, dayofweek = get_session_and_func("dayofweek")
    df = session.createDataFrame([("2015-04-08",)], ["dt"])
    assert df.select(dayofweek("dt").alias("day")).first()[0] == 4


def test_dayofmonth(get_session_and_func):
    session, dayofmonth = get_session_and_func("dayofmonth")
    df = session.createDataFrame([("2015-04-08",)], ["dt"])
    assert df.select(dayofmonth("dt").alias("day")).first()[0] == 8


def test_dayofyear(get_session_and_func):
    session, dayofyear = get_session_and_func("dayofyear")
    df = session.createDataFrame([("2015-04-08",)], ["dt"])
    assert df.select(dayofyear("dt").alias("day")).first()[0] == 98


def test_hour(get_session_and_func):
    session, hour = get_session_and_func("hour")
    df = session.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ["ts"])
    assert df.select(hour("ts").alias("hour")).first()[0] == 13


def test_minute(get_session_and_func):
    session, minute = get_session_and_func("minute")
    df = session.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ["ts"])
    assert df.select(minute("ts").alias("minute")).first()[0] == 8


def test_second(get_session_and_func):
    session, second = get_session_and_func("second")
    df = session.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ["ts"])
    assert df.select(second("ts").alias("second")).first()[0] == 15


def test_weekofyear(get_session_and_func):
    session, weekofyear = get_session_and_func("weekofyear")
    df = session.createDataFrame([("2015-04-08",)], ["dt"])
    assert df.select(weekofyear(df.dt).alias("week")).first()[0] == 15


def test_make_date(get_session_and_func):
    session, make_date = get_session_and_func("make_date")
    df = session.createDataFrame([(2020, 6, 26)], ["Y", "M", "D"])
    assert df.select(make_date(df.Y, df.M, df.D).alias("datefield")).first()[0] == datetime.date(
        2020, 6, 26
    )


def test_date_add(get_session_and_func):
    session, date_add = get_session_and_func("date_add")
    df = session.createDataFrame(
        [
            (
                "2015-04-08",
                2,
            )
        ],
        ["dt", "add"],
    )
    assert df.select(date_add(df.dt, 1).alias("next_date")).first()[0] == datetime.date(2015, 4, 9)
    assert df.select(date_add(df.dt, df.add.cast("integer")).alias("next_date")).first()[
        0
    ] == datetime.date(2015, 4, 10)
    assert df.select(date_add("dt", -1).alias("prev_date")).first()[0] == datetime.date(2015, 4, 7)


def test_date_sub(get_session_and_func):
    session, date_sub = get_session_and_func("date_sub")
    df = session.createDataFrame(
        [
            (
                "2015-04-08",
                2,
            )
        ],
        ["dt", "sub"],
    )
    assert df.select(date_sub(df.dt, 1).alias("prev_date")).first()[0] == datetime.date(2015, 4, 7)
    assert df.select(date_sub(df.dt, df.sub.cast("integer")).alias("prev_date")).first()[
        0
    ] == datetime.date(2015, 4, 6)
    assert df.select(date_sub("dt", -1).alias("next_date")).first()[0] == datetime.date(2015, 4, 9)


def test_date_diff(get_session_and_func):
    session, date_diff = get_session_and_func("date_diff")
    df = session.createDataFrame([("2015-04-08", "2015-05-10")], ["d1", "d2"])
    assert df.select(date_diff(df.d2, df.d1).alias("diff")).first()[0] == 32


def test_add_months(get_session_and_func):
    session, add_months = get_session_and_func("add_months")
    df = session.createDataFrame([("2015-04-08", 2)], ["dt", "add"])
    assert df.select(add_months(df.dt, 1).alias("next_month")).first()[0] == datetime.date(
        2015, 5, 8
    )
    assert df.select(add_months(df.dt, df.add.cast("integer")).alias("next_month")).first()[
        0
    ] == datetime.date(2015, 6, 8)
    assert df.select(add_months("dt", -2).alias("prev_month")).first()[0] == datetime.date(
        2015, 2, 8
    )


def test_months_between(get_session_and_func):
    session, months_between = get_session_and_func("months_between")
    df = session.createDataFrame([("1997-02-28 10:30:00", "1996-10-30")], ["date1", "date2"])
    if isinstance(session, (DuckDBSession, PostgresSession)):
        assert df.select(months_between(df.date1, df.date2).alias("months")).first()[0] == 4
        assert df.select(months_between(df.date1, df.date2, False).alias("months")).first()[0] == 4
    elif isinstance(session, SnowflakeSession):
        assert df.select(months_between(df.date1, df.date2).alias("months")).first()[0] == 3.935484
        assert (
            df.select(months_between(df.date1, df.date2, False).alias("months")).first()[0]
            == 3.935484
        )
    else:
        assert (
            df.select(months_between(df.date1, df.date2).alias("months")).first()[0] == 3.94959677
        )
        assert (
            df.select(months_between(df.date1, df.date2, False).alias("months")).first()[0]
            == 3.9495967741935485
        )


def test_to_date(get_session_and_func):
    session, to_date = get_session_and_func("to_date")
    df = session.createDataFrame([("1997-02-28 10:30:00",)], ["t"])
    assert df.select(to_date(df.t).alias("date")).first()[0] == datetime.date(1997, 2, 28)
    result = df.select(to_date(df.t, "yyyy-MM-dd HH:mm:ss").alias("date")).first()[0]
    if isinstance(session, (BigQuerySession, DuckDBSession)):
        assert result == datetime.date(1997, 2, 28)
    elif isinstance(session, PostgresSession):
        assert result == datetime.date(1997, 2, 28)
    elif isinstance(session, SnowflakeSession):
        assert result == datetime.date(1997, 2, 28)
    else:
        assert result == datetime.date(1997, 2, 28)


def test_to_timestamp(get_session_and_func):
    session, to_timestamp = get_session_and_func("to_timestamp")
    df = session.createDataFrame([("1997-02-28 10:30:00",)], ["t"])
    result = df.select(to_timestamp(df.t).alias("dt")).first()[0]
    assert result == datetime.datetime(1997, 2, 28, 10, 30)
    result = df.select(to_timestamp(df.t, "yyyy-MM-dd HH:mm:ss").alias("dt")).first()[0]
    assert result == datetime.datetime(1997, 2, 28, 10, 30)


def test_trunc(get_session_and_func):
    session, trunc = get_session_and_func("trunc")
    df = session.createDataFrame([("1997-02-28",)], ["d"])
    assert df.select(trunc(df.d, "year").alias("year")).first()[0] == datetime.date(1997, 1, 1)
    trunc_month = "month" if isinstance(session, BigQuerySession) else "mon"
    assert df.select(trunc(df.d, trunc_month).alias("month")).first()[0] == datetime.date(
        1997, 2, 1
    )


def test_date_trunc(get_session_and_func):
    session, date_trunc = get_session_and_func("date_trunc")
    df = session.createDataFrame([("1997-02-28 05:02:11",)], ["t"])
    assert df.select(date_trunc("year", df.t).alias("year")).first()[0] == datetime.datetime(
        1997,
        1,
        1,
        0,
        0,
    )
    assert df.select(date_trunc("month", df.t).alias("month")).first()[0] == datetime.datetime(
        1997,
        2,
        1,
        0,
        0,
    )


def test_next_day(get_session_and_func):
    session, next_day = get_session_and_func("next_day")
    df = session.createDataFrame([("2015-07-27",)], ["d"])
    assert df.select(next_day(df.d, "Sun").alias("date")).first()[0] == datetime.date(2015, 8, 2)


def test_last_day(get_session_and_func):
    session, last_day = get_session_and_func("last_day")
    df = session.createDataFrame([("1997-02-10",)], ["d"])
    assert df.select(last_day(df.d).alias("date")).first()[0] == datetime.date(1997, 2, 28)


def test_from_unixtime(get_session_and_func):
    session, from_unixtime = get_session_and_func("from_unixtime")
    df = session.createDataFrame([(1428476400,)], ["unix_time"])
    expected = "2015-04-08 07:00:00"
    assert df.select(from_unixtime("unix_time").alias("ts")).first()[0] == expected


def test_unix_timestamp(get_session_and_func):
    session, unix_timestamp = get_session_and_func("unix_timestamp")
    df = session.createDataFrame([("2015-04-08",)], ["dt"])
    result = df.select(unix_timestamp("dt", "yyyy-MM-dd").alias("unix_time")).first()[0]
    assert result == 1428451200


def test_from_utc_timestamp(get_session_and_func):
    session, from_utc_timestamp = get_session_and_func("from_utc_timestamp")
    df = session.createDataFrame([("1997-02-28 10:30:00", "JST")], ["ts", "tz"])
    assert df.select(from_utc_timestamp(df.ts, "PST").alias("local_time")).first()[
        0
    ] == datetime.datetime(1997, 2, 28, 2, 30)
    assert df.select(from_utc_timestamp(df.ts, df.tz).alias("local_time")).first()[
        0
    ] == datetime.datetime(1997, 2, 28, 19, 30)


def test_to_utc_timestamp(get_session_and_func):
    session, to_utc_timestamp = get_session_and_func("to_utc_timestamp")
    df = session.createDataFrame([("1997-02-28 10:30:00", "JST")], ["ts", "tz"])
    assert df.select(to_utc_timestamp(df.ts, "PST").alias("utc_time")).first()[
        0
    ] == datetime.datetime(1997, 2, 28, 18, 30)
    assert df.select(to_utc_timestamp(df.ts, df.tz).alias("utc_time")).first()[
        0
    ] == datetime.datetime(1997, 2, 28, 1, 30)


def test_timestamp_seconds(get_session_and_func):
    session, timestamp_seconds = get_session_and_func("timestamp_seconds")
    df = session.createDataFrame([(1230219000,)], ["unix_time"])
    expected = datetime.datetime(2008, 12, 25, 15, 30, 00)
    assert (
        df.select(timestamp_seconds(df.unix_time).alias("ts")).first()[0].replace(tzinfo=None)
        == expected
    )


def test_window(get_session_and_func, get_func):
    session, window = get_session_and_func("window")
    sum = get_func("sum", session)
    col = get_func("col", session)
    df = session.createDataFrame([(datetime.datetime(2016, 3, 11, 9, 0, 7), 1)]).toDF("date", "val")
    w = df.groupBy(window("date", "5 seconds")).agg(sum("val").alias("sum"))
    # SQLFrame does not support the syntax used in the example so the "col" function was used instead.
    # https://spark.apache.org/docs/3.4.0/api/python/reference/pyspark.sql/api/pyspark.sql.functions.window.html
    result = w.select(
        col("window.start").cast("string").alias("start"),
        col("window.end").cast("string").alias("end"),
        "sum",
    ).collect()
    assert result == [
        Row(start="2016-03-11 09:00:05", end="2016-03-11 09:00:10", sum=1),
    ]


def test_session_window(get_session_and_func, get_func):
    session, session_window = get_session_and_func("session_window")
    sum = get_func("sum", session)
    col = get_func("col", session)
    lit = get_func("lit", session)
    df = session.createDataFrame([("2016-03-11 09:00:07", 1)]).toDF("date", "val")
    w = df.groupBy(session_window("date", "5 seconds")).agg(sum("val").alias("sum"))
    # SQLFrame does not support the syntax used in the example so the "col" function was used instead.
    # https://spark.apache.org/docs/3.4.0/api/python/reference/pyspark.sql/api/pyspark.sql.functions.session_window.html
    assert w.select(
        col("session_window.start").cast("string").alias("start"),
        col("session_window.end").cast("string").alias("end"),
        "sum",
    ).collect() == [
        Row(start="2016-03-11 09:00:07", end="2016-03-11 09:00:12", sum=1),
    ]
    w = df.groupBy(session_window("date", lit("5 seconds"))).agg(sum("val").alias("sum"))
    assert w.select(
        col("session_window.start").cast("string").alias("start"),
        col("session_window.end").cast("string").alias("end"),
        "sum",
    ).collect() == [Row(start="2016-03-11 09:00:07", end="2016-03-11 09:00:12", sum=1)]


def test_crc32(get_session_and_func):
    session, crc32 = get_session_and_func("crc32")
    assert (
        session.createDataFrame([("ABC",)], ["a"]).select(crc32("a").alias("crc32")).first()[0]
        == 2743272264
    )


def test_md5(get_session_and_func):
    session, md5 = get_session_and_func("md5")
    assert (
        session.createDataFrame([("ABC",)], ["a"]).select(md5("a").alias("hash")).first()[0]
        == "902fbdd2b1df0c4f70b4a5d23525e932"
    )


def test_sha1(get_session_and_func):
    session, sha1 = get_session_and_func("sha1")
    assert (
        session.createDataFrame([("ABC",)], ["a"]).select(sha1("a").alias("hash")).first()[0]
        == "3c01bdbb26f358bab27f267924aa2c9a03fcfdb8"
    )


def test_sha2(get_session_and_func):
    session, sha2 = get_session_and_func("sha2")
    df = session.createDataFrame([["Alice"], ["Bob"]], ["name"])
    assert df.withColumn("sha2", sha2(df.name, 256)).collect() == [
        Row(name="Alice", value="3bc51062973c458d5a6f2d8d64a023246354ad7e064b1e4e009ec8a0699a3043"),
        Row(name="Bob", value="cd9fb1e148ccd8442e5aa74904cc73bf6fb54d1d54d333bd596aa9bb4bb4e961"),
    ]


def test_hash(get_session_and_func):
    session, hash = get_session_and_func("hash")
    df = session.createDataFrame([("ABC", "DEF")], ["c1", "c2"])
    if isinstance(session, DuckDBSession):
        assert df.select(hash("c1").alias("hash")).first()[0] == 1241521928161919141
        assert df.select(hash("c1", "c2").alias("hash")).first()[0] == 7524280102280623017
    # Bigquery only supports hashing a single column
    elif isinstance(session, BigQuerySession):
        assert df.select(hash("c1").alias("hash")).first()[0] == 228873345217803866
    elif isinstance(session, SnowflakeSession):
        assert df.select(hash("c1").alias("hash")).first()[0] == -2817530435410241181
        assert df.select(hash("c1", "c2").alias("hash")).first()[0] == -5568826177945960128
    else:
        assert df.select(hash("c1").alias("hash")).first()[0] == -757602832
        assert df.select(hash("c1", "c2").alias("hash")).first()[0] == 599895104


def test_xxhash64(get_session_and_func):
    session, xxhash64 = get_session_and_func("xxhash64")
    df = session.createDataFrame([("ABC", "DEF")], ["c1", "c2"])
    assert df.select(xxhash64("c1").alias("hash")).first()[0] == 4105715581806190027
    assert df.select(xxhash64("c1", "c2").alias("hash")).first()[0] == 3233247871021311208


def test_assert_true(get_session_and_func):
    session, assert_true = get_session_and_func("assert_true")
    df = session.createDataFrame([(0, 1)], ["a", "b"])
    assert df.select(assert_true(df.a < df.b).alias("r")).first()[0] is None
    assert df.select(assert_true(df.a < df.b, df.a).alias("r")).first()[0] is None
    assert df.select(assert_true(df.a < df.b, "error").alias("r")).first()[0] is None
    with pytest.raises(Exception):
        df.select(assert_true(df.a > df.b).alias("r")).collect()


def test_raise_error(get_session_and_func):
    session, raise_error = get_session_and_func("raise_error")
    df = session.range(1)
    with pytest.raises(Exception):
        df.select(raise_error("My error message")).collect()


def test_upper(get_session_and_func):
    session, upper = get_session_and_func("upper")
    df = session.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
    assert df.select(upper("value")).collect() == [
        Row(value="SPARK"),
        Row(value="PYSPARK"),
        Row(value="PANDAS API"),
    ]


def test_lower(get_session_and_func):
    session, lower = get_session_and_func("lower")
    df = session.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
    assert df.select(lower("value")).collect() == [
        Row(value="spark"),
        Row(value="pyspark"),
        Row(value="pandas api"),
    ]


def test_ascii(get_session_and_func):
    session, ascii = get_session_and_func("ascii")
    df = session.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
    assert df.select(ascii("value")).collect() == [
        Row(value=83),
        Row(value=80),
        Row(value=80),
    ]


def test_base64(get_session_and_func):
    session, base64 = get_session_and_func("base64")
    df = session.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
    assert df.select(base64("value")).collect() == [
        Row(value="U3Bhcms="),
        Row(value="UHlTcGFyaw=="),
        Row(value="UGFuZGFzIEFQSQ=="),
    ]


def test_unbase64(get_session_and_func):
    session, unbase64 = get_session_and_func("unbase64")
    df = session.createDataFrame(["U3Bhcms=", "UHlTcGFyaw==", "UGFuZGFzIEFQSQ=="], "STRING")
    results = df.select(unbase64("value")).collect()
    assert len(results) == 3
    assert len(results[0]) == 1
    expected = [b"Spark", b"PySpark", b"Pandas API"]
    if isinstance(session, SnowflakeSession):
        assert [r[0] for r in results] == ["Spark", "PySpark", "Pandas API"]
    elif isinstance(results[0][0], memoryview):
        assert [r[0].tobytes() for r in results] == expected
    else:
        assert [r[0] for r in results] == expected


def test_ltrim(get_session_and_func, get_func):
    session, ltrim = get_session_and_func("ltrim")
    length = get_func("length", session)
    df = session.createDataFrame(["   Spark", "Spark  ", " Spark"], "STRING")
    assert df.select(ltrim("value").alias("r")).withColumn("length", length("r")).collect() == [
        Row(r="Spark", length=5),
        Row(r="Spark  ", length=7),
        Row(r="Spark", length=5),
    ]


def test_rtrim(get_session_and_func, get_func):
    session, rtrim = get_session_and_func("rtrim")
    length = get_func("length", session)
    df = session.createDataFrame(["   Spark", "Spark  ", " Spark"], "STRING")
    assert df.select(rtrim("value").alias("r")).withColumn("length", length("r")).collect() == [
        Row(r="   Spark", length=8),
        Row(r="Spark", length=5),
        Row(r=" Spark", length=6),
    ]


def test_trim(get_session_and_func, get_func):
    session, trim = get_session_and_func("trim")
    length = get_func("length", session)
    df = session.createDataFrame(["   Spark", "Spark  ", " Spark"], "STRING")
    assert df.select(trim("value").alias("r")).withColumn("length", length("r")).collect() == [
        Row(r="Spark", length=5),
        Row(r="Spark", length=5),
        Row(r="Spark", length=5),
    ]


def test_concat_ws(get_session_and_func):
    session, concat_ws = get_session_and_func("concat_ws")
    df = session.createDataFrame([("abcd", "123")], ["s", "d"])
    assert df.select(concat_ws("-", df.s, df.d).alias("s")).collect() == [
        Row(s="abcd-123"),
    ]


def test_decode(get_session_and_func):
    session, decode = get_session_and_func("decode")
    df = session.createDataFrame([("abcd",)], ["a"])
    assert df.select(decode("a", "UTF-8")).collect() == [
        Row(value="abcd"),
    ]


def test_encode(get_session_and_func):
    session, encode = get_session_and_func("encode")
    df = session.createDataFrame([("abcd",)], ["c"])
    results = df.select(encode("c", "UTF-8")).collect()
    assert len(results) == 1
    assert len(results[0]) == 1
    if isinstance(results[0][0], memoryview):
        assert results[0][0].tobytes() == b"abcd"
    else:
        assert results[0][0] == bytearray(b"abcd")


def test_format_number(get_session_and_func):
    session, format_number = get_session_and_func("format_number")
    assert (
        session.createDataFrame([(5000,)], ["a"])
        .select(format_number("a", 4).alias("v"))
        .first()[0]
        == "5,000.0000"
    )


def test_format_string(get_session_and_func):
    session, format_string = get_session_and_func("format_string")
    df = session.createDataFrame([(5, "hello")], ["a", "b"])
    assert df.select(format_string("%d %s", df.a, df.b).alias("v")).first()[0] == "5 hello"


def test_instr(get_session_and_func):
    session, instr = get_session_and_func("instr")
    df = session.createDataFrame(
        [("abcd",)],
        [
            "s",
        ],
    )
    assert df.select(instr(df.s, "b").alias("s")).first()[0] == 2


def test_overlay(get_session_and_func):
    session, overlay = get_session_and_func("overlay")
    df = session.createDataFrame([("SPARK_SQL", "CORE")], ("x", "y"))
    assert df.select(overlay("x", "y", 7).alias("overlayed")).first()[0] == "SPARK_CORE"
    assert df.select(overlay("x", "y", 7, 0).alias("overlayed")).first()[0] == "SPARK_CORESQL"
    assert df.select(overlay("x", "y", 7, 2).alias("overlayed")).first()[0] == "SPARK_COREL"


def test_sentences(get_session_and_func, get_func):
    session, sentences = get_session_and_func("sentences")
    lit = get_func("lit", session)
    df = session.createDataFrame([["This is an example sentence."]], ["string"])
    assert df.select(sentences(df.string, lit("en"), lit("US"))).first()[0] == [
        ["This", "is", "an", "example", "sentence"]
    ]
    df = session.createDataFrame([["Hello world. How are you?"]], ["s"])
    assert df.select(sentences("s")).first()[0] == [["Hello", "world"], ["How", "are", "you"]]


def test_substring(get_session_and_func):
    session, substring = get_session_and_func("substring")
    df = session.createDataFrame(
        [("abcd",)],
        [
            "s",
        ],
    )
    assert df.select(substring(df.s, 1, 2).alias("s")).first()[0] == "ab"


def test_substring_index(get_session_and_func):
    session, substring_index = get_session_and_func("substring_index")
    df = session.createDataFrame([("a.b.c.d",)], ["s"])
    assert df.select(substring_index(df.s, ".", 2).alias("s")).first()[0] == "a.b"


def test_levenshtein(get_session_and_func):
    session, levenshtein = get_session_and_func("levenshtein")
    df = session.createDataFrame(
        [
            (
                "kitten",
                "sitting",
            )
        ],
        ["l", "r"],
    )
    assert df.select(levenshtein("l", "r").alias("d")).first()[0] == 3
    if not isinstance(session, SnowflakeSession):
        assert df.select(levenshtein("l", "r", 2).alias("d")).first()[0] == -1


def test_locate(get_session_and_func):
    session, locate = get_session_and_func("locate")
    df = session.createDataFrame(
        [("abcd",)],
        [
            "s",
        ],
    )
    assert df.select(locate("b", df.s, 1).alias("s")).first()[0] == 2


def test_lpad(get_session_and_func, get_func):
    session, lpad = get_session_and_func("lpad")
    df = session.createDataFrame(
        [("abcd",)],
        [
            "s",
        ],
    )
    assert df.select(lpad(df.s, 6, "#").alias("s")).first()[0] == "##abcd"


def test_rpad(get_session_and_func):
    session, rpad = get_session_and_func("rpad")
    df = session.createDataFrame(
        [("abcd",)],
        [
            "s",
        ],
    )
    assert df.select(rpad(df.s, 6, "#").alias("s")).first()[0] == "abcd##"


def test_repeat(get_session_and_func):
    session, repeat = get_session_and_func("repeat")
    df = session.createDataFrame(
        [("ab",)],
        [
            "s",
        ],
    )
    assert df.select(repeat(df.s, 3).alias("s")).first()[0] == "ababab"


def test_split(get_session_and_func):
    session, split = get_session_and_func("split")
    df = session.createDataFrame(
        [("oneAtwoBthreeC",)],
        [
            "s",
        ],
    )
    # Limit seems to be only supported by Spark so we only test that with spark
    if isinstance(session, (PySparkSession, SparkSession)):
        assert df.select(split(df.s, "[ABC]", 2).alias("s")).first()[0] == ["one", "twoBthreeC"]
    # Bigquery doesn't support regex in split
    if isinstance(session, (BigQuerySession, SnowflakeSession)):
        df = session.createDataFrame(
            [("one,two,three",)],
            [
                "s",
            ],
        )
        assert df.select(split(df.s, ",", -1).alias("s")).first()[0] == ["one", "two", "three"]
    else:
        assert df.select(split(df.s, "[ABC]", -1).alias("s")).first()[0] == [
            "one",
            "two",
            "three",
            "",
        ]


def test_regexp_extract(get_session_and_func):
    session, regexp_extract = get_session_and_func("regexp_extract")
    df = session.createDataFrame([("100-200",)], ["str"])
    # Only supports one capture group
    if isinstance(session, BigQuerySession):
        assert df.select(regexp_extract("str", r"(\d+)-[\d+]", 1).alias("d")).first()[0] == "100"
        assert df.select(regexp_extract("str", r"(\d+)", 1).alias("d")).first()[0] == "100"
    else:
        assert df.select(regexp_extract("str", r"(\d+)-(\d+)", 1).alias("d")).first()[0] == "100"
        df = session.createDataFrame([("foo",)], ["str"])
        assert df.select(regexp_extract("str", r"(\d+)", 1).alias("d")).first()[0] == ""
        df = session.createDataFrame([("aaaac",)], ["str"])
        assert df.select(regexp_extract("str", "(a+)(b)?(c)", 2).alias("d")).first()[0] == ""


def test_regexp_replace(get_session_and_func, get_func):
    session, regexp_replace = get_session_and_func("regexp_replace")
    col = get_func("col", session)
    df = session.createDataFrame([("100-200", r"(\d+)", "--")], ["str", "pattern", "replacement"])
    assert df.select(regexp_replace("str", r"(\d+)", "--").alias("d")).first()[0] == "-----"
    assert (
        df.select(regexp_replace("str", col("pattern"), col("replacement")).alias("d")).first()[0]
        == "-----"
    )


def test_initcap(get_session_and_func):
    session, initcap = get_session_and_func("initcap")
    df = session.createDataFrame([("ab cd",)], ["a"])
    assert df.select(initcap("a").alias("v")).first()[0] == "Ab Cd"


def test_soundex(get_session_and_func):
    session, soundex = get_session_and_func("soundex")
    df = session.createDataFrame([("Peters",), ("Uhrbach",)], ["name"])
    assert df.select(soundex(df.name).alias("soundex")).collect() == [
        Row(soundex="P362"),
        Row(soundex="U612"),
    ]


def test_bin(get_session_and_func):
    session, bin = get_session_and_func("bin")
    df = session.createDataFrame([2, 5], "INT")
    assert df.select(bin(df.value).alias("c")).collect() == [
        Row(value="10"),
        Row(value="101"),
    ]


def test_hex(get_session_and_func):
    session, hex = get_session_and_func("hex")
    df = session.createDataFrame([("ABC", 3)], ["a", "b"])
    # can't hex integers. Well it could work but need to know if it is an int or not: https://stackoverflow.com/questions/48775605/bigquery-cast-int64-to-bytes-or-binary-representation
    if isinstance(session, BigQuerySession):
        assert df.select(hex("a").alias("a")).collect() == [
            Row(a="414243"),
        ]
    elif isinstance(session, SnowflakeSession):
        assert df.select(hex("a"), hex("b")).collect() == [
            Row(a="414243", b="33"),
        ]
    else:
        assert df.select(hex("a"), hex("b")).collect() == [
            Row(a="414243", b="3"),
        ]


def test_unhex(get_session_and_func):
    session, unhex = get_session_and_func("unhex")
    df = session.createDataFrame([("414243",)], ["a"])
    if isinstance(session, SnowflakeSession):
        assert df.select(unhex("a").alias("a")).first()[0] == "ABC"
    else:
        assert df.select(unhex("a")).first()[0] == bytearray(b"ABC")


def test_length(get_session_and_func):
    session, length = get_session_and_func("length")
    df = session.createDataFrame([("ABC ",)], ["a"])
    assert df.select(length("a").alias("length")).first()[0] == 4


def test_octet_length(get_session_and_func):
    session, octet_length = get_session_and_func("octet_length")
    df = session.createDataFrame([("cat",), ("🐈",)], ["cat"])
    assert df.select(octet_length("cat")).collect() == [Row(value=3), Row(value=4)]


def test_bit_length(get_session_and_func):
    session, bit_length = get_session_and_func("bit_length")
    df = session.createDataFrame([("cat",), ("🐈",)], ["cat"])
    # Bigquery doesn't support the symbol
    if isinstance(session, BigQuerySession):
        assert df.select(bit_length("cat")).first()[0] == 24
    else:
        assert df.select(bit_length("cat")).collect() == [Row(value=24), Row(value=32)]


def test_translate(get_session_and_func):
    session, translate = get_session_and_func("translate")
    df = session.createDataFrame([("translate",)], ["a"])
    assert df.select(translate("a", "rnlt", "123").alias("r")).first()[0] == "1a2s3ae"


def test_array(get_session_and_func):
    session, array = get_session_and_func("array")
    df = session.createDataFrame([("Alice", 2), ("Bob", 5)], ("name", "age"))
    assert df.select(array("age", "age").alias("arr")).collect() == [
        Row(value=[2, 2]),
        Row(value=[5, 5]),
    ]
    assert df.select(array([df.age, df.age]).alias("arr")).collect() == [
        Row(value=[2, 2]),
        Row(value=[5, 5]),
    ]


def test_array_agg(get_session_and_func):
    session, array_agg = get_session_and_func("array_agg")
    df = session.createDataFrame([[1], [1], [2]], ["c"])
    assert df.agg(array_agg("c").alias("r")).collect() == [
        Row(r=[1, 1, 2]),
    ]


def test_array_append(get_session_and_func, get_func):
    session, array_append = get_session_and_func("array_append")
    lit = get_func("lit", session)
    df = session.range(1).select(lit(["b", "a", "c"]).alias("c1"), lit("c").alias("c2"))
    assert df.select(array_append(df.c1, df.c2)).collect() == [
        Row(value=["b", "a", "c", "c"]),
    ]
    assert df.select(array_append(df.c1, "x")).collect() == [
        Row(value=["b", "a", "c", "x"]),
    ]


def test_array_compact(get_session_and_func):
    session, array_compact = get_session_and_func("array_compact")
    df = session.createDataFrame([([1, None, 2, 3],), ([4, 5, None, 4],)], ["data"])
    assert df.select(array_compact(df.data)).collect() == [
        Row(value=[1, 2, 3]),
        Row(value=[4, 5, 4]),
    ]


def test_array_insert(get_session_and_func):
    session, array_insert = get_session_and_func("array_insert")
    df = session.createDataFrame(
        [(["a", "b", "c"], 2, "d"), (["c", "b", "a"], -2, "d")], ["data", "pos", "val"]
    )
    assert df.select(
        array_insert(df.data, df.pos.cast("integer"), df.val).alias("data")
    ).collect() == [
        Row(data=["a", "d", "b", "c"]),
        Row(data=["c", "b", "d", "a"]),
    ]
    assert df.select(array_insert(df.data, 5, "hello").alias("data")).collect() == [
        Row(data=["a", "b", "c", None, "hello"]),
        Row(data=["c", "b", "a", None, "hello"]),
    ]


def test_array_prepend(get_session_and_func):
    session, array_prepend = get_session_and_func("array_prepend")
    df = session.createDataFrame([([2, 3, 4],), ([],)], ["data"])
    assert df.select(array_prepend(df.data, 1)).collect() == [
        Row(value=[1, 2, 3, 4]),
        Row(value=[1]),
    ]


def test_array_size(get_session_and_func, get_func):
    session, array_size = get_session_and_func("array_size")
    # Snowflake doesn't support arrays in VALUES so we need to do it in select
    if isinstance(session, SnowflakeSession):
        lit = get_func("lit", session)
        assert session.range(1).select(
            array_size(lit(["a", "b", "c"])), array_size(lit(None))
        ).collect() == [Row(value=3, value2=None)]
    else:
        df = session.createDataFrame([([2, 1, 3],), (None,)], ["data"])
        assert df.select(array_size(df.data).alias("r")).collect() == [
            Row(r=3),
            Row(r=None),
        ]


def test_create_map(get_session_and_func, get_func):
    session, create_map = get_session_and_func("create_map")
    col = get_func("col", session)
    df = session.createDataFrame([("Alice", 2), ("Bob", 5)], ("name", "age"))
    if isinstance(session, (SparkSession, PySparkSession)):
        expected = [Row(value={"Alice": 2}), Row(value={"Bob": 5})]
    elif isinstance(session, DuckDBSession):
        expected = [Row(value=Row(**{"Alice": 2})), Row(value=Row(**{"Bob": 5}))]
    else:
        expected = [Row(value={"alice": 2}), Row(value={"bob": 5})]
    # Added the cast for age for Snowflake so the data type would be correct
    assert df.select(create_map("name", col("age").cast("int")).alias("blah")).collect() == expected
    assert df.select(create_map([df.name, df.age.cast("int")]).alias("blah")).collect() == expected


def test_map_from_arrays(get_session_and_func):
    session, map_from_arrays = get_session_and_func("map_from_arrays")
    df = session.createDataFrame([([2, 5], ["a", "b"])], ["k", "v"])
    assert df.select(map_from_arrays(df.k, df.v).alias("col")).first()[0] == {
        2: "a",
        5: "b",
    }


def test_array_contains(get_session_and_func, get_func):
    session, array_contains = get_session_and_func("array_contains")
    lit = get_func("lit", session)
    array_lit = lit(["a", "b", "c"])
    # Snowflake doesn't support arrays in VALUES so we need to do it in select
    if isinstance(session, SnowflakeSession):
        assert session.range(1).select(
            array_contains(array_lit, "a"), array_contains(array_lit, "d")
        ).collect() == [Row(value=True, value2=False)]
        assert session.range(1).select(
            array_contains(array_lit, lit("a")), array_contains(array_lit, lit("d"))
        ).collect() == [Row(value=True, value2=False)]


def test_arrays_overlap(get_session_and_func, get_func):
    session, arrays_overlap = get_session_and_func("arrays_overlap")
    lit = get_func("lit", session)
    assert session.range(1).select(
        arrays_overlap(lit(["a", "b"]), lit(["b", "c"])).alias("value"),
        arrays_overlap(lit(["a"]), lit(["b", "c"])).alias("value2"),
    ).collect() == [Row(value=True, value2=False)]


def test_slice(get_session_and_func, get_func):
    session, slice = get_session_and_func("slice")
    lit = get_func("lit", session)
    assert session.range(1).select(
        slice(lit([1, 2, 3]), 2, 2).alias("sliced1"),
        slice(lit([4, 5]), 2, 2).alias("sliced2"),
    ).collect() == [Row(sliced1=[2, 3], sliced2=[5])]


def test_array_join(get_session_and_func, get_func):
    session, array_join = get_session_and_func("array_join")
    lit = get_func("lit", session)
    if isinstance(session, SnowflakeSession):
        expected = [Row(value="a,b,c", value2="a,", value3="a,b,c", value4="a,")]
    else:
        expected = [Row(value="a,b,c", value2="a", value3="a,b,c", value4="a,NULL")]
    assert (
        session.range(1)
        .select(
            array_join(lit(["a", "b", "c"]), ",").alias("value1"),
            array_join(lit(["a", None]), ",").alias("value2"),
            array_join(lit(["a", "b", "c"]), ",", "NULL").alias("value3"),
            array_join(lit(["a", None]), ",", "NULL").alias("value4"),
        )
        .collect()
        == expected
    )


def test_concat(get_session_and_func):
    session, concat = get_session_and_func("concat")
    df = session.createDataFrame([("abcd", "123")], ["s", "d"])
    assert df.select(concat(df.s, df.d).alias("s")).first()[0] == "abcd123"
    # Some dialects don't support concating arrays. They would though if we could detect the data type
    # and use the appropriate function to array concat instead of string concat
    if not isinstance(session, (BigQuerySession, DuckDBSession, PostgresSession, SnowflakeSession)):
        df = session.createDataFrame([([1, 2], [3, 4], [5]), ([1, 2], None, [3])], ["a", "b", "c"])
        assert df.select(concat(df.a, df.b, df.c).alias("arr")).collect() == [
            Row(value=[1, 2, 3, 4, 5]),
            Row(value=None),
        ]


def test_array_position(get_session_and_func, get_func):
    session, array_position = get_session_and_func("array_position")
    lit = get_func("lit", session)
    assert session.range(1).select(
        array_position(lit(["c", "b", "a"]), "a").alias("value"),
        array_position(lit(["d"]), "a").alias("value2"),
    ).collect() == [Row(value=3, value2=0)]


def test_element_at(get_session_and_func, get_func):
    session, element_at = get_session_and_func("element_at")
    lit = get_func("lit", session)
    assert session.range(1).select(
        element_at(lit(["a", "b", "c"]), 1).alias("value"),
    ).collect() == [Row(value="a")]
    if not isinstance(session, (BigQuerySession, DuckDBSession, PostgresSession, SnowflakeSession)):
        df = session.createDataFrame([(["a", "b", "c"],)], ["data"])
        assert df.select(element_at(df.data, -1)).first()[0] == "c"
        df = session.createDataFrame([({"a": 1.0, "b": 2.0},)], ["data"])
        assert df.select(element_at(df.data, lit("a"))).first()[0] == 1.0


def test_array_remove(get_session_and_func, get_func):
    session, array_remove = get_session_and_func("array_remove")
    lit = get_func("lit", session)
    assert session.range(1).select(
        array_remove(lit([1, 2, 3, 1, 1]), 1).alias("value"),
        array_remove(lit([2]), 1).alias("value2"),
    ).collect() == [Row(value=[2, 3], value2=[2])]


def test_array_distinct(get_session_and_func, get_func):
    session, array_distinct = get_session_and_func("array_distinct")
    lit = get_func("lit", session)
    results = (
        session.range(1)
        .select(
            array_distinct(lit([1, 2, 3, 2])).alias("value"),
            array_distinct(lit([4, 5, 5, 4])).alias("value2"),
        )
        .collect()
    )
    assert results[0][0] in ([1, 2, 3], [3, 2, 1])
    assert results[0][1] in ([4, 5], [5, 4])


def test_array_intersect(get_session_and_func, get_func):
    session, array_intersect = get_session_and_func("array_intersect")
    lit = get_func("lit", session)
    assert session.range(1).select(
        array_intersect(lit(["b", "a", "c"]), lit(["c", "d", "a", "f"])).alias("value")
    ).first()[0] in ([["a", "c"], ["c", "a"]])


def test_array_union(get_session_and_func, get_func):
    session, array_union = get_session_and_func("array_union")
    lit = get_func("lit", session)
    assert Counter(
        session.range(1)
        .select(array_union(lit(["b", "a", "c"]), lit(["c", "d", "a", "f"])).alias("value"))
        .first()[0]
    ) == Counter(["b", "a", "c", "d", "f"])


def test_array_except(get_session_and_func, get_func):
    session, array_except = get_session_and_func("array_except")
    lit = get_func("lit", session)
    assert session.range(1).select(
        array_except(lit(["b", "a", "c"]), lit(["c", "d", "a", "f"])).alias("value")
    ).first()[0] == ["b"]


def test_explode(get_session_and_func):
    session, explode = get_session_and_func("explode")
    # Postgres doesn't support maps so we just test with list of int
    if isinstance(session, (BigQuerySession, PostgresSession)):
        df = session.createDataFrame([Row(a=1, intlist=[1, 2, 3])])
        assert df.select(explode(df.intlist).alias("anInt")).collect() == [
            Row(value=1),
            Row(value=2),
            Row(value=3),
        ]
    else:
        df = session.createDataFrame([Row(a=1, intlist=[1, 2, 3], mapfield={"a": "b"})])
        assert df.select(explode(df.intlist).alias("anInt")).collect() == [
            Row(value=1),
            Row(value=2),
            Row(value=3),
        ]


def test_pos_explode(get_session_and_func, get_func):
    session, posexplode = get_session_and_func("posexplode")
    lit = get_func("lit", session)
    result = session.range(1).select(posexplode(lit([1, 2, 3]))).collect()
    # BigQuery/Snowflake explodes with columns in flipped order
    if isinstance(session, (BigQuerySession, SnowflakeSession)):
        assert result == [
            Row(col=1, pos=0),
            Row(col=2, pos=1),
            Row(col=3, pos=2),
        ]
    else:
        assert result == [
            Row(pos=0, col=1),
            Row(pos=1, col=2),
            Row(pos=2, col=3),
        ]


def test_explode_outer(get_session_and_func, get_func):
    session, explode_outer = get_session_and_func("explode_outer")
    lit = get_func("lit", session)
    # Bigquery doesn't support maps
    if isinstance(session, BigQuerySession):
        df = session.createDataFrame([(1, ["foo", "bar"]), (3, None)], ("id", "an_array"))
        assert df.select("id", explode_outer("an_array")).collect() == [
            Row(id=1, col="foo"),
            Row(id=1, col="bar"),
            Row(id=3, col=None),
        ]
    # PySpark doesn't support dicts as a literal
    elif isinstance(session, PySparkSession):
        df = session.createDataFrame(
            [(1, ["foo", "bar"], {"x": 1.0}), (2, [], {}), (3, None, None)],
            ("id", "an_array", "a_map"),
        )
        assert df.select("id", "a_map", explode_outer("an_array")).collect() == [
            Row(id=1, a_map={"x": 1.0}, col="foo"),
            Row(id=1, a_map={"x": 1.0}, col="bar"),
            Row(id=2, a_map={}, col=None),
            Row(id=3, a_map=None, col=None),
        ]
        assert df.select("id", "an_array", explode_outer("a_map")).collect() == [
            Row(id=1, an_array=["foo", "bar"], key="x", value=1.0),
            Row(id=2, an_array=[], key=None, value=None),
            Row(id=3, an_array=None, key=None, value=None),
        ]
    elif isinstance(session, DatabricksSession):
        df = (
            session.range(1)
            .select(
                lit(1).alias("id"),
                lit(["foo", "bar"]).alias("an_array"),
                lit({"x": 1.0}).alias("a_map"),
            )
            .union(
                session.range(1).select(
                    lit(2).alias("id"),
                    lit([]).alias("an_array"),
                    lit({}).alias("a_map"),
                )
            )
            .union(
                session.range(1).select(
                    lit(3).alias("id"),
                    lit(None).alias("an_array"),
                    lit(None).alias("a_map"),
                )
            )
        )
        assert df.select("id", "a_map", explode_outer("an_array")).collect() == [
            Row(id=1, a_map={"x": Decimal("1.0")}, col="foo"),
            Row(id=1, a_map={"x": Decimal("1.0")}, col="bar"),
            Row(id=2, a_map=[], col=None),
            Row(id=3, a_map=None, col=None),
        ]
        assert df.select("id", "an_array", explode_outer("a_map")).collect() == [
            Row(id=1, an_array=["foo", "bar"], key="x", value=1.0),
            Row(id=2, an_array=[], key=None, value=None),
            Row(id=3, an_array=None, key=None, value=None),
        ]
    else:
        df = (
            session.range(1)
            .select(
                lit(1).alias("id"),
                lit(["foo", "bar"]).alias("an_array"),
                lit({"x": 1.0}).alias("a_map"),
            )
            .union(
                session.range(1).select(
                    lit(2).alias("id"),
                    lit([]).alias("an_array"),
                    lit({}).alias("a_map"),
                )
            )
            .union(
                session.range(1).select(
                    lit(3).alias("id"),
                    lit(None).alias("an_array"),
                    lit(None).alias("a_map"),
                )
            )
        )
        assert df.select("id", "a_map", explode_outer("an_array")).collect() == [
            Row(id=1, a_map={"x": 1.0}, col="foo"),
            Row(id=1, a_map={"x": 1.0}, col="bar"),
            Row(id=2, a_map={}, col=None),
            Row(id=3, a_map=None, col=None),
        ]
        assert df.select("id", "an_array", explode_outer("a_map")).collect() == [
            Row(id=1, an_array=["foo", "bar"], key="x", value=1.0),
            Row(id=2, an_array=[], key=None, value=None),
            Row(id=3, an_array=None, key=None, value=None),
        ]


def test_posexplode_outer(get_session_and_func):
    session, posexplode_outer = get_session_and_func("posexplode_outer")
    # Bigquery doesn't support maps
    if isinstance(session, BigQuerySession):
        df = session.createDataFrame([(1, ["foo", "bar"]), (3, None)], ("id", "an_array"))
        assert df.select("id", posexplode_outer("an_array")).collect() == [
            Row(id=1, col="foo", pos=0),
            Row(id=1, col="bar", pos=1),
            Row(id=3, col=None, pos=0),
        ]
    else:
        df = session.createDataFrame(
            [(1, ["foo", "bar"], {"x": 1.0}), (2, [], {}), (3, None, None)],
            ("id", "an_array", "a_map"),
        )
        assert df.select("id", "an_array", posexplode_outer("a_map")).collect() == [
            Row(id=1, an_array=["foo", "bar"], post=0, key="x", value=1.0),
            Row(id=2, an_array=[], post=None, key=None, value=None),
            Row(id=3, an_array=None, post=None, key=None, value=None),
        ]


def test_get_json_object(get_session_and_func):
    session, get_json_object = get_session_and_func("get_json_object")
    data = [("1", """{"f1": "value1", "f2": "value2"}"""), ("2", """{"f1": "value12"}""")]
    df = session.createDataFrame(data, ("key", "jstring"))
    result = df.select(
        df.key,
        get_json_object(df.jstring, "$.f1").alias("c0"),
        get_json_object(df.jstring, "$.f2").alias("c1"),
    ).collect()
    if isinstance(session, (BigQuerySession, DuckDBSession)):
        assert result == [
            Row(key="1", c0='"value1"', c1='"value2"'),
            Row(key="2", c0='"value12"', c1=None),
        ]
    else:
        assert result == [
            Row(key="1", c0="value1", c1="value2"),
            Row(key="2", c0="value12", c1=None),
        ]


def test_json_tuple(get_session_and_func):
    session, json_tuple = get_session_and_func("json_tuple")
    data = [("1", """{"f1": "value1", "f2": "value2"}"""), ("2", """{"f1": "value12"}""")]
    df = session.createDataFrame(data, ("key", "jstring"))
    result = df.select(df.key, json_tuple(df.jstring, "f1", "f2")).collect()
    assert result == [Row(key="1", c0="value1", c1="value2"), Row(key="2", c0="value12", c1=None)]


def test_from_json(get_session_and_func, get_types, get_func):
    session, from_json = get_session_and_func("from_json")
    schema_of_json = get_func("schema_of_json", session)
    lit = get_func("lit", session)
    data = [(1, """{"a": 1}""")]
    types = get_types(session)
    schema = types.StructType([types.StructField("a", types.IntegerType())])
    df = session.createDataFrame(data, ("key", "value"))
    assert df.select(from_json(df.value, schema).alias("json")).collect() == [Row(json=Row(a=1))]
    assert df.select(from_json(df.value, "a INT").alias("json")).collect() == [Row(json=Row(a=1))]
    assert df.select(from_json(df.value, "MAP<STRING,INT>").alias("json")).collect() == [
        Row(json={"a": 1})
    ]
    data = [(1, """[{"a": 1}]""")]
    schema = types.ArrayType(types.StructType([types.StructField("a", types.IntegerType())]))
    df = session.createDataFrame(data, ("key", "value"))
    assert df.select(from_json(df.value, schema).alias("json")).collect() == [Row(json=[Row(a=1)])]
    schema = schema_of_json(lit("""{"a": 0}"""))
    assert df.select(from_json(df.value, schema).alias("json")).collect() == [Row(json=Row(a=None))]
    data = [(1, """[1, 2, 3]""")]
    schema = types.ArrayType(types.IntegerType())
    df = session.createDataFrame(data, ("key", "value"))
    assert df.select(from_json(df.value, schema).alias("json")).collect() == [Row(json=[1, 2, 3])]


def test_to_json(get_session_and_func, get_types, get_func):
    session, to_json = get_session_and_func("to_json")
    data = [(1, Row(id=2, name="Alice", birthday=datetime.date(1995, 1, 9)))]
    df = session.createDataFrame(data, ("key", "value"))
    assert df.select(
        to_json(df.value, options={"ignoreNullFields": False, "dateFormat": "yyyy-MM-dd"}).alias(
            "json"
        )
    ).collect() == [Row(json='{"id":2,"name":"Alice","birthday":"1995-01-09"}')]
    data = [(1, [Row(id=2, name="Alice"), Row(id=3, name="Bob"), Row(id=4, name=None)])]
    df = session.createDataFrame(data, ("key", "value"))
    assert df.select(
        to_json(df.value, options={"ignoreNullFields": "false"}).alias("json")
    ).collect() == [
        Row(json='[{"id":2,"name":"Alice"},{"id":3,"name":"Bob"},{"id":4,"name":null}]')
    ]
    data = [(1, {"name": "Alice"})]
    df = session.createDataFrame(data, ("key", "value"))
    assert df.select(to_json(df.value).alias("json")).collect() == [Row(json='{"name":"Alice"}')]
    data = [(1, [{"name": "Alice"}, {"name": "Bob"}])]
    df = session.createDataFrame(data, ("key", "value"))
    assert df.select(to_json(df.value).alias("json")).collect() == [
        Row(json='[{"name":"Alice"},{"name":"Bob"}]')
    ]
    data = [(1, ["Alice", "Bob"])]
    df = session.createDataFrame(data, ("key", "value"))
    assert df.select(to_json(df.value).alias("json")).collect() == [Row(json='["Alice","Bob"]')]


def test_schema_of_json(get_session_and_func, get_func):
    session, schema_of_json = get_session_and_func("schema_of_json")
    lit = get_func("lit", session)
    df = session.range(1)
    assert (
        df.select(schema_of_json(lit('{"a": 0}')).alias("json")).first()[0] == "STRUCT<a: BIGINT>"
    )
    assert (
        df.select(
            schema_of_json("{a: 1}", {"allowUnquotedFieldNames": "true"}).alias("json")
        ).first()[0]
        == "STRUCT<a: BIGINT>"
    )


def test_schema_of_csv(get_session_and_func, get_func):
    session, schema_of_csv = get_session_and_func("schema_of_csv")
    lit = get_func("lit", session)
    df = session.range(1)
    assert (
        df.select(schema_of_csv(lit("1|a"), {"sep": "|"}).alias("csv")).first()[0]
        == "STRUCT<_c0: INT, _c1: STRING>"
    )
    assert (
        df.select(schema_of_csv("1|a", {"sep": "|"}).alias("csv")).first()[0]
        == "STRUCT<_c0: INT, _c1: STRING>"
    )


def test_to_csv(get_session_and_func):
    session, to_csv = get_session_and_func("to_csv")
    data = [(1, Row(age=2, name="Alice"))]
    df = session.createDataFrame(data, ("key", "value"))
    assert df.select(to_csv(df.value).alias("csv")).first()[0] == "2,Alice"


def test_size(get_session_and_func, get_func):
    session, size = get_session_and_func("size")
    lit = get_func("lit", session)
    assert session.range(1).select(
        size(lit([1, 2, 3])).alias("size"),
        size(lit([1])).alias("size2"),
    ).collect() == [Row(size=3, size2=1)]


def test_array_min(get_session_and_func, get_func):
    session, array_min = get_session_and_func("array_min")
    lit = get_func("lit", session)
    df = session.createDataFrame([([2, 1, 3],), ([None, 10, -1],)], ["data"])
    assert session.range(1).select(
        array_min(lit([2, 1, 3])).alias("min"),
        array_min(lit([None, 10, -1])).alias("min2"),
    ).collect() == [Row(min=1, min2=-1)]


def test_array_max(get_session_and_func, get_func):
    session, array_max = get_session_and_func("array_max")
    lit = get_func("lit", session)
    results = (
        session.range(1)
        .select(
            array_max(lit([2, 1, 3])).alias("max"),
            array_max(lit([None, 10, -1])).alias("max2"),
        )
        .collect()
    )
    assert results[0][0] == 3
    if isinstance(session, DuckDBSession):
        assert results[0][1] is None
    else:
        assert results[0][1] == 10


def test_sort_array(get_session_and_func, get_func):
    session, sort_array = get_session_and_func("sort_array")
    lit = get_func("lit", session)
    # Bigquery cannot have nulls in arrays
    if isinstance(session, BigQuerySession):
        df = session.createDataFrame([([2, 1, 3],), ([1],)], ["data"])
        assert df.select(sort_array(df.data).alias("r")).collect() == [
            Row(r=[1, 2, 3]),
            Row(r=[1]),
        ]
        assert df.select(sort_array(df.data, asc=False).alias("r")).collect() == [
            Row(r=[3, 2, 1]),
            Row(r=[1]),
        ]
        return
    # df = session.createDataFrame([([2, 1, None, 3],), ([1],)], ["data"])
    df = session.range(1).select(
        lit([2, 1, None, 3]).alias("data"),
        lit([1]).alias("data2"),
    )
    results1 = df.select(
        sort_array(df.data).alias("data"),
        sort_array(df.data2).alias("data2"),
    ).collect()
    if isinstance(session, DuckDBSession):
        assert results1 == [Row(data=[1, 2, 3, None], data2=[1])]
    else:
        assert results1 == [
            Row(data=[None, 1, 2, 3], data2=[1]),
        ]
    assert df.select(
        sort_array(df.data, asc=False).alias("data"),
        sort_array(df.data2, asc=False).alias("data2"),
    ).collect() == [
        Row(data=[3, 2, 1, None], data2=[1]),
    ]


def test_array_sort(get_session_and_func, get_func):
    session, array_sort = get_session_and_func("array_sort")
    when = get_func("when", session)
    lit = get_func("lit", session)
    length = get_func("length", session)
    # Bigquery cannot have nulls in arrays
    if isinstance(session, BigQuerySession):
        df = session.createDataFrame([([2, 1, 3],), ([1],)], ["data"])
        assert df.select(array_sort(df.data).alias("r")).collect() == [
            Row(r=[1, 2, 3]),
            Row(r=[1]),
        ]
        # ASC/DESC is strange on BigQuery but it is from a legacy bug.
        # Should be updated to no share the `sort_array` function
        assert df.select(array_sort(df.data, comparator=False).alias("r")).collect() == [
            Row(r=[3, 2, 1]),
            Row(r=[1]),
        ]
        return
    df = session.range(1).select(
        lit([2, 1, None, 3]).alias("data"),
        lit([1]).alias("data2"),
    )
    assert df.select(
        array_sort(df.data).alias("data"),
        array_sort(df.data2).alias("data2"),
    ).collect() == [
        Row(data=[1, 2, 3, None], data2=[1]),
    ]
    if not isinstance(session, SnowflakeSession):
        df = session.createDataFrame([(["foo", "foobar", None, "bar"],), (["foo"],)], ["data"])
        if isinstance(session, DuckDBSession):
            assert df.select(
                array_sort(
                    "data",
                    lambda x, y: when(x.isNull() | y.isNull(), lit(0)).otherwise(
                        length(y) - length(x)
                    ),
                ).alias("r")
            ).collect() == [Row(r=["bar", "foo", "foobar", None]), Row(r=["foo"])]
        else:
            assert df.select(
                array_sort(
                    "data",
                    lambda x, y: when(x.isNull() | y.isNull(), lit(0)).otherwise(
                        length(y) - length(x)
                    ),
                ).alias("r")
            ).collect() == [Row(r=["foobar", "foo", None, "bar"]), Row(r=["foo"])]


def test_bit_and(get_session_and_func):
    session, bit_and = get_session_and_func("bit_and")
    df = session.createDataFrame([[1], [1], [2]], ["c"])
    assert df.select(bit_and("c")).first() == Row(value=0)


def test_bit_or(get_session_and_func):
    session, bit_or = get_session_and_func("bit_or")
    df = session.createDataFrame([[1], [1], [2]], ["c"])
    assert df.select(bit_or("c")).first() == Row(value=3)


def test_bit_xor(get_session_and_func):
    session, bit_xor = get_session_and_func("bit_xor")
    df = session.createDataFrame([[1], [1], [2]], ["c"])
    assert df.select(bit_xor("c")).first() == Row(value=2)


def test_bit_count(get_session_and_func):
    session, bit_count = get_session_and_func("bit_count")
    df = session.createDataFrame([[1], [1], [2]], ["c"])
    assert df.select(bit_count("c")).collect() == [Row(value=1), Row(value=1), Row(value=1)]


def test_bit_get(get_session_and_func, get_func):
    session, bit_get = get_session_and_func("bit_get")
    lit = get_func("lit", session)
    df = session.createDataFrame([[1], [1], [2]], ["c"])
    assert df.select(bit_get("c", lit(1))).collect() == [Row(value=0), Row(value=0), Row(value=1)]


def test_getbit(get_session_and_func, get_func):
    session, getbit = get_session_and_func("getbit")
    lit = get_func("lit", session)
    df = session.createDataFrame([[1], [1], [2]], ["c"])
    assert df.select(getbit("c", lit(1))).collect() == [Row(value=0), Row(value=0), Row(value=1)]


def test_shuffle(get_session_and_func):
    session, shuffle = get_session_and_func("shuffle")
    df = session.createDataFrame([([1, 20, 3, 5],), ([1, 20, None, 3],)], ["data"])
    results = df.select(shuffle(df.data).alias("r")).collect()
    assert Counter(results[0][0]) == Counter([1, 20, 3, 5])
    assert Counter(results[1][0]) == Counter([1, 20, 3, None])


def test_reverse(get_session_and_func):
    session, reverse = get_session_and_func("reverse")
    df = session.createDataFrame([("Spark SQL",)], ["data"])
    assert df.select(reverse(df.data).alias("s")).collect() == [Row(s="LQS krapS")]
    if not isinstance(session, (BigQuerySession, DuckDBSession, PostgresSession)):
        df = session.createDataFrame([([2, 1, 3],), ([1],)], ["data"])
        assert df.select(reverse(df.data).alias("r")).collect() == [Row(r=[3, 1, 2]), Row(r=[1])]


def test_flatten(get_session_and_func, get_func):
    session, flatten = get_session_and_func("flatten")
    lit = get_func("lit", session)
    # df = session.createDataFrame([([[1, 2, 3], [4, 5], [6]],), ([None, [4, 5]],)], ["data"])
    df = session.range(1).select(
        lit([[1, 2, 3], [4, 5], [6]]).alias("data"),
        lit([None, [4, 5]]).alias("data2"),
    )
    results = df.select(
        flatten(df.data).alias("data"),
        flatten(df.data2).alias("data2"),
    ).collect()
    assert results[0][0] == [1, 2, 3, 4, 5, 6]
    if isinstance(session, DuckDBSession):
        assert results[0][1] == [4, 5]
    else:
        assert results[0][1] is None


def test_map_keys(get_session_and_func):
    session, map_keys = get_session_and_func("map_keys")
    if isinstance(session, SnowflakeSession):
        sql = "SELECT {'a': 1, 'b': 2}::MAP(VARCHAR, NUMBER) as data"
        # Ideally this would be ["a", "b"] since it would be normalized for Spark but all I get back is a list
        # of values and I wouldn't know if those values represent columns or not
        expected = ["A", "B"]
    else:
        sql = "SELECT map('a', 1, 'b', 2) as data"
        expected = ["a", "b"]
    df = session.sql(sql, dialect="snowflake" if isinstance(session, SnowflakeSession) else None)
    assert df.select(map_keys("data").alias("keys")).first()[0] == expected


def test_map_values(get_session_and_func):
    session, map_values = get_session_and_func("map_values")
    df = session.sql("SELECT map(1, 'a', 2, 'b') as data")
    assert df.select(map_values("data").alias("values")).first()[0] == ["a", "b"]


def test_map_entries(get_session_and_func):
    session, map_entries = get_session_and_func("map_entries")
    df = session.sql("SELECT map(1, 'a', 2, 'b') as data")
    assert df.select(map_entries("data").alias("entries")).first()[0] == [
        Row(key=1, value="a"),
        Row(key=2, value="b"),
    ]


def test_map_from_entries(get_session_and_func):
    session, map_from_entries = get_session_and_func("map_from_entries")
    df = session.sql("SELECT array(struct(1, 'a'), struct(2, 'b')) as data")
    assert df.select(map_from_entries("data").alias("map")).first()[0] == {1: "a", 2: "b"}


def test_array_repeat(get_session_and_func):
    session, array_repeat = get_session_and_func("array_repeat")
    df = session.createDataFrame([("ab",)], ["data"])
    assert df.select(array_repeat(df.data, 3).alias("r")).first()[0] == ["ab", "ab", "ab"]


def test_arrays_zip(get_session_and_func):
    session, arrays_zip = get_session_and_func("arrays_zip")
    df = session.createDataFrame([([1, 2, 3], [2, 4, 6], [3, 6])], ["vals1", "vals2", "vals3"])
    assert df.select(arrays_zip(df.vals1, df.vals2, df.vals3).alias("zipped")).first()[0] == [
        Row(vals1=1, vals2=2, vals3=3),
        Row(vals1=2, vals2=4, vals3=6),
        Row(vals1=3, vals2=6, vals3=None),
    ]


def test_map_concat(get_session_and_func):
    session, map_concat = get_session_and_func("map_concat")
    if isinstance(session, SnowflakeSession):
        sql = "SELECT {'a': 1, 'b': 2}::MAP(VARCHAR, NUMBER) as map1, {'c': 3}::MAP(VARCHAR, NUMBER) as map2"
        expected = {"a": 1, "b": 2, "c": 3}
    else:
        sql = "SELECT map(1, 'a', 2, 'b') as map1, map(3, 'c') as map2"
        expected = {1: "a", 2: "b", 3: "c"}
    df = session.sql(sql)
    assert df.select(map_concat("map1", "map2").alias("map3")).first()[0] == expected


def test_sequence(get_session_and_func):
    session, sequence = get_session_and_func("sequence")
    df = session.createDataFrame([(-2, 2)], ("C1", "C2"))
    assert df.select(sequence("C1", "C2").alias("r")).collect() == [
        Row(r=[-2, -1, 0, 1, 2]),
    ]
    df = session.createDataFrame([(4, -4, -2)], ("C1", "C2", "C3"))
    assert df.select(sequence("C1", "C2", "C3").alias("r")).collect() == [Row(r=[4, 2, 0, -2, -4])]


def test_from_csv(get_session_and_func, get_func):
    session, from_csv = get_session_and_func("from_csv")
    schema_of_csv = get_func("schema_of_csv", session)
    data = [("1,2,3",)]
    df = session.createDataFrame(data, ("value",))
    assert df.select(from_csv(df.value, "a INT, b INT, c INT").alias("csv")).collect() == [
        Row(csv=Row(a=1, b=2, c=3))
    ]
    value = data[0][0]
    assert df.select(from_csv(df.value, schema_of_csv(value)).alias("csv")).collect() == [
        Row(csv=Row(_c0=1, _c1=2, _c2=3))
    ]
    data = [("   abc",)]
    df = session.createDataFrame(data, ("value",))
    options = {"ignoreLeadingWhiteSpace": True}
    assert df.select(from_csv(df.value, "s string", options).alias("csv")).collect() == [
        Row(csv=Row(s="abc"))
    ]


def test_aggregate(get_session_and_func, get_func):
    session, aggregate = get_session_and_func("aggregate")
    struct = get_func("struct", session)
    lit = get_func("lit", session)
    df = session.createDataFrame([(1, [20.0, 4.0, 2.0, 6.0, 10.0])], ("id", "some_values"))
    assert (
        df.select(
            aggregate("some_values", lit(0.0).cast("double"), lambda acc, x: acc + x).alias("sum")
        ).first()[0]
        == 42.0
    )
    if isinstance(session, PySparkSession):

        def merge(acc, x):
            count = acc.count + 1
            sum = acc.sum + x
            return struct(count.alias("count"), sum.alias("sum"))

        assert (
            df.select(
                aggregate(
                    "some_values",
                    struct(lit(0).alias("count"), lit(0.0).alias("sum")),
                    merge,
                    lambda acc: acc.sum / acc.count,
                ).alias("mean")
            ).first()[0]
            == 8.4
        )


def test_transform(get_session_and_func, get_func):
    session, transform = get_session_and_func("transform")
    when = get_func("when", session)
    df = session.createDataFrame([(1, [1, 2, 3, 4])], ("key", "some_values"))
    assert df.select(transform("some_values", lambda x: x * 2).alias("doubled")).collect() == [
        Row(doubled=[2, 4, 6, 8])
    ]

    if isinstance(session, PySparkSession):

        def alternate(x, i):
            return when(i % 2 == 0, x).otherwise(-x)

        assert df.select(transform("some_values", alternate).alias("alternated")).collect() == [
            Row(alternated=[1, -2, 3, -4])
        ]


def test_exists(get_session_and_func):
    session, exists = get_session_and_func("exists")
    df = session.createDataFrame([(1, [1, 2, 3, 4]), (2, [3, -1, 0])], ("key", "some_values"))
    assert df.select(exists("some_values", lambda x: x < 0).alias("any_negative")).collect() == [
        Row(any_negative=False),
        Row(any_negative=True),
    ]


def test_forall(get_session_and_func):
    session, forall = get_session_and_func("forall")
    df = session.createDataFrame(
        [(1, ["bar"]), (2, ["foo", "bar"]), (3, ["foobar", "foo"])], ("key", "some_values")
    )
    assert df.select(
        forall("some_values", lambda x: x.rlike("foo")).alias("all_foo")
    ).collect() == [
        Row(all_foo=False),
        Row(all_foo=False),
        Row(all_foo=True),
    ]


def test_filter(get_session_and_func, get_func):
    session, filter = get_session_and_func("filter")
    month = get_func("month", session)
    to_date = get_func("to_date", session)
    df = session.createDataFrame(
        [(1, ["2018-09-20", "2019-02-03", "2019-07-01", "2020-06-01"])], ("key", "some_values")
    )

    def after_second_quarter(x):
        return month(to_date(x)) > 6

    assert df.select(
        filter("some_values", after_second_quarter).alias("after_second_quarter")
    ).first()[0] == ["2018-09-20", "2019-07-01"]


def test_zip_with(get_session_and_func, get_func):
    session, zip_with = get_session_and_func("zip_with")
    concat_ws = get_func("concat_ws", session)
    df = session.createDataFrame([(1, [1, 3, 5, 8], [0, 2, 4, 6])], ("id", "xs", "ys"))
    assert df.select(zip_with("xs", "ys", lambda x, y: x**y).alias("powers")).first()[0] == [
        1.0,
        9.0,
        625.0,
        262144.0,
    ]
    df = session.createDataFrame([(1, ["foo", "bar"], [1, 2, 3])], ("id", "xs", "ys"))
    assert df.select(
        zip_with("xs", "ys", lambda x, y: concat_ws("_", x, y)).alias("xs_ys")
    ).first()[0] == ["foo_1", "bar_2", "3"]


def test_transform_keys(get_session_and_func, get_func):
    session, transform_keys = get_session_and_func("transform_keys")
    upper = get_func("upper", session)
    concat_ws = get_func("concat_ws", session)
    lit = get_func("lit", session)
    df = session.createDataFrame([(1, {"foo": -2.0, "bar": 2.0})], ("id", "data"))
    if isinstance(session, DatabricksSession):
        row = df.select(
            transform_keys("data", lambda k, _: concat_ws("_", k, lit("a"))).alias("data_upper")
        ).head()
        expected = [("bar_a", 2.0), ("foo_a", -2.0)]
        assert sorted(row["data_upper"].items()) == expected
    else:
        row = df.select(transform_keys("data", lambda k, _: upper(k)).alias("data_upper")).head()
        expected = [("BAR", 2.0), ("FOO", -2.0)]
        assert sorted(row["data_upper"].items()) == expected


def test_transform_values(get_session_and_func, get_func):
    session, transform_values = get_session_and_func("transform_values")
    when = get_func("when", session)
    df = session.createDataFrame([(1, {"IT": 10.0, "SALES": 2.0, "OPS": 24.0})], ("id", "data"))
    row = df.select(
        transform_values(
            "data", lambda k, v: when(k.isin("IT", "OPS"), v + 10.0).otherwise(v)
        ).alias("new_data")
    ).head()
    if isinstance(session, (SparkSession, PySparkSession)):
        expected = [("IT", 20.0), ("OPS", 34.0), ("SALES", 2.0)]
    else:
        expected = [("it", 20.0), ("ops", 34.0), ("sales", 2.0)]
    assert sorted(row["new_data"].items()) == expected


def test_map_filter(get_session_and_func, get_func):
    session, map_filter = get_session_and_func("map_filter")
    df = session.createDataFrame([(1, {"foo": 42.0, "bar": 1.0, "baz": 32.0})], ("id", "data"))
    row = df.select(map_filter("data", lambda _, v: v > 30.0).alias("data_filtered")).head()
    assert sorted(row["data_filtered"].items()) == [("baz", 32.0), ("foo", 42.0)]


def test_map_zip_with(get_session_and_func, get_func):
    session, map_zip_with = get_session_and_func("map_zip_with")
    round = get_func("round", session)
    df = session.createDataFrame(
        [(1, {"IT": 24.0, "SALES": 12.00}, {"IT": 2.0, "SALES": 1.4})], ("id", "base", "ratio")
    )
    row = df.select(
        map_zip_with("base", "ratio", lambda k, v1, v2: round(v1 * v2, 2)).alias("updated_data")
    ).head()
    if isinstance(session, (SparkSession, PySparkSession)):
        expected = [("IT", 48.0), ("SALES", 16.8)]
    else:
        expected = [("it", 48.0), ("sales", 16.8)]
    assert sorted(row["updated_data"].items()) == expected


def test_nullif(get_session_and_func):
    session, nullif = get_session_and_func("nullif")
    df = session.createDataFrame(
        [
            (
                None,
                None,
            ),
            (
                1,
                9,
            ),
        ],
        ["a", "b"],
    )
    assert df.select(nullif(df.a, df.b).alias("r")).collect() == [Row(r=None), Row(r=1)]


def test_stack(get_session_and_func, get_func):
    session, stack = get_session_and_func("stack")
    lit = get_func("lit", session)
    df = session.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
    assert df.select(stack(lit(2), df.a, df.b, df.c)).collect() == [
        Row(key=1, value=2),
        Row(key=3, value=None),
    ]


def test_make_interval(get_session_and_func, get_func):
    session, make_interval = get_session_and_func("make_interval")
    df = session.createDataFrame(
        [[100, 11, 1, 1, 12, 30, 01.001001]], ["year", "month", "week", "day", "hour", "min", "sec"]
    )
    assert (
        df.select(
            make_interval(df.year, df.month, df.week, df.day, df.hour, df.min, df.sec)
            .cast("string")
            .alias("r")
        ).first()[0]
        == "100 years 11 months 8 days 12 hours 30 minutes 1.001001 seconds"
    )
    assert (
        df.select(
            make_interval(df.year, df.month, df.week, df.day, df.hour, df.min)
            .cast("string")
            .alias("r")
        ).first()[0]
        == "100 years 11 months 8 days 12 hours 30 minutes"
    )
    assert (
        df.select(
            make_interval(df.year, df.month, df.week, df.day, df.hour).cast("string").alias("r")
        ).first()[0]
        == "100 years 11 months 8 days 12 hours"
    )
    assert (
        df.select(
            make_interval(df.year, df.month, df.week, df.day).cast("string").alias("r")
        ).first()[0]
        == "100 years 11 months 8 days"
    )
    assert (
        df.select(make_interval(df.year, df.month, df.week).cast("string").alias("r")).first()[0]
        == "100 years 11 months 7 days"
    )
    assert (
        df.select(make_interval(df.year, df.month).cast("string").alias("r")).first()[0]
        == "100 years 11 months"
    )
    assert df.select(make_interval(df.year).cast("string").alias("r")).first()[0] == "100 years"


def test_try_add(get_session_and_func, get_func, get_types):
    session, try_add = get_session_and_func("try_add")
    to_date = get_func("to_date", session)
    make_interval = get_func("make_interval", session)
    lit = get_func("lit", session)
    types = get_types(session)
    df = session.createDataFrame([(1982, 15), (1990, 2)], ["birth", "age"])
    assert df.select(try_add(df.birth, df.age).alias("r")).collect() == [
        Row(r=1997),
        Row(r=1992),
    ]
    schema = types.StructType(
        [
            types.StructField("i", types.IntegerType(), True),
            types.StructField("d", types.StringType(), True),
        ]
    )
    df = session.createDataFrame([(1, "2015-09-30")], schema)
    df = df.select(df.i, to_date(df.d).alias("d"))
    assert df.select(try_add(df.d, df.i).alias("r")).collect() == [
        Row(r=datetime.date(2015, 10, 1))
    ]
    assert df.select(try_add(df.d, make_interval(df.i)).alias("r")).collect() == [
        Row(r=datetime.date(2016, 9, 30))
    ]
    assert df.select(
        try_add(df.d, make_interval(lit(0), lit(0), lit(0), df.i)).alias("r")
    ).collect() == [Row(r=datetime.date(2015, 10, 1))]
    assert df.select(
        try_add(make_interval(df.i), make_interval(df.i)).cast("string").alias("r")
    ).collect() == [Row(r="2 years")]


def test_try_avg(get_session_and_func, get_func):
    session, try_avg = get_session_and_func("try_avg")
    df = session.createDataFrame([(1982, 15), (1990, 2)], ["birth", "age"])
    assert df.select(try_avg("age")).first()[0] == 8.5


def test_try_divide(get_session_and_func, get_func):
    session, try_divide = get_session_and_func("try_divide")
    make_interval = get_func("make_interval", session)
    lit = get_func("lit", session)
    df = session.createDataFrame([(6000, 15), (1990, 2)], ["a", "b"])
    assert df.select(try_divide(df.a, df.b).alias("r")).collect() == [
        Row(r=400.0),
        Row(r=995.0),
    ]
    df = session.createDataFrame([(1, 2)], ["year", "month"])
    assert (
        df.select(try_divide(make_interval(df.year), df.month).cast("string").alias("r")).first()[0]
        == "6 months"
    )
    assert (
        df.select(
            try_divide(make_interval(df.year, df.month), lit(2)).cast("string").alias("r")
        ).first()[0]
        == "7 months"
    )
    assert (
        df.select(
            try_divide(make_interval(df.year, df.month), lit(0)).cast("string").alias("r")
        ).first()[0]
        is None
    )


def test_try_multiply(get_session_and_func, get_func):
    session, try_multiply = get_session_and_func("try_multiply")
    make_interval = get_func("make_interval", session)
    lit = get_func("lit", session)
    df = session.createDataFrame([(6000, 15), (1990, 2)], ["a", "b"])
    assert df.select(try_multiply(df.a, df.b).alias("r")).collect() == [
        Row(r=90000),
        Row(r=3980),
    ]
    df = session.createDataFrame([(2, 3)], ["a", "b"])
    assert (
        df.select(try_multiply(make_interval(df.a), df.b).cast("string").alias("r")).first()[0]
        == "6 years"
    )


def test_try_subtract(get_session_and_func, get_func, get_types):
    session, try_subtract = get_session_and_func("try_subtract")
    make_interval = get_func("make_interval", session)
    types = get_types(session)
    lit = get_func("lit", session)
    to_date = get_func("to_date", session)
    df = session.createDataFrame([(6000, 15), (1990, 2)], ["a", "b"])
    assert df.select(try_subtract(df.a, df.b).alias("r")).collect() == [
        Row(r=5985),
        Row(r=1988),
    ]
    schema = types.StructType(
        [
            types.StructField("i", types.IntegerType(), True),
            types.StructField("d", types.StringType(), True),
        ]
    )
    df = session.createDataFrame([(1, "2015-09-30")], schema)
    df = df.select(df.i, to_date(df.d).alias("d"))
    assert df.select(try_subtract(df.d, df.i).alias("r")).first()[0] == datetime.date(2015, 9, 29)
    assert df.select(try_subtract(df.d, make_interval(df.i)).alias("r")).first()[
        0
    ] == datetime.date(2014, 9, 30)
    assert df.select(
        try_subtract(df.d, make_interval(lit(0), lit(0), lit(0), df.i)).alias("r")
    ).first()[0] == datetime.date(2015, 9, 29)
    assert (
        df.select(
            try_subtract(make_interval(df.i), make_interval(df.i)).cast("string").alias("r")
        ).first()[0]
        == "0 seconds"
    )


def test_try_sum(get_session_and_func, get_func):
    session, try_sum = get_session_and_func("try_sum")
    assert session.range(10).select(try_sum("id")).first()[0] == 45


def test_try_to_binary(get_session_and_func, get_func):
    session, try_to_binary = get_session_and_func("try_to_binary")
    lit = get_func("lit", session)
    df = session.createDataFrame([("abc",)], ["e"])
    assert df.select(try_to_binary(df.e, lit("utf-8")).alias("r")).first()[0] == bytearray(b"abc")
    df = session.createDataFrame([("414243",)], ["e"])
    assert df.select(try_to_binary(df.e).alias("r")).first()[0] == bytearray(b"ABC")


def test_try_to_number(get_session_and_func, get_func):
    session, try_to_number = get_session_and_func("try_to_number")
    lit = get_func("lit", session)
    df = session.createDataFrame([("$78.12",)], ["e"])
    actual = df.select(try_to_number(df.e, lit("$99.99")).alias("r")).first()[0]
    if isinstance(session, (SparkSession, DatabricksSession)):
        expected = 78.12
    else:
        expected = Decimal("78.12")
    assert actual == expected


def test_to_binary(get_session_and_func, get_func):
    session, to_binary = get_session_and_func("to_binary")
    lit = get_func("lit", session)
    df = session.createDataFrame([("abc",)], ["e"])
    assert df.select(to_binary(df.e, lit("utf-8")).alias("r")).first()[0] == bytearray(b"abc")
    df = session.createDataFrame([("414243",)], ["e"])
    assert df.select(to_binary(df.e).alias("r")).first()[0] == bytearray(b"ABC")


def test_aes_decrypt(get_session_and_func, get_func):
    session, aes_decrypt = get_session_and_func("aes_decrypt")
    unhex = get_func("unhex", session)
    unbase64 = get_func("unbase64", session)
    df = session.createDataFrame(
        [
            (
                "AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4",
                "abcdefghijklmnop12345678ABCDEFGH",
                "GCM",
                "DEFAULT",
                "This is an AAD mixed into the input",
            )
        ],
        ["input", "key", "mode", "padding", "aad"],
    )
    assert df.select(
        aes_decrypt(unbase64(df.input), df.key, df.mode, df.padding, df.aad).alias("r")
    ).first()[0] == bytearray(b"Spark")
    df = session.createDataFrame(
        [
            (
                "AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg=",
                "abcdefghijklmnop12345678ABCDEFGH",
                "CBC",
                "DEFAULT",
            )
        ],
        ["input", "key", "mode", "padding"],
    )
    assert df.select(
        aes_decrypt(unbase64(df.input), df.key, df.mode, df.padding).alias("r")
    ).first()[0] == bytearray(b"Spark")
    assert df.select(aes_decrypt(unbase64(df.input), df.key, df.mode).alias("r")).first()[
        0
    ] == bytearray(b"Spark")
    df = session.createDataFrame(
        [
            (
                "83F16B2AA704794132802D248E6BFD4E380078182D1544813898AC97E709B28A94",
                "0000111122223333",
            )
        ],
        ["input", "key"],
    )
    assert df.select(aes_decrypt(unhex(df.input), df.key).alias("r")).first()[0] == bytearray(
        b"Spark"
    )


def test_aes_encrypt(get_session_and_func, get_func):
    session, aes_encrypt = get_session_and_func("aes_encrypt")
    to_binary = get_func("to_binary", session)
    base64 = get_func("base64", session)
    unbase64 = get_func("unbase64", session)
    lit = get_func("lit", session)
    aes_decrypt = get_func("aes_decrypt", session)
    df = session.createDataFrame(
        [
            (
                "Spark",
                "abcdefghijklmnop12345678ABCDEFGH",
                "GCM",
                "DEFAULT",
                "000000000000000000000000",
                "This is an AAD mixed into the input",
            )
        ],
        ["input", "key", "mode", "padding", "iv", "aad"],
    )
    assert (
        df.select(
            base64(
                aes_encrypt(
                    df.input, df.key, df.mode, df.padding, to_binary(df.iv, lit("hex")), df.aad
                )
            ).alias("r")
        ).first()[0]
        == "AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4"
    )
    assert (
        df.select(
            base64(
                aes_encrypt(df.input, df.key, df.mode, df.padding, to_binary(df.iv, lit("hex")))
            ).alias("r")
        ).first()[0]
        == "AAAAAAAAAAAAAAAAQiYi+sRNYDAOTjdSEcYBFsAWPL1f"
    )
    df = session.createDataFrame(
        [
            (
                "Spark SQL",
                "1234567890abcdef",
                "ECB",
                "PKCS",
            )
        ],
        ["input", "key", "mode", "padding"],
    )
    assert df.select(
        aes_decrypt(
            aes_encrypt(df.input, df.key, df.mode, df.padding), df.key, df.mode, df.padding
        ).alias("r")
    ).first()[0] == bytearray(b"Spark SQL")
    df = session.createDataFrame(
        [
            (
                "Spark SQL",
                "0000111122223333",
                "ECB",
            )
        ],
        ["input", "key", "mode"],
    )
    assert df.select(
        aes_decrypt(aes_encrypt(df.input, df.key, df.mode), df.key, df.mode).alias("r")
    ).first()[0] == bytearray(b"Spark SQL")
    df = session.createDataFrame(
        [
            (
                "Spark SQL",
                "abcdefghijklmnop",
            )
        ],
        ["input", "key"],
    )
    assert (
        df.select(
            aes_decrypt(unbase64(base64(aes_encrypt(df.input, df.key))), df.key)
            .cast("STRING")
            .alias("r")
        ).first()[0]
        == "Spark SQL"
    )


def test_bitmap_bit_position(get_session_and_func, get_func):
    session, bitmap_bit_position = get_session_and_func("bitmap_bit_position")
    df = session.createDataFrame([(123,)], ["a"])
    assert df.select(bitmap_bit_position(df.a).alias("r")).collect() == [Row(r=122)]


def test_bitmap_bucket_number(get_session_and_func, get_func):
    session, bitmap_bucket_number = get_session_and_func("bitmap_bucket_number")
    df = session.createDataFrame([(123,)], ["a"])
    assert df.select(bitmap_bucket_number(df.a).alias("r")).collect() == [Row(r=1)]


def test_bitmap_construct_agg(get_session_and_func, get_func):
    session, bitmap_construct_agg = get_session_and_func("bitmap_construct_agg")
    substring = get_func("substring", session)
    hex = get_func("hex", session)
    bitmap_bit_position = get_func("bitmap_bit_position", session)
    df = session.createDataFrame([(1,), (2,), (3,)], ["a"])
    assert (
        df.select(
            substring(hex(bitmap_construct_agg(bitmap_bit_position(df.a))), 0, 6).alias("r")
        ).first()[0]
        == "070000"
    )


def test_bitmap_count(get_session_and_func, get_func):
    session, bitmap_count = get_session_and_func("bitmap_count")
    to_binary = get_func("to_binary", session)
    lit = get_func("lit", session)
    df = session.createDataFrame([("FFFF",)], ["a"])
    assert df.select(bitmap_count(to_binary(df.a, lit("hex"))).alias("r")).first()[0] == 16


def test_bitmap_or_agg(get_session_and_func, get_func):
    session, bitmap_or_agg = get_session_and_func("bitmap_or_agg")
    to_binary = get_func("to_binary", session)
    lit = get_func("lit", session)
    substring = get_func("substring", session)
    hex = get_func("hex", session)
    df = session.createDataFrame([("10",), ("20",), ("40",)], ["a"])
    assert (
        df.select(
            substring(hex(bitmap_or_agg(to_binary(df.a, lit("hex")))), 0, 6).alias("r")
        ).first()[0]
        == "700000"
    )


def test_any_value(get_session_and_func):
    session, any_value = get_session_and_func("any_value")
    df = session.createDataFrame(
        [("c", None), ("a", 2), ("a", 3), ("b", 8), ("b", 2)], ["c1", "c2"]
    )
    non_ignore_nulls = df.select(any_value("c1"), any_value("c2")).collect()
    ignore_nulls = df.select(any_value("c1", True), any_value("c2", True)).collect()
    # Always ignores nulls
    if isinstance(session, (BigQuerySession, DuckDBSession)):
        assert non_ignore_nulls == [Row(value="c", value2=2)]
        assert ignore_nulls == [Row(value="c", value2=2)]
    # SQLGlot converts any_value to max
    elif isinstance(session, PostgresSession):
        assert non_ignore_nulls == [Row(value="c", value2=8)]
        assert ignore_nulls == [Row(value="c", value2=8)]
    # Always includes nulls
    elif isinstance(session, SnowflakeSession):
        assert non_ignore_nulls == [Row(value="c", value2=None)]
        assert ignore_nulls == [Row(value="c", value2=None)]
    else:
        assert non_ignore_nulls == [Row(value="c", value2=None)]
        assert ignore_nulls == [Row(value="c", value2=2)]


def test_approx_percentile(get_session_and_func, get_func):
    session, approx_percentile = get_session_and_func("approx_percentile")
    col = get_func("col", session)
    randn = get_func("randn", session)
    key = (col("id") % 3).alias("key")
    value = (randn(42) + key * 10).alias("value")
    df = session.range(0, 1000, 1, 1).select(key, value)
    assert df.select(approx_percentile("value", [0.25, 0.5, 0.75], 1000000)).collect() == [
        Row(value=[0.7264430125286507, 9.98975299938167, 19.335304783039014])
    ]
    assert sorted(df.groupBy("key").agg(approx_percentile("value", 0.5, 1000000)).collect()) == [
        Row(key=0, value=-0.03519435193070876),
        Row(key=1, value=9.990389751837329),
        Row(key=2, value=19.967859769284075),
    ]


def test_bool_and(get_session_and_func, get_func):
    session, bool_and = get_session_and_func("bool_and")
    df = session.createDataFrame([[True], [True], [True]], ["flag"])
    assert df.select(bool_and("flag")).collect() == [Row(flag=True)]
    df = session.createDataFrame([[True], [False], [True]], ["flag"])
    assert df.select(bool_and("flag")).collect() == [Row(flag=False)]
    df = session.createDataFrame([[False], [False], [False]], ["flag"])
    assert df.select(bool_and("flag")).collect() == [Row(flag=False)]


def test_bool_or(get_session_and_func, get_func):
    session, bool_or = get_session_and_func("bool_or")
    df = session.createDataFrame([[True], [True], [True]], ["flag"])
    assert df.select(bool_or("flag")).collect() == [Row(flag=True)]
    df = session.createDataFrame([[True], [False], [True]], ["flag"])
    assert df.select(bool_or("flag")).collect() == [Row(flag=True)]
    df = session.createDataFrame([[False], [False], [False]], ["flag"])
    assert df.select(bool_or("flag")).collect() == [Row(flag=False)]


def test_btrim(get_session_and_func, get_func):
    session, btrim = get_session_and_func("btrim")
    df = session.createDataFrame(
        [
            (
                "SSparkSQLS",
                "SL",
            )
        ],
        ["a", "b"],
    )
    assert df.select(btrim(df.a, df.b).alias("r")).collect() == [Row(r="parkSQ")]
    df = session.createDataFrame([("    SparkSQL   ",)], ["a"])
    assert df.select(btrim(df.a).alias("r")).collect() == [Row(r="SparkSQL")]


def test_call_function(get_session_and_func, get_func):
    session, call_function = get_session_and_func("call_function")
    df = session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "name"])
    assert df.select(call_function("avg", "id")).first()[0] == 2.0


def test_cardinality(get_session_and_func, get_func):
    session, cardinality = get_session_and_func("cardinality")
    df = session.createDataFrame([([1, 2, 3],), ([1],), ([],)], ["data"])
    assert df.select(cardinality("data")).collect() == [Row(value=3), Row(value=1), Row(value=0)]


def test_char(get_session_and_func, get_func):
    session, char = get_session_and_func("char")
    lit = get_func("lit", session)
    assert session.range(1).select(char(lit(65))).first()[0] == "A"


def test_char_length(get_session_and_func, get_func):
    session, char_length = get_session_and_func("char_length")
    lit = get_func("lit", session)
    assert session.range(1).select(char_length(lit("SparkSQL"))).first()[0] == 8


def test_character_length(get_session_and_func, get_func):
    session, character_length = get_session_and_func("character_length")
    lit = get_func("lit", session)
    assert session.range(1).select(character_length(lit("SparkSQL"))).first()[0] == 8


def test_contains(get_session_and_func, get_func):
    session, contains = get_session_and_func("contains")
    to_binary = get_func("to_binary", session)
    df = session.createDataFrame([("Spark SQL", "Spark")], ["a", "b"])
    assert df.select(contains(df.a, df.b).alias("r")).collect() == [Row(r=True)]
    df = session.createDataFrame(
        [
            (
                "414243",
                "4243",
            )
        ],
        ["c", "d"],
    )
    df = df.select(to_binary("c").alias("c"), to_binary("d").alias("d"))
    assert df.select(contains("c", "d"), contains("d", "c")).collect() == [
        Row(value=True, value2=False),
    ]


def test_convert_timezone(get_session_and_func, get_func):
    session, convert_timezone = get_session_and_func("convert_timezone")
    lit = get_func("lit", session)
    df = session.createDataFrame([("2015-04-08",)], ["dt"])
    expected = datetime.datetime(2015, 4, 8, 8, 0)
    assert df.select(convert_timezone(None, lit("Asia/Hong_Kong"), "dt").alias("ts")).collect() == [
        Row(ts=expected)
    ]
    assert df.select(
        convert_timezone(lit("America/Los_Angeles"), lit("Asia/Hong_Kong"), "dt").alias("ts")
    ).collect() == [Row(ts=datetime.datetime(2015, 4, 8, 15, 0))]


def test_count_if(get_session_and_func, get_func):
    session, count_if = get_session_and_func("count_if")
    df = session.createDataFrame([("a", 1), ("a", 2), ("a", 3), ("b", 8), ("b", 2)], ["c1", "c2"])
    assert df.select(count_if(df.c2 % 2 == 0)).collect() == [Row(value=3)]


def test_count_min_sketch(get_session_and_func, get_func):
    session, count_min_sketch = get_session_and_func("count_min_sketch")
    lit = get_func("lit", session)
    hex = get_func("hex", session)
    df = session.createDataFrame([[1], [2], [1]], ["data"])
    df = df.agg(count_min_sketch(df.data, lit(0.5), lit(0.5), lit(1)).alias("sketch"))
    assert df.select(hex(df.sketch).alias("r")).collect() == [
        Row(
            r="0000000100000000000000030000000100000004000000005D8D6AB90000000000000000000000000000000200000000000000010000000000000000"
        )
    ]


def test_curdate(get_session_and_func, get_func):
    session, curdate = get_session_and_func("curdate")
    assert session.range(1).select(curdate()).first()[0] == datetime.date.today()


def test_current_user(get_session_and_func, get_func):
    session, current_user = get_session_and_func("current_user")
    assert len(session.range(1).select(current_user()).first()[0]) > 0


def test_current_catalog(get_session_and_func, get_func):
    session, current_catalog = get_session_and_func("current_catalog")
    if isinstance(session, DatabricksSession):
        assert session.range(1).select(current_catalog()).first()[0] == "sqlframe"
    else:
        assert session.range(1).select(current_catalog()).first()[0] == "spark_catalog"


def test_current_database(get_session_and_func, get_func):
    session, current_database = get_session_and_func("current_database")
    assert session.range(1).select(current_database()).first()[0] in ("db1", "default", "public")


def test_current_schema(get_session_and_func, get_func):
    session, current_schema = get_session_and_func("current_schema")
    assert session.range(1).select(current_schema()).first()[0] in ("db1", "default", "public")


def test_current_timezone(get_session_and_func, get_func):
    session, current_timezone = get_session_and_func("current_timezone")
    assert session.range(1).select(current_timezone()).first()[0] == "UTC"


def test_date_from_unix_date(get_session_and_func, get_func):
    session, date_from_unix_date = get_session_and_func("date_from_unix_date")
    lit = get_func("lit", session)
    assert session.range(1).select(date_from_unix_date(lit(1))).first()[0] == datetime.date(
        1970, 1, 2
    )


def test_date_part(get_session_and_func, get_func):
    session, date_part = get_session_and_func("date_part")
    lit = get_func("lit", session)
    df = session.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ["ts"])
    assert df.select(
        date_part(lit("YEAR"), "ts").alias("year"),
        date_part(lit("month"), "ts").alias("month"),
        date_part(lit("WEEK"), "ts").alias("week"),
        date_part(lit("D"), "ts").alias("day"),
        date_part(lit("M"), "ts").alias("minute"),
        date_part(lit("S"), "ts").alias("second"),
    ).collect() == [Row(year=2015, month=4, week=15, day=8, minute=8, second=Decimal("15.000000"))]


def test_dateadd(get_session_and_func, get_func):
    session, dateadd = get_session_and_func("dateadd")
    assert session.createDataFrame(
        [
            (
                "2015-04-08",
                2,
            )
        ],
        ["dt", "add"],
    ).select(dateadd("dt", 1)).first()[0] == datetime.date(2015, 4, 9)
    assert session.createDataFrame(
        [
            (
                "2015-04-08",
                2,
            )
        ],
        ["dt", "add"],
    ).select(dateadd("dt", 2)).first()[0] == datetime.date(2015, 4, 10)
    assert session.createDataFrame(
        [
            (
                "2015-04-08",
                2,
            )
        ],
        ["dt", "add"],
    ).select(dateadd("dt", -1)).first()[0] == datetime.date(2015, 4, 7)


def test_datediff(get_session_and_func, get_func):
    session, datediff = get_session_and_func("datediff")
    df = session.createDataFrame([("2015-04-08", "2015-05-10")], ["d1", "d2"])
    assert df.select(datediff(df.d2, df.d1).alias("diff")).first()[0] == 32


def test_datepart(get_session_and_func, get_func):
    session, datepart = get_session_and_func("datepart")
    lit = get_func("lit", session)
    df = session.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ["ts"])
    assert df.select(
        datepart(lit("YEAR"), "ts").alias("year"),
        datepart(lit("month"), "ts").alias("month"),
        datepart(lit("WEEK"), "ts").alias("week"),
        datepart(lit("D"), "ts").alias("day"),
        datepart(lit("M"), "ts").alias("minute"),
        datepart(lit("S"), "ts").alias("second"),
    ).collect() == [Row(year=2015, month=4, week=15, day=8, minute=8, second=Decimal("15.000000"))]


def test_day(get_session_and_func, get_func):
    session, day = get_session_and_func("day")
    df = session.createDataFrame([("2015-04-08",)], ["dt"])
    assert df.select(day("dt").alias("day")).first()[0] == 8
    df = session.createDataFrame([(datetime.datetime(2025, 4, 8),)], ["dt"])
    assert df.select(day("dt").alias("day")).first()[0] == 8
    df = session.createDataFrame([("2015-04-08 01:00:00",)], ["dt"])
    assert df.select(day("dt").alias("day")).first()[0] == 8


def test_elt(get_session_and_func, get_func):
    session, elt = get_session_and_func("elt")
    df = session.createDataFrame([(1, "scala", "java")], ["a", "b", "c"])
    assert df.select(elt(df.a, df.b, df.c).alias("r")).first()[0] == "scala"


def test_endswith(get_session_and_func, get_func):
    session, endswith = get_session_and_func("endswith")
    to_binary = get_func("to_binary", session)
    df = session.createDataFrame(
        [
            (
                "Spark SQL",
                "Spark",
            )
        ],
        ["a", "b"],
    )
    assert df.select(endswith(df.a, df.b).alias("r")).collect() == [Row(r=False)]
    df = session.createDataFrame(
        [
            (
                "414243",
                "4243",
            )
        ],
        ["e", "f"],
    )
    df = df.select(to_binary("e").alias("e"), to_binary("f").alias("f"))
    assert df.select(endswith("e", "f"), endswith("f", "e")).collect() == [
        Row(value1=True, value2=False)
    ]


def test_equal_null(get_session_and_func, get_func):
    session, equal_null = get_session_and_func("equal_null")
    df = session.createDataFrame(
        [
            (
                None,
                None,
            ),
            (
                1,
                9,
            ),
        ],
        ["a", "b"],
    )
    assert df.select(equal_null(df.a, df.b).alias("r")).collect() == [Row(r=True), Row(r=False)]


def test_every(get_session_and_func, get_func):
    session, every = get_session_and_func("every")
    assert session.createDataFrame([[True], [True], [True]], ["flag"]).select(
        every("flag")
    ).collect() == [Row(value=True)]
    assert session.createDataFrame([[True], [False], [True]], ["flag"]).select(
        every("flag")
    ).collect() == [Row(value=False)]
    assert session.createDataFrame([[False], [False], [False]], ["flag"]).select(
        every("flag")
    ).collect() == [Row(value=False)]


def test_extract(get_session_and_func, get_func):
    session, extract = get_session_and_func("extract")
    lit = get_func("lit", session)
    df = session.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ["ts"])
    result = df.select(
        extract(lit("YEAR"), "ts").alias("year"),
        extract(lit("MONTH"), "ts").alias("month"),
        extract(lit("WEEK"), "ts").alias("week"),
        extract(lit("DAY"), "ts").alias("day"),
        extract(lit("MINUTE"), "ts").alias("minute"),
        extract(lit("SECOND"), "ts").alias("second"),
    ).collect()
    if isinstance(session, BigQuerySession):
        assert result == [Row(year=2015, month=4, week=14, day=8, minute=8, second=15)]
    else:
        assert result == [
            Row(year=2015, month=4, week=15, day=8, minute=8, second=Decimal("15.000000"))
        ]


def test_find_in_set(get_session_and_func, get_func):
    session, find_in_set = get_session_and_func("find_in_set")
    df = session.createDataFrame([("ab", "abc,b,ab,c,def")], ["a", "b"])
    assert df.select(find_in_set(df.a, df.b).alias("r")).first()[0] == 3


def test_first_value(get_session_and_func, get_func):
    session, first_value = get_session_and_func("first_value")
    assert session.createDataFrame(
        [(None, 1), ("a", 2), ("a", 3), ("b", 8), ("b", 2)], ["a", "b"]
    ).select(first_value("a"), first_value("b")).collect() == [Row(value1=None, value2=1)]
    assert session.createDataFrame(
        [(None, 1), ("a", 2), ("a", 3), ("b", 8), ("b", 2)], ["a", "b"]
    ).select(first_value("a", True), first_value("b", True)).collect() == [
        Row(value1="a", value2=1)
    ]


def test_get(get_session_and_func, get_func):
    session, get = get_session_and_func("get")
    col = get_func("col", session)
    df = session.createDataFrame([(["a", "b", "c"], 1)], ["data", "index"])
    assert df.select(get(df.data, 1)).collect() == [Row(value="b")]
    assert df.select(get(df.data, -1)).collect() == [Row(value=None)]
    assert df.select(get(df.data, 3)).collect() == [Row(value=None)]
    assert df.select(get(df.data, "index")).collect() == [Row(value="b")]
    assert df.select(get(df.data, col("index") - 1)).collect() == [Row(value="a")]


def test_get_active_spark_context(get_session_and_func):
    session, get_active_spark_context = get_session_and_func("get_active_spark_context")
    if isinstance(session, PySparkSession):
        assert session.sparkContext == get_active_spark_context()
    else:
        assert session.spark_session.sparkContext == get_active_spark_context()


def test_grouping(get_session_and_func, get_func):
    session, grouping = get_session_and_func("grouping")
    sum = get_func("sum", session)
    df = session.createDataFrame([("Alice", 2), ("Bob", 5)], ("name", "age"))
    assert df.cube("name").agg(grouping("name"), sum("age")).orderBy("name").collect() == [
        Row(name=None, value1=1, value2=7),
        Row(name="Alice", value1=0, value2=2),
        Row(name="Bob", value1=0, value2=5),
    ]


def test_histogram_numeric(get_session_and_func, get_func):
    session, histogram_numeric = get_session_and_func("histogram_numeric")
    lit = get_func("lit", session)
    df = session.createDataFrame([("a", 1), ("a", 2), ("a", 3), ("b", 8), ("b", 2)], ["c1", "c2"])
    if isinstance(session, DatabricksSession):
        assert df.select(histogram_numeric("c2", lit(5))).collect() == [
            Row(
                value=[
                    Row(x=1, y=1.0),
                    Row(x=2, y=1.0),
                    Row(x=2, y=1.0),
                    Row(x=3, y=1.0),
                    Row(x=8, y=1.0),
                ]
            )
        ]
    else:
        assert df.select(histogram_numeric("c2", lit(5))).collect() == [
            Row(value=[Row(x=1, y=1.0), Row(x=2, y=2.0), Row(x=3, y=1.0), Row(x=8, y=1.0)])
        ]


def test_hll_sketch_agg(get_session_and_func, get_func):
    session, hll_sketch_agg = get_session_and_func("hll_sketch_agg")
    lit = get_func("lit", session)
    hll_sketch_estimate = get_func("hll_sketch_estimate", session)
    df = session.createDataFrame([1, 2, 2, 3], "INT")
    assert (
        df.agg(hll_sketch_estimate(hll_sketch_agg("value")).alias("distinct_cnt")).first()[0] == 3
    )
    assert (
        df.agg(hll_sketch_estimate(hll_sketch_agg("value", lit(12))).alias("distinct_cnt")).first()[
            0
        ]
        == 3
    )
    assert (
        df.agg(hll_sketch_estimate(hll_sketch_agg("value", 12)).alias("distinct_cnt")).first()[0]
        == 3
    )


def test_hll_sketch_estimate(get_session_and_func, get_func):
    session, hll_sketch_estimate = get_session_and_func("hll_sketch_estimate")
    hll_sketch_agg = get_func("hll_sketch_agg", session)
    df = session.createDataFrame([1, 2, 2, 3], "INT")
    assert (
        df.agg(hll_sketch_estimate(hll_sketch_agg("value")).alias("distinct_cnt")).first()[0] == 3
    )


def test_hll_union(get_session_and_func, get_func):
    session, hll_union = get_session_and_func("hll_union")
    hll_sketch_agg = get_func("hll_sketch_agg", session)
    hll_sketch_estimate = get_func("hll_sketch_estimate", session)
    df = session.createDataFrame([(1, 4), (2, 5), (2, 5), (3, 6)], "struct<v1:int,v2:int>")
    df = df.agg(hll_sketch_agg("v1").alias("sketch1"), hll_sketch_agg("v2").alias("sketch2"))
    df = df.withColumn("distinct_cnt", hll_sketch_estimate(hll_union("sketch1", "sketch2")))
    assert df.drop("sketch1", "sketch2").first()[0] == 6


def test_hll_union_agg(get_session_and_func, get_func):
    session, hll_union_agg = get_session_and_func("hll_union_agg")
    hll_sketch_agg = get_func("hll_sketch_agg", session)
    hll_sketch_estimate = get_func("hll_sketch_estimate", session)
    lit = get_func("lit", session)
    col = get_func("col", session)
    df1 = session.createDataFrame([1, 2, 2, 3], "INT")
    df1 = df1.agg(hll_sketch_agg("value").alias("sketch"))
    df2 = session.createDataFrame([4, 5, 5, 6], "INT")
    df2 = df2.agg(hll_sketch_agg("value").alias("sketch"))
    df3 = df1.union(df2).agg(hll_sketch_estimate(hll_union_agg("sketch")).alias("distinct_cnt"))
    assert df3.drop("sketch").first()[0] == 6
    df4 = df1.union(df2).agg(
        hll_sketch_estimate(hll_union_agg("sketch", lit(False))).alias("distinct_cnt")
    )
    assert df4.drop("sketch").first()[0] == 6
    df5 = df1.union(df2).agg(
        hll_sketch_estimate(hll_union_agg(col("sketch"), lit(False))).alias("distinct_cnt")
    )
    assert df5.drop("sketch").first()[0] == 6


def test_ifnull(get_session_and_func, get_func):
    session, ifnull = get_session_and_func("ifnull")
    lit = get_func("lit", session)
    df = session.createDataFrame([(None,), (1,)], ["e"])
    assert df.select(ifnull(df.e, lit(8))).collect() == [
        Row(value=8),
        Row(value=1),
    ]


def test_ilike(get_session_and_func, get_func):
    session, ilike = get_session_and_func("ilike")
    lit = get_func("lit", session)
    df = session.createDataFrame([("Spark", "_park")], ["a", "b"])
    assert df.select(ilike(df.a, df.b).alias("r")).collect() == [Row(r=True)]
    df = session.createDataFrame(
        [("%SystemDrive%/Users/John", "/%SystemDrive/%//Users%")], ["a", "b"]
    )
    assert df.select(ilike(df.a, df.b, lit("/")).alias("r")).collect() == [Row(r=True)]


def test_inline(get_session_and_func, get_func):
    session, inline = get_session_and_func("inline")
    df = session.createDataFrame([Row(structlist=[Row(a=1, b=2), Row(a=3, b=4)])])
    assert df.select(inline(df.structlist)).collect() == [Row(a=1, b=2), Row(a=3, b=4)]


def test_inline_outer(get_session_and_func, get_func):
    session, inline_outer = get_session_and_func("inline_outer")
    df = session.createDataFrame(
        [Row(id=1, structlist=[Row(a=1, b=2), Row(a=3, b=4)]), Row(id=2, structlist=[])]
    )
    assert df.select("id", inline_outer(df.structlist)).collect() == [
        Row(id=1, a=1, b=2),
        Row(id=1, a=3, b=4),
        Row(id=2, a=None, b=None),
    ]


def test_isnotnull(get_session_and_func, get_func):
    session, isnotnull = get_session_and_func("isnotnull")
    df = session.createDataFrame([(None,), (1,)], ["e"])
    assert df.select(isnotnull(df.e).alias("r")).collect() == [Row(r=False), Row(r=True)]


def test_java_method(get_session_and_func, get_func):
    session, java_method = get_session_and_func("java_method")
    lit = get_func("lit", session)
    assert (
        session.range(1)
        .select(
            java_method(
                lit("java.util.UUID"),
                lit("fromString"),
                lit("a5cf6c42-0c85-418f-af6c-3e4e5b1328f2"),
            )
        )
        .first()[0]
        == "a5cf6c42-0c85-418f-af6c-3e4e5b1328f2"
    )


def test_json_array_length(get_session_and_func, get_func):
    session, json_array_length = get_session_and_func("json_array_length")
    df = session.createDataFrame([(None,), ("[1, 2, 3]",), ("[]",)], ["data"])
    assert df.select(json_array_length(df.data).alias("r")).collect() == [
        Row(r=None),
        Row(r=3),
        Row(r=0),
    ]


def test_json_object_keys(get_session_and_func, get_func):
    session, json_object_keys = get_session_and_func("json_object_keys")
    df = session.createDataFrame([(None,), ("{}",), ('{"key1":1, "key2":2}',)], ["data"])
    assert df.select(json_object_keys(df.data).alias("r")).collect() == [
        Row(r=None),
        Row(r=[]),
        Row(r=["key1", "key2"]),
    ]


def test_last_value(get_session_and_func, get_func):
    session, last_value = get_session_and_func("last_value")
    assert session.createDataFrame(
        [("a", 1), ("a", 2), ("a", 3), ("b", 8), (None, 2)], ["a", "b"]
    ).select(last_value("a"), last_value("b")).collect() == [Row(value1=None, value2=2)]
    assert session.createDataFrame(
        [("a", 1), ("a", 2), ("a", 3), ("b", 8), (None, 2)], ["a", "b"]
    ).select(last_value("a", True), last_value("b", True)).collect() == [Row(value1="b", value2=2)]


def test_lcase(get_session_and_func, get_func):
    session, lcase = get_session_and_func("lcase")
    lit = get_func("lit", session)
    assert session.range(1).select(lcase(lit("Spark"))).first()[0] == "spark"


def test_left(get_session_and_func, get_func):
    session, left = get_session_and_func("left")
    df = session.createDataFrame(
        [
            (
                "Spark SQL",
                3,
            )
        ],
        ["a", "b"],
    )
    assert df.select(left(df.a, df.b).alias("r")).collect() == [Row(r="Spa")]


def test_like(get_session_and_func, get_func):
    session, like = get_session_and_func("like")
    lit = get_func("lit", session)
    df = session.createDataFrame([("Spark", "_park")], ["a", "b"])
    assert df.select(like(df.a, df.b).alias("r")).collect() == [Row(r=True)]
    df = session.createDataFrame(
        [("%SystemDrive%/Users/John", "/%SystemDrive/%//Users%")], ["a", "b"]
    )
    assert df.select(like(df.a, df.b, lit("/")).alias("r")).collect() == [Row(r=True)]


def test_ln(get_session_and_func, get_func):
    session, ln = get_session_and_func("ln")
    df = session.createDataFrame([(4,)], ["a"])
    if isinstance(session, SnowflakeSession):
        assert df.select(ln("a")).first()[0] == 1.386294361
    else:
        assert df.select(ln("a")).first()[0] == 1.3862943611198906


def test_localtimestamp(get_session_and_func, get_func):
    session, localtimestamp = get_session_and_func("localtimestamp")
    df = session.range(1)
    now = datetime.datetime.now().replace(microsecond=0)
    assert df.select(localtimestamp()).first()[0] - now <= datetime.timedelta(seconds=3)


def test_make_dt_interval(get_session_and_func, get_func):
    session, make_dt_interval = get_session_and_func("make_dt_interval")
    df = session.createDataFrame([[1, 12, 30, 01.001001]], ["day", "hour", "min", "sec"])
    assert df.select(make_dt_interval(df.day, df.hour, df.min, df.sec).alias("r")).first()[
        0
    ] == datetime.timedelta(days=1, seconds=45001, microseconds=1001)
    assert df.select(make_dt_interval(df.day, df.hour, df.min).alias("r")).first()[
        0
    ] == datetime.timedelta(days=1, seconds=45000)
    assert df.select(make_dt_interval(df.day, df.hour).alias("r")).first()[0] == datetime.timedelta(
        days=1, seconds=43200
    )
    assert df.select(make_dt_interval(df.day).alias("r")).first()[0] == datetime.timedelta(days=1)
    assert df.select(make_dt_interval().alias("r")).first()[0] == datetime.timedelta(0)


def test_make_timestamp(get_session_and_func, get_func):
    session, make_timestamp = get_session_and_func("make_timestamp")
    df = session.createDataFrame(
        [[2014, 12, 28, 6, 30, 45.887, "CET"]],
        ["year", "month", "day", "hour", "min", "sec", "timezone"],
    )
    assert df.select(
        make_timestamp(df.year, df.month, df.day, df.hour, df.min, df.sec, df.timezone).alias("r")
    ).first()[0] == datetime.datetime(2014, 12, 28, 5, 30, 45, 887000)
    assert df.select(
        make_timestamp(df.year, df.month, df.day, df.hour, df.min, df.sec).alias("r")
    ).first()[0] == datetime.datetime(2014, 12, 28, 6, 30, 45, 887000)


def test_make_timestamp_ltz(get_session_and_func, get_func):
    session, make_timestamp_ltz = get_session_and_func("make_timestamp_ltz")
    df = session.createDataFrame(
        [[2014, 12, 28, 6, 30, 45.887, "CET"]],
        ["year", "month", "day", "hour", "min", "sec", "timezone"],
    )
    assert df.select(
        make_timestamp_ltz(df.year, df.month, df.day, df.hour, df.min, df.sec, df.timezone)
    ).first()[0] == datetime.datetime(2014, 12, 28, 5, 30, 45, 887000)
    assert df.select(
        make_timestamp_ltz(df.year, df.month, df.day, df.hour, df.min, df.sec)
    ).first()[0] == datetime.datetime(2014, 12, 28, 6, 30, 45, 887000)


def test_make_timestamp_ntz(get_session_and_func, get_func):
    session, make_timestamp_ntz = get_session_and_func("make_timestamp_ntz")
    df = session.createDataFrame(
        [[2014, 12, 28, 6, 30, 45.887]], ["year", "month", "day", "hour", "min", "sec"]
    )
    assert df.select(
        make_timestamp_ntz(df.year, df.month, df.day, df.hour, df.min, df.sec)
    ).first()[0] == datetime.datetime(2014, 12, 28, 6, 30, 45, 887000)


def test_make_ym_interval(get_session_and_func, get_func):
    session, make_ym_interval = get_session_and_func("make_ym_interval")
    df = session.createDataFrame([[2014, 12]], ["year", "month"])
    assert df.select(make_ym_interval(df.year, df.month).alias("r")).first()[0] == 24180


def test_map_contains_key(get_session_and_func, get_func):
    session, map_contains_key = get_session_and_func("map_contains_key")
    df = session.sql("SELECT map(1, 'a', 2, 'b') as data")
    assert df.select(map_contains_key("data", 1)).collect() == [Row(value=True)]
    assert df.select(map_contains_key("data", -1)).collect() == [Row(value=False)]


def test_mask(get_session_and_func, get_func):
    session, mask = get_session_and_func("mask")
    lit = get_func("lit", session)
    df = session.createDataFrame([("AbCD123-@$#",), ("abcd-EFGH-8765-4321",)], ["data"])
    assert df.select(mask(df.data).alias("r")).collect() == [
        Row(r="XxXXnnn-@$#"),
        Row(r="xxxx-XXXX-nnnn-nnnn"),
    ]
    assert df.select(mask(df.data, lit("Y")).alias("r")).collect() == [
        Row(r="YxYYnnn-@$#"),
        Row(r="xxxx-YYYY-nnnn-nnnn"),
    ]
    assert df.select(mask(df.data, lit("Y"), lit("y")).alias("r")).collect() == [
        Row(r="YyYYnnn-@$#"),
        Row(r="yyyy-YYYY-nnnn-nnnn"),
    ]
    assert df.select(mask(df.data, lit("Y"), lit("y"), lit("d")).alias("r")).collect() == [
        Row(r="YyYYddd-@$#"),
        Row(r="yyyy-YYYY-dddd-dddd"),
    ]
    assert df.select(
        mask(df.data, lit("Y"), lit("y"), lit("d"), lit("*")).alias("r")
    ).collect() == [Row(r="YyYYddd****"), Row(r="yyyy*YYYY*dddd*dddd")]


def test_median(get_session_and_func, get_func):
    session, median = get_session_and_func("median")
    df = session.createDataFrame(
        [
            ("Java", 2012, 20000),
            ("dotNET", 2012, 5000),
            ("Java", 2012, 22000),
            ("dotNET", 2012, 10000),
            ("dotNET", 2013, 48000),
            ("Java", 2013, 30000),
        ],
        schema=("course", "year", "earnings"),
    )
    assert df.groupby("course").agg(median("earnings")).collect() == [
        Row(value1="Java", value2=22000.0),
        Row(value1="dotNET", value2=10000.0),
    ]


def test_mode(get_session_and_func, get_func):
    session, mode = get_session_and_func("mode")
    df = session.createDataFrame(
        [
            ("Java", 2012, 20000),
            ("dotNET", 2012, 5000),
            ("Java", 2012, 20000),
            ("dotNET", 2012, 5000),
            ("dotNET", 2013, 48000),
            ("Java", 2013, 30000),
        ],
        schema=("course", "year", "earnings"),
    )
    assert df.groupby("course").agg(mode("year")).orderBy("course").collect() == [
        Row(value1="Java", value2=2012),
        Row(value1="dotNET", value2=2012),
    ]


def test_named_struct(get_session_and_func, get_func):
    session, named_struct = get_session_and_func("named_struct")
    lit = get_func("lit", session)
    df = session.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
    assert df.select(named_struct(lit("x"), df.a, lit("y"), df.b).alias("r")).collect() == [
        Row(r=Row(x=1, y=2))
    ]


def test_negative(get_session_and_func, get_func):
    session, negative = get_session_and_func("negative")
    assert session.range(3).select(negative("id")).collect() == [
        Row(value=0),
        Row(value=-1),
        Row(value=-2),
    ]


def test_negate(get_session_and_func, get_func):
    session, negate = get_session_and_func("negate")
    assert session.range(3).select(negate("id")).collect() == [
        Row(value=0),
        Row(value=-1),
        Row(value=-2),
    ]


def test_now(get_session_and_func):
    session, now = get_session_and_func("now")
    df = session.range(1)
    # The current date can depend on how the connection is configured so we check for dates around today
    result = df.select(now()).first()[0]
    assert isinstance(result, datetime.datetime)
    assert result.date() in (
        datetime.date.today() - datetime.timedelta(days=1),
        datetime.date.today(),
        datetime.date.today() + datetime.timedelta(days=1),
    )


def test_nvl(get_session_and_func, get_func):
    session, nvl = get_session_and_func("nvl")
    df = session.createDataFrame(
        [
            (
                None,
                8,
            ),
            (
                1,
                9,
            ),
        ],
        ["a", "b"],
    )
    assert df.select(nvl(df.a, df.b).alias("r")).collect() == [Row(r=8), Row(r=1)]


def test_nvl2(get_session_and_func, get_func):
    session, nvl2 = get_session_and_func("nvl2")
    df = session.createDataFrame(
        [
            (
                None,
                8,
                6,
            ),
            (
                1,
                9,
                9,
            ),
        ],
        ["a", "b", "c"],
    )
    assert df.select(nvl2(df.a, df.b, df.c).alias("r")).collect() == [Row(r=6), Row(r=9)]


def test_parse_url(get_session_and_func, get_func):
    session, parse_url = get_session_and_func("parse_url")
    df = session.createDataFrame(
        [
            (
                "http://spark.apache.org/path?query=1",
                "QUERY",
                "query",
            )
        ],
        ["a", "b", "c"],
    )
    assert df.select(parse_url(df.a, df.b, df.c).alias("r")).collect() == [Row(r="1")]
    assert df.select(parse_url(df.a, df.b).alias("r")).collect() == [Row(r="query=1")]


def test_pi(get_session_and_func, get_func):
    session, pi = get_session_and_func("pi")
    assert session.range(1).select(pi()).first()[0] == math.pi


def test_pmod(get_session_and_func, get_func):
    session, pmod = get_session_and_func("pmod")
    df = session.createDataFrame(
        [
            (1.0, float("nan")),
            (float("nan"), 2.0),
            (10.0, 3.0),
            (float("nan"), float("nan")),
            (-3.0, 4.0),
            (-10.0, 3.0),
            (-5.0, -6.0),
            (7.0, -8.0),
            (1.0, 2.0),
        ],
        ("a", "b"),
    )
    for i, row in enumerate(df.select(pmod("a", "b")).collect()):
        value = row[0]
        if i in {0, 1, 3}:
            assert math.isnan(value)
        elif i == 2:
            assert value == 1.0
        elif i == 4:
            assert value == 1.0
        elif i == 5:
            assert value == 2.0
        elif i == 6:
            assert value == -5.0
        elif i == 7:
            assert value == 7.0
        elif i == 8:
            assert value == 1.0
        else:
            raise RuntimeError("Too many results")


def test_position(get_session_and_func, get_func):
    session, position = get_session_and_func("position")
    assert (
        session.createDataFrame(
            [
                (
                    "bar",
                    "foobarbar",
                    5,
                )
            ],
            ["a", "b", "c"],
        )
        .select(position("a", "b", "c"))
        .first()[0]
        == 7
    )
    assert (
        session.createDataFrame(
            [
                (
                    "bar",
                    "foobarbar",
                    5,
                )
            ],
            ["a", "b", "c"],
        )
        .select(position("a", "b"))
        .first()[0]
        == 4
    )


def test_positive(get_session_and_func, get_func):
    session, positive = get_session_and_func("positive")
    df = session.createDataFrame([(-1,), (0,), (1,)], ["v"])
    assert df.select(positive("v").alias("p")).collect() == [
        Row(value=-1),
        Row(value=0),
        Row(value=1),
    ]


def test_printf(get_session_and_func, get_func):
    session, printf = get_session_and_func("printf")
    assert (
        session.createDataFrame(
            [
                (
                    "aa%d%s",
                    123,
                    "cc",
                )
            ],
            ["a", "b", "c"],
        )
        .select(printf("a", "b", "c"))
        .first()[0]
        == "aa123cc"
    )


def test_product(get_session_and_func, get_func):
    session, product = get_session_and_func("product")
    col = get_func("col", session)
    df = session.range(1, 10).toDF("x").withColumn("mod3", col("x") % 3)
    prods = df.groupBy("mod3").agg(product("x").alias("product"))
    assert prods.orderBy("mod3").collect() == [
        Row(value1=0, value2=162.0),
        Row(value1=1, value2=28.0),
        Row(value1=2, value2=80.0),
    ]


def test_reduce(get_session_and_func, get_func):
    session, reduce = get_session_and_func("reduce")
    lit = get_func("lit", session)
    struct = get_func("struct", session)
    df = session.createDataFrame([(1, [20.0, 4.0, 2.0, 6.0, 10.0])], ("id", "values"))
    assert (
        df.select(
            reduce("values", lit(0.0).cast("double"), lambda acc, x: acc + x).alias("sum")
        ).first()[0]
        == 42.0
    )

    if isinstance(session, PySparkSession):

        def merge(acc, x):
            count = acc.count + 1
            sum = acc.sum + x
            return struct(count.alias("count"), sum.alias("sum"))

        assert (
            df.select(
                reduce(
                    "values",
                    struct(lit(0).alias("count"), lit(0.0).alias("sum")),
                    merge,
                    lambda acc: acc.sum / acc.count,
                ).alias("mean")
            ).first()[0]
            == 8.4
        )


def test_reflect(get_session_and_func, get_func):
    session, reflect = get_session_and_func("reflect")
    lit = get_func("lit", session)
    df = session.createDataFrame([("a5cf6c42-0c85-418f-af6c-3e4e5b1328f2",)], ["a"])
    assert (
        df.select(reflect(lit("java.util.UUID"), lit("fromString"), df.a).alias("r")).first()[0]
        == "a5cf6c42-0c85-418f-af6c-3e4e5b1328f2"
    )


def test_regexp(get_session_and_func, get_func):
    session, regexp = get_session_and_func("regexp")
    lit = get_func("lit", session)
    col = get_func("col", session)
    assert session.createDataFrame([("1a 2b 14m", r"(\d+)")], ["str", "regexp"]).select(
        regexp("str", lit(r"(\d+)"))
    ).collect() == [Row(value=True)]
    assert session.createDataFrame([("1a 2b 14m", r"(\d+)")], ["str", "regexp"]).select(
        regexp("str", lit(r"\d{2}b")).alias("r")
    ).collect() == [Row(r=False)]
    assert session.createDataFrame([("1a 2b 14m", r"(\d+)")], ["str", "regexp"]).select(
        regexp("str", col("regexp")).alias("r")
    ).collect() == [Row(r=True)]


def test_regexp_count(get_session_and_func, get_func):
    session, regexp_count = get_session_and_func("regexp_count")
    lit = get_func("lit", session)
    col = get_func("col", session)
    df = session.createDataFrame([("1a 2b 14m", r"\d+")], ["str", "regexp"])
    assert df.select(regexp_count("str", lit(r"\d+")).alias("d")).first()[0] == 3
    assert df.select(regexp_count("str", lit(r"mmm")).alias("d")).first()[0] == 0
    assert df.select(regexp_count("str", col("regexp")).alias("d")).first()[0] == 3


def test_regexp_extract_all(get_session_and_func, get_func):
    session, regexp_extract_all = get_session_and_func("regexp_extract_all")
    lit = get_func("lit", session)
    col = get_func("col", session)
    df = session.createDataFrame([("100-200, 300-400", r"(\d+)-(\d+)")], ["str", "regexp"])
    assert df.select(regexp_extract_all("str", lit(r"(\d+)-(\d+)")).alias("d")).first()[0] == [
        "100",
        "300",
    ]
    assert df.select(regexp_extract_all("str", lit(r"(\d+)-(\d+)"), 1).alias("d")).first()[0] == [
        "100",
        "300",
    ]
    assert df.select(regexp_extract_all("str", lit(r"(\d+)-(\d+)"), 2).alias("d")).first()[0] == [
        "200",
        "400",
    ]
    assert df.select(regexp_extract_all("str", col("regexp")).alias("d")).first()[0] == [
        "100",
        "300",
    ]


def test_regexp_instr(get_session_and_func, get_func):
    session, regexp_instr = get_session_and_func("regexp_instr")
    lit = get_func("lit", session)
    col = get_func("col", session)
    df = session.createDataFrame([("1a 2b 14m", r"\d+(a|b|m)")], ["str", "regexp"])
    assert df.select(regexp_instr("str", lit(r"\d+(a|b|m)")).alias("d")).first()[0] == 1
    assert df.select(regexp_instr("str", lit(r"\d+(a|b|m)"), 1).alias("d")).first()[0] == 1
    assert df.select(regexp_instr("str", lit(r"\d+(a|b|m)"), 2).alias("d")).first()[0] == 1
    assert df.select(regexp_instr("str", col("regexp")).alias("d")).first()[0] == 1


def test_regexp_like(get_session_and_func, get_func):
    session, regexp_like = get_session_and_func("regexp_like")
    lit = get_func("lit", session)
    col = get_func("col", session)
    assert session.createDataFrame([("1a 2b 14m", r"(\d+)")], ["str", "regexp_col"]).select(
        regexp_like("str", lit(r"(\d+)"))
    ).collect() == [Row(value=True)]
    assert session.createDataFrame([("1a 2b 14m", r"(\d+)")], ["str", "regexp_col"]).select(
        regexp_like("str", lit(r"\d{2}b"))
    ).collect() == [Row(value=False)]
    assert session.createDataFrame([("1a 2b 14m", r"(\d+)")], ["str", "regexp_col"]).select(
        regexp_like("str", col("regexp_col"))
    ).collect() == [Row(value=True)]


def test_regexp_substr(get_session_and_func, get_func):
    session, regexp_substr = get_session_and_func("regexp_substr")
    lit = get_func("lit", session)
    col = get_func("col", session)
    df = session.createDataFrame([("1a 2b 14m", r"\d+")], ["str", "regexp"])
    assert df.select(regexp_substr("str", lit(r"\d+")).alias("d")).collect() == [Row(d="1")]
    assert df.select(regexp_substr("str", lit(r"mmm")).alias("d")).collect() == [Row(d=None)]
    assert df.select(regexp_substr("str", col("regexp")).alias("d")).collect() == [Row(d="1")]


def test_regr_avgx(get_session_and_func, get_func):
    session, regr_avgx = get_session_and_func("regr_avgx")
    col = get_func("col", session)
    randn = get_func("randn", session)
    x = (col("id") % 3).alias("x")
    y = (randn(42) + x * 10).alias("y")
    df = session.range(0, 1000, 1, 1).select(x, y)
    assert df.select(regr_avgx("y", "x")).first()[0] == 0.999


def test_regr_avgy(get_session_and_func, get_func):
    session, regr_avgy = get_session_and_func("regr_avgy")
    col = get_func("col", session)
    randn = get_func("randn", session)
    x = (col("id") % 3).alias("x")
    y = (randn(42) + x * 10).alias("y")
    df = session.range(0, 1000, 1, 1).select(x, y)
    assert df.select(regr_avgy("y", "x")).first()[0] == 9.980732994136464


def test_regr_count(get_session_and_func, get_func):
    session, regr_count = get_session_and_func("regr_count")
    col = get_func("col", session)
    randn = get_func("randn", session)
    x = (col("id") % 3).alias("x")
    y = (randn(42) + x * 10).alias("y")
    df = session.range(0, 1000, 1, 1).select(x, y)
    assert df.select(regr_count("y", "x")).first()[0] == 1000


def test_regr_intercept(get_session_and_func, get_func):
    session, regr_intercept = get_session_and_func("regr_intercept")
    col = get_func("col", session)
    randn = get_func("randn", session)
    x = (col("id") % 3).alias("x")
    y = (randn(42) + x * 10).alias("y")
    df = session.range(0, 1000, 1, 1).select(x, y)
    assert math.isclose(df.select(regr_intercept("y", "x")).first()[0], -0.04961745990969568)


def test_regr_r2(get_session_and_func, get_func):
    session, regr_r2 = get_session_and_func("regr_r2")
    col = get_func("col", session)
    randn = get_func("randn", session)
    x = (col("id") % 3).alias("x")
    y = (randn(42) + x * 10).alias("y")
    df = session.range(0, 1000, 1, 1).select(x, y)
    assert math.isclose(df.select(regr_r2("y", "x")).first()[0], 0.9851908293645436)


def test_regr_slope(get_session_and_func, get_func):
    session, regr_slope = get_session_and_func("regr_slope")
    col = get_func("col", session)
    randn = get_func("randn", session)
    x = (col("id") % 3).alias("x")
    y = (randn(42) + x * 10).alias("y")
    df = session.range(0, 1000, 1, 1).select(x, y)
    assert math.isclose(df.select(regr_slope("y", "x")).first()[0], 10.040390844891048)


def test_regr_sxx(get_session_and_func, get_func):
    session, regr_sxx = get_session_and_func("regr_sxx")
    col = get_func("col", session)
    randn = get_func("randn", session)
    x = (col("id") % 3).alias("x")
    y = (randn(42) + x * 10).alias("y")
    df = session.range(0, 1000, 1, 1).select(x, y)
    assert math.isclose(df.select(regr_sxx("y", "x")).first()[0], 666.9989999999996)


def test_regr_sxy(get_session_and_func, get_func):
    session, regr_sxy = get_session_and_func("regr_sxy")
    col = get_func("col", session)
    randn = get_func("randn", session)
    x = (col("id") % 3).alias("x")
    y = (randn(42) + x * 10).alias("y")
    df = session.range(0, 1000, 1, 1).select(x, y)
    assert math.isclose(df.select(regr_sxy("y", "x")).first()[0], 6696.93065315148)


def test_regr_syy(get_session_and_func, get_func):
    session, regr_syy = get_session_and_func("regr_syy")
    col = get_func("col", session)
    randn = get_func("randn", session)
    x = (col("id") % 3).alias("x")
    y = (randn(42) + x * 10).alias("y")
    df = session.range(0, 1000, 1, 1).select(x, y)
    assert math.isclose(df.select(regr_syy("y", "x")).first()[0], 68250.53503811295)


def test_replace(get_session_and_func, get_func):
    session, replace = get_session_and_func("replace")
    df = session.createDataFrame(
        [
            (
                "ABCabc",
                "abc",
                "DEF",
            )
        ],
        ["a", "b", "c"],
    )
    assert df.select(replace(df.a, df.b, df.c).alias("r")).first()[0] == "ABCDEF"
    assert df.select(replace(df.a, df.b).alias("r")).first()[0] == "ABC"


def test_right(get_session_and_func, get_func):
    session, right = get_session_and_func("right")
    df = session.createDataFrame(
        [
            (
                "Spark SQL",
                3,
            )
        ],
        ["a", "b"],
    )
    assert df.select(right(df.a, df.b).alias("r")).first()[0] == "SQL"


def test_rlike(get_session_and_func, get_func):
    session, rlike = get_session_and_func("rlike")
    lit = get_func("lit", session)
    col = get_func("col", session)
    df = session.createDataFrame([("1a 2b 14m", r"(\d+)")], ["str", "regexp"])
    assert df.select(rlike("str", lit(r"(\d+)")).alias("d")).collect() == [Row(d=True)]
    assert df.select(rlike("str", lit(r"\d{2}b")).alias("d")).collect() == [Row(d=False)]
    assert df.select(rlike("str", col("regexp")).alias("d")).collect() == [Row(d=True)]


def test_sha(get_session_and_func, get_func):
    session, sha = get_session_and_func("sha")
    lit = get_func("lit", session)
    assert (
        session.range(1).select(sha(lit("Spark"))).first()[0]
        == "85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c"
    )


def test_sign(get_session_and_func, get_func):
    session, sign = get_session_and_func("sign")
    lit = get_func("lit", session)
    assert session.range(1).select(sign(lit(-5)), sign(lit(6))).collect() == [
        Row(value1=-1.0, value2=1.0)
    ]


def test_some(get_session_and_func, get_func):
    session, some = get_session_and_func("some")
    assert session.createDataFrame([[True], [True], [True]], ["flag"]).select(
        some("flag")
    ).collect() == [Row(value=True)]
    assert session.createDataFrame([[True], [False], [True]], ["flag"]).select(
        some("flag")
    ).collect() == [Row(value=True)]
    assert session.createDataFrame([[False], [False], [False]], ["flag"]).select(
        some("flag")
    ).collect() == [Row(value=False)]


def test_spark_partition_id(get_session_and_func, get_func):
    session, spark_partition_id = get_session_and_func("spark_partition_id")
    df = session.range(2)
    assert df.repartition(1).select(spark_partition_id().alias("pid")).collect() == [
        Row(pid=0),
        Row(pid=0),
    ]


def test_split_part(get_session_and_func, get_func):
    session, split_part = get_session_and_func("split_part")
    df = session.createDataFrame(
        [
            (
                "11.12.13",
                ".",
                3,
            )
        ],
        ["a", "b", "c"],
    )
    assert df.select(split_part(df.a, df.b, df.c).alias("r")).first()[0] == "13"


def test_startswith(get_session_and_func, get_func):
    session, startswith = get_session_and_func("startswith")
    df = session.createDataFrame(
        [
            (
                "Spark SQL",
                "Spark",
            )
        ],
        ["a", "b"],
    )
    assert df.select(startswith(df.a, df.b).alias("r")).collect() == [Row(r=True)]
    df = session.createDataFrame(
        [
            (
                "414243",
                "4142",
            )
        ],
        ["e", "f"],
    )
    to_binary = get_func("to_binary", session)
    df = df.select(to_binary("e").alias("e"), to_binary("f").alias("f"))
    assert df.select(startswith("e", "f"), startswith("f", "e")).collect() == [
        Row(value1=True, value2=False)
    ]


def test_std(get_session_and_func, get_func):
    session, std = get_session_and_func("std")
    assert session.range(6).select(std("id")).first()[0] == 1.8708286933869707


def test_str_to_map(get_session_and_func, get_func):
    session, str_to_map = get_session_and_func("str_to_map")
    lit = get_func("lit", session)
    df = session.createDataFrame([("a:1,b:2,c:3",)], ["e"])
    assert df.select(str_to_map(df.e, lit(","), lit(":")).alias("r")).first()[0] == {
        "a": "1",
        "b": "2",
        "c": "3",
    }
    df = session.createDataFrame([("a:1,b:2,c:3",)], ["e"])
    assert df.select(str_to_map(df.e, lit(",")).alias("r")).first()[0] == {
        "a": "1",
        "b": "2",
        "c": "3",
    }
    df = session.createDataFrame([("a:1,b:2,c:3",)], ["e"])
    assert df.select(str_to_map(df.e).alias("r")).first()[0] == {"a": "1", "b": "2", "c": "3"}


def test_substr(get_session_and_func, get_func):
    session, substr = get_session_and_func("substr")
    assert (
        session.createDataFrame(
            [
                (
                    "Spark SQL",
                    5,
                    1,
                )
            ],
            ["a", "b", "c"],
        )
        .select(substr("a", "b", "c"))
        .first()[0]
        == "k"
    )
    assert (
        session.createDataFrame(
            [
                (
                    "Spark SQL",
                    5,
                    1,
                )
            ],
            ["a", "b", "c"],
        )
        .select(substr("a", "b"))
        .first()[0]
        == "k SQL"
    )


def test_timestamp_micros(get_session_and_func, get_func):
    session, timestamp_micros = get_session_and_func("timestamp_micros")
    time_df = session.createDataFrame([(1230219000,)], ["unix_time"])
    assert time_df.select(timestamp_micros(time_df.unix_time).alias("ts")).first()[
        0
    ] == datetime.datetime(1970, 1, 1, 0, 20, 30, 219000)


def test_timestamp_millis(get_session_and_func, get_func):
    session, timestamp_millis = get_session_and_func("timestamp_millis")
    time_df = session.createDataFrame([(1230219000,)], ["unix_time"])
    assert time_df.select(timestamp_millis(time_df.unix_time).alias("ts")).first()[
        0
    ] == datetime.datetime(1970, 1, 15, 5, 43, 39)


def test_to_char(get_session_and_func, get_func):
    session, to_char = get_session_and_func("to_char")
    lit = get_func("lit", session)
    df = session.createDataFrame([(78.12,)], ["e"])
    assert df.select(to_char(df.e, lit("$99.99")).alias("r")).first()[0] == "$78.12"


def test_to_number(get_session_and_func, get_func):
    session, to_number = get_session_and_func("to_number")
    lit = get_func("lit", session)
    df = session.createDataFrame([("$78.12",)], ["e"])
    result = df.select(to_number(df.e, lit("$99.99")).alias("r")).first()[0]
    if isinstance(session, PySparkSession):
        assert result == Decimal("78.12")
    else:
        assert result == 78.12


def test_to_timestamp_ltz(get_session_and_func, get_func):
    session, to_timestamp_ltz = get_session_and_func("to_timestamp_ltz")
    lit = get_func("lit", session)
    df = session.createDataFrame([("2016-12-31",)], ["e"])
    assert df.select(to_timestamp_ltz(df.e, lit("yyyy-MM-dd")).alias("r")).first()[
        0
    ] == datetime.datetime(2016, 12, 31, 0, 0)
    df = session.createDataFrame([("2016-12-31",)], ["e"])
    assert df.select(to_timestamp_ltz(df.e).alias("r")).first()[0] == datetime.datetime(
        2016, 12, 31, 0, 0
    )


def test_to_timestamp_ntz(get_session_and_func, get_func):
    session, to_timestamp_ntz = get_session_and_func("to_timestamp_ntz")
    lit = get_func("lit", session)
    df = session.createDataFrame([("2016-04-08",)], ["e"])
    assert df.select(to_timestamp_ntz(df.e, lit("yyyy-MM-dd")).alias("r")).first()[
        0
    ] == datetime.datetime(2016, 4, 8, 0, 0)
    df = session.createDataFrame([("2016-04-08",)], ["e"])
    assert df.select(to_timestamp_ntz(df.e).alias("r")).first()[0] == datetime.datetime(
        2016, 4, 8, 0, 0
    )


def test_to_unix_timestamp(get_session_and_func, get_func):
    session, to_unix_timestamp = get_session_and_func("to_unix_timestamp")
    lit = get_func("lit", session)
    df = session.createDataFrame([("2016-04-08",)], ["e"])
    result = df.select(to_unix_timestamp(df.e, lit("yyyy-MM-dd")).alias("r")).first()[0]
    if isinstance(session, (DuckDBSession, DatabricksSession)):
        assert result == 1460073600.0
    else:
        assert result == 1460073600
    # DuckDB requires the value to match the format which the default format is "yyyy-MM-dd HH:mm:ss".
    # https://spark.apache.org/docs/latest/api/sql/#to_unix_timestamp
    if isinstance(session, DuckDBSession):
        pass
    else:
        df = session.createDataFrame([("2016-04-08",)], ["e"])
        assert df.select(to_unix_timestamp(df.e).alias("r")).collect() == [Row(r=None)]


def test_to_varchar(get_session_and_func, get_func):
    session, to_varchar = get_session_and_func("to_varchar")
    lit = get_func("lit", session)
    df = session.createDataFrame([(78.12,)], ["e"])
    assert df.select(to_varchar(df.e, lit("$99.99")).alias("r")).first()[0] == "$78.12"


def test_try_aes_decrypt(get_session_and_func, get_func):
    session, try_aes_decrypt = get_session_and_func("try_aes_decrypt")
    unbase64 = get_func("unbase64", session)
    unhex = get_func("unhex", session)
    df = session.createDataFrame(
        [
            (
                "AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4",
                "abcdefghijklmnop12345678ABCDEFGH",
                "GCM",
                "DEFAULT",
                "This is an AAD mixed into the input",
            )
        ],
        ["input", "key", "mode", "padding", "aad"],
    )
    assert df.select(
        try_aes_decrypt(unbase64(df.input), df.key, df.mode, df.padding, df.aad).alias("r")
    ).first()[0] == bytearray(b"Spark")
    df = session.createDataFrame(
        [
            (
                "AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg=",
                "abcdefghijklmnop12345678ABCDEFGH",
                "CBC",
                "DEFAULT",
            )
        ],
        ["input", "key", "mode", "padding"],
    )
    assert df.select(
        try_aes_decrypt(unbase64(df.input), df.key, df.mode, df.padding).alias("r")
    ).first()[0] == bytearray(b"Spark")
    assert df.select(try_aes_decrypt(unbase64(df.input), df.key, df.mode).alias("r")).first()[
        0
    ] == bytearray(b"Spark")

    df = session.createDataFrame(
        [
            (
                "83F16B2AA704794132802D248E6BFD4E380078182D1544813898AC97E709B28A94",
                "0000111122223333",
            )
        ],
        ["input", "key"],
    )
    assert df.select(try_aes_decrypt(unhex(df.input), df.key).alias("r")).first()[0] == bytearray(
        b"Spark"
    )


def test_try_element_at(get_session_and_func, get_func):
    session, try_element_at = get_session_and_func("try_element_at")
    lit = get_func("lit", session)
    df = session.createDataFrame([(["a", "b", "c"],)], ["data"])
    assert df.select(try_element_at(df.data, lit(1)).alias("r")).first()[0] == "a"
    if isinstance(session, PostgresSession):
        assert df.select(try_element_at(df.data, lit(-1)).alias("r")).first()[0] is None
    else:
        assert df.select(try_element_at(df.data, lit(-1)).alias("r")).first()[0] == "c"
    df = session.createDataFrame([({"a": 1.0, "b": 2.0},)], ["data"])
    # if isinstance(session, DuckDBSession):
    #     assert df.select(try_element_at(df.data, lit("a")).alias("r")).first()[0] == [1.0]
    if isinstance(session, PostgresSession):
        pass
    else:
        assert df.select(try_element_at(df.data, lit("a")).alias("r")).first()[0] == 1.0


def test_try_to_timestamp(get_session_and_func, get_func):
    session, try_to_timestamp = get_session_and_func("try_to_timestamp")
    lit = get_func("lit", session)
    df = session.createDataFrame([("1997-02-28 10:30:00",)], ["t"])
    result = df.select(try_to_timestamp(df.t).alias("dt")).first()[0]
    assert result == datetime.datetime(1997, 2, 28, 10, 30)
    result = df.select(try_to_timestamp(df.t, lit("yyyy-MM-dd HH:mm:ss")).alias("dt")).first()[0]
    assert result == datetime.datetime(1997, 2, 28, 10, 30)


def test_ucase(get_session_and_func, get_func):
    session, ucase = get_session_and_func("ucase")
    lit = get_func("lit", session)
    assert session.range(1).select(ucase(lit("Spark"))).first()[0] == "SPARK"


def test_unix_date(get_session_and_func, get_func):
    session, unix_date = get_session_and_func("unix_date")
    to_date = get_func("to_date", session)
    df = session.createDataFrame([("1970-01-02",)], ["t"])
    assert df.select(unix_date(to_date(df.t)).alias("n")).first()[0] == 1


def test_unix_micros(get_session_and_func, get_func):
    session, unix_micros = get_session_and_func("unix_micros")
    to_timestamp = get_func("to_timestamp", session)
    df = session.createDataFrame([("2015-07-22 10:00:00",)], ["t"])
    assert df.select(unix_micros(to_timestamp(df.t)).alias("n")).first()[0] == 1437559200000000
    if not isinstance(session, SnowflakeSession):
        df = session.createDataFrame([(datetime.datetime(2021, 3, 1, 12, 34, 56, 49000),)], ["t"])
        assert df.select(unix_micros(df.t).alias("n")).first()[0] == 1614602096049000


def test_unix_millis(get_session_and_func, get_func):
    session, unix_millis = get_session_and_func("unix_millis")
    to_timestamp = get_func("to_timestamp", session)
    df = session.createDataFrame([("2015-07-22 10:00:00",)], ["t"])
    assert df.select(unix_millis(to_timestamp(df.t)).alias("n")).first()[0] == 1437559200000


def test_unix_seconds(get_session_and_func, get_func):
    session, unix_seconds = get_session_and_func("unix_seconds")
    to_timestamp = get_func("to_timestamp", session)
    df = session.createDataFrame([("2015-07-22 10:00:00",)], ["t"])
    assert df.select(unix_seconds(to_timestamp(df.t)).alias("n")).first()[0] == 1437559200


def test_url_decode(get_session_and_func, get_func):
    session, url_decode = get_session_and_func("url_decode")
    df = session.createDataFrame([("https%3A%2F%2Fspark.apache.org",)], ["a"])
    assert df.select(url_decode(df.a).alias("r")).first()[0] == "https://spark.apache.org"


def test_url_encode(get_session_and_func, get_func):
    session, url_encode = get_session_and_func("url_encode")
    df = session.createDataFrame([("https://spark.apache.org",)], ["a"])
    assert df.select(url_encode(df.a).alias("r")).first()[0] == "https%3A%2F%2Fspark.apache.org"


def test_version(get_session_and_func, get_func):
    session, version = get_session_and_func("version")
    result = session.range(1).select(version()).first()[0]
    assert len(result.split(" ")) == 2
    assert len(result.split(" ")[0].split(".")) == 3


def test_weekday(get_session_and_func, get_func):
    session, weekday = get_session_and_func("weekday")
    df = session.createDataFrame([("2015-04-08",)], ["dt"])
    assert df.select(weekday("dt").alias("day")).first()[0] == 2


def test_width_bucket(get_session_and_func, get_func):
    session, width_bucket = get_session_and_func("width_bucket")
    df = session.createDataFrame(
        [(5.3, 0.2, 10.6, 5), (-2.1, 1.3, 3.4, 3), (8.1, 0.0, 5.7, 4), (-0.9, 5.2, 0.5, 2)],
        ["v", "min", "max", "n"],
    )
    assert df.select(width_bucket("v", "min", "max", "n")).collect() == [
        Row(value=3),
        Row(value=0),
        Row(value=5),
        Row(value=3),
    ]


def test_window_time(get_session_and_func, get_func):
    session, window_time = get_session_and_func("window_time")
    window = get_func("window", session)
    sum = get_func("sum", session)
    col = get_func("col", session)
    df = session.createDataFrame(
        [(datetime.datetime(2016, 3, 11, 9, 0, 7), 1)],
    ).toDF("date", "val")
    w = df.groupBy(window("date", "5 seconds")).agg(sum("val").alias("sum"))
    # SQLFrame does not support the syntax used in the example so the "col" function was used instead.
    # https://spark.apache.org/docs/3.4.0/api/python/reference/pyspark.sql/api/pyspark.sql.functions.window_time.html
    assert w.select(
        col("window.end").cast("string").alias("end"),
        window_time(w.window).cast("string").alias("window_time"),
        "sum",
    ).collect() == [Row(end="2016-03-11 09:00:10", window_time="2016-03-11 09:00:09.999999", sum=1)]


def test_xpath(get_session_and_func, get_func):
    session, xpath = get_session_and_func("xpath")
    lit = get_func("lit", session)
    df = session.createDataFrame([("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>",)], ["x"])
    assert df.select(xpath(df.x, lit("a/b/text()")).alias("r")).first()[0] == ["b1", "b2", "b3"]


def test_xpath_boolean(get_session_and_func, get_func):
    session, xpath_boolean = get_session_and_func("xpath_boolean")
    lit = get_func("lit", session)
    df = session.createDataFrame([("<a><b>1</b></a>",)], ["x"])
    assert df.select(xpath_boolean(df.x, lit("a/b")).alias("r")).collect() == [Row(r=True)]


def test_xpath_double(get_session_and_func, get_func):
    session, xpath_double = get_session_and_func("xpath_double")
    lit = get_func("lit", session)
    df = session.createDataFrame([("<a><b>1</b><b>2</b></a>",)], ["x"])
    assert df.select(xpath_double(df.x, lit("sum(a/b)")).alias("r")).first()[0] == 3.0


def test_xpath_float(get_session_and_func, get_func):
    session, xpath_float = get_session_and_func("xpath_float")
    lit = get_func("lit", session)
    df = session.createDataFrame([("<a><b>1</b><b>2</b></a>",)], ["x"])
    assert df.select(xpath_float(df.x, lit("sum(a/b)")).alias("r")).first()[0] == 3.0


def test_xpath_int(get_session_and_func, get_func):
    session, xpath_int = get_session_and_func("xpath_int")
    lit = get_func("lit", session)
    df = session.createDataFrame([("<a><b>1</b><b>2</b></a>",)], ["x"])
    assert df.select(xpath_int(df.x, lit("sum(a/b)")).alias("r")).first()[0] == 3


def test_xpath_long(get_session_and_func, get_func):
    session, xpath_long = get_session_and_func("xpath_long")
    lit = get_func("lit", session)
    df = session.createDataFrame([("<a><b>1</b><b>2</b></a>",)], ["x"])
    assert df.select(xpath_long(df.x, lit("sum(a/b)")).alias("r")).first()[0] == 3


def test_xpath_number(get_session_and_func, get_func):
    session, xpath_number = get_session_and_func("xpath_number")
    lit = get_func("lit", session)
    assert (
        session.createDataFrame([("<a><b>1</b><b>2</b></a>",)], ["x"])
        .select(xpath_number("x", lit("sum(a/b)")))
        .first()[0]
        == 3.0
    )


def test_xpath_short(get_session_and_func, get_func):
    session, xpath_short = get_session_and_func("xpath_short")
    lit = get_func("lit", session)
    df = session.createDataFrame([("<a><b>1</b><b>2</b></a>",)], ["x"])
    assert df.select(xpath_short(df.x, lit("sum(a/b)")).alias("r")).first()[0] == 3


def test_xpath_string(get_session_and_func, get_func):
    session, xpath_string = get_session_and_func("xpath_string")
    lit = get_func("lit", session)
    df = session.createDataFrame([("<a><b>b</b><c>cc</c></a>",)], ["x"])
    assert df.select(xpath_string(df.x, lit("a/c")).alias("r")).first()[0] == "cc"


def test_is_string(get_session_and_func, get_func):
    session, _is_string = get_session_and_func("_is_string")
    lit = get_func("lit", session)
    assert session.range(1).select(_is_string(lit("value")), _is_string(lit(1))).collect() == [
        Row(v1=True, v2=False)
    ]


def test_is_date(get_session_and_func, get_func):
    session, _is_date = get_session_and_func("_is_date")
    to_date = get_func("to_date", session)
    lit = get_func("lit", session)
    assert session.range(1).select(
        _is_date(to_date(lit("2021-01-01"), "yyyy-MM-dd")), _is_date(lit("2021-01-01"))
    ).collect() == [Row(v1=True, v2=False)]


def test_is_int_variant(get_session_and_func, get_func):
    session, _is_int_variant = get_session_and_func("_is_int_variant")
    lit = get_func("lit", session)
    result = (
        session.range(1)
        .select(
            _is_int_variant(lit(1).cast("integer")).alias("v1"),
            _is_int_variant(lit(1).cast("smallint")).alias("v2"),
            _is_int_variant(lit(1).cast("bigint")).alias("v3"),
            _is_int_variant(lit(1.0)).alias("v4"),
            _is_int_variant(lit("1")).alias("v5"),
        )
        .collect()
    )
    # Snowflake treats a number with decimal point as int unless explicitly casted
    if isinstance(session, SnowflakeSession):
        assert result == [Row(v1=True, v2=True, v3=True, v4=True, v5=False)]
    else:
        assert result == [Row(v1=True, v2=True, v3=True, v4=False, v5=False)]


def test_is_array(get_session_and_func, get_func):
    session, _is_array = get_session_and_func("_is_array")
    lit = get_func("lit", session)
    result = (
        session.range(1)
        .select(
            _is_array(lit([1, 2, 3])).alias("v1"),
            _is_array(lit(["1", "2", "3"])).alias("v2"),
            _is_array(lit(1)).alias("v3"),
        )
        .collect()
    )
    assert result == [Row(v1=True, v2=True, v3=False)]


# https://github.com/eakmanrq/sqlframe/issues/265
def test_infinite(get_session_and_func):
    session, lit = get_session_and_func("lit")
    df = session.createDataFrame(
        [
            {"a": float("inf")},
            {"a": float("-inf")},
        ]
    )
    assert df.collect() == [Row(a=float("inf")), Row(a=float("-inf"))]
