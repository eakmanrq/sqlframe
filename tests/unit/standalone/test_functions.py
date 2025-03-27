import datetime
import inspect

import pytest
from sqlglot import expressions as exp
from sqlglot import parse_one
from sqlglot.errors import ErrorLevel

from sqlframe.base.types import Row
from sqlframe.standalone import functions as SF


@pytest.mark.parametrize("name,func", inspect.getmembers(SF, inspect.isfunction))
def test_invoke_anonymous(name, func):
    # array_size - converts to `size` but `array_size` and `size` behave differently
    # exists - the spark exists takes a lambda function and the exists in SQLGlot seems more basic
    # make_interval - SQLGlot doesn't support week
    # to_char - convert to a cast that ignores the format provided
    # ltrim/rtrim - don't seem to convert correctly on some engines
    ignore_funcs = {
        "array_size",
        "exists",
        "make_interval",
        "to_char",
        "ltrim",
        "rtrim",
        "ascii",
        "current_schema",
    }
    if "invoke_anonymous_function" in inspect.getsource(func) and name not in ignore_funcs:
        func = parse_one(f"{name}()", read="spark", error_level=ErrorLevel.IGNORE)
        assert isinstance(func, exp.Anonymous)


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.lit("test"), "'test'"),
        (SF.lit(30), "30"),
        (SF.lit(10.10), "10.1"),
        (SF.lit(False), "FALSE"),
        (SF.lit(None), "NULL"),
        (SF.lit(datetime.date(2022, 1, 1)), "CAST('2022-01-01' AS DATE)"),
        (
            SF.lit(datetime.datetime(2022, 1, 1, 1, 1, 1)),
            "CAST('2022-01-01 01:01:01' AS TIMESTAMP)",
        ),
        (
            SF.lit(datetime.datetime(2022, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc)),
            "CAST('2022-01-01 01:01:01+00:00' AS TIMESTAMP)",
        ),
        (SF.lit({"cola": 1, "colb": "test"}), "MAP('cola', 1, 'colb', 'test')"),
        (SF.lit(Row(cola=1, colb="test")), "STRUCT(1 AS cola, 'test' AS colb)"),
        (SF.lit(float("inf")), "'inf'"),
        (SF.lit(float("-inf")), "'-inf'"),
    ],
)
def test_lit(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.col("cola"), "cola"),
        (SF.col("table.cola"), "table.cola"),
        (SF.col("table.a space in name"), "table.`a space in name`"),
        (SF.col("a space in table.a space in name"), "`a space in table`.`a space in name`"),
    ],
)
def test_col(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.col("cola").alias("colb"), "cola AS colb"),
        # SQLGlot Spark normalizer when we create a Column object will lowercase the alias value
        (SF.col("cola").alias("A Space in Name"), "cola AS `a space in name`"),
    ],
)
def test_alias(expression, expected):
    assert expression.expression.sql(dialect="spark") == expected


def test_asc():
    asc_str = SF.asc("cola")
    # ASC is removed from output since that is default so we can't check sql
    assert isinstance(asc_str.expression, exp.Ordered)
    asc_col = SF.asc(SF.col("cola"))
    assert isinstance(asc_col.expression, exp.Ordered)


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.desc("cola"), "cola DESC"),
        (SF.desc(SF.col("cola")), "cola DESC"),
    ],
)
def test_desc(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.desc("cola"), "cola DESC"),
        (SF.desc(SF.col("cola")), "cola DESC"),
    ],
)
def test_sqrt(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.abs("cola"), "ABS(cola)"),
        (SF.abs(SF.col("cola")), "ABS(cola)"),
    ],
)
def test_abs(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.max("cola"), "MAX(cola)"),
        (SF.max(SF.col("cola")), "MAX(cola)"),
    ],
)
def test_max(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.min("cola"), "MIN(cola)"),
        (SF.min(SF.col("cola")), "MIN(cola)"),
    ],
)
def test_min(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.max_by("cola", "colb"), "MAX_BY(cola, colb)"),
        (SF.max_by(SF.col("cola"), SF.col("colb")), "MAX_BY(cola, colb)"),
    ],
)
def test_max_by(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.min_by("cola", "colb"), "MIN_BY(cola, colb)"),
        (SF.min_by(SF.col("cola"), SF.col("colb")), "MIN_BY(cola, colb)"),
    ],
)
def test_min_by(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.count("cola"), "COUNT(cola)"),
        (SF.count(SF.col("cola")), "COUNT(cola)"),
    ],
)
def test_count(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sum("cola"), "SUM(cola)"),
        (SF.sum(SF.col("cola")), "SUM(cola)"),
    ],
)
def test_sum(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.avg("cola"), "AVG(cola)"),
        (SF.avg(SF.col("cola")), "AVG(cola)"),
    ],
)
def test_avg(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.mean("cola"), "AVG(cola)"),
        (SF.mean(SF.col("cola")), "AVG(cola)"),
    ],
)
def test_mean(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sumDistinct("cola"), "SUM(DISTINCT cola)"),
        (SF.sumDistinct(SF.col("cola")), "SUM(DISTINCT cola)"),
    ],
)
def test_sum_distinct(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.acos("cola"), "ACOS(cola)"),
        (SF.acos(SF.col("cola")), "ACOS(cola)"),
    ],
)
def test_acos(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.acosh("cola"), "ACOSH(cola)"),
        (SF.acosh(SF.col("cola")), "ACOSH(cola)"),
    ],
)
def test_acosh(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.asin("cola"), "ASIN(cola)"),
        (SF.asin(SF.col("cola")), "ASIN(cola)"),
    ],
)
def test_asin(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.asinh("cola"), "ASINH(cola)"),
        (SF.asinh(SF.col("cola")), "ASINH(cola)"),
    ],
)
def test_asinh(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.atan("cola"), "ATAN(cola)"),
        (SF.atan(SF.col("cola")), "ATAN(cola)"),
    ],
)
def test_atan(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.atan2("cola", "colb"), "ATAN2(cola, colb)"),
        (SF.atan2(SF.col("cola"), SF.col("colb")), "ATAN2(cola, colb)"),
        (SF.atan2(10.10, "colb"), "ATAN2(10.1, colb)"),
        (SF.atan2("cola", 10.10), "ATAN2(cola, 10.1)"),
    ],
)
def test_atan2(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.atanh("cola"), "ATANH(cola)"),
        (SF.atanh(SF.col("cola")), "ATANH(cola)"),
    ],
)
def test_atanh(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.cbrt("cola"), "CBRT(cola)"),
        (SF.cbrt(SF.col("cola")), "CBRT(cola)"),
    ],
)
def test_cbrt(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.ceil("cola"), "CEIL(cola)"),
        (SF.ceil(SF.col("cola")), "CEIL(cola)"),
    ],
)
def test_ceil(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.cos("cola"), "COS(cola)"),
        (SF.cos(SF.col("cola")), "COS(cola)"),
    ],
)
def test_cos(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.cosh("cola"), "COSH(cola)"),
        (SF.cosh(SF.col("cola")), "COSH(cola)"),
    ],
)
def test_cosh(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.cot("cola"), "COT(cola)"),
        (SF.cot(SF.col("cola")), "COT(cola)"),
    ],
)
def test_cot(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.csc("cola"), "CSC(cola)"),
        (SF.csc(SF.col("cola")), "CSC(cola)"),
    ],
)
def test_csc(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


def test_e():
    assert SF.e().column_expression.sql(dialect="spark") == "E()"


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.exp("cola"), "EXP(cola)"),
        (SF.exp(SF.col("cola")), "EXP(cola)"),
    ],
)
def test_exp(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.expm1("cola"), "EXPM1(cola)"),
        (SF.expm1(SF.col("cola")), "EXPM1(cola)"),
    ],
)
def test_expm1(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.floor("cola"), "FLOOR(cola)"),
        (SF.floor(SF.col("cola")), "FLOOR(cola)"),
    ],
)
def test_floor(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.log("cola"), "LN(cola)"),
        (SF.log(SF.col("cola")), "LN(cola)"),
        (SF.log(10.0, "age"), "LOG(10.0, age)"),
    ],
)
def test_log(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.log10("cola"), "LOG(10, cola)"),
        (SF.log10(SF.col("cola")), "LOG(10, cola)"),
    ],
)
def test_log10(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.log1p("cola"), "LOG1P(cola)"),
        (SF.log1p(SF.col("cola")), "LOG1P(cola)"),
    ],
)
def test_log1p(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.log2("cola"), "LOG(2, cola)"),
        (SF.log2(SF.col("cola")), "LOG(2, cola)"),
    ],
)
def test_log2(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.rint("cola"), "RINT(cola)"),
        (SF.rint(SF.col("cola")), "RINT(cola)"),
    ],
)
def test_rint(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sec("cola"), "SEC(cola)"),
        (SF.sec(SF.col("cola")), "SEC(cola)"),
    ],
)
def test_sec(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.signum("cola"), "SIGN(cola)"),
        (SF.signum(SF.col("cola")), "SIGN(cola)"),
    ],
)
def test_signum(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sin("cola"), "SIN(cola)"),
        (SF.sin(SF.col("cola")), "SIN(cola)"),
    ],
)
def test_sin(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sinh("cola"), "SINH(cola)"),
        (SF.sinh(SF.col("cola")), "SINH(cola)"),
    ],
)
def test_sinh(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.tan("cola"), "TAN(cola)"),
        (SF.tan(SF.col("cola")), "TAN(cola)"),
    ],
)
def test_tan(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.tanh("cola"), "TANH(cola)"),
        (SF.tanh(SF.col("cola")), "TANH(cola)"),
    ],
)
def test_tanh(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.degrees("cola"), "DEGREES(cola)"),
        (SF.degrees(SF.col("cola")), "DEGREES(cola)"),
        (SF.toDegrees(SF.col("cola")), "DEGREES(cola)"),
    ],
)
def test_degrees(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.radians("cola"), "RADIANS(cola)"),
        (SF.radians(SF.col("cola")), "RADIANS(cola)"),
        (SF.toRadians(SF.col("cola")), "RADIANS(cola)"),
    ],
)
def test_radians(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bitwise_not("cola"), "~cola"),
        (SF.bitwise_not(SF.col("cola")), "~cola"),
        (SF.bitwiseNOT(SF.col("cola")), "~cola"),
    ],
)
def test_bitwise_not(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.asc_nulls_first("cola"), "cola ASC"),
        (SF.asc_nulls_first(SF.col("cola")), "cola ASC"),
    ],
)
def test_asc_nulls_first(expression, expected):
    assert isinstance(expression.expression, exp.Ordered)
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.asc_nulls_last("cola"), "cola ASC NULLS LAST"),
        (SF.asc_nulls_last(SF.col("cola")), "cola ASC NULLS LAST"),
    ],
)
def test_asc_nulls_last(expression, expected):
    assert isinstance(expression.expression, exp.Ordered)
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.desc_nulls_first("cola"), "cola DESC NULLS FIRST"),
        (SF.desc_nulls_first(SF.col("cola")), "cola DESC NULLS FIRST"),
    ],
)
def test_desc_nulls_first(expression, expected):
    assert isinstance(expression.expression, exp.Ordered)
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.desc_nulls_first("cola"), "cola DESC NULLS FIRST"),
        (SF.desc_nulls_first(SF.col("cola")), "cola DESC NULLS FIRST"),
    ],
)
def test_desc_nulls_last(expression, expected):
    assert isinstance(expression.expression, exp.Ordered)
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.stddev("cola"), "STDDEV(cola)"),
        (SF.stddev(SF.col("cola")), "STDDEV(cola)"),
    ],
)
def test_stddev(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.stddev_samp("cola"), "STDDEV_SAMP(cola)"),
        (SF.stddev_samp(SF.col("cola")), "STDDEV_SAMP(cola)"),
    ],
)
def test_stddev_samp(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.stddev_pop("cola"), "STDDEV_POP(cola)"),
        (SF.stddev_pop(SF.col("cola")), "STDDEV_POP(cola)"),
    ],
)
def test_stddev_pop(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.variance("cola"), "VARIANCE(cola)"),
        (SF.variance(SF.col("cola")), "VARIANCE(cola)"),
    ],
)
def test_variance(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.var_samp("cola"), "VARIANCE(cola)"),
        (SF.var_samp(SF.col("cola")), "VARIANCE(cola)"),
    ],
)
def test_var_samp(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.var_pop("cola"), "VAR_POP(cola)"),
        (SF.var_pop(SF.col("cola")), "VAR_POP(cola)"),
    ],
)
def test_var_pop(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.skewness("cola"), "SKEWNESS(cola)"),
        (SF.skewness(SF.col("cola")), "SKEWNESS(cola)"),
    ],
)
def test_skewness(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.kurtosis("cola"), "KURTOSIS(cola)"),
        (SF.kurtosis(SF.col("cola")), "KURTOSIS(cola)"),
    ],
)
def test_kurtosis(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.collect_list("cola"), "COLLECT_LIST(cola)"),
        (SF.collect_list(SF.col("cola")), "COLLECT_LIST(cola)"),
    ],
)
def test_collect_list(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.collect_set("cola"), "COLLECT_SET(cola)"),
        (SF.collect_set(SF.col("cola")), "COLLECT_SET(cola)"),
    ],
)
def test_collect_set(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.hypot("cola", "colb"), "HYPOT(cola, colb)"),
        (SF.hypot(SF.col("cola"), SF.col("colb")), "HYPOT(cola, colb)"),
        (SF.hypot(10.10, "colb"), "HYPOT(10.1, colb)"),
        (SF.hypot("cola", 10.10), "HYPOT(cola, 10.1)"),
    ],
)
def test_hypot(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.pow("cola", "colb"), "POWER(cola, colb)"),
        (SF.pow(SF.col("cola"), SF.col("colb")), "POWER(cola, colb)"),
        (SF.pow(10.10, "colb"), "POWER(10.1, colb)"),
        (SF.pow("cola", 10.10), "POWER(cola, 10.1)"),
    ],
)
def test_pow(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.power("cola", "colb"), "POWER(cola, colb)"),
        (SF.power(SF.col("cola"), SF.col("colb")), "POWER(cola, colb)"),
        (SF.power(10.10, "colb"), "POWER(10.1, colb)"),
        (SF.power("cola", 10.10), "POWER(cola, 10.1)"),
    ],
)
def test_power(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.row_number(), "ROW_NUMBER()"),
        (SF.row_number(), "ROW_NUMBER()"),
    ],
)
def test_row_number(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.dense_rank(), "DENSE_RANK()"),
        (SF.dense_rank(), "DENSE_RANK()"),
    ],
)
def test_dense_rank(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.rank(), "RANK()"),
        (SF.rank(), "RANK()"),
    ],
)
def test_rank(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.cume_dist(), "CUME_DIST()"),
        (SF.cume_dist(), "CUME_DIST()"),
    ],
)
def test_cume_dist(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.percent_rank(), "PERCENT_RANK()"),
        (SF.percent_rank(), "PERCENT_RANK()"),
    ],
)
def test_percent_rank(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.approx_count_distinct("cola"), "APPROX_COUNT_DISTINCT(cola)"),
        (SF.approx_count_distinct("cola", 0.05), "APPROX_COUNT_DISTINCT(cola, 0.05)"),
        (SF.approx_count_distinct(SF.col("cola")), "APPROX_COUNT_DISTINCT(cola)"),
        (SF.approx_count_distinct(SF.col("cola"), 0.05), "APPROX_COUNT_DISTINCT(cola, 0.05)"),
        (SF.approxCountDistinct(SF.col("cola")), "APPROX_COUNT_DISTINCT(cola)"),
    ],
)
def test_approx_count_distinct(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.coalesce("cola", "colb", "colc"), "COALESCE(cola, colb, colc)"),
        (SF.coalesce(SF.col("cola"), "colb", SF.col("colc")), "COALESCE(cola, colb, colc)"),
        (SF.coalesce(SF.col("cola")), "COALESCE(cola)"),
    ],
)
def test_coalesce(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.corr("cola", "colb"), "CORR(cola, colb)"),
        (SF.corr(SF.col("cola"), "colb"), "CORR(cola, colb)"),
    ],
)
def test_corr(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.covar_pop("cola", "colb"), "COVAR_POP(cola, colb)"),
        (SF.covar_pop(SF.col("cola"), "colb"), "COVAR_POP(cola, colb)"),
    ],
)
def test_covar_pop(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.covar_samp("cola", "colb"), "COVAR_SAMP(cola, colb)"),
        (SF.covar_samp(SF.col("cola"), "colb"), "COVAR_SAMP(cola, colb)"),
    ],
)
def test_covar_samp(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.count_distinct("cola"), "COUNT(DISTINCT cola)"),
        (SF.count_distinct(SF.col("cola")), "COUNT(DISTINCT cola)"),
        (SF.countDistinct(SF.col("cola")), "COUNT(DISTINCT cola)"),
        (SF.count_distinct(SF.col("cola"), "colb"), "COUNT(DISTINCT cola, colb)"),
    ],
)
def test_count_distinct(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.first("cola"), "FIRST(cola)"),
        (SF.first(SF.col("cola")), "FIRST(cola)"),
        (SF.first(SF.col("cola"), True), "FIRST(cola) IGNORE NULLS"),
    ],
)
def test_first(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.grouping_id("cola", "colb"), "GROUPING_ID(cola, colb)"),
        (SF.grouping_id(SF.col("cola"), SF.col("colb")), "GROUPING_ID(cola, colb)"),
        (SF.grouping_id(), "GROUPING_ID()"),
        (SF.grouping_id("cola"), "GROUPING_ID(cola)"),
    ],
)
def test_grouping_id(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


def test_input_file_name():
    assert SF.input_file_name().sql() == "''"


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.isnan("cola"), "ISNAN(cola)"),
        (SF.isnan(SF.col("cola")), "ISNAN(cola)"),
    ],
)
def test_isnan(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.isnull("cola"), "ISNULL(cola)"),
        (SF.isnull(SF.col("cola")), "ISNULL(cola)"),
    ],
)
def test_isnull(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.last("cola"), "LAST(cola)"),
        (SF.last(SF.col("cola")), "LAST(cola)"),
        (SF.last(SF.col("cola"), True), "LAST(cola) IGNORE NULLS"),
    ],
)
def test_last(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.monotonically_increasing_id(), "MONOTONICALLY_INCREASING_ID()"),
    ],
)
def test_monotonically_increasing_id(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.nanvl("cola", "colb"), "NANVL(cola, colb)"),
        (SF.nanvl(SF.col("cola"), SF.col("colb")), "NANVL(cola, colb)"),
    ],
)
def test_nanvl(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.percentile_approx("cola", [0.5, 0.4, 0.1]),
            "PERCENTILE_APPROX(cola, ARRAY(0.5, 0.4, 0.1))",
        ),
        (
            SF.percentile_approx(SF.col("cola"), [0.5, 0.4, 0.1]),
            "PERCENTILE_APPROX(cola, ARRAY(0.5, 0.4, 0.1))",
        ),
        (SF.percentile_approx("cola", 0.1, 100), "PERCENTILE_APPROX(cola, 0.1, 100)"),
    ],
)
def test_percentile_approx(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.rand(0), "RAND(0)"),
        (SF.rand(), "RAND()"),
    ],
)
def test_rand(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.randn(0), "RANDN(0)"),
        (SF.randn(), "RANDN()"),
    ],
)
def test_randn(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.round("cola", 0), "ROUND(cola, 0)"),
        (SF.round(SF.col("cola"), 0), "ROUND(cola, 0)"),
        (SF.round("cola"), "ROUND(cola)"),
    ],
)
def test_round(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bround("cola", 0), "BROUND(cola, 0)"),
        (SF.bround(SF.col("cola"), 0), "BROUND(cola, 0)"),
        (SF.bround("cola"), "BROUND(cola)"),
    ],
)
def test_bround(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.shiftleft("cola", 1), "SHIFTLEFT(cola, 1)"),
        (SF.shiftleft(SF.col("cola"), 1), "SHIFTLEFT(cola, 1)"),
        (SF.shiftLeft("cola", 1), "SHIFTLEFT(cola, 1)"),
    ],
)
def test_shiftleft(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.shiftright("cola", 1), "SHIFTRIGHT(cola, 1)"),
        (SF.shiftright(SF.col("cola"), 1), "SHIFTRIGHT(cola, 1)"),
        (SF.shiftRight("cola", 1), "SHIFTRIGHT(cola, 1)"),
    ],
)
def test_shiftright(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.shiftrightunsigned("cola", 1), "SHIFTRIGHTUNSIGNED(CAST(cola AS BIGINT), 1)"),
        (SF.shiftrightunsigned(SF.col("cola"), 1), "SHIFTRIGHTUNSIGNED(CAST(cola AS BIGINT), 1)"),
        (SF.shiftRightUnsigned("cola", 1), "SHIFTRIGHTUNSIGNED(CAST(cola AS BIGINT), 1)"),
    ],
)
def test_shiftrightunsigned(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


def test_expr():
    assert SF.expr("LENGTH(name)").column_expression.sql(dialect="spark") == "LENGTH(name)"


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.struct("cola", "colb", "colc"), "STRUCT(cola AS cola, colb AS colb, colc AS colc)"),
        (
            SF.struct(SF.col("cola"), SF.col("colb"), SF.col("colc")),
            "STRUCT(cola AS cola, colb AS colb, colc AS colc)",
        ),
        (SF.struct("cola"), "STRUCT(cola AS cola)"),
        (SF.struct(["cola", "colb", "colc"]), "STRUCT(cola AS cola, colb AS colb, colc AS colc)"),
    ],
)
def test_struct(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.greatest("cola"), "GREATEST(cola)"),
        (SF.greatest(SF.col("cola")), "GREATEST(cola)"),
        (
            SF.greatest("col1", "col2", SF.col("col3"), SF.col("col4")),
            "GREATEST(col1, col2, col3, col4)",
        ),
    ],
)
def test_greatest(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.least("cola"), "LEAST(cola)"),
        (SF.least(SF.col("cola")), "LEAST(cola)"),
        (SF.least("col1", "col2", SF.col("col3"), SF.col("col4")), "LEAST(col1, col2, col3, col4)"),
    ],
)
def test_least(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.when(SF.col("cola") == 2, 1), "CASE WHEN cola = 2 THEN 1 END"),
        (
            SF.when(SF.col("cola") == 2, SF.col("colb") + 2),
            "CASE WHEN cola = 2 THEN (colb + 2) END",
        ),
    ],
)
def test_when(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.conv("cola", 2, 16), "CONV(cola, 2, 16)"),
        (SF.conv(SF.col("cola"), 2, 16), "CONV(cola, 2, 16)"),
    ],
)
def test_conv(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.factorial("cola"), "FACTORIAL(cola)"),
        (SF.factorial(SF.col("cola")), "FACTORIAL(cola)"),
    ],
)
def test_factorial(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.lag("cola", 3, "colc"), "LAG(cola, 3, colc)"),
        (SF.lag(SF.col("cola"), 3, "colc"), "LAG(cola, 3, colc)"),
        (SF.lag("cola", 3), "LAG(cola, 3)"),
        (SF.lag("cola"), "LAG(cola, 1)"),
    ],
)
def test_lag(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.lead("cola", 3, "colc"), "LEAD(cola, 3, colc)"),
        (SF.lead(SF.col("cola"), 3, "colc"), "LEAD(cola, 3, colc)"),
        (SF.lead("cola", 3), "LEAD(cola, 3)"),
        (SF.lead("cola"), "LEAD(cola, 1)"),
    ],
)
def test_lead(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.nth_value("cola", 3), "NTH_VALUE(cola, 3)"),
        (SF.nth_value(SF.col("cola"), 3), "NTH_VALUE(cola, 3)"),
        (SF.nth_value("cola"), "NTH_VALUE(cola, 1)"),
        (SF.nth_value("cola", ignoreNulls=True), "NTH_VALUE(cola, 1) IGNORE NULLS"),
    ],
)
def test_nth_value(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


def test_ntile():
    assert SF.ntile(2).column_expression.sql(dialect="spark") == "NTILE(2)"


def test_current_date():
    assert SF.current_date().column_expression.sql(dialect="spark") == "CURRENT_DATE"


def test_current_timestamp():
    assert SF.current_timestamp().column_expression.sql(dialect="spark") == "CURRENT_TIMESTAMP()"


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.date_format("cola", "MM/dd/yyy"), "DATE_FORMAT(cola, 'MM/dd/yyy')"),
        (
            SF.date_format(SF.col("cola"), "MM/dd/yyy"),
            "DATE_FORMAT(cola, 'MM/dd/yyy')",
        ),
    ],
)
def test_date_format(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.year("cola"), "YEAR(TO_DATE(cola))"),
        (SF.year(SF.col("cola")), "YEAR(TO_DATE(cola))"),
    ],
)
def test_year(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.quarter("cola"), "QUARTER(TO_DATE(cola))"),
        (SF.quarter(SF.col("cola")), "QUARTER(TO_DATE(cola))"),
    ],
)
def test_quarter(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.month("cola"), "MONTH(TO_DATE(cola))"),
        (SF.month(SF.col("cola")), "MONTH(TO_DATE(cola))"),
    ],
)
def test_month(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.dayofweek("cola"), "DAYOFWEEK(TO_DATE(cola))"),
        (SF.dayofweek(SF.col("cola")), "DAYOFWEEK(TO_DATE(cola))"),
    ],
)
def test_dayofweek(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.dayofmonth("cola"), "DAYOFMONTH(TO_DATE(cola))"),
        (SF.dayofmonth(SF.col("cola")), "DAYOFMONTH(TO_DATE(cola))"),
    ],
)
def test_dayofmonth(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.dayofyear("cola"), "DAYOFYEAR(TO_DATE(cola))"),
        (SF.dayofyear(SF.col("cola")), "DAYOFYEAR(TO_DATE(cola))"),
    ],
)
def test_dayofyear(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.hour("cola"), "HOUR(cola)"),
        (SF.hour(SF.col("cola")), "HOUR(cola)"),
    ],
)
def test_hour(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.minute("cola"), "MINUTE(cola)"),
        (SF.minute(SF.col("cola")), "MINUTE(cola)"),
    ],
)
def test_minute(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.second("cola"), "SECOND(cola)"),
        (SF.second(SF.col("cola")), "SECOND(cola)"),
    ],
)
def test_second(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.weekofyear("cola"), "WEEKOFYEAR(TO_DATE(cola))"),
        (SF.weekofyear(SF.col("cola")), "WEEKOFYEAR(TO_DATE(cola))"),
    ],
)
def test_weekofyear(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.make_date("cola", "colb", "colc"), "MAKE_DATE(cola, colb, colc)"),
        (SF.make_date(SF.col("cola"), SF.col("colb"), "colc"), "MAKE_DATE(cola, colb, colc)"),
    ],
)
def test_make_date(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.date_add("cola", 2), "CAST(DATE_ADD(CAST(cola AS DATE), 2) AS DATE)"),
        (SF.date_add(SF.col("cola"), 2), "CAST(DATE_ADD(CAST(cola AS DATE), 2) AS DATE)"),
        (SF.date_add("cola", "colb"), "CAST(DATE_ADD(CAST(cola AS DATE), colb) AS DATE)"),
        (
            SF.date_add(SF.current_date(), 5),
            "CAST(DATE_ADD(CAST(CURRENT_DATE AS DATE), 5) AS DATE)",
        ),
    ],
)
def test_date_add(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.date_sub("cola", 2), "CAST(DATE_ADD(CAST(cola AS DATE), -2) AS DATE)"),
        (SF.date_sub(SF.col("cola"), 2), "CAST(DATE_ADD(CAST(cola AS DATE), -2) AS DATE)"),
        (SF.date_sub("cola", "colb"), "CAST(DATE_ADD(CAST(cola AS DATE), colb * -1) AS DATE)"),
    ],
)
def test_date_sub(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.date_diff("cola", "colb"), "DATEDIFF(CAST(cola AS DATE), CAST(colb AS DATE))"),
        (
            SF.date_diff(SF.col("cola"), SF.col("colb")),
            "DATEDIFF(CAST(cola AS DATE), CAST(colb AS DATE))",
        ),
    ],
)
def test_date_diff(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.add_months("cola", 2), "CAST((CAST(cola AS DATE) + INTERVAL 2 MONTH) AS DATE)"),
        (SF.add_months(SF.col("cola"), 2), "CAST((CAST(cola AS DATE) + INTERVAL 2 MONTH) AS DATE)"),
        (SF.add_months("cola", "colb"), "CAST((CAST(cola AS DATE) + INTERVAL colb MONTH) AS DATE)"),
    ],
)
def test_add_months(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.months_between("cola", "colb"), "MONTHS_BETWEEN(cola, colb)"),
        (SF.months_between(SF.col("cola"), SF.col("colb")), "MONTHS_BETWEEN(cola, colb)"),
        (SF.months_between("cola", "colb", True), "MONTHS_BETWEEN(cola, colb, TRUE)"),
    ],
)
def test_months_between(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.to_date("cola"), "TO_DATE(cola)"),
        (SF.to_date(SF.col("cola")), "TO_DATE(cola)"),
        (SF.to_date("cola", "yy-MM-dd"), "TO_DATE(cola, 'yy-MM-dd')"),
    ],
)
def test_to_date(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.to_timestamp("cola"), "CAST(cola AS TIMESTAMP_LTZ)"),
        (SF.to_timestamp(SF.col("cola")), "CAST(cola AS TIMESTAMP_LTZ)"),
        (SF.to_timestamp("cola", "yyyy-MM-dd"), "TO_TIMESTAMP(cola, 'yyyy-MM-dd')"),
    ],
)
def test_to_timestamp(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.trunc("cola", "year"), "CAST(TRUNC(CAST(cola AS DATE), 'YEAR') AS DATE)"),
        (SF.trunc(SF.col("cola"), "year"), "CAST(TRUNC(CAST(cola AS DATE), 'YEAR') AS DATE)"),
    ],
)
def test_trunc(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.date_trunc("year", "cola"),
            "CAST(DATE_TRUNC('YEAR', CAST(cola AS TIMESTAMP)) AS TIMESTAMP)",
        ),
        (
            SF.date_trunc("YEAR", SF.col("cola")),
            "CAST(DATE_TRUNC('YEAR', CAST(cola AS TIMESTAMP)) AS TIMESTAMP)",
        ),
    ],
)
def test_date_trunc(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.next_day("cola", "Mon"), "NEXT_DAY(cola, 'Mon')"),
        (SF.next_day(SF.col("cola"), "Mon"), "NEXT_DAY(cola, 'Mon')"),
    ],
)
def test_next_day(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.last_day("cola"), "LAST_DAY(cola)"),
        (SF.last_day(SF.col("cola")), "LAST_DAY(cola)"),
    ],
)
def test_last_day(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.from_unixtime("cola"), "FROM_UNIXTIME(cola)"),
        (SF.from_unixtime(SF.col("cola")), "FROM_UNIXTIME(cola)"),
        (SF.from_unixtime("cola", "yyyy-MM-dd HH:mm"), "FROM_UNIXTIME(cola, 'yyyy-MM-dd HH:mm')"),
    ],
)
def test_from_unixtime(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.unix_timestamp("cola"), "CAST(UNIX_TIMESTAMP(cola) AS BIGINT)"),
        (SF.unix_timestamp(SF.col("cola")), "CAST(UNIX_TIMESTAMP(cola) AS BIGINT)"),
        (
            SF.unix_timestamp("cola", "yyyy-MM-dd HH:mm"),
            "CAST(UNIX_TIMESTAMP(cola, 'yyyy-MM-dd HH:mm') AS BIGINT)",
        ),
        (SF.unix_timestamp(), "CAST(UNIX_TIMESTAMP() AS BIGINT)"),
    ],
)
def test_unix_timestamp(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.from_utc_timestamp("cola", "PST"), "FROM_UTC_TIMESTAMP(cola, 'PST')"),
        (SF.from_utc_timestamp(SF.col("cola"), "PST"), "FROM_UTC_TIMESTAMP(cola, 'PST')"),
        (SF.from_utc_timestamp("cola", SF.col("colb")), "FROM_UTC_TIMESTAMP(cola, colb)"),
    ],
)
def test_from_utc_timestamp(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.to_utc_timestamp("cola", "PST"), "TO_UTC_TIMESTAMP(cola, 'PST')"),
        (SF.to_utc_timestamp(SF.col("cola"), "PST"), "TO_UTC_TIMESTAMP(cola, 'PST')"),
        (SF.to_utc_timestamp("cola", SF.col("colb")), "TO_UTC_TIMESTAMP(cola, colb)"),
    ],
)
def test_to_utc_timestamp(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.timestamp_seconds("cola"), "CAST(FROM_UNIXTIME(cola) AS TIMESTAMP)"),
        (SF.timestamp_seconds("cola"), "CAST(FROM_UNIXTIME(cola) AS TIMESTAMP)"),
    ],
)
def test_timestamp_seconds(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.window("cola", "10 minutes"), "WINDOW(cola, '10 minutes')"),
        (SF.window(SF.col("cola"), "10 minutes"), "WINDOW(cola, '10 minutes')"),
        (
            SF.window("cola", "2 minutes 30 seconds", "30 seconds", "15 seconds"),
            "WINDOW(cola, '2 minutes 30 seconds', '30 seconds', '15 seconds')",
        ),
        (
            SF.window("cola", "2 minutes 30 seconds", "30 seconds"),
            "WINDOW(cola, '2 minutes 30 seconds', '30 seconds')",
        ),
        (
            SF.window("cola", "2 minutes 30 seconds", startTime="15 seconds"),
            "WINDOW(cola, '2 minutes 30 seconds', '2 minutes 30 seconds', '15 seconds')",
        ),
    ],
)
def test_window(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.session_window("cola", "5 seconds"), "SESSION_WINDOW(cola, '5 seconds')"),
        (
            SF.session_window(SF.col("cola"), SF.lit("5 seconds")),
            "SESSION_WINDOW(cola, '5 seconds')",
        ),
    ],
)
def test_session_window(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.crc32(SF.lit("SQLFrame")), "CRC32('SQLFrame')"),
        (SF.crc32(SF.col("cola")), "CRC32(cola)"),
    ],
)
def test_crc32(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.md5(SF.lit("SQLFrame")), "MD5('SQLFrame')"),
        (SF.md5(SF.col("cola")), "MD5(cola)"),
    ],
)
def test_md5(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sha1(SF.lit("SQLFrame")), "SHA('SQLFrame')"),
        (SF.sha1(SF.col("cola")), "SHA(cola)"),
    ],
)
def test_sha1(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sha2(SF.lit("Spark"), 256), "SHA2('Spark', 256)"),
        (SF.sha2(SF.col("cola"), 256), "SHA2(cola, 256)"),
    ],
)
def test_sha2(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.hash("cola", "colb", "colc"), "HASH(cola, colb, colc)"),
        (SF.hash(SF.col("cola"), SF.col("colb"), SF.col("colc")), "HASH(cola, colb, colc)"),
    ],
)
def test_hash(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.xxhash64("cola", "colb", "colc"), "XXHASH64(cola, colb, colc)"),
        (SF.xxhash64(SF.col("cola"), SF.col("colb"), SF.col("colc")), "XXHASH64(cola, colb, colc)"),
    ],
)
def test_xxhash64(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.assert_true(SF.col("cola") < SF.col("colb")), "ASSERT_TRUE(cola < colb)"),
        (
            SF.assert_true(SF.col("cola") < SF.col("colb"), SF.col("colc")),
            "ASSERT_TRUE(cola < colb, colc)",
        ),
        (
            SF.assert_true(SF.col("cola") < SF.col("colb"), "error"),
            "ASSERT_TRUE(cola < colb, 'error')",
        ),
    ],
)
def test_assert_true(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.raise_error("custom error message"), "RAISE_ERROR('custom error message')"),
        (SF.raise_error(SF.col("cola")), "RAISE_ERROR(cola)"),
    ],
)
def test_raise_error(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.upper("cola"), "UPPER(cola)"),
        (SF.upper(SF.col("cola")), "UPPER(cola)"),
    ],
)
def test_upper(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.lower("cola"), "LOWER(cola)"),
        (SF.lower(SF.col("cola")), "LOWER(cola)"),
    ],
)
def test_lower(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.ascii(SF.lit(2)), "ASCII(2)"),
        (SF.ascii(SF.col("cola")), "ASCII(cola)"),
    ],
)
def test_ascii(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.base64(SF.lit(2)), "BASE64(2)"),
        (SF.base64(SF.col("cola")), "BASE64(cola)"),
    ],
)
def test_base64(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.unbase64(SF.lit(2)), "UNBASE64(2)"),
        (SF.unbase64(SF.col("cola")), "UNBASE64(cola)"),
    ],
)
def test_unbase64(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.ltrim(SF.lit("Spark")), "LTRIM('Spark')"),
        (SF.ltrim(SF.col("cola")), "LTRIM(cola)"),
    ],
)
def test_ltrim(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.rtrim(SF.lit("SQLFrame")), "RTRIM('SQLFrame')"),
        (SF.rtrim(SF.col("cola")), "RTRIM(cola)"),
    ],
)
def test_rtrim(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.trim(SF.lit("SQLFrame")), "TRIM('SQLFrame')"),
        (SF.trim(SF.col("cola")), "TRIM(cola)"),
    ],
)
def test_trim(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.concat_ws("-", "cola", "colb"), "CONCAT_WS('-', cola, colb)"),
        (SF.concat_ws("-", SF.col("cola"), SF.col("colb")), "CONCAT_WS('-', cola, colb)"),
    ],
)
def test_concat_ws(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.decode("cola", "US-ASCII"), "DECODE(cola, 'US-ASCII')"),
        (SF.decode(SF.col("cola"), "US-ASCII"), "DECODE(cola, 'US-ASCII')"),
    ],
)
def test_decode(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.encode("cola", "US-ASCII"), "ENCODE(cola, 'US-ASCII')"),
        (SF.encode(SF.col("cola"), "US-ASCII"), "ENCODE(cola, 'US-ASCII')"),
    ],
)
def test_encode(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.format_number("cola", 4), "FORMAT_NUMBER(cola, 4)"),
        (SF.format_number(SF.col("cola"), 4), "FORMAT_NUMBER(cola, 4)"),
    ],
)
def test_format_number(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.format_string("%d %s", "cola", "colb", "colc"),
            "FORMAT_STRING('%d %s', cola, colb, colc)",
        ),
        (
            SF.format_string("%d %s", SF.col("cola"), SF.col("colb"), SF.col("colc")),
            "FORMAT_STRING('%d %s', cola, colb, colc)",
        ),
    ],
)
def test_format_string(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.instr("cola", "test"), "LOCATE('test', cola)"),
        (SF.instr(SF.col("cola"), "test"), "LOCATE('test', cola)"),
    ],
)
def test_instr(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.overlay("cola", "colb", 3, 7), "OVERLAY(cola PLACING colb FROM 3 FOR 7)"),
        (
            SF.overlay(SF.col("cola"), SF.col("colb"), SF.lit(3), SF.lit(7)),
            "OVERLAY(cola PLACING colb FROM 3 FOR 7)",
        ),
        (SF.overlay("cola", "colb", 3), "OVERLAY(cola PLACING colb FROM 3)"),
    ],
)
def test_overlay(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sentences("cola", SF.lit("en"), SF.lit("US")), "SENTENCES(cola, 'en', 'US')"),
        (SF.sentences(SF.col("cola"), SF.lit("en"), SF.lit("US")), "SENTENCES(cola, 'en', 'US')"),
        (SF.sentences("cola", SF.lit("en")), "SENTENCES(cola, 'en')"),
        (SF.sentences(SF.col("cola"), country=SF.lit("US")), "SENTENCES(cola, 'en', 'US')"),
        (SF.sentences(SF.col("cola")), "SENTENCES(cola)"),
    ],
)
def test_sentences(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.substring("cola", 2, 3), "SUBSTRING(cola, 2, 3)"),
        (SF.substring(SF.col("cola"), 2, 3), "SUBSTRING(cola, 2, 3)"),
    ],
)
def test_substring(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.substring_index("cola", ".", 2), "SUBSTRING_INDEX(cola, '.', 2)"),
        (SF.substring_index(SF.col("cola"), ".", 2), "SUBSTRING_INDEX(cola, '.', 2)"),
    ],
)
def test_substring_index(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.levenshtein("cola", "colb"), "LEVENSHTEIN(cola, colb)"),
        (SF.levenshtein(SF.col("cola"), SF.col("colb")), "LEVENSHTEIN(cola, colb)"),
    ],
)
def test_levenshtein(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.locate("test", "cola", 3), "LOCATE('test', cola, 3)"),
        (SF.locate("test", SF.col("cola"), 3), "LOCATE('test', cola, 3)"),
        (SF.locate("test", "cola"), "LOCATE('test', cola)"),
    ],
)
def test_locate(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.lpad("cola", 3, "#"), "LPAD(cola, 3, '#')"),
        (SF.lpad(SF.col("cola"), 3, "#"), "LPAD(cola, 3, '#')"),
    ],
)
def test_lpad(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.rpad("cola", 3, "#"), "RPAD(cola, 3, '#')"),
        (SF.rpad(SF.col("cola"), 3, "#"), "RPAD(cola, 3, '#')"),
    ],
)
def test_rpad(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.repeat("cola", 3), "REPEAT(cola, 3)"),
        (SF.repeat(SF.col("cola"), 3), "REPEAT(cola, 3)"),
    ],
)
def test_repeat(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.split("cola", "[ABC]", 3), "SPLIT(cola, '[ABC]', 3)"),
        (SF.split(SF.col("cola"), "[ABC]", 3), "SPLIT(cola, '[ABC]', 3)"),
        (SF.split("cola", "[ABC]"), "SPLIT(cola, '[ABC]')"),
    ],
)
def test_split(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.regexp_extract("cola", r"(\d+)-(\d+)", 2),
            "REGEXP_EXTRACT(cola, '(\\\\d+)-(\\\\d+)', 2)",
        ),
        (
            SF.regexp_extract(SF.col("cola"), r"(\d+)-(\d+)", 2),
            "REGEXP_EXTRACT(cola, '(\\\\d+)-(\\\\d+)', 2)",
        ),
        (
            SF.regexp_extract("cola", r"(\d+)-(\d+)", 1),
            "REGEXP_EXTRACT(cola, '(\\\\d+)-(\\\\d+)')",
        ),
        (
            SF.regexp_extract(SF.col("cola"), r"(\d+)-(\d+)"),
            "REGEXP_EXTRACT(cola, '(\\\\d+)-(\\\\d+)')",
        ),
    ],
)
def test_regexp_extract(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.regexp_replace("cola", r"(\d+)", "--"), "REGEXP_REPLACE(cola, '(\\\\d+)', '--')"),
        (
            SF.regexp_replace(SF.col("cola"), r"(\d+)", "--"),
            "REGEXP_REPLACE(cola, '(\\\\d+)', '--')",
        ),
    ],
)
def test_regexp_replace(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.initcap("cola"), "INITCAP(cola)"),
        (SF.initcap(SF.col("cola")), "INITCAP(cola)"),
    ],
)
def test_initcap(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.soundex("cola"), "SOUNDEX(cola)"),
        (SF.soundex(SF.col("cola")), "SOUNDEX(cola)"),
    ],
)
def test_soundex(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bin("cola"), "BIN(cola)"),
        (SF.bin(SF.col("cola")), "BIN(cola)"),
    ],
)
def test_bin(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.hex("cola"), "HEX(cola)"),
        (SF.hex(SF.col("cola")), "HEX(cola)"),
    ],
)
def test_hex(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.unhex("cola"), "UNHEX(cola)"),
        (SF.unhex(SF.col("cola")), "UNHEX(cola)"),
    ],
)
def test_unhex(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.length("cola"), "LENGTH(cola)"),
        (SF.length(SF.col("cola")), "LENGTH(cola)"),
    ],
)
def test_length(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.octet_length("cola"), "OCTET_LENGTH(cola)"),
        (SF.octet_length(SF.col("cola")), "OCTET_LENGTH(cola)"),
    ],
)
def test_octet_length(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bit_length("cola"), "BIT_LENGTH(cola)"),
        (SF.bit_length(SF.col("cola")), "BIT_LENGTH(cola)"),
    ],
)
def test_bit_length(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.translate("cola", "abc", "xyz"), "TRANSLATE(cola, 'abc', 'xyz')"),
        (SF.translate(SF.col("cola"), "abc", "xyz"), "TRANSLATE(cola, 'abc', 'xyz')"),
    ],
)
def test_translate(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array("cola", "colb"), "ARRAY(cola, colb)"),
        (SF.array(SF.col("cola"), SF.col("colb")), "ARRAY(cola, colb)"),
        (SF.array(["cola", "colb"]), "ARRAY(cola, colb)"),
    ],
)
def test_array(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_agg("cola"), "COLLECT_LIST(cola)"),
        (SF.array_agg(SF.col("cola")), "COLLECT_LIST(cola)"),
    ],
)
def test_array_agg(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_append("cola", "val"), "ARRAY_APPEND(cola, 'val')"),
        (SF.array_append(SF.col("cola"), SF.col("colb")), "ARRAY_APPEND(cola, colb)"),
    ],
)
def test_array_append(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_compact("cola"), "ARRAY_COMPACT(cola)"),
        (SF.array_compact(SF.col("cola")), "ARRAY_COMPACT(cola)"),
    ],
)
def test_array_compact(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_insert("cola", 1, "val"), "ARRAY_INSERT(cola, 1, 'val')"),
        (SF.array_insert(SF.col("cola"), 1, SF.col("colb")), "ARRAY_INSERT(cola, 1, colb)"),
        (SF.array_insert("cola", "colb", "val"), "ARRAY_INSERT(cola, colb, 'val')"),
    ],
)
def test_array_insert(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_prepend("cola", "-"), "ARRAY_PREPEND(cola, '-')"),
        (SF.array_prepend(SF.col("cola"), SF.col("colb")), "ARRAY_PREPEND(cola, colb)"),
    ],
)
def test_array_prepend(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_size("cola"), "SIZE(cola)"),
        (SF.array_size(SF.col("cola")), "SIZE(cola)"),
    ],
)
def test_array_size(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bit_and("cola"), "BIT_AND(cola)"),
        (SF.bit_and(SF.col("cola")), "BIT_AND(cola)"),
    ],
)
def test_bit_and(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bit_or("cola"), "BIT_OR(cola)"),
        (SF.bit_or(SF.col("cola")), "BIT_OR(cola)"),
    ],
)
def test_bit_or(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bit_xor("cola"), "BIT_XOR(cola)"),
        (SF.bit_xor(SF.col("cola")), "BIT_XOR(cola)"),
    ],
)
def test_bit_xor(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bit_count("cola"), "BIT_COUNT(cola)"),
        (SF.bit_count(SF.col("cola")), "BIT_COUNT(cola)"),
    ],
)
def test_bit_count(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bit_get("cola", "colb"), "BIT_GET(cola, colb)"),
        (SF.bit_get(SF.col("cola"), SF.col("colb")), "BIT_GET(cola, colb)"),
    ],
)
def test_bit_get(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.getbit("cola", "colb"), "GETBIT(cola, colb)"),
        (SF.getbit(SF.col("cola"), SF.lit(1)), "GETBIT(cola, 1)"),
    ],
)
def test_getbit(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.create_map("keya", "valuea", "keyb", "valueb"), "MAP(keya, valuea, keyb, valueb)"),
        (
            SF.create_map(SF.col("keya"), SF.col("valuea"), SF.col("keyb"), SF.col("valueb")),
            "MAP(keya, valuea, keyb, valueb)",
        ),
        (SF.create_map(["keya", "valuea", "keyb", "valueb"]), "MAP(keya, valuea, keyb, valueb)"),
    ],
)
def test_create_map(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.map_from_arrays("cola", "colb"), "MAP_FROM_ARRAYS(cola, colb)"),
        (SF.map_from_arrays(SF.col("cola"), SF.col("colb")), "MAP_FROM_ARRAYS(cola, colb)"),
    ],
)
def test_map_from_arrays(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_contains("cola", "test"), "ARRAY_CONTAINS(cola, 'test')"),
        (SF.array_contains(SF.col("cola"), "test"), "ARRAY_CONTAINS(cola, 'test')"),
        (SF.array_contains("cola", SF.col("colb")), "ARRAY_CONTAINS(cola, colb)"),
    ],
)
def test_array_contains(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.arrays_overlap("cola", "colb"), "cola && colb"),
        (SF.arrays_overlap(SF.col("cola"), SF.col("colb")), "cola && colb"),
    ],
)
def test_arrays_overlap(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.slice("cola", SF.col("colb"), SF.col("colc")), "SLICE(cola, colb, colc)"),
        (SF.slice(SF.col("cola"), SF.col("colb"), SF.col("colc")), "SLICE(cola, colb, colc)"),
        (SF.slice("cola", 1, 10), "SLICE(cola, 1, 10)"),
    ],
)
def test_slice(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.array_join("cola", "-", "NULL_REPLACEMENT"),
            "ARRAY_JOIN(cola, '-', 'NULL_REPLACEMENT')",
        ),
        (
            SF.array_join(SF.col("cola"), "-", "NULL_REPLACEMENT"),
            "ARRAY_JOIN(cola, '-', 'NULL_REPLACEMENT')",
        ),
        (SF.array_join("cola", "-"), "ARRAY_JOIN(cola, '-')"),
    ],
)
def test_array_join(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.concat("cola", "colb"), "CONCAT(cola, colb)"),
        (SF.concat(SF.col("cola"), SF.col("colb")), "CONCAT(cola, colb)"),
        (SF.concat("cola"), "CONCAT(cola)"),
    ],
)
def test_concat(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_position("cola", SF.col("colb")), "COALESCE(ARRAY_POSITION(cola, colb), 0)"),
        (
            SF.array_position(SF.col("cola"), SF.col("colb")),
            "COALESCE(ARRAY_POSITION(cola, colb), 0)",
        ),
        (SF.array_position("cola", "test"), "COALESCE(ARRAY_POSITION(cola, 'test'), 0)"),
    ],
)
def test_array_position(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.element_at("cola", SF.col("colb")), "ELEMENT_AT(cola, colb)"),
        (SF.element_at(SF.col("cola"), SF.col("colb")), "ELEMENT_AT(cola, colb)"),
        (SF.element_at("cola", "test"), "ELEMENT_AT(cola, 'test')"),
    ],
)
def test_element_at(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_remove("cola", SF.col("colb")), "ARRAY_REMOVE(cola, colb)"),
        (SF.array_remove(SF.col("cola"), SF.col("colb")), "ARRAY_REMOVE(cola, colb)"),
        (SF.array_remove("cola", "test"), "ARRAY_REMOVE(cola, 'test')"),
    ],
)
def test_array_remove(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_distinct("cola"), "ARRAY_DISTINCT(cola)"),
        (SF.array_distinct(SF.col("cola")), "ARRAY_DISTINCT(cola)"),
    ],
)
def test_array_distinct(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_intersect("cola", "colb"), "ARRAY_INTERSECT(cola, colb)"),
        (SF.array_intersect(SF.col("cola"), SF.col("colb")), "ARRAY_INTERSECT(cola, colb)"),
    ],
)
def test_array_intersect(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_union("cola", "colb"), "ARRAY_UNION(cola, colb)"),
        (SF.array_union(SF.col("cola"), SF.col("colb")), "ARRAY_UNION(cola, colb)"),
    ],
)
def test_array_union(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_except("cola", "colb"), "ARRAY_EXCEPT(cola, colb)"),
        (SF.array_except(SF.col("cola"), SF.col("colb")), "ARRAY_EXCEPT(cola, colb)"),
    ],
)
def test_array_except(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.explode("cola"), "EXPLODE(cola)"),
        (SF.explode(SF.col("cola")), "EXPLODE(cola)"),
    ],
)
def test_explode(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.posexplode("cola"), "POSEXPLODE(cola)"),
        (SF.posexplode(SF.col("cola")), "POSEXPLODE(cola)"),
    ],
)
def test_pos_explode(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.explode_outer("cola"), "EXPLODE_OUTER(cola)"),
        (SF.explode_outer(SF.col("cola")), "EXPLODE_OUTER(cola)"),
    ],
)
def test_explode_outer(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.posexplode_outer("cola"), "POSEXPLODE_OUTER(cola)"),
        (SF.posexplode_outer(SF.col("cola")), "POSEXPLODE_OUTER(cola)"),
    ],
)
def test_posexplode_outer(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.get_json_object("cola", "$.f1"), "GET_JSON_OBJECT(cola, '$.f1')"),
        (SF.get_json_object(SF.col("cola"), "$.f1"), "GET_JSON_OBJECT(cola, '$.f1')"),
    ],
)
def test_get_json_object(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.json_tuple("cola", "f1", "f2"), "JSON_TUPLE(cola, 'f1', 'f2')"),
        (SF.json_tuple(SF.col("cola"), "f1", "f2"), "JSON_TUPLE(cola, 'f1', 'f2')"),
    ],
)
def test_json_tuple(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.from_json("cola", "cola INT", dict(timestampFormat="dd/MM/yyyy")),
            "FROM_JSON(cola, 'cola INT', MAP('timestampFormat', 'dd/MM/yyyy'))",
        ),
        (
            SF.from_json(SF.col("cola"), "cola INT", dict(timestampFormat="dd/MM/yyyy")),
            "FROM_JSON(cola, 'cola INT', MAP('timestampFormat', 'dd/MM/yyyy'))",
        ),
        (SF.from_json("cola", "cola INT"), "FROM_JSON(cola, 'cola INT')"),
    ],
)
def test_from_json(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.to_json("cola", dict(timestampFormat="dd/MM/yyyy")),
            "TO_JSON(cola, MAP('timestampFormat', 'dd/MM/yyyy'))",
        ),
        (
            SF.to_json(SF.col("cola"), dict(timestampFormat="dd/MM/yyyy")),
            "TO_JSON(cola, MAP('timestampFormat', 'dd/MM/yyyy'))",
        ),
        (SF.to_json("cola"), "TO_JSON(cola)"),
    ],
)
def test_to_json(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.schema_of_json(SF.col("cola"), dict(timestampFormat="dd/MM/yyyy")),
            "SCHEMA_OF_JSON(cola, MAP('timestampFormat', 'dd/MM/yyyy'))",
        ),
        (
            SF.schema_of_json(SF.col("cola"), dict(timestampFormat="dd/MM/yyyy")),
            "SCHEMA_OF_JSON(cola, MAP('timestampFormat', 'dd/MM/yyyy'))",
        ),
        (SF.schema_of_json(SF.col("cola")), "SCHEMA_OF_JSON(cola)"),
    ],
)
def test_schema_of_json(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.schema_of_csv(SF.col("cola"), dict(timestampFormat="dd/MM/yyyy")),
            "SCHEMA_OF_CSV(cola, MAP('timestampFormat', 'dd/MM/yyyy'))",
        ),
        (
            SF.schema_of_csv(SF.col("cola"), dict(timestampFormat="dd/MM/yyyy")),
            "SCHEMA_OF_CSV(cola, MAP('timestampFormat', 'dd/MM/yyyy'))",
        ),
        (SF.schema_of_csv(SF.col("cola")), "SCHEMA_OF_CSV(cola)"),
    ],
)
def test_schema_of_csv(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.to_csv("cola", dict(timestampFormat="dd/MM/yyyy")),
            "TO_CSV(cola, MAP('timestampFormat', 'dd/MM/yyyy'))",
        ),
        (
            SF.to_csv(SF.col("cola"), dict(timestampFormat="dd/MM/yyyy")),
            "TO_CSV(cola, MAP('timestampFormat', 'dd/MM/yyyy'))",
        ),
        (SF.to_csv("cola"), "TO_CSV(cola)"),
    ],
)
def test_to_csv(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.size("cola"), "SIZE(cola)"),
        (SF.size(SF.col("cola")), "SIZE(cola)"),
    ],
)
def test_size(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_min("cola"), "ARRAY_MIN(cola)"),
        (SF.array_min(SF.col("cola")), "ARRAY_MIN(cola)"),
    ],
)
def test_array_min(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_max("cola"), "ARRAY_MAX(cola)"),
        (SF.array_max(SF.col("cola")), "ARRAY_MAX(cola)"),
    ],
)
def test_array_max(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sort_array("cola", False), "SORT_ARRAY(cola, FALSE)"),
        (SF.sort_array(SF.col("cola"), False), "SORT_ARRAY(cola, FALSE)"),
        (SF.sort_array("cola"), "SORT_ARRAY(cola)"),
    ],
)
def test_sort_array(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_sort("cola"), "ARRAY_SORT(cola)"),
        (SF.array_sort(SF.col("cola")), "ARRAY_SORT(cola)"),
        (
            SF.array_sort(
                "cola",
                lambda x, y: SF.when(x.isNull() | y.isNull(), SF.lit(0)).otherwise(
                    SF.length(y) - SF.length(x)
                ),
            ),
            "ARRAY_SORT(cola, (x, y) -> CASE WHEN (x IS NULL OR y IS NULL) THEN 0 ELSE (LENGTH(y) - LENGTH(x)) END)",
        ),
    ],
)
def test_array_sort(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.shuffle("cola"), "SHUFFLE(cola)"),
        (SF.shuffle(SF.col("cola")), "SHUFFLE(cola)"),
    ],
)
def test_shuffle(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.reverse("cola"), "REVERSE(cola)"),
        (SF.reverse(SF.col("cola")), "REVERSE(cola)"),
    ],
)
def test_reverse(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.flatten("cola"), "FLATTEN(cola)"),
        (SF.flatten(SF.col("cola")), "FLATTEN(cola)"),
    ],
)
def test_flatten(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.map_keys("cola"), "MAP_KEYS(cola)"),
        (SF.map_keys(SF.col("cola")), "MAP_KEYS(cola)"),
    ],
)
def test_map_keys(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.map_values("cola"), "MAP_VALUES(cola)"),
        (SF.map_values(SF.col("cola")), "MAP_VALUES(cola)"),
    ],
)
def test_map_values(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.map_entries("cola"), "MAP_ENTRIES(cola)"),
        (SF.map_entries(SF.col("cola")), "MAP_ENTRIES(cola)"),
    ],
)
def test_map_entries(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.map_from_entries("cola"), "MAP_FROM_ENTRIES(cola)"),
        (SF.map_from_entries(SF.col("cola")), "MAP_FROM_ENTRIES(cola)"),
    ],
)
def test_map_from_entries(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_repeat("cola", 2), "ARRAY_REPEAT(cola, 2)"),
        (SF.array_repeat(SF.col("cola"), 2), "ARRAY_REPEAT(cola, 2)"),
    ],
)
def test_array_repeat(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.arrays_zip("cola", "colb"), "ARRAYS_ZIP(cola, colb)"),
        (SF.arrays_zip(SF.col("cola"), SF.col("colb")), "ARRAYS_ZIP(cola, colb)"),
        (SF.arrays_zip("cola"), "ARRAYS_ZIP(cola)"),
    ],
)
def test_arrays_zip(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.map_concat("cola", "colb"), "MAP_CONCAT(cola, colb)"),
        (SF.map_concat(SF.col("cola"), SF.col("colb")), "MAP_CONCAT(cola, colb)"),
        (SF.map_concat("cola"), "MAP_CONCAT(cola)"),
    ],
)
def test_map_concat(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sequence("cola", "colb", "colc"), "SEQUENCE(cola, colb, colc)"),
        (SF.sequence(SF.col("cola"), SF.col("colb"), SF.col("colc")), "SEQUENCE(cola, colb, colc)"),
        (SF.sequence("cola", "colb"), "SEQUENCE(cola, colb)"),
    ],
)
def test_sequence(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.from_csv("cola", "cola INT", dict(timestampFormat="dd/MM/yyyy")),
            "FROM_CSV(cola, 'cola INT', MAP('timestampFormat', 'dd/MM/yyyy'))",
        ),
        (
            SF.from_csv(SF.col("cola"), "cola INT", dict(timestampFormat="dd/MM/yyyy")),
            "FROM_CSV(cola, 'cola INT', MAP('timestampFormat', 'dd/MM/yyyy'))",
        ),
        (SF.from_csv("cola", "cola INT"), "FROM_CSV(cola, 'cola INT')"),
    ],
)
def test_from_csv(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.aggregate("cola", SF.lit(0), lambda acc, x: acc + x, lambda acc: acc * 2),
            "AGGREGATE(cola, 0, (acc, x) -> (acc + x), acc -> (acc * 2))",
        ),
        (
            SF.aggregate(SF.col("cola"), SF.lit(0), lambda acc, x: acc + x, lambda acc: acc * 2),
            "AGGREGATE(cola, 0, (acc, x) -> (acc + x), acc -> (acc * 2))",
        ),
        (
            SF.aggregate("cola", SF.lit(0), lambda acc, x: acc + x),
            "AGGREGATE(cola, 0, (acc, x) -> (acc + x))",
        ),
        (
            SF.aggregate(
                "cola",
                SF.lit(0),
                lambda accumulator, target: accumulator + target,
                lambda accumulator: accumulator * 2,
            ),
            "AGGREGATE(cola, 0, (accumulator, target) -> (accumulator + target), accumulator -> (accumulator * 2))",
        ),
    ],
)
def test_aggregate(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.transform("cola", lambda x: x * 2), "TRANSFORM(cola, x -> (x * 2))"),
        (SF.transform(SF.col("cola"), lambda x, i: x * i), "TRANSFORM(cola, (x, i) -> (x * i))"),
        (
            SF.transform("cola", lambda target, row_count: target * row_count),
            "TRANSFORM(cola, (target, row_count) -> (target * row_count))",
        ),
    ],
)
def test_transform(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.exists("cola", lambda x: x % 2 == 0), "EXISTS(cola, x -> (x % 2) = 0)"),
        (SF.exists(SF.col("cola"), lambda x: x % 2 == 0), "EXISTS(cola, x -> (x % 2) = 0)"),
        (SF.exists("cola", lambda target: target > 0), "EXISTS(cola, target -> target > 0)"),
    ],
)
def test_exists(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.forall("cola", lambda x: x.rlike("foo")), "FORALL(cola, x -> x RLIKE 'foo')"),
        (SF.forall(SF.col("cola"), lambda x: x.rlike("foo")), "FORALL(cola, x -> x RLIKE 'foo')"),
        (
            SF.forall("cola", lambda target: target.rlike("foo")),
            "FORALL(cola, target -> target RLIKE 'foo')",
        ),
    ],
)
def test_forall(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.filter("cola", lambda x: SF.month(SF.to_date(x)) > SF.lit(6)),
            "FILTER(cola, x -> MONTH(TO_DATE(x)) > 6)",
        ),
        (
            SF.filter(SF.col("cola"), lambda x, i: SF.month(SF.to_date(x)) > SF.lit(i)),
            "FILTER(cola, (x, i) -> MONTH(TO_DATE(x)) > i)",
        ),
        (
            SF.filter(
                "cola", lambda target, row_count: SF.month(SF.to_date(target)) > SF.lit(row_count)
            ),
            "FILTER(cola, (target, row_count) -> MONTH(TO_DATE(target)) > row_count)",
        ),
    ],
)
def test_filter(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.zip_with("cola", "colb", lambda x, y: SF.concat_ws("_", x, y)),
            "ZIP_WITH(cola, colb, (x, y) -> CONCAT_WS('_', x, y))",
        ),
        (
            SF.zip_with(SF.col("cola"), SF.col("colb"), lambda x, y: SF.concat_ws("_", x, y)),
            "ZIP_WITH(cola, colb, (x, y) -> CONCAT_WS('_', x, y))",
        ),
        (
            SF.zip_with("cola", "colb", lambda l, r: SF.concat_ws("_", l, r)),
            "ZIP_WITH(cola, colb, (l, r) -> CONCAT_WS('_', l, r))",
        ),
    ],
)
def test_zip_with(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.transform_keys("cola", lambda k, v: SF.upper(k)),
            "TRANSFORM_KEYS(cola, (k, v) -> UPPER(k))",
        ),
        (
            SF.transform_keys(SF.col("cola"), lambda k, v: SF.upper(k)),
            "TRANSFORM_KEYS(cola, (k, v) -> UPPER(k))",
        ),
        (
            SF.transform_keys("cola", lambda key, _: SF.upper(key)),
            "TRANSFORM_KEYS(cola, (key, _) -> UPPER(key))",
        ),
    ],
)
def test_transform_keys(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.transform_values("cola", lambda k, v: SF.upper(v)),
            "TRANSFORM_VALUES(cola, (k, v) -> UPPER(v))",
        ),
        (
            SF.transform_values(SF.col("cola"), lambda k, v: SF.upper(v)),
            "TRANSFORM_VALUES(cola, (k, v) -> UPPER(v))",
        ),
        (
            SF.transform_values("cola", lambda _, value: SF.upper(value)),
            "TRANSFORM_VALUES(cola, (_, value) -> UPPER(value))",
        ),
    ],
)
def test_transform_values(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.map_filter("cola", lambda k, v: k > v), "MAP_FILTER(cola, (k, v) -> k > v)"),
        (SF.map_filter(SF.col("cola"), lambda k, v: k > v), "MAP_FILTER(cola, (k, v) -> k > v)"),
        (
            SF.map_filter("cola", lambda key, value: key > value),
            "MAP_FILTER(cola, (key, value) -> key > value)",
        ),
    ],
)
def test_map_filter(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


def test_map_zip_with():
    assert (
        SF.map_zip_with(
            "base", "ratio", lambda k, v1, v2: SF.round(v1 * v2, 2)
        ).column_expression.sql(dialect="spark")
        == "MAP_ZIP_WITH(base, ratio, (k, v1, v2) -> ROUND((v1 * v2), 2))"
    )


def test_nullif():
    assert SF.nullif("cola", "colb").column_expression.sql(dialect="spark") == "NULLIF(cola, colb)"


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.stack("cola", "colb"), "STACK(cola, colb)"),
        (SF.stack(SF.col("cola"), SF.col("colb")), "STACK(cola, colb)"),
        (SF.stack("cola"), "STACK(cola)"),
        (SF.stack("cola", "colb", "colc", "cold"), "STACK(cola, colb, colc, cold)"),
    ],
)
def test_stack(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.make_interval("cola", "colb", None, SF.lit(100)),
            "MAKE_INTERVAL(cola, colb, NULL, 100)",
        ),
    ],
)
def test_make_interval(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.try_add("cola", "colb"), "TRY_ADD(cola, colb)"),
        (SF.try_add(SF.col("cola"), SF.col("colb")), "TRY_ADD(cola, colb)"),
    ],
)
def test_try_add(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.try_avg("cola"), "TRY_AVG(cola)"),
        (SF.try_avg(SF.col("cola")), "TRY_AVG(cola)"),
    ],
)
def test_try_avg(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.try_divide("cola", "colb"), "TRY_DIVIDE(cola, colb)"),
        (SF.try_divide(SF.col("cola"), SF.col("colb")), "TRY_DIVIDE(cola, colb)"),
    ],
)
def test_try_divide(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.try_multiply("cola", "colb"), "TRY_MULTIPLY(cola, colb)"),
        (SF.try_multiply(SF.col("cola"), SF.col("colb")), "TRY_MULTIPLY(cola, colb)"),
    ],
)
def test_try_multiply(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.try_subtract("cola", "colb"), "TRY_SUBTRACT(cola, colb)"),
        (SF.try_subtract(SF.col("cola"), SF.col("colb")), "TRY_SUBTRACT(cola, colb)"),
    ],
)
def test_try_subtract(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.try_sum("cola"), "TRY_SUM(cola)"),
        (SF.try_sum(SF.col("cola")), "TRY_SUM(cola)"),
    ],
)
def test_try_sum(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.try_to_binary("cola"), "TRY_TO_BINARY(cola)"),
        (SF.try_to_binary(SF.col("cola")), "TRY_TO_BINARY(cola)"),
        (SF.try_to_binary("cola", SF.lit("UTF-8")), "TRY_TO_BINARY(cola, 'UTF-8')"),
    ],
)
def test_try_to_binary(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.try_to_number("cola"), "TRY_TO_NUMBER(cola)"),
        (SF.try_to_number(SF.col("cola")), "TRY_TO_NUMBER(cola)"),
        (SF.try_to_number(SF.col("cola"), SF.lit("$99.99")), "TRY_TO_NUMBER(cola, '$99.99')"),
    ],
)
def test_try_to_number(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.aes_decrypt("cola", "colb", "colc", "cold", "cole"),
            "AES_DECRYPT(cola, colb, colc, cold, cole)",
        ),
        (
            SF.aes_decrypt(
                SF.col("cola"), SF.col("colb"), SF.col("colc"), SF.col("cold"), SF.col("cole")
            ),
            "AES_DECRYPT(cola, colb, colc, cold, cole)",
        ),
        (SF.aes_decrypt("cola", SF.col("colb")), "AES_DECRYPT(cola, colb)"),
        (
            SF.aes_decrypt(SF.col("cola"), SF.col("colb"), padding="colc"),
            "AES_DECRYPT(cola, colb, NULL, colc)",
        ),
    ],
)
def test_aes_decrypt(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.aes_encrypt("cola", "colb", "colc", "cold", "cole", "colf"),
            "AES_ENCRYPT(cola, colb, colc, cold, cole, colf)",
        ),
        (
            SF.aes_encrypt(
                SF.col("cola"),
                SF.col("colb"),
                SF.col("colc"),
                SF.col("cold"),
                SF.col("cole"),
                SF.col("colf"),
            ),
            "AES_ENCRYPT(cola, colb, colc, cold, cole, colf)",
        ),
        (SF.aes_encrypt("cola", SF.col("colb")), "AES_ENCRYPT(cola, colb)"),
        (
            SF.aes_encrypt(SF.col("cola"), SF.col("colb"), padding="colc"),
            "AES_ENCRYPT(cola, colb, NULL, colc)",
        ),
    ],
)
def test_aes_encrypt(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bitmap_bit_position("cola"), "BITMAP_BIT_POSITION(cola)"),
        (SF.bitmap_bit_position(SF.col("cola")), "BITMAP_BIT_POSITION(cola)"),
    ],
)
def test_bitmap_bit_position(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bitmap_bucket_number("cola"), "BITMAP_BUCKET_NUMBER(cola)"),
        (SF.bitmap_bucket_number(SF.col("cola")), "BITMAP_BUCKET_NUMBER(cola)"),
    ],
)
def test_bitmap_bucket_number(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bitmap_construct_agg("cola"), "BITMAP_CONSTRUCT_AGG(cola)"),
        (SF.bitmap_construct_agg(SF.col("cola")), "BITMAP_CONSTRUCT_AGG(cola)"),
    ],
)
def test_bitmap_construct_agg(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bitmap_count("cola"), "BITMAP_COUNT(cola)"),
        (SF.bitmap_count(SF.col("cola")), "BITMAP_COUNT(cola)"),
    ],
)
def test_bitmap_count(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bitmap_or_agg("cola"), "BITMAP_OR_AGG(cola)"),
        (SF.bitmap_or_agg(SF.col("cola")), "BITMAP_OR_AGG(cola)"),
    ],
)
def test_bitmap_or_agg(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.any_value("cola", False), "ANY_VALUE(cola)"),
        (SF.any_value(SF.col("cola"), True), "ANY_VALUE(cola) IGNORE NULLS"),
    ],
)
def test_any_value(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.approx_percentile("cola", 0.5), "PERCENTILE_APPROX(cola, 0.5, 10000)"),
        (
            SF.approx_percentile(SF.col("cola"), SF.col("colb")),
            "PERCENTILE_APPROX(cola, colb, 10000)",
        ),
        (SF.approx_percentile("cola", 0.5, 100), "PERCENTILE_APPROX(cola, 0.5, 100)"),
        (
            SF.approx_percentile("cola", [0.5, 0.75], 100),
            "PERCENTILE_APPROX(cola, ARRAY(0.5, 0.75), 100)",
        ),
    ],
)
def test_approx_percentile(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bool_and("cola"), "BOOL_AND(cola)"),
        (SF.bool_and(SF.col("cola")), "BOOL_AND(cola)"),
    ],
)
def test_bool_and(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bool_or("cola"), "BOOL_OR(cola)"),
        (SF.bool_or(SF.col("cola")), "BOOL_OR(cola)"),
    ],
)
def test_bool_or(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.btrim("cola"), "TRIM(cola)"),
        (SF.btrim(SF.col("cola")), "TRIM(cola)"),
        (SF.btrim("cola", "chars"), "TRIM(chars FROM cola)"),
        (SF.btrim("cola", SF.lit("chars")), "TRIM('chars' FROM cola)"),
    ],
)
def test_btrim(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bucket(1, "cola"), "BUCKET(1, cola)"),
        (SF.bucket(SF.col("bucket_col"), SF.col("cola")), "BUCKET(bucket_col, cola)"),
    ],
)
def test_bucket(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.call_function("blah", "cola"), "BLAH(cola)"),
        (SF.call_function("blah", SF.col("cola")), "BLAH(cola)"),
        (SF.call_function("blah", "cola", "colb"), "BLAH(cola, colb)"),
        (SF.call_function("blah"), "BLAH()"),
    ],
)
def test_call_function(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.cardinality("cola"), "CARDINALITY(cola)"),
        (SF.cardinality(SF.col("cola")), "CARDINALITY(cola)"),
    ],
)
def test_cardinality(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.char("cola"), "CHR(cola)"),
        (SF.char(SF.col("cola")), "CHR(cola)"),
    ],
)
def test_char(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.char_length("cola"), "LENGTH(cola)"),
        (SF.char_length(SF.col("cola")), "LENGTH(cola)"),
    ],
)
def test_char_length(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.character_length("cola"), "LENGTH(cola)"),
        (SF.character_length(SF.col("cola")), "LENGTH(cola)"),
    ],
)
def test_character_length(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.contains("cola", "colb"), "CONTAINS(cola, colb)"),
        (SF.contains(SF.col("cola"), SF.col("colb")), "CONTAINS(cola, colb)"),
    ],
)
def test_contains(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.convert_timezone(None, SF.col("cola"), "colb"),
            "CONVERT_TIMEZONE(cola, CAST(colb AS TIMESTAMP_LTZ))",
        ),
        (
            SF.convert_timezone(None, SF.col("cola"), SF.col("colb")),
            "CONVERT_TIMEZONE(cola, CAST(colb AS TIMESTAMP_LTZ))",
        ),
        (
            SF.convert_timezone(SF.col("colc"), SF.col("cola"), "colb"),
            "CONVERT_TIMEZONE(colc, cola, CAST(colb AS TIMESTAMP_LTZ))",
        ),
    ],
)
def test_convert_timezone(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.count_if("cola"), "COUNT_IF(cola)"),
        (SF.count_if(SF.col("cola")), "COUNT_IF(cola)"),
    ],
)
def test_count_if(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.count_min_sketch("cola", "colb", "colc", "cold"),
            "COUNT_MIN_SKETCH(cola, CAST(colb AS DOUBLE), CAST(colc AS DOUBLE), cold)",
        ),
        (
            SF.count_min_sketch(SF.col("cola"), SF.col("colb"), SF.col("colc"), SF.col("cold")),
            "COUNT_MIN_SKETCH(cola, CAST(colb AS DOUBLE), CAST(colc AS DOUBLE), cold)",
        ),
    ],
)
def test_count_min_sketch(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.curdate(), "CURDATE()"),
    ],
)
def test_curdate(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.current_catalog(), "CURRENT_CATALOG()"),
    ],
)
def test_current_catalog(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.current_database(), "CURRENT_DATABASE()"),
    ],
)
def test_current_database(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.current_schema(), "CURRENT_DATABASE()"),
    ],
)
def test_current_schema(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.current_timezone(), "CURRENT_TIMEZONE()"),
    ],
)
def test_current_timezone(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.current_user(), "CURRENT_USER()"),
    ],
)
def test_current_user(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.date_from_unix_date("cola"), "DATE_FROM_UNIX_DATE(cola)"),
        (SF.date_from_unix_date(SF.col("cola")), "DATE_FROM_UNIX_DATE(cola)"),
    ],
)
def test_date_from_unix_date(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.date_part("cola", "colb"), "DATE_PART(cola, colb)"),
        (SF.date_part(SF.col("cola"), SF.col("colb")), "DATE_PART(cola, colb)"),
    ],
)
def test_date_part(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.dateadd("cola", 1), "CAST(DATE_ADD(CAST(cola AS DATE), 1) AS DATE)"),
        (
            SF.dateadd(SF.col("cola"), SF.col("colb")),
            "CAST(DATE_ADD(CAST(cola AS DATE), colb) AS DATE)",
        ),
    ],
)
def test_dateadd(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.datediff("cola", "colb"), "DATEDIFF(CAST(cola AS DATE), CAST(colb AS DATE))"),
        (
            SF.datediff(SF.col("cola"), SF.col("colb")),
            "DATEDIFF(CAST(cola AS DATE), CAST(colb AS DATE))",
        ),
    ],
)
def test_datediff(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.datepart("cola", "colb"), "DATEPART(cola, colb)"),
        (SF.datepart(SF.col("cola"), SF.col("colb")), "DATEPART(cola, colb)"),
    ],
)
def test_datepart(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.day("cola"), "DAY(cola)"),
        (SF.day(SF.col("cola")), "DAY(cola)"),
    ],
)
def test_day(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.days("cola"), "DAYS(cola)"),
        (SF.days(SF.col("cola")), "DAYS(cola)"),
    ],
)
def test_days(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.elt("cola"), "ELT(cola)"),
        (SF.elt(SF.col("cola")), "ELT(cola)"),
        (SF.elt("cola", "colb", "colc"), "ELT(cola, colb, colc)"),
    ],
)
def test_elt(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.endswith("cola", "colb"), "ENDSWITH(cola, colb)"),
        (SF.endswith(SF.col("cola"), SF.col("colb")), "ENDSWITH(cola, colb)"),
    ],
)
def test_endswith(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.equal_null("cola", "colb"), "EQUAL_NULL(cola, colb)"),
        (SF.equal_null(SF.col("cola"), SF.col("colb")), "EQUAL_NULL(cola, colb)"),
    ],
)
def test_equal_null(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.every("cola"), "EVERY(cola)"),
        (SF.every(SF.col("cola")), "EVERY(cola)"),
    ],
)
def test_every(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.extract("cola", "colb"), "EXTRACT(cola FROM colb)"),
        (SF.extract(SF.col("cola"), SF.col("colb")), "EXTRACT(cola FROM colb)"),
    ],
)
def test_extract(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.find_in_set("cola", "colb"), "FIND_IN_SET(cola, colb)"),
        (SF.find_in_set(SF.col("cola"), SF.col("colb")), "FIND_IN_SET(cola, colb)"),
    ],
)
def test_find_in_set(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.first_value("cola"), "FIRST_VALUE(cola)"),
        (SF.first_value(SF.col("cola")), "FIRST_VALUE(cola)"),
        (SF.first_value("cola", True), "FIRST_VALUE(cola) IGNORE NULLS"),
        (SF.first_value("cola", False), "FIRST_VALUE(cola)"),
    ],
)
def test_first_value(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.get("cola", "colb"), "GET(cola, colb)"),
        (SF.get(SF.col("cola"), SF.col("colb")), "GET(cola, colb)"),
        (SF.get("cola", 1), "GET(cola, 1)"),
    ],
)
def test_get(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.grouping("cola"), "GROUPING(cola)"),
        (SF.grouping(SF.col("cola")), "GROUPING(cola)"),
    ],
)
def test_grouping(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.histogram_numeric("cola", "colb"), "HISTOGRAM_NUMERIC(cola, colb)"),
        (SF.histogram_numeric(SF.col("cola"), SF.col("colb")), "HISTOGRAM_NUMERIC(cola, colb)"),
    ],
)
def test_histogram_numeric(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.hll_sketch_agg("cola"), "HLL_SKETCH_AGG(cola)"),
        (SF.hll_sketch_agg(SF.col("cola")), "HLL_SKETCH_AGG(cola)"),
        (SF.hll_sketch_agg("cola", 12), "HLL_SKETCH_AGG(cola, 12)"),
        (SF.hll_sketch_agg("cola", SF.lit(12)), "HLL_SKETCH_AGG(cola, 12)"),
    ],
)
def test_hll_sketch_agg(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.hll_sketch_estimate("cola"), "HLL_SKETCH_ESTIMATE(cola)"),
        (SF.hll_sketch_estimate(SF.col("cola")), "HLL_SKETCH_ESTIMATE(cola)"),
    ],
)
def test_hll_sketch_estimate(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.hll_union("cola", "colb"), "HLL_UNION(cola, colb)"),
        (SF.hll_union(SF.col("cola"), SF.col("colb")), "HLL_UNION(cola, colb)"),
        (SF.hll_union("cola", "colb", False), "HLL_UNION(cola, colb, FALSE)"),
    ],
)
def test_hll_union(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.hll_union_agg("cola"), "HLL_UNION_AGG(cola)"),
        (SF.hll_union_agg(SF.col("cola")), "HLL_UNION_AGG(cola)"),
        (SF.hll_union_agg("cola", SF.lit(True)), "HLL_UNION_AGG(cola, TRUE)"),
        (SF.hll_union_agg("cola", False), "HLL_UNION_AGG(cola, FALSE)"),
    ],
)
def test_hll_union_agg(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.hours("cola"), "HOURS(cola)"),
        (SF.hours(SF.col("cola")), "HOURS(cola)"),
    ],
)
def test_hours(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.ifnull("cola", "colb"), "COALESCE(cola, colb)"),
        (SF.ifnull(SF.col("cola"), SF.col("colb")), "COALESCE(cola, colb)"),
    ],
)
def test_ifnull(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.ilike("cola", "colb"), "cola ILIKE colb"),
        (SF.ilike(SF.col("cola"), SF.col("colb")), "cola ILIKE colb"),
        (SF.ilike("cola", "colb", SF.col("colc")), "cola ILIKE colb ESCAPE colc"),
    ],
)
def test_ilike(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.inline("cola"), "INLINE(cola)"),
        (SF.inline(SF.col("cola")), "INLINE(cola)"),
    ],
)
def test_inline(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.inline_outer("cola"), "INLINE_OUTER(cola)"),
        (SF.inline_outer(SF.col("cola")), "INLINE_OUTER(cola)"),
    ],
)
def test_inline_outer(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.isnotnull("cola"), "ISNOTNULL(cola)"),
        (SF.isnotnull(SF.col("cola")), "ISNOTNULL(cola)"),
    ],
)
def test_isnotnull(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.java_method("cola"), "JAVA_METHOD(cola)"),
        (SF.java_method(SF.col("cola")), "JAVA_METHOD(cola)"),
        (SF.java_method("cola", "colb"), "JAVA_METHOD(cola, colb)"),
        (SF.java_method("cola", "colb", "colc"), "JAVA_METHOD(cola, colb, colc)"),
    ],
)
def test_java_method(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.json_array_length("cola"), "JSON_ARRAY_LENGTH(cola)"),
        (SF.json_array_length(SF.col("cola")), "JSON_ARRAY_LENGTH(cola)"),
    ],
)
def test_json_array_length(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.json_object_keys("cola"), "JSON_OBJECT_KEYS(cola)"),
        (SF.json_object_keys(SF.col("cola")), "JSON_OBJECT_KEYS(cola)"),
    ],
)
def test_json_object_keys(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.last_value("cola"), "LAST_VALUE(cola)"),
        (SF.last_value(SF.col("cola")), "LAST_VALUE(cola)"),
        (SF.last_value("cola", True), "LAST_VALUE(cola) IGNORE NULLS"),
        (SF.last_value("cola", False), "LAST_VALUE(cola)"),
    ],
)
def test_last_value(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.lcase("cola"), "LOWER(cola)"),
        (SF.lcase(SF.col("cola")), "LOWER(cola)"),
    ],
)
def test_lcase(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.left("cola", "colb"), "LEFT(cola, colb)"),
        (SF.left(SF.col("cola"), SF.col("colb")), "LEFT(cola, colb)"),
    ],
)
def test_left(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.like("cola", "colb"), "cola LIKE colb"),
        (SF.like(SF.col("cola"), SF.col("colb")), "cola LIKE colb"),
        (SF.like("cola", "colb", SF.col("colc")), "cola LIKE colb ESCAPE colc"),
    ],
)
def test_like(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.ln("cola"), "LN(cola)"),
        (SF.ln(SF.col("cola")), "LN(cola)"),
    ],
)
def test_ln(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.localtimestamp(), "LOCALTIMESTAMP()"),
        (SF.localtimestamp(), "LOCALTIMESTAMP()"),
    ],
)
def test_localtimestamp(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.make_dt_interval(), "MAKE_DT_INTERVAL(0, 0, 0, 0)"),
        (SF.make_dt_interval(SF.col("cola")), "MAKE_DT_INTERVAL(cola, 0, 0, 0)"),
        (SF.make_dt_interval(SF.col("cola"), SF.col("colb")), "MAKE_DT_INTERVAL(cola, colb, 0, 0)"),
        (
            SF.make_dt_interval(SF.col("cola"), SF.col("colb"), SF.col("colc")),
            "MAKE_DT_INTERVAL(cola, colb, colc, 0)",
        ),
        (
            SF.make_dt_interval(SF.col("cola"), SF.col("colb"), SF.col("colc"), SF.col("cold")),
            "MAKE_DT_INTERVAL(cola, colb, colc, cold)",
        ),
    ],
)
def test_make_dt_interval(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.make_timestamp("cola", "colb", "colc", "cold", "cole", "colf"),
            "MAKE_TIMESTAMP(cola, colb, colc, cold, cole, colf)",
        ),
        (
            SF.make_timestamp(
                SF.col("cola"),
                SF.col("colb"),
                SF.col("colc"),
                SF.col("cold"),
                SF.col("cole"),
                SF.col("colf"),
            ),
            "MAKE_TIMESTAMP(cola, colb, colc, cold, cole, colf)",
        ),
        (
            SF.make_timestamp("cola", "colb", "colc", "cold", "cole", "colf", "colg"),
            "MAKE_TIMESTAMP(cola, colb, colc, cold, cole, colf, colg)",
        ),
    ],
)
def test_make_timestamp(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.make_timestamp_ltz("cola", "colb", "colc", "cold", "cole", "colf"),
            "MAKE_TIMESTAMP_LTZ(cola, colb, colc, cold, cole, colf)",
        ),
        (
            SF.make_timestamp_ltz(
                SF.col("cola"),
                SF.col("colb"),
                SF.col("colc"),
                SF.col("cold"),
                SF.col("cole"),
                SF.col("colf"),
            ),
            "MAKE_TIMESTAMP_LTZ(cola, colb, colc, cold, cole, colf)",
        ),
        (
            SF.make_timestamp_ltz("cola", "colb", "colc", "cold", "cole", "colf", "colg"),
            "MAKE_TIMESTAMP_LTZ(cola, colb, colc, cold, cole, colf, colg)",
        ),
    ],
)
def test_make_timestamp_ltz(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.make_timestamp_ntz("cola", "colb", "colc", "cold", "cole", "colf"),
            "MAKE_TIMESTAMP_NTZ(cola, colb, colc, cold, cole, colf)",
        ),
        (
            SF.make_timestamp_ntz(
                SF.col("cola"),
                SF.col("colb"),
                SF.col("colc"),
                SF.col("cold"),
                SF.col("cole"),
                SF.col("colf"),
            ),
            "MAKE_TIMESTAMP_NTZ(cola, colb, colc, cold, cole, colf)",
        ),
    ],
)
def test_make_timestamp_ntz(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.make_ym_interval("cola", "colb"), "MAKE_YM_INTERVAL(cola, colb)"),
        (SF.make_ym_interval(SF.col("cola"), SF.col("colb")), "MAKE_YM_INTERVAL(cola, colb)"),
        (SF.make_ym_interval("cola"), "MAKE_YM_INTERVAL(cola, 0)"),
        (SF.make_ym_interval(None, "colb"), "MAKE_YM_INTERVAL(0, colb)"),
        (SF.make_ym_interval(), "MAKE_YM_INTERVAL(0, 0)"),
    ],
)
def test_make_ym_interval(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.map_contains_key("cola", 1), "MAP_CONTAINS_KEY(cola, 1)"),
        (SF.map_contains_key(SF.col("cola"), SF.lit(1)), "MAP_CONTAINS_KEY(cola, 1)"),
    ],
)
def test_map_contains_key(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.mask("cola"), "MASK(cola, 'X', 'x', 'n', NULL)"),
        (SF.mask(SF.col("cola")), "MASK(cola, 'X', 'x', 'n', NULL)"),
        (SF.mask("cola", "colb"), "MASK(cola, colb, 'x', 'n', NULL)"),
        (SF.mask("cola", "colb", "colc"), "MASK(cola, colb, colc, 'n', NULL)"),
        (SF.mask("cola", "colb", "colc", "cold"), "MASK(cola, colb, colc, cold, NULL)"),
        (SF.mask("cola", "colb", "colc", "cold", "cole"), "MASK(cola, colb, colc, cold, cole)"),
        (SF.mask("cola", None, "colb"), "MASK(cola, 'X', colb, 'n', NULL)"),
    ],
)
def test_mask(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.median("cola"), "MEDIAN(cola)"),
        (SF.median(SF.col("cola")), "MEDIAN(cola)"),
    ],
)
def test_median(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.mode("cola"), "MODE(cola)"),
        (SF.mode(SF.col("cola")), "MODE(cola)"),
    ],
)
def test_mode(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.months("cola"), "MONTHS(cola)"),
        (SF.months(SF.col("cola")), "MONTHS(cola)"),
    ],
)
def test_months(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.named_struct("cola"), "NAMED_STRUCT(cola)"),
        (SF.named_struct(SF.col("cola")), "NAMED_STRUCT(cola)"),
        (SF.named_struct("cola", "colb"), "NAMED_STRUCT(cola, colb)"),
    ],
)
def test_named_struct(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.negate("cola"), "NEGATIVE(cola)"),
        (SF.negate(SF.col("cola")), "NEGATIVE(cola)"),
    ],
)
def test_negate(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.negative("cola"), "NEGATIVE(cola)"),
        (SF.negative(SF.col("cola")), "NEGATIVE(cola)"),
    ],
)
def test_negative(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.now(), "CURRENT_TIMESTAMP()"),
        (SF.now(), "CURRENT_TIMESTAMP()"),
    ],
)
def test_now(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.nvl("cola", "colb"), "COALESCE(cola, colb)"),
        (SF.nvl(SF.col("cola"), SF.col("colb")), "COALESCE(cola, colb)"),
    ],
)
def test_nvl(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.nvl2("cola", "colb", "colc"), "NVL2(cola, colb, colc)"),
        (SF.nvl2(SF.col("cola"), SF.col("colb"), SF.col("colc")), "NVL2(cola, colb, colc)"),
    ],
)
def test_nvl2(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.parse_url("cola", "colb"), "PARSE_URL(cola, colb)"),
        (SF.parse_url(SF.col("cola"), SF.col("colb")), "PARSE_URL(cola, colb)"),
        (SF.parse_url("cola", "colb", "colc"), "PARSE_URL(cola, colb, colc)"),
    ],
)
def test_parse_url(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.pi(), "PI()"),
    ],
)
def test_pi(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.pmod("cola", "colb"), "PMOD(cola, colb)"),
        (SF.pmod(SF.col("cola"), SF.col("colb")), "PMOD(cola, colb)"),
        (SF.pmod("cola", 2.0), "PMOD(cola, 2.0)"),
        (SF.pmod(1.0, "colb"), "PMOD(1.0, colb)"),
    ],
)
def test_pmod(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.position("cola", "colb"), "LOCATE(cola, colb)"),
        (SF.position(SF.col("cola"), SF.col("colb")), "LOCATE(cola, colb)"),
        (SF.position("cola", "colb", "colc"), "LOCATE(cola, colb, colc)"),
    ],
)
def test_position(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.positive("cola"), "POSITIVE(cola)"),
        (SF.positive(SF.col("cola")), "POSITIVE(cola)"),
    ],
)
def test_positive(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.printf("cola", "colb"), "PRINTF(cola, colb)"),
        (SF.printf(SF.col("cola"), SF.col("colb")), "PRINTF(cola, colb)"),
        (SF.printf("cola", "colb", "colc"), "PRINTF(cola, colb, colc)"),
    ],
)
def test_printf(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.product("cola"), "PRODUCT(cola)"),
        (SF.product(SF.col("cola")), "PRODUCT(cola)"),
    ],
)
def test_product(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.reduce("cola", "colb", lambda acc, x: acc + x),
            "AGGREGATE(cola, colb, (acc, x) -> (acc + x))",
        ),
        (
            SF.reduce(SF.col("cola"), SF.col("colb"), lambda acc, x: acc + x),
            "AGGREGATE(cola, colb, (acc, x) -> (acc + x))",
        ),
        (
            SF.reduce("cola", "colb", lambda acc, x: acc + x, lambda acc: acc + 1),
            "AGGREGATE(cola, colb, (acc, x) -> (acc + x), acc -> (acc + 1))",
        ),
    ],
)
def test_reduce(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.reflect("cola"), "REFLECT(cola)"),
        (SF.reflect(SF.col("cola")), "REFLECT(cola)"),
        (SF.reflect("cola", "colb", "colc"), "REFLECT(cola, colb, colc)"),
    ],
)
def test_reflect(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.regexp("cola", "colb"), "REGEXP(cola, colb)"),
        (SF.regexp(SF.col("cola"), SF.col("colb")), "REGEXP(cola, colb)"),
    ],
)
def test_regexp(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.regexp_count("cola", "colb"), "REGEXP_COUNT(cola, colb)"),
        (SF.regexp_count(SF.col("cola"), SF.col("colb")), "REGEXP_COUNT(cola, colb)"),
    ],
)
def test_regexp_count(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.regexp_extract_all("cola", "colb"), "REGEXP_EXTRACT_ALL(cola, colb)"),
        (SF.regexp_extract_all(SF.col("cola"), SF.col("colb")), "REGEXP_EXTRACT_ALL(cola, colb)"),
        (
            SF.regexp_extract_all("cola", "colb", SF.col("colc")),
            "REGEXP_EXTRACT_ALL(cola, colb, colc)",
        ),
        (SF.regexp_extract_all("cola", "colb", 2), "REGEXP_EXTRACT_ALL(cola, colb, 2)"),
    ],
)
def test_regexp_extract_all(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.regexp_instr("cola", "colb"), "REGEXP_INSTR(cola, colb)"),
        (SF.regexp_instr(SF.col("cola"), SF.col("colb")), "REGEXP_INSTR(cola, colb)"),
        (SF.regexp_instr("cola", "colb", SF.col("colc")), "REGEXP_INSTR(cola, colb, colc)"),
        (SF.regexp_instr("cola", "colb", 1), "REGEXP_INSTR(cola, colb, 1)"),
    ],
)
def test_regexp_instr(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.regexp_like("cola", "colb"), "cola RLIKE colb"),
        (SF.regexp_like(SF.col("cola"), SF.col("colb")), "cola RLIKE colb"),
    ],
)
def test_regexp_like(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.regexp_substr("cola", "colb"), "REGEXP_SUBSTR(cola, colb)"),
        (SF.regexp_substr(SF.col("cola"), SF.col("colb")), "REGEXP_SUBSTR(cola, colb)"),
    ],
)
def test_regexp_substr(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.regr_avgx("cola", "colb"), "REGR_AVGX(cola, colb)"),
        (SF.regr_avgx(SF.col("cola"), SF.col("colb")), "REGR_AVGX(cola, colb)"),
    ],
)
def test_regr_avgx(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.regr_avgy("cola", "colb"), "REGR_AVGY(cola, colb)"),
        (SF.regr_avgy(SF.col("cola"), SF.col("colb")), "REGR_AVGY(cola, colb)"),
    ],
)
def test_regr_avgy(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.regr_count("cola", "colb"), "REGR_COUNT(cola, colb)"),
        (SF.regr_count(SF.col("cola"), SF.col("colb")), "REGR_COUNT(cola, colb)"),
    ],
)
def test_regr_count(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.regr_intercept("cola", "colb"), "REGR_INTERCEPT(cola, colb)"),
        (SF.regr_intercept(SF.col("cola"), SF.col("colb")), "REGR_INTERCEPT(cola, colb)"),
    ],
)
def test_regr_intercept(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.regr_r2("cola", "colb"), "REGR_R2(cola, colb)"),
        (SF.regr_r2(SF.col("cola"), SF.col("colb")), "REGR_R2(cola, colb)"),
    ],
)
def test_regr_r2(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.regr_slope("cola", "colb"), "REGR_SLOPE(cola, colb)"),
        (SF.regr_slope(SF.col("cola"), SF.col("colb")), "REGR_SLOPE(cola, colb)"),
    ],
)
def test_regr_slope(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.regr_sxx("cola", "colb"), "REGR_SXX(cola, colb)"),
        (SF.regr_sxx(SF.col("cola"), SF.col("colb")), "REGR_SXX(cola, colb)"),
    ],
)
def test_regr_sxx(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.regr_sxy("cola", "colb"), "REGR_SXY(cola, colb)"),
        (SF.regr_sxy(SF.col("cola"), SF.col("colb")), "REGR_SXY(cola, colb)"),
    ],
)
def test_regr_sxy(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.regr_syy("cola", "colb"), "REGR_SYY(cola, colb)"),
        (SF.regr_syy(SF.col("cola"), SF.col("colb")), "REGR_SYY(cola, colb)"),
    ],
)
def test_regr_syy(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.replace("cola", "colb"), "REPLACE(cola, colb)"),
        (SF.replace(SF.col("cola"), SF.col("colb")), "REPLACE(cola, colb)"),
        (SF.replace("cola", "colb", "colc"), "REPLACE(cola, colb, colc)"),
    ],
)
def test_replace(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.right("cola", "colb"), "RIGHT(cola, colb)"),
        (SF.right(SF.col("cola"), SF.col("colb")), "RIGHT(cola, colb)"),
    ],
)
def test_right(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.rlike("cola", "colb"), "cola RLIKE colb"),
        (SF.rlike(SF.col("cola"), SF.col("colb")), "cola RLIKE colb"),
    ],
)
def test_rlike(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sha("cola"), "SHA(cola)"),
        (SF.sha(SF.col("cola")), "SHA(cola)"),
    ],
)
def test_sha(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sign("cola"), "SIGN(cola)"),
        (SF.sign(SF.col("cola")), "SIGN(cola)"),
    ],
)
def test_sign(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.some("cola"), "SOME(cola)"),
        (SF.some(SF.col("cola")), "SOME(cola)"),
    ],
)
def test_some(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.spark_partition_id(), "SPARK_PARTITION_ID()"),
    ],
)
def test_spark_partition_id(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.split_part("cola", "colb", "colc"), "SPLIT_PART(cola, colb, colc)"),
        (
            SF.split_part(SF.col("cola"), SF.col("colb"), SF.col("colc")),
            "SPLIT_PART(cola, colb, colc)",
        ),
    ],
)
def test_split_part(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.startswith("cola", "colb"), "STARTSWITH(cola, colb)"),
        (SF.startswith(SF.col("cola"), SF.col("colb")), "STARTSWITH(cola, colb)"),
    ],
)
def test_startswith(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.std("cola"), "STD(cola)"),
        (SF.std(SF.col("cola")), "STD(cola)"),
    ],
)
def test_std(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.str_to_map("cola"), "STR_TO_MAP(cola, ',', ':')"),
        (SF.str_to_map(SF.col("cola")), "STR_TO_MAP(cola, ',', ':')"),
        (SF.str_to_map("cola", "colb"), "STR_TO_MAP(cola, colb, ':')"),
        (SF.str_to_map("cola", "colb", "colc"), "STR_TO_MAP(cola, colb, colc)"),
        (SF.str_to_map("cola", None, "colc"), "STR_TO_MAP(cola, ',', colc)"),
    ],
)
def test_str_to_map(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.substr("cola", "colb"), "SUBSTRING(cola, colb)"),
        (SF.substr(SF.col("cola"), SF.col("colb")), "SUBSTRING(cola, colb)"),
        (SF.substr("cola", "colb", "colc"), "SUBSTRING(cola, colb, colc)"),
    ],
)
def test_substr(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.timestamp_micros("cola"), "TIMESTAMP_MICROS(cola)"),
        (SF.timestamp_micros(SF.col("cola")), "TIMESTAMP_MICROS(cola)"),
    ],
)
def test_timestamp_micros(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.timestamp_millis("cola"), "TIMESTAMP_MILLIS(cola)"),
        (SF.timestamp_millis(SF.col("cola")), "TIMESTAMP_MILLIS(cola)"),
    ],
)
def test_timestamp_millis(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.to_char("cola", "colb"), "TO_CHAR(cola, colb)"),
        (SF.to_char(SF.col("cola"), SF.col("colb")), "TO_CHAR(cola, colb)"),
    ],
)
def test_to_char(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.to_number("cola", "colb"), "TO_NUMBER(cola, colb)"),
        (SF.to_number(SF.col("cola"), SF.col("colb")), "TO_NUMBER(cola, colb)"),
    ],
)
def test_to_number(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "input, output",
    [
        (SF.to_str(1), "1"),
        (SF.to_str(False), "false"),
        (SF.to_str(None), None),
    ],
)
def test_to_str(input, output):
    assert input == output


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.to_timestamp_ltz("cola"), "TO_TIMESTAMP_LTZ(cola)"),
        (SF.to_timestamp_ltz(SF.col("cola")), "TO_TIMESTAMP_LTZ(cola)"),
        (SF.to_timestamp_ltz("cola", "colb"), "TO_TIMESTAMP_LTZ(cola, colb)"),
    ],
)
def test_to_timestamp_ltz(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.to_timestamp_ntz("cola"), "TO_TIMESTAMP_NTZ(cola)"),
        (SF.to_timestamp_ntz(SF.col("cola")), "TO_TIMESTAMP_NTZ(cola)"),
        (SF.to_timestamp_ntz("cola", "colb"), "TO_TIMESTAMP_NTZ(cola, colb)"),
    ],
)
def test_to_timestamp_ntz(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.to_unix_timestamp("cola"), "UNIX_TIMESTAMP(cola)"),
        (SF.to_unix_timestamp(SF.col("cola")), "UNIX_TIMESTAMP(cola)"),
    ],
)
def test_to_unix_timestamp(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.to_varchar("cola", "colb"), "TO_VARCHAR(cola, colb)"),
        (SF.to_varchar(SF.col("cola"), SF.col("colb")), "TO_VARCHAR(cola, colb)"),
    ],
)
def test_to_varchar(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.try_aes_decrypt("cola", "colb"), "TRY_AES_DECRYPT(cola, colb, 'GCM', 'DEFAULT', '')"),
        (
            SF.try_aes_decrypt(SF.col("cola"), SF.col("colb")),
            "TRY_AES_DECRYPT(cola, colb, 'GCM', 'DEFAULT', '')",
        ),
        (
            SF.try_aes_decrypt("cola", "colb", "colc"),
            "TRY_AES_DECRYPT(cola, colb, colc, 'DEFAULT', '')",
        ),
        (
            SF.try_aes_decrypt("cola", "colb", "colc", "cold"),
            "TRY_AES_DECRYPT(cola, colb, colc, cold, '')",
        ),
        (
            SF.try_aes_decrypt("cola", "colb", "colc", "cold", "cole"),
            "TRY_AES_DECRYPT(cola, colb, colc, cold, cole)",
        ),
        (
            SF.try_aes_decrypt("cola", "colb", "colc", None, "cole"),
            "TRY_AES_DECRYPT(cola, colb, colc, 'DEFAULT', cole)",
        ),
    ],
)
def test_try_aes_decrypt(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.try_element_at("cola", "colb"), "TRY_ELEMENT_AT(cola, colb)"),
        (SF.try_element_at(SF.col("cola"), SF.col("colb")), "TRY_ELEMENT_AT(cola, colb)"),
    ],
)
def test_try_element_at(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.try_to_timestamp("cola"), "TRY_TO_TIMESTAMP(cola, 'yyyy-MM-dd HH:mm:ss')"),
        (SF.try_to_timestamp(SF.col("cola")), "TRY_TO_TIMESTAMP(cola, 'yyyy-MM-dd HH:mm:ss')"),
        (SF.try_to_timestamp("cola", "blah"), "TRY_TO_TIMESTAMP(cola, 'blah')"),
    ],
)
def test_try_to_timestamp(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.ucase("cola"), "UPPER(cola)"),
        (SF.ucase(SF.col("cola")), "UPPER(cola)"),
    ],
)
def test_ucase(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.unix_date("cola"), "DATEDIFF(DAY, CAST('1970-01-01' AS DATE), cola)"),
        (SF.unix_date(SF.col("cola")), "DATEDIFF(DAY, CAST('1970-01-01' AS DATE), cola)"),
    ],
)
def test_unix_date(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.unix_micros("cola"), "UNIX_MICROS(cola)"),
        (SF.unix_micros(SF.col("cola")), "UNIX_MICROS(cola)"),
    ],
)
def test_unix_micros(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.unix_millis("cola"), "UNIX_MILLIS(cola)"),
        (SF.unix_millis(SF.col("cola")), "UNIX_MILLIS(cola)"),
    ],
)
def test_unix_millis(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.unix_seconds("cola"), "UNIX_SECONDS(cola)"),
        (SF.unix_seconds(SF.col("cola")), "UNIX_SECONDS(cola)"),
    ],
)
def test_unix_seconds(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.url_decode("cola"), "URL_DECODE(cola)"),
        (SF.url_decode(SF.col("cola")), "URL_DECODE(cola)"),
    ],
)
def test_url_decode(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.url_encode("cola"), "URL_ENCODE(cola)"),
        (SF.url_encode(SF.col("cola")), "URL_ENCODE(cola)"),
    ],
)
def test_url_encode(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.version(), "VERSION()"),
    ],
)
def test_version(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.weekday("cola"), "WEEKDAY(cola)"),
        (SF.weekday(SF.col("cola")), "WEEKDAY(cola)"),
    ],
)
def test_weekday(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.width_bucket("cola", "colb", "colc", "cold"), "WIDTH_BUCKET(cola, colb, colc, cold)"),
        (
            SF.width_bucket(SF.col("cola"), SF.col("colb"), SF.col("colc"), SF.col("cold")),
            "WIDTH_BUCKET(cola, colb, colc, cold)",
        ),
        (
            SF.width_bucket("cola", "colb", "colc", 1),
            "WIDTH_BUCKET(cola, colb, colc, 1)",
        ),
    ],
)
def test_width_bucket(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.window_time("cola"), "WINDOW_TIME(cola)"),
        (SF.window_time(SF.col("cola")), "WINDOW_TIME(cola)"),
    ],
)
def test_window_time(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.xpath("cola", "colb"), "XPATH(cola, colb)"),
        (SF.xpath(SF.col("cola"), SF.col("colb")), "XPATH(cola, colb)"),
    ],
)
def test_xpath(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.xpath_boolean("cola", "colb"), "XPATH_BOOLEAN(cola, colb)"),
        (SF.xpath_boolean(SF.col("cola"), SF.col("colb")), "XPATH_BOOLEAN(cola, colb)"),
    ],
)
def test_xpath_boolean(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.xpath_double("cola", "colb"), "XPATH_DOUBLE(cola, colb)"),
        (SF.xpath_double(SF.col("cola"), SF.col("colb")), "XPATH_DOUBLE(cola, colb)"),
    ],
)
def test_xpath_double(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.xpath_float("cola", "colb"), "XPATH_FLOAT(cola, colb)"),
        (SF.xpath_float(SF.col("cola"), SF.col("colb")), "XPATH_FLOAT(cola, colb)"),
    ],
)
def test_xpath_float(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.xpath_int("cola", "colb"), "XPATH_INT(cola, colb)"),
        (SF.xpath_int(SF.col("cola"), SF.col("colb")), "XPATH_INT(cola, colb)"),
    ],
)
def test_xpath_int(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.xpath_long("cola", "colb"), "XPATH_LONG(cola, colb)"),
        (SF.xpath_long(SF.col("cola"), SF.col("colb")), "XPATH_LONG(cola, colb)"),
    ],
)
def test_xpath_long(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.xpath_number("cola", "colb"), "XPATH_NUMBER(cola, colb)"),
        (SF.xpath_number(SF.col("cola"), SF.col("colb")), "XPATH_NUMBER(cola, colb)"),
    ],
)
def test_xpath_number(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.xpath_short("cola", "colb"), "XPATH_SHORT(cola, colb)"),
        (SF.xpath_short(SF.col("cola"), SF.col("colb")), "XPATH_SHORT(cola, colb)"),
    ],
)
def test_xpath_short(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.xpath_string("cola", "colb"), "XPATH_STRING(cola, colb)"),
        (SF.xpath_string(SF.col("cola"), SF.col("colb")), "XPATH_STRING(cola, colb)"),
    ],
)
def test_xpath_string(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.years("cola"), "YEARS(cola)"),
        (SF.years(SF.col("cola")), "YEARS(cola)"),
    ],
)
def test_years(expression, expected):
    assert expression.column_expression.sql(dialect="spark") == expected
