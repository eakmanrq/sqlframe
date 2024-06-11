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
    if "invoke_anonymous_function" in inspect.getsource(func):
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
    ],
)
def test_lit(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.col("cola").alias("colb"), "cola AS colb"),
        # SQLGlot Spark normalizer when we create a Column object will lowercase the alias value
        (SF.col("cola").alias("A Space in Name"), "cola AS `a space in name`"),
    ],
)
def test_alias(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.desc("cola"), "cola DESC"),
        (SF.desc(SF.col("cola")), "cola DESC"),
    ],
)
def test_sqrt(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.abs("cola"), "ABS(cola)"),
        (SF.abs(SF.col("cola")), "ABS(cola)"),
    ],
)
def test_abs(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.max("cola"), "MAX(cola)"),
        (SF.max(SF.col("cola")), "MAX(cola)"),
    ],
)
def test_max(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.min("cola"), "MIN(cola)"),
        (SF.min(SF.col("cola")), "MIN(cola)"),
    ],
)
def test_min(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.max_by("cola", "colb"), "MAX_BY(cola, colb)"),
        (SF.max_by(SF.col("cola"), SF.col("colb")), "MAX_BY(cola, colb)"),
    ],
)
def test_max_by(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.min_by("cola", "colb"), "MIN_BY(cola, colb)"),
        (SF.min_by(SF.col("cola"), SF.col("colb")), "MIN_BY(cola, colb)"),
    ],
)
def test_min_by(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.count("cola"), "COUNT(cola)"),
        (SF.count(SF.col("cola")), "COUNT(cola)"),
    ],
)
def test_count(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sum("cola"), "SUM(cola)"),
        (SF.sum(SF.col("cola")), "SUM(cola)"),
    ],
)
def test_sum(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.avg("cola"), "AVG(cola)"),
        (SF.avg(SF.col("cola")), "AVG(cola)"),
    ],
)
def test_avg(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.mean("cola"), "AVG(cola)"),
        (SF.mean(SF.col("cola")), "AVG(cola)"),
    ],
)
def test_mean(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sumDistinct("cola"), "SUM(DISTINCT cola)"),
        (SF.sumDistinct(SF.col("cola")), "SUM(DISTINCT cola)"),
    ],
)
def test_sum_distinct(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.acos("cola"), "ACOS(cola)"),
        (SF.acos(SF.col("cola")), "ACOS(cola)"),
    ],
)
def test_acos(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.acosh("cola"), "ACOSH(cola)"),
        (SF.acosh(SF.col("cola")), "ACOSH(cola)"),
    ],
)
def test_acosh(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.asin("cola"), "ASIN(cola)"),
        (SF.asin(SF.col("cola")), "ASIN(cola)"),
    ],
)
def test_asin(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.asinh("cola"), "ASINH(cola)"),
        (SF.asinh(SF.col("cola")), "ASINH(cola)"),
    ],
)
def test_asinh(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.atan("cola"), "ATAN(cola)"),
        (SF.atan(SF.col("cola")), "ATAN(cola)"),
    ],
)
def test_atan(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.atanh("cola"), "ATANH(cola)"),
        (SF.atanh(SF.col("cola")), "ATANH(cola)"),
    ],
)
def test_atanh(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.cbrt("cola"), "CBRT(cola)"),
        (SF.cbrt(SF.col("cola")), "CBRT(cola)"),
    ],
)
def test_cbrt(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.ceil("cola"), "CEIL(cola)"),
        (SF.ceil(SF.col("cola")), "CEIL(cola)"),
    ],
)
def test_ceil(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.cos("cola"), "COS(cola)"),
        (SF.cos(SF.col("cola")), "COS(cola)"),
    ],
)
def test_cos(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.cosh("cola"), "COSH(cola)"),
        (SF.cosh(SF.col("cola")), "COSH(cola)"),
    ],
)
def test_cosh(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.cot("cola"), "COT(cola)"),
        (SF.cot(SF.col("cola")), "COT(cola)"),
    ],
)
def test_cot(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.csc("cola"), "CSC(cola)"),
        (SF.csc(SF.col("cola")), "CSC(cola)"),
    ],
)
def test_csc(expression, expected):
    assert expression.sql() == expected


def test_e():
    assert SF.e().sql() == "E()"


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.exp("cola"), "EXP(cola)"),
        (SF.exp(SF.col("cola")), "EXP(cola)"),
    ],
)
def test_exp(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.expm1("cola"), "EXPM1(cola)"),
        (SF.expm1(SF.col("cola")), "EXPM1(cola)"),
    ],
)
def test_expm1(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.floor("cola"), "FLOOR(cola)"),
        (SF.floor(SF.col("cola")), "FLOOR(cola)"),
    ],
)
def test_floor(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.log("cola"), "LN(cola)"),
        (SF.log(SF.col("cola")), "LN(cola)"),
        (SF.log(10.0, "age"), "LOG(10.0, age)"),
    ],
)
def test_log(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.log10("cola"), "LOG(10, cola)"),
        (SF.log10(SF.col("cola")), "LOG(10, cola)"),
    ],
)
def test_log10(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.log1p("cola"), "LOG1P(cola)"),
        (SF.log1p(SF.col("cola")), "LOG1P(cola)"),
    ],
)
def test_log1p(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.log2("cola"), "LOG(2, cola)"),
        (SF.log2(SF.col("cola")), "LOG(2, cola)"),
    ],
)
def test_log2(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.rint("cola"), "RINT(cola)"),
        (SF.rint(SF.col("cola")), "RINT(cola)"),
    ],
)
def test_rint(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sec("cola"), "SEC(cola)"),
        (SF.sec(SF.col("cola")), "SEC(cola)"),
    ],
)
def test_sec(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.signum("cola"), "SIGN(cola)"),
        (SF.signum(SF.col("cola")), "SIGN(cola)"),
    ],
)
def test_signum(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sin("cola"), "SIN(cola)"),
        (SF.sin(SF.col("cola")), "SIN(cola)"),
    ],
)
def test_sin(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sinh("cola"), "SINH(cola)"),
        (SF.sinh(SF.col("cola")), "SINH(cola)"),
    ],
)
def test_sinh(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.tan("cola"), "TAN(cola)"),
        (SF.tan(SF.col("cola")), "TAN(cola)"),
    ],
)
def test_tan(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.tanh("cola"), "TANH(cola)"),
        (SF.tanh(SF.col("cola")), "TANH(cola)"),
    ],
)
def test_tanh(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.degrees("cola"), "DEGREES(cola)"),
        (SF.degrees(SF.col("cola")), "DEGREES(cola)"),
        (SF.toDegrees(SF.col("cola")), "DEGREES(cola)"),
    ],
)
def test_degrees(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.radians("cola"), "RADIANS(cola)"),
        (SF.radians(SF.col("cola")), "RADIANS(cola)"),
        (SF.toRadians(SF.col("cola")), "RADIANS(cola)"),
    ],
)
def test_radians(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bitwise_not("cola"), "~cola"),
        (SF.bitwise_not(SF.col("cola")), "~cola"),
        (SF.bitwiseNOT(SF.col("cola")), "~cola"),
    ],
)
def test_bitwise_not(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.asc_nulls_first("cola"), "cola ASC"),
        (SF.asc_nulls_first(SF.col("cola")), "cola ASC"),
    ],
)
def test_asc_nulls_first(expression, expected):
    assert isinstance(expression.expression, exp.Ordered)
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.asc_nulls_last("cola"), "cola ASC NULLS LAST"),
        (SF.asc_nulls_last(SF.col("cola")), "cola ASC NULLS LAST"),
    ],
)
def test_asc_nulls_last(expression, expected):
    assert isinstance(expression.expression, exp.Ordered)
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.desc_nulls_first("cola"), "cola DESC NULLS FIRST"),
        (SF.desc_nulls_first(SF.col("cola")), "cola DESC NULLS FIRST"),
    ],
)
def test_desc_nulls_first(expression, expected):
    assert isinstance(expression.expression, exp.Ordered)
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.desc_nulls_first("cola"), "cola DESC NULLS FIRST"),
        (SF.desc_nulls_first(SF.col("cola")), "cola DESC NULLS FIRST"),
    ],
)
def test_desc_nulls_last(expression, expected):
    assert isinstance(expression.expression, exp.Ordered)
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.stddev("cola"), "STDDEV(cola)"),
        (SF.stddev(SF.col("cola")), "STDDEV(cola)"),
    ],
)
def test_stddev(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.stddev_samp("cola"), "STDDEV_SAMP(cola)"),
        (SF.stddev_samp(SF.col("cola")), "STDDEV_SAMP(cola)"),
    ],
)
def test_stddev_samp(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.stddev_pop("cola"), "STDDEV_POP(cola)"),
        (SF.stddev_pop(SF.col("cola")), "STDDEV_POP(cola)"),
    ],
)
def test_stddev_pop(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.variance("cola"), "VARIANCE(cola)"),
        (SF.variance(SF.col("cola")), "VARIANCE(cola)"),
    ],
)
def test_variance(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.var_samp("cola"), "VARIANCE(cola)"),
        (SF.var_samp(SF.col("cola")), "VARIANCE(cola)"),
    ],
)
def test_var_samp(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.var_pop("cola"), "VAR_POP(cola)"),
        (SF.var_pop(SF.col("cola")), "VAR_POP(cola)"),
    ],
)
def test_var_pop(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.skewness("cola"), "SKEWNESS(cola)"),
        (SF.skewness(SF.col("cola")), "SKEWNESS(cola)"),
    ],
)
def test_skewness(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.kurtosis("cola"), "KURTOSIS(cola)"),
        (SF.kurtosis(SF.col("cola")), "KURTOSIS(cola)"),
    ],
)
def test_kurtosis(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.collect_list("cola"), "COLLECT_LIST(cola)"),
        (SF.collect_list(SF.col("cola")), "COLLECT_LIST(cola)"),
    ],
)
def test_collect_list(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.collect_set("cola"), "COLLECT_SET(cola)"),
        (SF.collect_set(SF.col("cola")), "COLLECT_SET(cola)"),
    ],
)
def test_collect_set(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.row_number(), "ROW_NUMBER()"),
        (SF.row_number(), "ROW_NUMBER()"),
    ],
)
def test_row_number(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.dense_rank(), "DENSE_RANK()"),
        (SF.dense_rank(), "DENSE_RANK()"),
    ],
)
def test_dense_rank(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.rank(), "RANK()"),
        (SF.rank(), "RANK()"),
    ],
)
def test_rank(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.cume_dist(), "CUME_DIST()"),
        (SF.cume_dist(), "CUME_DIST()"),
    ],
)
def test_cume_dist(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.percent_rank(), "PERCENT_RANK()"),
        (SF.percent_rank(), "PERCENT_RANK()"),
    ],
)
def test_percent_rank(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.coalesce("cola", "colb", "colc"), "COALESCE(cola, colb, colc)"),
        (SF.coalesce(SF.col("cola"), "colb", SF.col("colc")), "COALESCE(cola, colb, colc)"),
        (SF.coalesce(SF.col("cola")), "COALESCE(cola)"),
    ],
)
def test_coalesce(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.corr("cola", "colb"), "CORR(cola, colb)"),
        (SF.corr(SF.col("cola"), "colb"), "CORR(cola, colb)"),
    ],
)
def test_corr(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.covar_pop("cola", "colb"), "COVAR_POP(cola, colb)"),
        (SF.covar_pop(SF.col("cola"), "colb"), "COVAR_POP(cola, colb)"),
    ],
)
def test_covar_pop(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.covar_samp("cola", "colb"), "COVAR_SAMP(cola, colb)"),
        (SF.covar_samp(SF.col("cola"), "colb"), "COVAR_SAMP(cola, colb)"),
    ],
)
def test_covar_samp(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.first("cola"), "FIRST(cola)"),
        (SF.first(SF.col("cola")), "FIRST(cola)"),
        (SF.first(SF.col("cola"), True), "FIRST(cola) IGNORE NULLS"),
    ],
)
def test_first(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.isnull("cola"), "ISNULL(cola)"),
        (SF.isnull(SF.col("cola")), "ISNULL(cola)"),
    ],
)
def test_isnull(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.last("cola"), "LAST(cola)"),
        (SF.last(SF.col("cola")), "LAST(cola)"),
        (SF.last(SF.col("cola"), True), "LAST(cola) IGNORE NULLS"),
    ],
)
def test_last(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.monotonically_increasing_id(), "MONOTONICALLY_INCREASING_ID()"),
    ],
)
def test_monotonically_increasing_id(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.nanvl("cola", "colb"), "NANVL(cola, colb)"),
        (SF.nanvl(SF.col("cola"), SF.col("colb")), "NANVL(cola, colb)"),
    ],
)
def test_nanvl(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.rand(0), "RAND(0)"),
        (SF.rand(), "RAND()"),
    ],
)
def test_rand(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.randn(0), "RANDN(0)"),
        (SF.randn(), "RANDN()"),
    ],
)
def test_randn(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.round("cola", 0), "ROUND(cola, 0)"),
        (SF.round(SF.col("cola"), 0), "ROUND(cola, 0)"),
        (SF.round("cola"), "ROUND(cola)"),
    ],
)
def test_round(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bround("cola", 0), "BROUND(cola, 0)"),
        (SF.bround(SF.col("cola"), 0), "BROUND(cola, 0)"),
        (SF.bround("cola"), "BROUND(cola)"),
    ],
)
def test_bround(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.shiftleft("cola", 1), "SHIFTLEFT(cola, 1)"),
        (SF.shiftleft(SF.col("cola"), 1), "SHIFTLEFT(cola, 1)"),
        (SF.shiftLeft("cola", 1), "SHIFTLEFT(cola, 1)"),
    ],
)
def test_shiftleft(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.shiftright("cola", 1), "SHIFTRIGHT(cola, 1)"),
        (SF.shiftright(SF.col("cola"), 1), "SHIFTRIGHT(cola, 1)"),
        (SF.shiftRight("cola", 1), "SHIFTRIGHT(cola, 1)"),
    ],
)
def test_shiftright(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.shiftrightunsigned("cola", 1), "SHIFTRIGHTUNSIGNED(CAST(cola AS BIGINT), 1)"),
        (SF.shiftrightunsigned(SF.col("cola"), 1), "SHIFTRIGHTUNSIGNED(CAST(cola AS BIGINT), 1)"),
        (SF.shiftRightUnsigned("cola", 1), "SHIFTRIGHTUNSIGNED(CAST(cola AS BIGINT), 1)"),
    ],
)
def test_shiftrightunsigned(expression, expected):
    assert expression.sql() == expected


def test_expr():
    assert SF.expr("LENGTH(name)").sql() == "LENGTH(name)"


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.struct("cola", "colb", "colc"), "STRUCT(cola, colb, colc)"),
        (SF.struct(SF.col("cola"), SF.col("colb"), SF.col("colc")), "STRUCT(cola, colb, colc)"),
        (SF.struct("cola"), "STRUCT(cola)"),
        (SF.struct(["cola", "colb", "colc"]), "STRUCT(cola, colb, colc)"),
    ],
)
def test_struct(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.least("cola"), "LEAST(cola)"),
        (SF.least(SF.col("cola")), "LEAST(cola)"),
        (SF.least("col1", "col2", SF.col("col3"), SF.col("col4")), "LEAST(col1, col2, col3, col4)"),
    ],
)
def test_least(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.conv("cola", 2, 16), "CONV(cola, 2, 16)"),
        (SF.conv(SF.col("cola"), 2, 16), "CONV(cola, 2, 16)"),
    ],
)
def test_conv(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.factorial("cola"), "FACTORIAL(cola)"),
        (SF.factorial(SF.col("cola")), "FACTORIAL(cola)"),
    ],
)
def test_factorial(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


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
    assert expression.sql() == expected


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
    assert expression.sql() == expected


def test_ntile():
    assert SF.ntile(2).sql() == "NTILE(2)"


def test_current_date():
    assert SF.current_date().sql() == "CURRENT_DATE"


def test_current_timestamp():
    assert SF.current_timestamp().sql() == "CURRENT_TIMESTAMP()"


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.date_format("cola", "MM/dd/yyy"), "DATE_FORMAT(CAST(cola AS TIMESTAMP), 'MM/dd/yyy')"),
        (
            SF.date_format(SF.col("cola"), "MM/dd/yyy"),
            "DATE_FORMAT(CAST(cola AS TIMESTAMP), 'MM/dd/yyy')",
        ),
    ],
)
def test_date_format(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.year("cola"), "YEAR(TO_DATE(cola))"),
        (SF.year(SF.col("cola")), "YEAR(TO_DATE(cola))"),
    ],
)
def test_year(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.quarter("cola"), "QUARTER(TO_DATE(cola))"),
        (SF.quarter(SF.col("cola")), "QUARTER(TO_DATE(cola))"),
    ],
)
def test_quarter(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.month("cola"), "MONTH(TO_DATE(cola))"),
        (SF.month(SF.col("cola")), "MONTH(TO_DATE(cola))"),
    ],
)
def test_month(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.dayofweek("cola"), "DAYOFWEEK(TO_DATE(cola))"),
        (SF.dayofweek(SF.col("cola")), "DAYOFWEEK(TO_DATE(cola))"),
    ],
)
def test_dayofweek(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.dayofmonth("cola"), "DAYOFMONTH(TO_DATE(cola))"),
        (SF.dayofmonth(SF.col("cola")), "DAYOFMONTH(TO_DATE(cola))"),
    ],
)
def test_dayofmonth(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.dayofyear("cola"), "DAYOFYEAR(TO_DATE(cola))"),
        (SF.dayofyear(SF.col("cola")), "DAYOFYEAR(TO_DATE(cola))"),
    ],
)
def test_dayofyear(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.hour("cola"), "HOUR(cola)"),
        (SF.hour(SF.col("cola")), "HOUR(cola)"),
    ],
)
def test_hour(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.minute("cola"), "MINUTE(cola)"),
        (SF.minute(SF.col("cola")), "MINUTE(cola)"),
    ],
)
def test_minute(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.second("cola"), "SECOND(cola)"),
        (SF.second(SF.col("cola")), "SECOND(cola)"),
    ],
)
def test_second(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.weekofyear("cola"), "WEEKOFYEAR(TO_DATE(cola))"),
        (SF.weekofyear(SF.col("cola")), "WEEKOFYEAR(TO_DATE(cola))"),
    ],
)
def test_weekofyear(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.make_date("cola", "colb", "colc"), "MAKE_DATE(cola, colb, colc)"),
        (SF.make_date(SF.col("cola"), SF.col("colb"), "colc"), "MAKE_DATE(cola, colb, colc)"),
    ],
)
def test_make_date(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.date_sub("cola", 2), "CAST(DATE_ADD(CAST(cola AS DATE), -2) AS DATE)"),
        (SF.date_sub(SF.col("cola"), 2), "CAST(DATE_ADD(CAST(cola AS DATE), -2) AS DATE)"),
        (SF.date_sub("cola", "colb"), "CAST(DATE_ADD(CAST(cola AS DATE), colb * -1) AS DATE)"),
    ],
)
def test_date_sub(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.add_months("cola", 2), "CAST((CAST(cola AS DATE) + INTERVAL 2 MONTH) AS DATE)"),
        (SF.add_months(SF.col("cola"), 2), "CAST((CAST(cola AS DATE) + INTERVAL 2 MONTH) AS DATE)"),
        (SF.add_months("cola", "colb"), "CAST((CAST(cola AS DATE) + INTERVAL colb MONTH) AS DATE)"),
    ],
)
def test_add_months(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.months_between("cola", "colb"), "MONTHS_BETWEEN(cola, colb)"),
        (SF.months_between(SF.col("cola"), SF.col("colb")), "MONTHS_BETWEEN(cola, colb)"),
        (SF.months_between("cola", "colb", True), "MONTHS_BETWEEN(cola, colb, TRUE)"),
    ],
)
def test_months_between(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.to_date("cola"), "TO_DATE(cola)"),
        (SF.to_date(SF.col("cola")), "TO_DATE(cola)"),
        (SF.to_date("cola", "yy-MM-dd"), "TO_DATE(cola, 'yy-MM-dd')"),
    ],
)
def test_to_date(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.to_timestamp("cola"), "CAST(cola AS TIMESTAMP)"),
        (SF.to_timestamp(SF.col("cola")), "CAST(cola AS TIMESTAMP)"),
        (SF.to_timestamp("cola", "yyyy-MM-dd"), "TO_TIMESTAMP(cola, 'yyyy-MM-dd')"),
    ],
)
def test_to_timestamp(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.trunc("cola", "year"), "CAST(TRUNC(CAST(cola AS DATE), 'YEAR') AS DATE)"),
        (SF.trunc(SF.col("cola"), "year"), "CAST(TRUNC(CAST(cola AS DATE), 'YEAR') AS DATE)"),
    ],
)
def test_trunc(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.next_day("cola", "Mon"), "NEXT_DAY(cola, 'Mon')"),
        (SF.next_day(SF.col("cola"), "Mon"), "NEXT_DAY(cola, 'Mon')"),
    ],
)
def test_next_day(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.last_day("cola"), "LAST_DAY(cola)"),
        (SF.last_day(SF.col("cola")), "LAST_DAY(cola)"),
    ],
)
def test_last_day(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.from_unixtime("cola"), "FROM_UNIXTIME(cola)"),
        (SF.from_unixtime(SF.col("cola")), "FROM_UNIXTIME(cola)"),
        (SF.from_unixtime("cola", "yyyy-MM-dd HH:mm"), "FROM_UNIXTIME(cola, 'yyyy-MM-dd HH:mm')"),
    ],
)
def test_from_unixtime(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.from_utc_timestamp("cola", "PST"), "FROM_UTC_TIMESTAMP(cola, 'PST')"),
        (SF.from_utc_timestamp(SF.col("cola"), "PST"), "FROM_UTC_TIMESTAMP(cola, 'PST')"),
        (SF.from_utc_timestamp("cola", SF.col("colb")), "FROM_UTC_TIMESTAMP(cola, colb)"),
    ],
)
def test_from_utc_timestamp(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.to_utc_timestamp("cola", "PST"), "TO_UTC_TIMESTAMP(cola, 'PST')"),
        (SF.to_utc_timestamp(SF.col("cola"), "PST"), "TO_UTC_TIMESTAMP(cola, 'PST')"),
        (SF.to_utc_timestamp("cola", SF.col("colb")), "TO_UTC_TIMESTAMP(cola, colb)"),
    ],
)
def test_to_utc_timestamp(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.timestamp_seconds("cola"), "CAST(FROM_UNIXTIME(cola) AS TIMESTAMP)"),
        (SF.timestamp_seconds("cola"), "CAST(FROM_UNIXTIME(cola) AS TIMESTAMP)"),
    ],
)
def test_timestamp_seconds(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.crc32(SF.lit("SQLFrame")), "CRC32('SQLFrame')"),
        (SF.crc32(SF.col("cola")), "CRC32(cola)"),
    ],
)
def test_crc32(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.md5(SF.lit("SQLFrame")), "MD5('SQLFrame')"),
        (SF.md5(SF.col("cola")), "MD5(cola)"),
    ],
)
def test_md5(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sha1(SF.lit("SQLFrame")), "SHA('SQLFrame')"),
        (SF.sha1(SF.col("cola")), "SHA(cola)"),
    ],
)
def test_sha1(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sha2(SF.lit("Spark"), 256), "SHA2('Spark', 256)"),
        (SF.sha2(SF.col("cola"), 256), "SHA2(cola, 256)"),
    ],
)
def test_sha2(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.hash("cola", "colb", "colc"), "HASH(cola, colb, colc)"),
        (SF.hash(SF.col("cola"), SF.col("colb"), SF.col("colc")), "HASH(cola, colb, colc)"),
    ],
)
def test_hash(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.xxhash64("cola", "colb", "colc"), "XXHASH64(cola, colb, colc)"),
        (SF.xxhash64(SF.col("cola"), SF.col("colb"), SF.col("colc")), "XXHASH64(cola, colb, colc)"),
    ],
)
def test_xxhash64(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.raise_error("custom error message"), "RAISE_ERROR('custom error message')"),
        (SF.raise_error(SF.col("cola")), "RAISE_ERROR(cola)"),
    ],
)
def test_raise_error(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.upper("cola"), "UPPER(cola)"),
        (SF.upper(SF.col("cola")), "UPPER(cola)"),
    ],
)
def test_upper(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.lower("cola"), "LOWER(cola)"),
        (SF.lower(SF.col("cola")), "LOWER(cola)"),
    ],
)
def test_lower(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.ascii(SF.lit(2)), "ASCII(2)"),
        (SF.ascii(SF.col("cola")), "ASCII(cola)"),
    ],
)
def test_ascii(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.base64(SF.lit(2)), "BASE64(2)"),
        (SF.base64(SF.col("cola")), "BASE64(cola)"),
    ],
)
def test_base64(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.unbase64(SF.lit(2)), "UNBASE64(2)"),
        (SF.unbase64(SF.col("cola")), "UNBASE64(cola)"),
    ],
)
def test_unbase64(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.ltrim(SF.lit("Spark")), "LTRIM('Spark')"),
        (SF.ltrim(SF.col("cola")), "LTRIM(cola)"),
    ],
)
def test_ltrim(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.rtrim(SF.lit("SQLFrame")), "RTRIM('SQLFrame')"),
        (SF.rtrim(SF.col("cola")), "RTRIM(cola)"),
    ],
)
def test_rtrim(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.trim(SF.lit("SQLFrame")), "TRIM('SQLFrame')"),
        (SF.trim(SF.col("cola")), "TRIM(cola)"),
    ],
)
def test_trim(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.concat_ws("-", "cola", "colb"), "CONCAT_WS('-', cola, colb)"),
        (SF.concat_ws("-", SF.col("cola"), SF.col("colb")), "CONCAT_WS('-', cola, colb)"),
    ],
)
def test_concat_ws(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.decode("cola", "US-ASCII"), "DECODE(cola, 'US-ASCII')"),
        (SF.decode(SF.col("cola"), "US-ASCII"), "DECODE(cola, 'US-ASCII')"),
    ],
)
def test_decode(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.encode("cola", "US-ASCII"), "ENCODE(cola, 'US-ASCII')"),
        (SF.encode(SF.col("cola"), "US-ASCII"), "ENCODE(cola, 'US-ASCII')"),
    ],
)
def test_encode(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.format_number("cola", 4), "FORMAT_NUMBER(cola, 4)"),
        (SF.format_number(SF.col("cola"), 4), "FORMAT_NUMBER(cola, 4)"),
    ],
)
def test_format_number(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.instr("cola", "test"), "LOCATE('test', cola)"),
        (SF.instr(SF.col("cola"), "test"), "LOCATE('test', cola)"),
    ],
)
def test_instr(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.overlay("cola", "colb", 3, 7), "OVERLAY(cola, colb, 3, 7)"),
        (
            SF.overlay(SF.col("cola"), SF.col("colb"), SF.lit(3), SF.lit(7)),
            "OVERLAY(cola, colb, 3, 7)",
        ),
        (SF.overlay("cola", "colb", 3), "OVERLAY(cola, colb, 3)"),
    ],
)
def test_overlay(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.substring("cola", 2, 3), "SUBSTRING(cola, 2, 3)"),
        (SF.substring(SF.col("cola"), 2, 3), "SUBSTRING(cola, 2, 3)"),
    ],
)
def test_substring(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.substring_index("cola", ".", 2), "SUBSTRING_INDEX(cola, '.', 2)"),
        (SF.substring_index(SF.col("cola"), ".", 2), "SUBSTRING_INDEX(cola, '.', 2)"),
    ],
)
def test_substring_index(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.levenshtein("cola", "colb"), "LEVENSHTEIN(cola, colb)"),
        (SF.levenshtein(SF.col("cola"), SF.col("colb")), "LEVENSHTEIN(cola, colb)"),
    ],
)
def test_levenshtein(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.locate("test", "cola", 3), "LOCATE('test', cola, 3)"),
        (SF.locate("test", SF.col("cola"), 3), "LOCATE('test', cola, 3)"),
        (SF.locate("test", "cola"), "LOCATE('test', cola)"),
    ],
)
def test_locate(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.lpad("cola", 3, "#"), "LPAD(cola, 3, '#')"),
        (SF.lpad(SF.col("cola"), 3, "#"), "LPAD(cola, 3, '#')"),
    ],
)
def test_lpad(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.rpad("cola", 3, "#"), "RPAD(cola, 3, '#')"),
        (SF.rpad(SF.col("cola"), 3, "#"), "RPAD(cola, 3, '#')"),
    ],
)
def test_rpad(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.repeat("cola", 3), "REPEAT(cola, 3)"),
        (SF.repeat(SF.col("cola"), 3), "REPEAT(cola, 3)"),
    ],
)
def test_repeat(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.split("cola", "[ABC]", 3), "SPLIT(cola, '[ABC]', 3)"),
        (SF.split(SF.col("cola"), "[ABC]", 3), "SPLIT(cola, '[ABC]', 3)"),
        (SF.split("cola", "[ABC]"), "SPLIT(cola, '[ABC]')"),
    ],
)
def test_split(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.regexp_extract("cola", r"(\d+)-(\d+)", 1),
            "REGEXP_EXTRACT(cola, '(\\\\d+)-(\\\\d+)', 1)",
        ),
        (
            SF.regexp_extract(SF.col("cola"), r"(\d+)-(\d+)", 1),
            "REGEXP_EXTRACT(cola, '(\\\\d+)-(\\\\d+)', 1)",
        ),
        (
            SF.regexp_extract(SF.col("cola"), r"(\d+)-(\d+)"),
            "REGEXP_EXTRACT(cola, '(\\\\d+)-(\\\\d+)')",
        ),
    ],
)
def test_regexp_extract(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.initcap("cola"), "INITCAP(cola)"),
        (SF.initcap(SF.col("cola")), "INITCAP(cola)"),
    ],
)
def test_initcap(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.soundex("cola"), "SOUNDEX(cola)"),
        (SF.soundex(SF.col("cola")), "SOUNDEX(cola)"),
    ],
)
def test_soundex(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bin("cola"), "BIN(cola)"),
        (SF.bin(SF.col("cola")), "BIN(cola)"),
    ],
)
def test_bin(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.hex("cola"), "HEX(cola)"),
        (SF.hex(SF.col("cola")), "HEX(cola)"),
    ],
)
def test_hex(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.unhex("cola"), "UNHEX(cola)"),
        (SF.unhex(SF.col("cola")), "UNHEX(cola)"),
    ],
)
def test_unhex(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.length("cola"), "LENGTH(cola)"),
        (SF.length(SF.col("cola")), "LENGTH(cola)"),
    ],
)
def test_length(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.octet_length("cola"), "OCTET_LENGTH(cola)"),
        (SF.octet_length(SF.col("cola")), "OCTET_LENGTH(cola)"),
    ],
)
def test_octet_length(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.bit_length("cola"), "BIT_LENGTH(cola)"),
        (SF.bit_length(SF.col("cola")), "BIT_LENGTH(cola)"),
    ],
)
def test_bit_length(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.translate("cola", "abc", "xyz"), "TRANSLATE(cola, 'abc', 'xyz')"),
        (SF.translate(SF.col("cola"), "abc", "xyz"), "TRANSLATE(cola, 'abc', 'xyz')"),
    ],
)
def test_translate(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array("cola", "colb"), "ARRAY(cola, colb)"),
        (SF.array(SF.col("cola"), SF.col("colb")), "ARRAY(cola, colb)"),
        (SF.array(["cola", "colb"]), "ARRAY(cola, colb)"),
    ],
)
def test_array(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.map_from_arrays("cola", "colb"), "MAP_FROM_ARRAYS(cola, colb)"),
        (SF.map_from_arrays(SF.col("cola"), SF.col("colb")), "MAP_FROM_ARRAYS(cola, colb)"),
    ],
)
def test_map_from_arrays(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_contains("cola", "test"), "ARRAY_CONTAINS(cola, 'test')"),
        (SF.array_contains(SF.col("cola"), "test"), "ARRAY_CONTAINS(cola, 'test')"),
        (SF.array_contains("cola", SF.col("colb")), "ARRAY_CONTAINS(cola, colb)"),
    ],
)
def test_array_contains(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.arrays_overlap("cola", "colb"), "ARRAY_OVERLAPS(cola, colb)"),
        (SF.arrays_overlap(SF.col("cola"), SF.col("colb")), "ARRAY_OVERLAPS(cola, colb)"),
    ],
)
def test_arrays_overlap(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.slice("cola", SF.col("colb"), SF.col("colc")), "SLICE(cola, colb, colc)"),
        (SF.slice(SF.col("cola"), SF.col("colb"), SF.col("colc")), "SLICE(cola, colb, colc)"),
        (SF.slice("cola", 1, 10), "SLICE(cola, 1, 10)"),
    ],
)
def test_slice(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.concat("cola", "colb"), "CONCAT(cola, colb)"),
        (SF.concat(SF.col("cola"), SF.col("colb")), "CONCAT(cola, colb)"),
        (SF.concat("cola"), "CONCAT(cola)"),
    ],
)
def test_concat(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.element_at("cola", SF.col("colb")), "ELEMENT_AT(cola, colb)"),
        (SF.element_at(SF.col("cola"), SF.col("colb")), "ELEMENT_AT(cola, colb)"),
        (SF.element_at("cola", "test"), "ELEMENT_AT(cola, 'test')"),
    ],
)
def test_element_at(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_remove("cola", SF.col("colb")), "ARRAY_REMOVE(cola, colb)"),
        (SF.array_remove(SF.col("cola"), SF.col("colb")), "ARRAY_REMOVE(cola, colb)"),
        (SF.array_remove("cola", "test"), "ARRAY_REMOVE(cola, 'test')"),
    ],
)
def test_array_remove(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_distinct("cola"), "ARRAY_DISTINCT(cola)"),
        (SF.array_distinct(SF.col("cola")), "ARRAY_DISTINCT(cola)"),
    ],
)
def test_array_distinct(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_intersect("cola", "colb"), "ARRAY_INTERSECT(cola, colb)"),
        (SF.array_intersect(SF.col("cola"), SF.col("colb")), "ARRAY_INTERSECT(cola, colb)"),
    ],
)
def test_array_intersect(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_union("cola", "colb"), "ARRAY_UNION(cola, colb)"),
        (SF.array_union(SF.col("cola"), SF.col("colb")), "ARRAY_UNION(cola, colb)"),
    ],
)
def test_array_union(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_except("cola", "colb"), "ARRAY_EXCEPT(cola, colb)"),
        (SF.array_except(SF.col("cola"), SF.col("colb")), "ARRAY_EXCEPT(cola, colb)"),
    ],
)
def test_array_except(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.explode("cola"), "EXPLODE(cola)"),
        (SF.explode(SF.col("cola")), "EXPLODE(cola)"),
    ],
)
def test_explode(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.posexplode("cola"), "POSEXPLODE(cola)"),
        (SF.posexplode(SF.col("cola")), "POSEXPLODE(cola)"),
    ],
)
def test_pos_explode(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.explode_outer("cola"), "EXPLODE_OUTER(cola)"),
        (SF.explode_outer(SF.col("cola")), "EXPLODE_OUTER(cola)"),
    ],
)
def test_explode_outer(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.posexplode_outer("cola"), "POSEXPLODE_OUTER(cola)"),
        (SF.posexplode_outer(SF.col("cola")), "POSEXPLODE_OUTER(cola)"),
    ],
)
def test_posexplode_outer(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.get_json_object("cola", "$.f1"), "GET_JSON_OBJECT(cola, '$.f1')"),
        (SF.get_json_object(SF.col("cola"), "$.f1"), "GET_JSON_OBJECT(cola, '$.f1')"),
    ],
)
def test_get_json_object(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.json_tuple("cola", "f1", "f2"), "JSON_TUPLE(cola, 'f1', 'f2')"),
        (SF.json_tuple(SF.col("cola"), "f1", "f2"), "JSON_TUPLE(cola, 'f1', 'f2')"),
    ],
)
def test_json_tuple(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.schema_of_json("cola", dict(timestampFormat="dd/MM/yyyy")),
            "SCHEMA_OF_JSON(cola, MAP('timestampFormat', 'dd/MM/yyyy'))",
        ),
        (
            SF.schema_of_json(SF.col("cola"), dict(timestampFormat="dd/MM/yyyy")),
            "SCHEMA_OF_JSON(cola, MAP('timestampFormat', 'dd/MM/yyyy'))",
        ),
        (SF.schema_of_json("cola"), "SCHEMA_OF_JSON(cola)"),
    ],
)
def test_schema_of_json(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            SF.schema_of_csv("cola", dict(timestampFormat="dd/MM/yyyy")),
            "SCHEMA_OF_CSV(cola, MAP('timestampFormat', 'dd/MM/yyyy'))",
        ),
        (
            SF.schema_of_csv(SF.col("cola"), dict(timestampFormat="dd/MM/yyyy")),
            "SCHEMA_OF_CSV(cola, MAP('timestampFormat', 'dd/MM/yyyy'))",
        ),
        (SF.schema_of_csv("cola"), "SCHEMA_OF_CSV(cola)"),
    ],
)
def test_schema_of_csv(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.size("cola"), "SIZE(cola)"),
        (SF.size(SF.col("cola")), "SIZE(cola)"),
    ],
)
def test_size(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_min("cola"), "ARRAY_MIN(cola)"),
        (SF.array_min(SF.col("cola")), "ARRAY_MIN(cola)"),
    ],
)
def test_array_min(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_max("cola"), "ARRAY_MAX(cola)"),
        (SF.array_max(SF.col("cola")), "ARRAY_MAX(cola)"),
    ],
)
def test_array_max(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sort_array("cola", False), "SORT_ARRAY(cola, FALSE)"),
        (SF.sort_array(SF.col("cola"), False), "SORT_ARRAY(cola, FALSE)"),
        (SF.sort_array("cola"), "SORT_ARRAY(cola)"),
    ],
)
def test_sort_array(expression, expected):
    assert expression.sql() == expected


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
            "ARRAY_SORT(cola, (x, y) -> CASE WHEN x IS NULL OR y IS NULL THEN 0 ELSE (LENGTH(y) - LENGTH(x)) END)",
        ),
    ],
)
def test_array_sort(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.shuffle("cola"), "SHUFFLE(cola)"),
        (SF.shuffle(SF.col("cola")), "SHUFFLE(cola)"),
    ],
)
def test_shuffle(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.reverse("cola"), "REVERSE(cola)"),
        (SF.reverse(SF.col("cola")), "REVERSE(cola)"),
    ],
)
def test_reverse(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.flatten("cola"), "FLATTEN(cola)"),
        (SF.flatten(SF.col("cola")), "FLATTEN(cola)"),
    ],
)
def test_flatten(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.map_keys("cola"), "MAP_KEYS(cola)"),
        (SF.map_keys(SF.col("cola")), "MAP_KEYS(cola)"),
    ],
)
def test_map_keys(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.map_values("cola"), "MAP_VALUES(cola)"),
        (SF.map_values(SF.col("cola")), "MAP_VALUES(cola)"),
    ],
)
def test_map_values(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.map_entries("cola"), "MAP_ENTRIES(cola)"),
        (SF.map_entries(SF.col("cola")), "MAP_ENTRIES(cola)"),
    ],
)
def test_map_entries(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.map_from_entries("cola"), "MAP_FROM_ENTRIES(cola)"),
        (SF.map_from_entries(SF.col("cola")), "MAP_FROM_ENTRIES(cola)"),
    ],
)
def test_map_from_entries(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.array_repeat("cola", 2), "ARRAY_REPEAT(cola, 2)"),
        (SF.array_repeat(SF.col("cola"), 2), "ARRAY_REPEAT(cola, 2)"),
    ],
)
def test_array_repeat(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.arrays_zip("cola", "colb"), "ARRAYS_ZIP(cola, colb)"),
        (SF.arrays_zip(SF.col("cola"), SF.col("colb")), "ARRAYS_ZIP(cola, colb)"),
        (SF.arrays_zip("cola"), "ARRAYS_ZIP(cola)"),
    ],
)
def test_arrays_zip(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.map_concat("cola", "colb"), "MAP_CONCAT(cola, colb)"),
        (SF.map_concat(SF.col("cola"), SF.col("colb")), "MAP_CONCAT(cola, colb)"),
        (SF.map_concat("cola"), "MAP_CONCAT(cola)"),
    ],
)
def test_map_concat(expression, expected):
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.sequence("cola", "colb", "colc"), "SEQUENCE(cola, colb, colc)"),
        (SF.sequence(SF.col("cola"), SF.col("colb"), SF.col("colc")), "SEQUENCE(cola, colb, colc)"),
        (SF.sequence("cola", "colb"), "SEQUENCE(cola, colb)"),
    ],
)
def test_sequence(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


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
    assert expression.sql() == expected


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
    assert expression.sql() == expected


@pytest.mark.parametrize(
    "expression, expected",
    [
        (SF.exists("cola", lambda x: x % 2 == 0), "EXISTS(cola, x -> (x % 2) = 0)"),
        (SF.exists(SF.col("cola"), lambda x: x % 2 == 0), "EXISTS(cola, x -> (x % 2) = 0)"),
        (SF.exists("cola", lambda target: target > 0), "EXISTS(cola, target -> target > 0)"),
    ],
)
def test_exists(expression, expected):
    assert expression.sql() == expected


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
    assert expression.sql() == expected


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
    assert expression.sql() == expected


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
    assert expression.sql() == expected


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
    assert expression.sql() == expected


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
    assert expression.sql() == expected


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
    assert expression.sql() == expected


def test_map_zip_with():
    assert (
        SF.map_zip_with("base", "ratio", lambda k, v1, v2: SF.round(v1 * v2, 2)).sql()
        == "MAP_ZIP_WITH(base, ratio, (k, v1, v2) -> ROUND((v1 * v2), 2))"
    )


def test_nullif():
    assert SF.nullif("cola", "colb").sql() == "NULLIF(cola, colb)"


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
    assert expression.sql() == expected
