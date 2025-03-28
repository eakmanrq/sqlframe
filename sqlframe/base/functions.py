# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import decimal
import logging
import typing as t

from sqlglot import Dialect
from sqlglot import exp as expression
from sqlglot.dialects.dialect import time_format
from sqlglot.helper import ensure_list
from sqlglot.helper import flatten as _flatten

from sqlframe.base.column import Column
from sqlframe.base.decorators import func_metadata as meta
from sqlframe.base.util import (
    get_func_from_session,
)

if t.TYPE_CHECKING:
    from pyspark.sql.session import SparkContext

    from sqlframe.base._typing import ColumnOrLiteral, ColumnOrName
    from sqlframe.base.session import DF, _BaseSession
    from sqlframe.base.types import ArrayType, StructType

logger = logging.getLogger(__name__)


def _get_session() -> _BaseSession:
    from sqlframe.base.session import _BaseSession

    return _BaseSession()


@meta()
def col(column_name: t.Union[ColumnOrName, t.Any]) -> Column:
    from sqlframe.base.session import _BaseSession

    dialect = _BaseSession().input_dialect
    if isinstance(column_name, str):
        col_expression = expression.to_column(column_name, dialect=dialect).transform(
            dialect.normalize_identifier
        )
        case_sensitive_expression = expression.to_column(column_name, dialect=dialect)
        if not isinstance(
            case_sensitive_expression, (expression.Star, expression.Literal, expression.Null)
        ):
            col_expression._meta = {
                "display_name": case_sensitive_expression.this.this,
                **(col_expression._meta or {}),
            }

        return Column(col_expression)
    return Column(column_name)


@meta()
def lit(value: t.Optional[t.Any] = None) -> Column:
    if isinstance(value, str):
        return Column(expression.Literal.string(value))
    if isinstance(value, float) and value in {float("inf"), float("-inf")}:
        return Column(expression.Literal.string(str(value)))
    return Column(value)


@meta()
def greatest(*cols: ColumnOrName) -> Column:
    if len(cols) > 1:
        return Column.invoke_expression_over_column(
            cols[0], expression.Greatest, expressions=cols[1:]
        )
    return Column.invoke_expression_over_column(cols[0], expression.Greatest)


@meta()
def least(*cols: ColumnOrName) -> Column:
    if len(cols) > 1:
        return Column.invoke_expression_over_column(cols[0], expression.Least, expressions=cols[1:])
    return Column.invoke_expression_over_column(cols[0], expression.Least)


@meta(unsupported_engines="bigquery")
def count_distinct(col: ColumnOrName, *cols: ColumnOrName) -> Column:
    columns = [Column.ensure_col(x) for x in [col] + list(cols)]
    return Column(
        expression.Count(
            this=expression.Distinct(expressions=[x.column_expression for x in columns])
        )
    )


countDistinct = count_distinct


@meta()
def when(condition: Column, value: t.Any) -> Column:
    true_value = value if isinstance(value, Column) else lit(value)
    return Column(
        expression.Case(
            ifs=[expression.If(this=condition.column_expression, true=true_value.column_expression)]
        )
    )


@meta()
def asc(col: ColumnOrName) -> Column:
    return Column.ensure_col(col).asc()


@meta()
def desc(col: ColumnOrName):
    return Column.ensure_col(col).desc()


@meta(unsupported_engines="*")
def broadcast(df: DF) -> DF:
    return df.hint("broadcast")


@meta()
def sqrt(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Sqrt)


@meta()
def abs(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Abs)


@meta()
def max(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Max)


@meta()
def min(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Min)


@meta(unsupported_engines="postgres")
def max_by(col: ColumnOrName, ord: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.ArgMax, expression=ord)


@meta(unsupported_engines="postgres")
def min_by(col: ColumnOrName, ord: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.ArgMin, expression=ord)


@meta()
def count(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Count)


@meta()
def sum(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Sum)


@meta()
def avg(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Avg)


mean = avg


@meta()
def sumDistinct(col: ColumnOrName) -> Column:
    return Column(
        expression.Sum(
            this=expression.Distinct(expressions=[Column.ensure_col(col).column_expression])
        )
    )


sum_distinct = sumDistinct


# Product does not have a SQL function available
# @meta(unsupported_engines="*")
# def product(col: ColumnOrName) -> Column:
# raise NotImplementedError("Product is not currently implemented")


@meta()
def acos(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ACOS")


@meta(unsupported_engines="duckdb")
def acosh(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ACOSH")


@meta()
def asin(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ASIN")


@meta(unsupported_engines="duckdb")
def asinh(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ASINH")


@meta()
def atan(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ATAN")


@meta()
def atan2(col1: t.Union[ColumnOrName, float], col2: t.Union[ColumnOrName, float]) -> Column:
    col1_value = lit(col1) if isinstance(col1, (int, float)) else col1
    col2_value = lit(col2) if isinstance(col2, (int, float)) else col2

    return Column.invoke_anonymous_function(col1_value, "ATAN2", col2_value)


@meta(unsupported_engines="duckdb")
def atanh(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ATANH")


@meta()
def cbrt(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Cbrt)


@meta()
def ceil(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Ceil)


ceiling = ceil


@meta()
def cos(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "COS")


@meta(unsupported_engines="duckdb")
def cosh(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "COSH")


@meta()
def cot(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "COT")


@meta(unsupported_engines=["duckdb", "postgres", "snowflake"])
def csc(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "CSC")


@meta()
def e() -> Column:
    from sqlframe.base.function_alternatives import e_literal

    session = _get_session()

    if (
        session._is_bigquery
        or session._is_duckdb
        or session._is_postgres
        or session._is_redshift
        or session._is_snowflake
    ):
        return e_literal()

    return Column(expression.Anonymous(this="e"))


@meta()
def exp(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Exp)


@meta()
def expm1(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import expm1_from_exp

    session = _get_session()

    if session._is_bigquery or session._is_duckdb or session._is_postgres or session._is_snowflake:
        return expm1_from_exp(col)

    return Column.invoke_anonymous_function(col, "EXPM1")


@meta()
def factorial(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import (
        factorial_ensure_int,
        factorial_from_case_statement,
    )

    session = _get_session()

    if session._is_duckdb:
        return factorial_ensure_int(col)

    if session._is_bigquery:
        return factorial_from_case_statement(col)

    return Column.invoke_anonymous_function(col, "FACTORIAL")


@meta()
def floor(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Floor)


@meta()
def log10(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(lit(10), expression.Log, expression=col)


@meta()
def log1p(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import log1p_from_log

    session = _get_session()

    if session._is_bigquery or session._is_duckdb or session._is_postgres or session._is_snowflake:
        return log1p_from_log(col)

    return Column.invoke_anonymous_function(col, "LOG1P")


@meta()
def log2(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(lit(2), expression.Log, expression=col)


@meta()
def log(arg1: t.Union[ColumnOrName, float], arg2: t.Optional[ColumnOrName] = None) -> Column:
    arg1_value = lit(arg1) if isinstance(arg1, (int, float)) else arg1

    if arg2 is None:
        return Column.invoke_expression_over_column(arg1_value, expression.Ln)
    return Column.invoke_expression_over_column(arg1_value, expression.Log, expression=arg2)


@meta()
def rint(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import rint_from_round

    session = _get_session()

    if session._is_bigquery or session._is_duckdb or session._is_postgres or session._is_snowflake:
        return rint_from_round(col)

    return Column.invoke_anonymous_function(col, "RINT")


@meta(unsupported_engines=["duckdb", "postgres", "snowflake"])
def sec(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "SEC")


@meta()
def signum(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Sign)


@meta()
def sin(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "SIN")


@meta(unsupported_engines="duckdb")
def sinh(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "SINH")


@meta()
def tan(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "TAN")


@meta(unsupported_engines="duckdb")
def tanh(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "TANH")


@meta()
def degrees(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import degrees_bgutil

    session = _get_session()

    if session._is_bigquery:
        return degrees_bgutil(col)

    return Column.invoke_anonymous_function(col, "DEGREES")


toDegrees = degrees


@meta()
def radians(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import radians_bgutil

    session = _get_session()

    if session._is_bigquery:
        return radians_bgutil(col)

    return Column.invoke_anonymous_function(col, "RADIANS")


toRadians = radians


@meta()
def bitwiseNOT(col: ColumnOrName) -> Column:
    return bitwise_not(col)


@meta()
def bitwise_not(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import bitwise_not_from_bitnot

    session = _get_session()

    if session._is_snowflake:
        return bitwise_not_from_bitnot(col)

    return Column.invoke_expression_over_column(col, expression.BitwiseNot)


@meta()
def asc_nulls_first(col: ColumnOrName) -> Column:
    return Column.ensure_col(col).asc_nulls_first()


@meta()
def asc_nulls_last(col: ColumnOrName) -> Column:
    return Column.ensure_col(col).asc_nulls_last()


@meta()
def desc_nulls_first(col: ColumnOrName) -> Column:
    return Column.ensure_col(col).desc_nulls_first()


@meta()
def desc_nulls_last(col: ColumnOrName) -> Column:
    return Column.ensure_col(col).desc_nulls_last()


@meta()
def stddev(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Stddev)


@meta()
def stddev_samp(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.StddevSamp)


@meta()
def stddev_pop(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.StddevPop)


@meta()
def variance(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Variance)


@meta()
def var_samp(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Variance)


@meta()
def var_pop(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.VariancePop)


@meta(unsupported_engines=["bigquery", "postgres"])
def skewness(col: ColumnOrName) -> Column:
    session = _get_session()

    func_name = "SKEWNESS"

    if session._is_snowflake:
        func_name = "SKEW"

    if session._is_duckdb or session._is_snowflake:
        when_func = get_func_from_session("when")
        count_func = get_func_from_session("count")
        count_star = count_func("*")
        lit_func = get_func_from_session("lit")
        sqrt_func = get_func_from_session("sqrt")
        col = Column.ensure_col(col)
        return (
            when_func(count_star == lit_func(0), lit_func(None))
            .when(count_star == lit_func(1), lit_func(float("nan")))
            .when(count_star == lit_func(2), lit_func(0.0))
            .otherwise(
                Column.invoke_anonymous_function(col, func_name)
                * (count_star - lit_func(2))
                / (sqrt_func(count_star * (count_star - lit_func(1))))
            )
        )

    return Column.invoke_anonymous_function(col, func_name)


@meta(unsupported_engines=["bigquery", "postgres"])
def kurtosis(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import kurtosis_from_kurtosis_pop

    session = _get_session()

    if session._is_duckdb:
        return kurtosis_from_kurtosis_pop(col)

    return Column.invoke_anonymous_function(col, "KURTOSIS")


@meta()
def collect_list(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.ArrayAgg)


@meta()
def collect_set(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import collect_set_from_list_distinct

    session = _get_session()

    if session._is_bigquery or session._is_duckdb or session._is_postgres:
        return collect_set_from_list_distinct(col)

    return Column.invoke_expression_over_column(col, expression.ArrayUniqueAgg)


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def hypot(col1: t.Union[ColumnOrName, float], col2: t.Union[ColumnOrName, float]) -> Column:
    col1_value = lit(col1) if isinstance(col1, (int, float)) else col1
    col2_value = lit(col2) if isinstance(col2, (int, float)) else col2

    return Column.invoke_anonymous_function(col1_value, "HYPOT", col2_value)


@meta()
def pow(col1: t.Union[ColumnOrName, float], col2: t.Union[ColumnOrName, float]) -> Column:
    col1_value = lit(col1) if isinstance(col1, (int, float)) else col1
    col2_value = lit(col2) if isinstance(col2, (int, float)) else col2

    return Column.invoke_expression_over_column(col1_value, expression.Pow, expression=col2_value)


power = pow


@meta()
def row_number() -> Column:
    return Column(expression.Anonymous(this="ROW_NUMBER"))


@meta()
def dense_rank() -> Column:
    return Column(expression.Anonymous(this="DENSE_RANK"))


@meta()
def rank() -> Column:
    return Column(expression.Anonymous(this="RANK"))


@meta()
def cume_dist() -> Column:
    return Column(expression.Anonymous(this="CUME_DIST"))


@meta()
def percent_rank() -> Column:
    return Column(expression.Anonymous(this="PERCENT_RANK"))


@meta(unsupported_engines="postgres")
def approx_count_distinct(col: ColumnOrName, rsd: t.Optional[float] = None) -> Column:
    if rsd is None:
        return Column.invoke_expression_over_column(col, expression.ApproxDistinct)
    return Column.invoke_expression_over_column(col, expression.ApproxDistinct, accuracy=lit(rsd))


approxCountDistinct = approx_count_distinct


@meta()
def coalesce(*cols: ColumnOrName) -> Column:
    if len(cols) > 1:
        return Column.invoke_expression_over_column(
            cols[0], expression.Coalesce, expressions=cols[1:]
        )
    return Column.invoke_expression_over_column(cols[0], expression.Coalesce)


@meta()
def corr(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col1, expression.Corr, expression=col2)


@meta()
def covar_pop(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col1, expression.CovarPop, expression=col2)


@meta()
def covar_samp(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col1, expression.CovarSamp, expression=col2)


@meta(unsupported_engines=["bigquery", "postgres", "snowflake"])
def first(col: ColumnOrName, ignorenulls: t.Optional[bool] = None) -> Column:
    session = _get_session()

    if session._is_duckdb:
        ignorenulls = None

    this = Column.invoke_expression_over_column(col, expression.First)
    if ignorenulls:
        return Column.invoke_expression_over_column(this, expression.IgnoreNulls)
    return this


@meta(unsupported_engines=["bigquery", "postgres"])
def grouping_id(*cols: ColumnOrName) -> Column:
    if not cols:
        return Column.invoke_anonymous_function(None, "GROUPING_ID")
    if len(cols) == 1:
        return Column.invoke_anonymous_function(cols[0], "GROUPING_ID")
    return Column.invoke_anonymous_function(cols[0], "GROUPING_ID", *cols[1:])


@meta()
def input_file_name() -> Column:
    from sqlframe.base.session import _BaseSession

    return Column(expression.Literal.string(_BaseSession()._last_loaded_file or ""))


@meta()
def isnan(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import isnan_using_equal

    session = _get_session()

    if session._is_postgres or session._is_snowflake:
        return isnan_using_equal(col)

    return Column.invoke_expression_over_column(col, expression.IsNan)


@meta()
def isnull(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import isnull_using_equal

    session = _get_session()

    if session._is_bigquery or session._is_duckdb or session._is_postgres or session._is_snowflake:
        return isnull_using_equal(col)

    return Column.invoke_anonymous_function(col, "ISNULL")


@meta(unsupported_engines=["bigquery", "postgres", "snowflake"])
def last(col: ColumnOrName, ignorenulls: t.Optional[bool] = None) -> Column:
    this = Column.invoke_expression_over_column(col, expression.Last)
    if ignorenulls:
        return Column.invoke_expression_over_column(this, expression.IgnoreNulls)
    return this


@meta(unsupported_engines=["duckdb", "postgres", "bigquery", "snowflake", "redshift"])
def monotonically_increasing_id() -> Column:
    return Column.invoke_anonymous_function(None, "MONOTONICALLY_INCREASING_ID")


@meta()
def nanvl(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import nanvl_as_case

    session = _get_session()

    if session._is_bigquery or session._is_duckdb or session._is_postgres or session._is_snowflake:
        return nanvl_as_case(col1, col2)

    return Column.invoke_anonymous_function(col1, "NANVL", col2)


@meta(unsupported_engines="postgres")
def percentile_approx(
    col: ColumnOrName,
    percentage: t.Union[ColumnOrLiteral, t.List[float], t.Tuple[float]],
    accuracy: t.Optional[t.Union[ColumnOrLiteral, int]] = None,
) -> Column:
    from sqlframe.base.function_alternatives import (
        percentile_approx_without_accuracy_and_max_array,
        percentile_approx_without_accuracy_and_plural,
    )

    session = _get_session()

    if session._is_bigquery:
        return percentile_approx_without_accuracy_and_plural(col, percentage, accuracy)  # type: ignore

    if session._is_duckdb:
        if accuracy:
            logger.warning("Accuracy is ignored since it is not supported in this dialect")
        accuracy = None

    if session._is_snowflake and isinstance(percentage, (list, tuple)):
        return percentile_approx_without_accuracy_and_max_array(col, percentage, accuracy)  # type: ignore

    if accuracy:
        return Column.invoke_expression_over_column(
            col, expression.ApproxQuantile, quantile=lit(percentage), accuracy=accuracy
        )
    return Column.invoke_expression_over_column(
        col, expression.ApproxQuantile, quantile=lit(percentage)
    )


@meta(unsupported_engines="bigquery")
def percentile(
    col: ColumnOrName,
    percentage: t.Union[ColumnOrLiteral, t.List[float], t.Tuple[float]],
    frequency: t.Optional[ColumnOrLiteral] = None,
) -> Column:
    from sqlframe.base.function_alternatives import percentile_without_disc

    session = _get_session()

    if session._is_databricks or session._is_spark:
        return percentile_without_disc(col, percentage, frequency)

    if frequency:
        logger.warning("Frequency is not supported in all engines")
    return Column.invoke_expression_over_column(
        col, expression.PercentileDisc, expression=lit(percentage)
    )


@meta()
def rand(seed: t.Optional[int] = None) -> Column:
    session = _get_session()

    if session._is_bigquery or session._is_duckdb or session._is_postgres:
        if seed:
            logger.warning("Seed is ignored since it is not supported in this dialect")
        seed = None

    if seed is not None:
        return Column.invoke_expression_over_column(None, expression.Rand, this=lit(seed))
    return Column.invoke_expression_over_column(None, expression.Rand)


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def randn(seed: t.Optional[int] = None) -> Column:
    if seed is not None:
        return Column.invoke_expression_over_column(None, expression.Randn, this=lit(seed))
    return Column.invoke_expression_over_column(None, expression.Randn)


@meta()
def round(col: ColumnOrName, scale: t.Optional[int] = None) -> Column:
    session = _get_session()

    if session._is_postgres:
        col = Column.ensure_col(col).cast("numeric")

    if scale is not None:
        return Column.invoke_expression_over_column(col, expression.Round, decimals=scale)
    return Column.invoke_expression_over_column(col, expression.Round)


@meta(unsupported_engines=["duckdb", "postgres"])
def bround(col: ColumnOrName, scale: t.Optional[int] = None) -> Column:
    from sqlframe.base.function_alternatives import (
        bround_bgutil,
        bround_using_half_even,
    )

    session = _get_session()

    if session._is_bigquery:
        return bround_bgutil(col, scale)

    if session._is_snowflake:
        return bround_using_half_even(col, scale)

    if scale is not None:
        return Column.invoke_anonymous_function(col, "BROUND", lit(scale))
    return Column.invoke_anonymous_function(col, "BROUND")


@meta()
def shiftleft(col: ColumnOrName, numBits: int) -> Column:
    from sqlframe.base.function_alternatives import shiftleft_from_bitshiftleft

    session = _get_session()

    if session._is_snowflake:
        return shiftleft_from_bitshiftleft(col, numBits)

    return Column.invoke_expression_over_column(
        col, expression.BitwiseLeftShift, expression=lit(numBits)
    )


shiftLeft = shiftleft


@meta()
def shiftright(col: ColumnOrName, numBits: int) -> Column:
    from sqlframe.base.function_alternatives import shiftright_from_bitshiftright

    session = _get_session()

    if session._is_snowflake:
        return shiftright_from_bitshiftright(col, numBits)

    return Column.invoke_expression_over_column(
        col, expression.BitwiseRightShift, expression=lit(numBits)
    )


shiftRight = shiftright


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def shiftrightunsigned(col: ColumnOrName, numBits: int) -> Column:
    return Column.invoke_anonymous_function(
        Column.ensure_col(col).cast("bigint"), "SHIFTRIGHTUNSIGNED", lit(numBits)
    )


shiftRightUnsigned = shiftrightunsigned


@meta()
def expr(str: str) -> Column:
    return Column(str)


@meta(unsupported_engines=["postgres"])
def struct(col: t.Union[ColumnOrName, t.Iterable[ColumnOrName]], *cols: ColumnOrName) -> Column:
    session = _get_session()
    col_func = get_func_from_session("col")

    columns = [col_func(x) for x in ensure_list(col) + list(cols)]
    expressions = []
    for column in columns:
        expressions.append(
            expression.PropertyEQ(
                this=expression.parse_identifier(
                    column.alias_or_name, dialect=session.input_dialect
                ),
                expression=column.column_expression,
            )
        )
    return Column(expression.Struct(expressions=expressions))


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def conv(col: ColumnOrName, fromBase: int, toBase: int) -> Column:
    return Column.invoke_anonymous_function(col, "CONV", lit(fromBase), lit(toBase))


@meta()
def lag(
    col: ColumnOrName, offset: t.Optional[int] = 1, default: t.Optional[ColumnOrLiteral] = None
) -> Column:
    if default is not None:
        return Column.invoke_expression_over_column(
            col, expression.Lag, offset=lit(offset), default=default
        )
    return Column.invoke_expression_over_column(col, expression.Lag, offset=lit(offset))


@meta()
def lead(
    col: ColumnOrName, offset: t.Optional[int] = 1, default: t.Optional[t.Any] = None
) -> Column:
    if default is not None:
        return Column.invoke_expression_over_column(
            col, expression.Lead, offset=lit(offset), default=default
        )
    return Column.invoke_expression_over_column(col, expression.Lead, offset=lit(offset))


@meta()
def nth_value(
    col: ColumnOrName, offset: t.Optional[int] = 1, ignoreNulls: t.Optional[bool] = None
) -> Column:
    this = Column.invoke_expression_over_column(col, expression.NthValue, offset=lit(offset))
    if ignoreNulls is not None:
        return Column.invoke_expression_over_column(this, expression.IgnoreNulls)
    return this


@meta()
def ntile(n: int) -> Column:
    return Column.invoke_anonymous_function(None, "NTILE", lit(n))


@meta()
def current_date() -> Column:
    return Column.invoke_expression_over_column(None, expression.CurrentDate)


@meta()
def current_timestamp() -> Column:
    return Column.invoke_expression_over_column(None, expression.CurrentTimestamp)


@meta()
def date_format(col: ColumnOrName, format: str) -> Column:
    from sqlframe.base.session import _BaseSession

    return Column.invoke_expression_over_column(
        Column(expression.TimeStrToTime(this=Column.ensure_col(col).column_expression)),
        expression.TimeToStr,
        format=_BaseSession().format_time(format),
    )


@meta()
def year(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import year_from_extract

    session = _get_session()

    if session._is_bigquery or session._is_postgres:
        return year_from_extract(col)

    return Column.invoke_expression_over_column(
        Column(expression.TsOrDsToDate(this=Column.ensure_col(col).column_expression)),
        expression.Year,
    )


@meta()
def quarter(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import quarter_from_extract

    session = _get_session()

    if session._is_bigquery or session._is_postgres:
        return quarter_from_extract(col)

    return Column(
        expression.Anonymous(
            this="QUARTER",
            expressions=[expression.TsOrDsToDate(this=Column.ensure_col(col).column_expression)],
        )
    )


@meta()
def month(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import month_from_extract

    session = _get_session()

    if session._is_bigquery or session._is_postgres:
        return month_from_extract(col)

    return Column.invoke_expression_over_column(
        Column(expression.TsOrDsToDate(this=Column.ensure_col(col).column_expression)),
        expression.Month,
    )


@meta()
def dayofweek(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import (
        dayofweek_from_extract,
        dayofweek_from_extract_with_isodow,
    )

    session = _get_session()

    if session._is_bigquery:
        return dayofweek_from_extract(col)

    if session._is_postgres:
        return dayofweek_from_extract_with_isodow(col) + 1

    result = Column.invoke_expression_over_column(
        Column(expression.TsOrDsToDate(this=Column.ensure_col(col).column_expression)),
        expression.DayOfWeek,
    )
    if session._is_duckdb or session._is_snowflake:
        return result + 1
    return result


@meta()
def dayofmonth(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import dayofmonth_from_extract_with_day

    session = _get_session()

    if session._is_bigquery or session._is_postgres:
        return dayofmonth_from_extract_with_day(col)

    return Column.invoke_expression_over_column(
        Column(expression.TsOrDsToDate(this=Column.ensure_col(col).column_expression)),
        expression.DayOfMonth,
    )


@meta()
def dayofyear(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import (
        dayofyear_from_extract,
        dayofyear_from_extract_doy,
    )

    session = _get_session()

    if session._is_bigquery:
        return dayofyear_from_extract(col)

    if session._is_postgres:
        return dayofyear_from_extract_doy(col)

    return Column.invoke_expression_over_column(
        Column(expression.TsOrDsToDate(this=Column.ensure_col(col).column_expression)),
        expression.DayOfYear,
    )


@meta()
def hour(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import hour_from_extract

    session = _get_session()

    if session._is_bigquery or session._is_postgres:
        return hour_from_extract(col)

    return Column.invoke_anonymous_function(col, "HOUR")


@meta()
def minute(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import minute_from_extract

    session = _get_session()

    if session._is_bigquery or session._is_postgres:
        return minute_from_extract(col)

    return Column.invoke_anonymous_function(col, "MINUTE")


@meta()
def second(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import second_from_extract

    session = _get_session()

    if session._is_bigquery or session._is_postgres:
        return second_from_extract(col)

    return Column.invoke_anonymous_function(col, "SECOND")


@meta()
def weekofyear(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import (
        weekofyear_from_extract_as_isoweek,
        weekofyear_from_extract_as_week,
    )

    session = _get_session()

    if session._is_bigquery:
        return weekofyear_from_extract_as_isoweek(col)

    if session._is_postgres:
        return weekofyear_from_extract_as_week(col)

    return Column.invoke_expression_over_column(
        Column(expression.TsOrDsToDate(this=Column.ensure_col(col).column_expression)),
        expression.WeekOfYear,
    )


@meta()
def make_date(year: ColumnOrName, month: ColumnOrName, day: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import (
        make_date_date_from_parts,
        make_date_from_date_func,
    )

    session = _get_session()

    if session._is_bigquery:
        return make_date_from_date_func(year, month, day)

    if session._is_postgres:
        year = Column.ensure_col(year).cast("integer")
        month = Column.ensure_col(month).cast("integer")
        day = Column.ensure_col(day).cast("integer")

    if session._is_snowflake:
        return make_date_date_from_parts(year, month, day)

    return Column.invoke_anonymous_function(year, "MAKE_DATE", month, day)


@meta()
def date_add(
    col: ColumnOrName, days: t.Union[ColumnOrName, int], cast_as_date: bool = True
) -> Column:
    from sqlframe.base.function_alternatives import date_add_no_date_sub

    session = _get_session()
    date_sub_func = get_func_from_session("date_sub")
    original_days = None

    if session._is_postgres and not isinstance(days, int):
        original_days = days
        days = 1

    if session._is_snowflake:
        return date_add_no_date_sub(col, days, cast_as_date)

    if isinstance(days, int):
        if days < 0:
            return date_sub_func(col, days * -1)
        days = lit(days)
    result = Column.invoke_expression_over_column(
        Column.ensure_col(col).cast("date"),
        expression.DateAdd,
        expression=days,
        unit=expression.Var(this="DAY"),
    )

    if session._is_postgres and original_days is not None:
        result = result * Column.ensure_col(original_days)

    if cast_as_date:
        return result.cast("date")
    return result


@meta()
def date_sub(
    col: ColumnOrName, days: t.Union[ColumnOrName, int], cast_as_date: bool = True
) -> Column:
    """
    Non-standard argument: cast_as_date
    """
    from sqlframe.base.function_alternatives import date_sub_by_date_add

    session = _get_session()
    date_add_func = get_func_from_session("date_add")
    original_days = None

    if session._is_postgres and not isinstance(days, int):
        original_days = days
        days = 1

    if session._is_snowflake:
        return date_sub_by_date_add(col, days, cast_as_date)

    if isinstance(days, int):
        if days < 0:
            return date_add_func(col, days * -1)
        days = lit(days)
    result = Column.invoke_expression_over_column(
        Column.ensure_col(col).cast("date"),
        expression.DateSub,
        expression=days,
        unit=expression.Var(this="DAY"),
    )

    if session._is_postgres and original_days is not None:
        result = result * Column.ensure_col(original_days)

    if cast_as_date:
        return result.cast("date")
    return result


@meta()
def date_diff(end: ColumnOrName, start: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import date_diff_with_subtraction

    session = _get_session()

    if session._is_postgres:
        return date_diff_with_subtraction(end, start)

    return Column.invoke_expression_over_column(
        Column.ensure_col(end).cast("date"),
        expression.DateDiff,
        expression=Column.ensure_col(start).cast("date"),
    )


@meta()
def add_months(
    start: ColumnOrName, months: t.Union[ColumnOrName, int], cast_as_date: bool = True
) -> Column:
    """
    Non-standard argument: cast_as_date
    """
    from sqlframe.base.function_alternatives import add_months_using_func

    lit = get_func_from_session("lit")
    session = _get_session()
    original_months = months

    if session._is_databricks or session._is_postgres or session._is_spark:
        months = 1

    if session._is_snowflake:
        return add_months_using_func(start, months, cast_as_date)

    start_col = Column(start).cast("date")

    if isinstance(months, int):
        if months < 0:
            end_col = Column(
                expression.Interval(
                    this=lit(months * -1).column_expression, unit=expression.Var(this="MONTH")
                )
            )
            result = start_col - end_col
        else:
            end_col = Column(
                expression.Interval(
                    this=lit(months).column_expression, unit=expression.Var(this="MONTH")
                )
            )
            result = start_col + end_col
    else:
        end_col = Column(
            expression.Interval(
                this=Column.ensure_col(months).column_expression, unit=expression.Var(this="MONTH")
            )
        )
        result = start_col + end_col

    if session._is_databricks or session._is_postgres or session._is_spark:
        multiple_value = (
            lit(original_months)
            if isinstance(original_months, int)
            else Column.ensure_col(original_months)
        )
        result = Column.ensure_col(result.column_expression.unnest()) * multiple_value

    if cast_as_date:
        return result.cast("date")
    return result


@meta()
def months_between(
    date1: ColumnOrName, date2: ColumnOrName, roundOff: t.Optional[bool] = None
) -> Column:
    from sqlframe.base.function_alternatives import (
        months_between_bgutils,
        months_between_from_age_and_extract,
    )

    session = _get_session()
    original_roundoff = roundOff

    if session._is_bigquery:
        return months_between_bgutils(date1, date2, roundOff)

    if session._is_postgres:
        return months_between_from_age_and_extract(date1, date2, roundOff)

    if session._is_snowflake:
        date1 = Column.ensure_col(date1).cast("date")
        date2 = Column.ensure_col(date2).cast("date")
        roundOff = None

    if roundOff is None:
        result = Column.invoke_expression_over_column(
            date1, expression.MonthsBetween, expression=date2
        )
    else:
        result = Column.invoke_expression_over_column(
            date1, expression.MonthsBetween, expression=date2, roundoff=lit(roundOff)
        )

    if session._is_snowflake and original_roundoff:
        return result.cast("bigint")
    return result


@meta()
def to_date(col: ColumnOrName, format: t.Optional[str] = None) -> Column:
    session = _get_session()

    if session._is_bigquery:
        to_timestamp_func = get_func_from_session("to_timestamp")
        col = to_timestamp_func(col, format)

    if session._is_snowflake:
        format = format or session.default_time_format

    if format is not None:
        return Column.invoke_expression_over_column(
            col, expression.TsOrDsToDate, format=session.format_time(format)
        )
    return Column.invoke_expression_over_column(col, expression.TsOrDsToDate)


@meta()
def to_timestamp(col: ColumnOrName, format: t.Optional[str] = None) -> Column:
    from sqlframe.base.function_alternatives import (
        to_timestamp_just_timestamp,
        to_timestamp_tz,
        to_timestamp_with_time_zone,
    )

    session = _get_session()

    if session._is_duckdb:
        return to_timestamp_tz(col, format)

    if session._is_bigquery:
        return to_timestamp_just_timestamp(col, format)

    if session._is_postgres:
        return to_timestamp_with_time_zone(col, format)

    if format is not None:
        return Column.invoke_expression_over_column(
            col, expression.StrToTime, format=session.format_time(format)
        )

    return Column.ensure_col(col).cast("timestampltz")


@meta()
def trunc(col: ColumnOrName, format: str) -> Column:
    return Column.invoke_expression_over_column(
        Column(col).cast("date"), expression.DateTrunc, unit=lit(format)
    ).cast("date")


@meta()
def date_trunc(format: str, timestamp: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(
        Column(timestamp).cast("timestamp"), expression.TimestampTrunc, unit=lit(format)
    ).cast("timestamp")


@meta(unsupported_engines=["duckdb", "postgres"])
def next_day(col: ColumnOrName, dayOfWeek: str) -> Column:
    from sqlframe.base.function_alternatives import next_day_bgutil

    session = _get_session()

    if session._is_bigquery:
        return next_day_bgutil(col, dayOfWeek)

    return Column.invoke_anonymous_function(col, "NEXT_DAY", lit(dayOfWeek))


@meta()
def last_day(col: ColumnOrName) -> Column:
    session = _get_session()

    if session._is_bigquery or session._is_duckdb or session._is_postgres or session._is_snowflake:
        col = Column.ensure_col(col).cast("date")

    return Column.invoke_expression_over_column(col, expression.LastDay)


@meta()
def from_unixtime(col: ColumnOrName, format: t.Optional[str] = None) -> Column:
    from sqlframe.base.function_alternatives import (
        from_unixtime_bigutil,
        from_unixtime_from_timestamp,
    )

    session = _get_session()

    if session._is_bigquery:
        return from_unixtime_bigutil(col, format)

    if session._is_postgres or session._is_snowflake:
        return from_unixtime_from_timestamp(col, format)

    return Column.invoke_expression_over_column(
        col,
        expression.UnixToStr,
        format=session.format_time(format),
    )


@meta()
def unix_timestamp(
    timestamp: t.Optional[ColumnOrName] = None, format: t.Optional[str] = None
) -> Column:
    from sqlframe.base.function_alternatives import (
        unix_timestamp_bgutil,
        unix_timestamp_from_extract,
    )

    session = _get_session()

    if session._is_bigquery:
        return unix_timestamp_bgutil(timestamp, format)

    if session._is_postgres or session._is_snowflake:
        return unix_timestamp_from_extract(timestamp, format)

    return Column.invoke_expression_over_column(
        timestamp,
        expression.StrToUnix,
        format=session.format_time(format),
    ).cast("bigint")


@meta(unsupported_engines=["duckdb", "postgres", "bigquery", "snowflake", "redshift"])
def from_utc_timestamp(timestamp: ColumnOrName, tz: ColumnOrName) -> Column:
    tz_column = tz if isinstance(tz, Column) else lit(tz)
    return Column.invoke_expression_over_column(timestamp, expression.AtTimeZone, zone=tz_column)


@meta(unsupported_engines=["duckdb", "postgres", "bigquery", "snowflake", "redshift"])
def to_utc_timestamp(timestamp: ColumnOrName, tz: ColumnOrName) -> Column:
    tz_column = tz if isinstance(tz, Column) else lit(tz)
    return Column.invoke_expression_over_column(timestamp, expression.FromTimeZone, zone=tz_column)


@meta()
def timestamp_seconds(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.UnixToTime)


@meta(unsupported_engines=["*", "spark"])
def window(
    timeColumn: ColumnOrName,
    windowDuration: str,
    slideDuration: t.Optional[str] = None,
    startTime: t.Optional[str] = None,
) -> Column:
    if slideDuration is not None and startTime is not None:
        value = Column.invoke_anonymous_function(
            timeColumn, "WINDOW", lit(windowDuration), lit(slideDuration), lit(startTime)
        )
    elif slideDuration is not None:
        value = Column.invoke_anonymous_function(
            timeColumn, "WINDOW", lit(windowDuration), lit(slideDuration)
        )
    elif startTime is not None:
        value = Column.invoke_anonymous_function(
            timeColumn, "WINDOW", lit(windowDuration), lit(windowDuration), lit(startTime)
        )
    else:
        value = Column.invoke_anonymous_function(timeColumn, "WINDOW", lit(windowDuration))
    return value


@meta(unsupported_engines=["duckdb", "postgres", "bigquery", "redshift", "snowflake", "spark"])
def session_window(timeColumn: ColumnOrName, gapDuration: ColumnOrName) -> Column:
    gap_duration_column = gapDuration if isinstance(gapDuration, Column) else lit(gapDuration)
    return Column.invoke_anonymous_function(timeColumn, "SESSION_WINDOW", gap_duration_column)


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def crc32(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "CRC32")


@meta()
def md5(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.MD5)


@meta(unsupported_engines=["postgres"])
def sha1(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import sha1_force_sha1_and_to_hex

    session = _get_session()

    if session._is_bigquery:
        return sha1_force_sha1_and_to_hex(col)

    return Column.invoke_expression_over_column(col, expression.SHA)


@meta(unsupported_engines=["bigquery", "postgres"])
def sha2(col: ColumnOrName, numBits: int) -> Column:
    from sqlframe.base.function_alternatives import sha2_sha265

    session = _get_session()

    if session._is_duckdb:
        if numBits in [256, 0]:
            return sha2_sha265(col)
        else:
            raise ValueError("This dialect only supports SHA-265 (numBits=256 or numBits=0)")

    return Column.invoke_expression_over_column(col, expression.SHA2, length=lit(numBits))


@meta(unsupported_engines=["postgres"])
def hash(*cols: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import hash_from_farm_fingerprint

    session = _get_session()

    if session._is_bigquery:
        return hash_from_farm_fingerprint(*cols)

    args = cols[1:] if len(cols) > 1 else []
    return Column.invoke_anonymous_function(cols[0], "HASH", *args)


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def xxhash64(*cols: ColumnOrName) -> Column:
    args = cols[1:] if len(cols) > 1 else []
    return Column.invoke_anonymous_function(cols[0], "XXHASH64", *args)


@meta(unsupported_engines=["duckdb", "postgres", "bigquery", "snowflake", "redshift"])
def assert_true(col: ColumnOrName, errorMsg: t.Optional[ColumnOrName] = None) -> Column:
    if errorMsg is not None:
        error_msg_col = errorMsg if isinstance(errorMsg, Column) else lit(errorMsg)
        return Column.invoke_anonymous_function(col, "ASSERT_TRUE", error_msg_col)
    return Column.invoke_anonymous_function(col, "ASSERT_TRUE")


@meta(unsupported_engines=["duckdb", "postgres", "bigquery", "snowflake", "redshift"])
def raise_error(errorMsg: ColumnOrName) -> Column:
    error_msg_col = errorMsg if isinstance(errorMsg, Column) else lit(errorMsg)
    return Column.invoke_anonymous_function(error_msg_col, "RAISE_ERROR")


@meta()
def upper(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Upper)


@meta()
def lower(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Lower)


@meta()
def ascii(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ASCII")


@meta()
def base64(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import (
        base64_from_base64_encode,
        base64_from_blob,
        base64_from_encode,
    )

    session = _get_session()

    if session._is_bigquery or session._is_duckdb:
        return base64_from_blob(col)

    if session._is_postgres:
        return base64_from_encode(col)

    if session._is_snowflake:
        return base64_from_base64_encode(col)

    return Column.invoke_expression_over_column(col, expression.ToBase64)


@meta()
def unbase64(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import (
        unbase64_from_base64_decode_string,
        unbase64_from_decode,
    )

    session = _get_session()

    if session._is_postgres:
        return unbase64_from_decode(col)

    if session._is_snowflake:
        return unbase64_from_base64_decode_string(col)

    return Column.invoke_expression_over_column(col, expression.FromBase64)


@meta()
def ltrim(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "LTRIM")


@meta()
def rtrim(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "RTRIM")


@meta()
def trim(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Trim)


@meta()
def concat_ws(sep: str, *cols: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import concat_ws_from_array_to_string

    session = _get_session()

    if session._is_bigquery:
        return concat_ws_from_array_to_string(sep, *cols)

    return Column.invoke_expression_over_column(
        None, expression.ConcatWs, expressions=[lit(sep)] + list(cols)
    )


@meta(unsupported_engines=["bigquery", "snowflake"])
def decode(col: ColumnOrName, charset: str) -> Column:
    from sqlframe.base.function_alternatives import (
        decode_from_blob,
        decode_from_convert_from,
    )

    session = _get_session()

    if session._is_duckdb:
        return decode_from_blob(col, charset)

    if session._is_postgres:
        return decode_from_convert_from(col, charset)

    return Column.invoke_expression_over_column(
        col, expression.Decode, charset=expression.Literal.string(charset)
    )


@meta(unsupported_engines=["bigquery", "snowflake"])
def encode(col: ColumnOrName, charset: str) -> Column:
    from sqlframe.base.function_alternatives import encode_from_convert_to

    session = _get_session()

    if session._is_postgres:
        return encode_from_convert_to(col, charset)

    return Column.invoke_expression_over_column(
        col, expression.Encode, charset=expression.Literal.string(charset)
    )


@meta(unsupported_engines="duckdb")
def format_number(col: ColumnOrName, d: int) -> Column:
    from sqlframe.base.function_alternatives import (
        format_number_bgutil,
        format_number_from_to_char,
    )

    session = _get_session()

    if session._is_bigquery:
        return format_number_bgutil(col, d)

    if session._is_postgres or session._is_snowflake:
        return format_number_from_to_char(col, d)

    return Column.invoke_anonymous_function(col, "FORMAT_NUMBER", lit(d))


@meta(unsupported_engines="snowflake")
def format_string(format: str, *cols: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import (
        format_string_with_format,
        format_string_with_pipes,
    )

    session = _get_session()

    if session._is_duckdb:
        return format_string_with_pipes(format, *cols)

    if session._is_bigquery or session._is_postgres:
        return format_string_with_format(format, *cols)

    format_col = lit(format)
    columns = [Column.ensure_col(x) for x in cols]
    return Column.invoke_anonymous_function(format_col, "FORMAT_STRING", *columns)


@meta()
def instr(col: ColumnOrName, substr: str) -> Column:
    from sqlframe.base.function_alternatives import instr_using_strpos

    session = _get_session()

    if session._is_bigquery:
        return instr_using_strpos(col, substr)

    return Column.invoke_expression_over_column(col, expression.StrPosition, substr=lit(substr))


@meta()
def overlay(
    src: ColumnOrName,
    replace: ColumnOrName,
    pos: t.Union[ColumnOrName, int],
    len: t.Optional[t.Union[ColumnOrName, int]] = None,
) -> Column:
    from sqlframe.base.function_alternatives import overlay_from_substr

    session = _get_session()

    if session._is_bigquery or session._is_duckdb or session._is_snowflake:
        return overlay_from_substr(src, replace, pos, len)
    return Column.invoke_expression_over_column(
        src,
        expression.Overlay,
        **{
            "expression": Column(replace).column_expression,
            "from": lit(pos).column_expression,
            "for": lit(len).column_expression if len is not None else None,
        },
    )


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def sentences(
    string: ColumnOrName,
    language: t.Optional[ColumnOrName] = None,
    country: t.Optional[ColumnOrName] = None,
) -> Column:
    if language is not None and country is not None:
        return Column.invoke_anonymous_function(string, "SENTENCES", language, country)
    if language is not None:
        return Column.invoke_anonymous_function(string, "SENTENCES", language)
    if country is not None:
        return Column.invoke_anonymous_function(string, "SENTENCES", lit("en"), country)
    return Column.invoke_anonymous_function(string, "SENTENCES")


@meta()
def substring(str: ColumnOrName, pos: int, len: int) -> Column:
    return Column.ensure_col(str).substr(pos, len)


@meta(unsupported_engines=["duckdb", "postgres", "snowflake"])
def substring_index(str: ColumnOrName, delim: str, count: int) -> Column:
    from sqlframe.base.function_alternatives import substring_index_bgutil

    session = _get_session()

    if session._is_bigquery:
        return substring_index_bgutil(str, delim, count)

    return Column.invoke_anonymous_function(str, "SUBSTRING_INDEX", lit(delim), lit(count))


@meta(unsupported_engines="bigquery")
def levenshtein(
    left: ColumnOrName, right: ColumnOrName, threshold: t.Optional[int] = None
) -> Column:
    from sqlframe.base.function_alternatives import levenshtein_edit_distance

    session = _get_session()

    if session._is_snowflake:
        return levenshtein_edit_distance(left, right, threshold)

    value: t.Union[expression.Case, expression.Levenshtein] = expression.Levenshtein(
        this=Column.ensure_col(left).column_expression,
        expression=Column.ensure_col(right).column_expression,
    )
    if threshold is not None:
        value = (
            expression.case()
            .when(expression.LTE(this=value, expression=lit(threshold).column_expression), value)
            .else_(lit(-1).column_expression)
        )
    return Column(value)


@meta(unsupported_engines="bigquery")
def locate(substr: str, str: ColumnOrName, pos: t.Optional[int] = None) -> Column:
    substr_col = lit(substr)
    if pos is not None:
        return Column.invoke_expression_over_column(
            str, expression.StrPosition, substr=substr_col, position=pos
        )
    return Column.invoke_expression_over_column(str, expression.StrPosition, substr=substr_col)


@meta()
def lpad(col: ColumnOrName, len: int, pad: str) -> Column:
    return Column(
        expression.Pad(
            this=Column.ensure_col(col).column_expression,
            expression=lit(len).column_expression,
            fill_pattern=lit(pad).column_expression,
            # We can use `invoke_expression_over_column` because this is an actual bool instead of literal bool
            is_left=True,
        )
    )


@meta()
def rpad(col: ColumnOrName, len: int, pad: str) -> Column:
    return Column(
        expression.Pad(
            this=Column.ensure_col(col).column_expression,
            expression=lit(len).column_expression,
            fill_pattern=lit(pad).column_expression,
            # We can use `invoke_expression_over_column` because this is an actual bool instead of literal bool
            is_left=False,
        )
    )


@meta()
def repeat(col: ColumnOrName, n: int) -> Column:
    return Column.invoke_expression_over_column(col, expression.Repeat, times=lit(n))


@meta()
def split(str: ColumnOrName, pattern: str, limit: t.Optional[int] = None) -> Column:
    from sqlframe.base.function_alternatives import (
        split_from_regex_split_to_array,
        split_with_split,
    )

    session = _get_session()

    if session._is_duckdb:
        if limit is not None:
            logger.warning("Limit is ignored since it is not supported in this dialect")
        limit = None

    if session._is_bigquery or session._is_snowflake:
        return split_with_split(str, pattern, limit)

    if session._is_postgres:
        return split_from_regex_split_to_array(str, pattern, limit)

    if limit is not None:
        return Column.invoke_expression_over_column(
            str, expression.RegexpSplit, expression=lit(pattern), limit=lit(limit)
        )
    return Column.invoke_expression_over_column(
        str, expression.RegexpSplit, expression=lit(pattern)
    )


@meta(unsupported_engines="postgres")
def regexp_extract(str: ColumnOrName, pattern: str, idx: t.Optional[int] = None) -> Column:
    session = _get_session()

    if idx is not None:
        result = Column.invoke_expression_over_column(
            str,
            expression.RegexpExtract,
            expression=lit(pattern),
            group=lit(idx),
        )
    else:
        result = Column.invoke_expression_over_column(
            str, expression.RegexpExtract, expression=lit(pattern)
        )

    if session._is_snowflake:
        coalesce_func = get_func_from_session("coalesce")

        result = coalesce_func(result, lit(""))

    return result


@meta()
def regexp_replace(
    str: ColumnOrName, pattern: str, replacement: str, position: t.Optional[int] = None
) -> Column:
    from sqlframe.base.function_alternatives import regexp_replace_global_option

    session = _get_session()

    if session._is_duckdb or session._is_postgres:
        return regexp_replace_global_option(str, pattern, replacement, position)

    if position is not None:
        return Column.invoke_expression_over_column(
            str,
            expression.RegexpReplace,
            expression=lit(pattern),
            replacement=lit(replacement),
            position=lit(position),
        )
    return Column.invoke_expression_over_column(
        str,
        expression.RegexpReplace,
        expression=lit(pattern),
        replacement=lit(replacement),
    )


@meta(unsupported_engines="duckdb")
def initcap(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Initcap)


@meta()
def soundex(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "SOUNDEX")


@meta(unsupported_engines=["postgres", "snowflake"])
def bin(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import bin_bgutil

    session = _get_session()

    if session._is_bigquery:
        return bin_bgutil(col)

    return Column.invoke_anonymous_function(col, "BIN")


@meta(unsupported_engines="postgres")
def hex(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import (
        hex_casted_as_bytes,
        hex_using_encode,
    )

    session = _get_session()

    if session._is_bigquery:
        return hex_casted_as_bytes(col)

    if session._is_snowflake:
        return hex_using_encode(col)

    return Column.invoke_expression_over_column(col, expression.Hex)


@meta(unsupported_engines="postgres")
def unhex(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import unhex_hex_decode_str

    session = _get_session()

    if session._is_snowflake:
        return unhex_hex_decode_str(col)

    return Column.invoke_expression_over_column(col, expression.Unhex)


@meta()
def length(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Length)


@meta(unsupported_engines="duckdb")
def octet_length(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "OCTET_LENGTH")


@meta()
def bit_length(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import bit_length_from_length

    session = _get_session()

    if session._is_bigquery:
        return bit_length_from_length(col)

    return Column.invoke_anonymous_function(col, "BIT_LENGTH")


@meta()
def translate(srcCol: ColumnOrName, matching: str, replace: str) -> Column:
    return Column.invoke_anonymous_function(srcCol, "TRANSLATE", lit(matching), lit(replace))


@meta()
def array(*cols: t.Union[ColumnOrName, t.Iterable[ColumnOrName]]) -> Column:
    columns = _flatten(cols) if not isinstance(cols[0], (str, Column)) else cols
    return Column.invoke_expression_over_column(None, expression.Array, expressions=columns)


@meta(unsupported_engines="*")
def array_agg(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.ArrayAgg)


@meta()
def array_append(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    from sqlframe.base.function_alternatives import (
        array_append_list_append,
        array_append_using_array_cat,
    )

    session = _get_session()

    if session._is_bigquery:
        return array_append_using_array_cat(col, value)

    if session._is_duckdb:
        return array_append_list_append(col, value)

    value = value if isinstance(value, Column) else lit(value)
    return Column.invoke_anonymous_function(col, "ARRAY_APPEND", value)


@meta(unsupported_engines="*")
def array_compact(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ARRAY_COMPACT")


@meta(unsupported_engines="*")
def array_insert(
    col: ColumnOrName, pos: t.Union[ColumnOrName, int], value: ColumnOrLiteral
) -> Column:
    value = value if isinstance(value, Column) else lit(value)
    if isinstance(pos, int):
        pos = lit(pos)
    return Column.invoke_anonymous_function(col, "ARRAY_INSERT", pos, value)  # type: ignore


@meta(unsupported_engines="*")
def array_prepend(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    value = value if isinstance(value, Column) else lit(value)
    return Column.invoke_anonymous_function(col, "ARRAY_PREPEND", value)


@meta()
def array_size(col: ColumnOrName) -> Column:
    session = _get_session()
    if session._is_spark or session._is_databricks:
        return Column.invoke_anonymous_function(col, "ARRAY_SIZE")
    return Column.invoke_expression_over_column(col, expression.ArraySize)


@meta(unsupported_engines="*")
def bit_and(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "BIT_AND")


@meta(unsupported_engines="*")
def bit_or(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "BIT_OR")


@meta(unsupported_engines="*")
def bit_xor(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "BIT_XOR")


@meta(unsupported_engines="*")
def bit_count(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "BIT_COUNT")


@meta(unsupported_engines="*")
def bit_get(col: ColumnOrName, pos: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "BIT_GET", pos)


@meta(unsupported_engines="*")
def getbit(col: ColumnOrName, pos: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "GETBIT", pos)


@meta(unsupported_engines=["bigquery", "postgres"])
def create_map(*cols: t.Union[ColumnOrName, t.Iterable[ColumnOrName]]) -> Column:
    session = _get_session()

    cols = list(_flatten(cols)) if not isinstance(cols[0], (str, Column)) else cols  # type: ignore
    result = Column.invoke_expression_over_column(
        None,
        expression.VarMap,
        keys=array(*cols[::2]).column_expression,
        values=array(*cols[1::2]).column_expression,
    )
    if not session._is_snowflake:
        return result

    col1_dtype = col(cols[0]).dtype or "VARCHAR"
    col2_dtype = col(cols[1]).dtype or "VARCHAR"
    return result.cast(f"MAP({col1_dtype}, {col2_dtype})")


@meta(unsupported_engines=["bigquery", "postgres", "snowflake"])
def map_from_arrays(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(None, expression.Map, keys=col1, values=col2)


@meta()
def array_contains(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    from sqlframe.base.function_alternatives import array_contains_any

    session = _get_session()
    lit_func = get_func_from_session("lit")

    if session._is_postgres:
        return array_contains_any(col, value)

    value = value if isinstance(value, Column) else lit_func(value)

    if session._is_snowflake:
        value = value.cast("variant")

    return Column.invoke_expression_over_column(
        col, expression.ArrayContains, expression=value.column_expression
    )


@meta(unsupported_engines="bigquery")
def arrays_overlap(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import (
        arrays_overlap_as_plural,
        arrays_overlap_renamed,
        arrays_overlap_using_intersect,
    )

    session = _get_session()

    if session._is_duckdb:
        return arrays_overlap_using_intersect(col1, col2)

    if session._is_databricks or session._is_spark:
        return arrays_overlap_renamed(col1, col2)

    if session._is_snowflake:
        return arrays_overlap_as_plural(col1, col2)

    return Column.invoke_expression_over_column(col1, expression.ArrayOverlaps, expression=col2)


@meta()
def slice(
    x: ColumnOrName, start: t.Union[ColumnOrName, int], length: t.Union[ColumnOrName, int]
) -> Column:
    from sqlframe.base.function_alternatives import (
        slice_as_array_slice,
        slice_as_list_slice,
        slice_bgutil,
        slice_with_brackets,
    )

    session = _get_session()

    if session._is_bigquery:
        return slice_bgutil(x, start, length)

    if session._is_duckdb:
        return slice_as_list_slice(x, start, length)

    if session._is_postgres:
        return slice_with_brackets(x, start, length)

    if session._is_snowflake:
        return slice_as_array_slice(x, start, length)

    start_col = lit(start) if isinstance(start, int) else start
    length_col = lit(length) if isinstance(length, int) else length
    return Column.invoke_anonymous_function(x, "SLICE", start_col, length_col)


@meta()
def array_join(
    col: ColumnOrName, delimiter: str, null_replacement: t.Optional[str] = None
) -> Column:
    session = _get_session()

    if session._is_snowflake:
        if null_replacement is not None:
            logger.warning("Null replacement is ignored since it is not supported in this dialect")
        null_replacement = None

    if null_replacement is not None:
        return Column.invoke_expression_over_column(
            col, expression.ArrayToString, expression=lit(delimiter), null=lit(null_replacement)
        )
    return Column.invoke_expression_over_column(
        col, expression.ArrayToString, expression=lit(delimiter)
    )


@meta()
def concat(*cols: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(None, expression.Concat, expressions=cols)


@meta()
def array_position(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    from sqlframe.base.function_alternatives import (
        array_position_bgutil,
        array_position_cast_variant_and_flip,
    )

    session = _get_session()

    if session._is_bigquery:
        return array_position_bgutil(col, value)

    if session._is_snowflake:
        return array_position_cast_variant_and_flip(col, value)

    value_col = value if isinstance(value, Column) else lit(value)
    # Some engines return NULL if item is not found but Spark expects 0 so we coalesce to 0
    return coalesce(Column.invoke_anonymous_function(col, "ARRAY_POSITION", value_col), lit(0))


@meta()
def element_at(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    from sqlframe.base.function_alternatives import element_at_using_brackets

    session = _get_session()

    if session._is_bigquery or session._is_duckdb or session._is_postgres or session._is_snowflake:
        return element_at_using_brackets(col, value)

    value_col = value if isinstance(value, Column) else lit(value)
    return Column.invoke_anonymous_function(col, "ELEMENT_AT", value_col)


@meta()
def array_remove(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    from sqlframe.base.function_alternatives import (
        array_remove_bgutil,
        array_remove_using_filter,
    )

    session = _get_session()

    if session._is_bigquery:
        return array_remove_bgutil(col, value)

    if session._is_duckdb:
        return array_remove_using_filter(col, value)

    value_col = value if isinstance(value, Column) else lit(value)
    return Column.invoke_anonymous_function(col, "ARRAY_REMOVE", value_col)


@meta(unsupported_engines="postgres")
def array_distinct(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import array_distinct_bgutil

    session = _get_session()

    if session._is_bigquery:
        return array_distinct_bgutil(col)

    return Column.invoke_anonymous_function(col, "ARRAY_DISTINCT")


@meta(unsupported_engines=["bigquery", "postgres"])
def array_intersect(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import array_intersect_using_intersection

    session = _get_session()

    if session._is_snowflake:
        return array_intersect_using_intersection(col1, col2)

    return Column.invoke_anonymous_function(col1, "ARRAY_INTERSECT", Column.ensure_col(col2))


@meta(unsupported_engines=["postgres"])
def array_union(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import (
        array_union_using_array_concat,
        array_union_using_list_concat,
    )

    session = _get_session()

    if session._is_duckdb:
        return array_union_using_list_concat(col1, col2)

    if session._is_bigquery or session._is_snowflake:
        return array_union_using_array_concat(col1, col2)

    return Column.invoke_anonymous_function(col1, "ARRAY_UNION", Column.ensure_col(col2))


@meta(unsupported_engines=["bigquery", "duckdb", "postgres"])
def array_except(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col1, "ARRAY_EXCEPT", Column.ensure_col(col2))


@meta()
def explode(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Explode)


@meta(unsupported_engines=["duckdb", "postgres"])
def posexplode(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Posexplode)


@meta(unsupported_engines=["duckdb", "postgres", "snowflake"])
def explode_outer(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.ExplodeOuter)


@meta(unsupported_engines=["duckdb", "postgres", "snowflake"])
def posexplode_outer(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.PosexplodeOuter)


# Snowflake doesn't support JSONPath which is what this function uses
@meta(unsupported_engines="snowflake")
def get_json_object(col: ColumnOrName, path: str) -> Column:
    from sqlframe.base.function_alternatives import (
        get_json_object_using_arrow_op,
        get_json_object_using_function,
    )

    session = _get_session()

    if session._is_databricks:
        return get_json_object_using_function(col, path)

    if session._is_postgres:
        return get_json_object_using_arrow_op(col, path)

    return Column.invoke_expression_over_column(col, expression.JSONExtract, expression=lit(path))


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def json_tuple(col: ColumnOrName, *fields: str) -> Column:
    return Column.invoke_anonymous_function(col, "JSON_TUPLE", *[lit(field) for field in fields])


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def from_json(
    col: ColumnOrName,
    schema: t.Union[ArrayType, StructType, Column, str],
    options: t.Optional[t.Dict[str, str]] = None,
) -> Column:
    from sqlframe.base.types import ArrayType, StructType

    if isinstance(schema, (ArrayType, StructType)):
        schema = schema.simpleString()
    schema = schema if isinstance(schema, Column) else lit(schema)
    if options is not None:
        options_col = create_map([lit(str(x)) for x in _flatten(options.items())])
        return Column.invoke_anonymous_function(col, "FROM_JSON", schema, options_col)
    return Column.invoke_anonymous_function(col, "FROM_JSON", schema)


@meta(unsupported_engines=["bigquery", "postgres", "snowflake"])
def to_json(col: ColumnOrName, options: t.Optional[t.Dict[str, str]] = None) -> Column:
    session = _get_session()
    if session._is_duckdb:
        # check if Spark options match DuckDB's default behavior
        is_spark_equivalent = (
            options
            and list(options.keys()) == ["ignoreNullFields"]  # only one option specified
            and str(options.get("ignoreNullFields", "true")).lower() == "false"
        )
        if not is_spark_equivalent:
            logger.warning(
                "Options for `to_json()` ignored, since not supported in this dialect."
                + " Potential `null` values are included in the returned JSON string."
                + " This is different from Spark's default behavior."
            )
        options = None

    if options is not None:
        options_col = create_map([lit(str(x)) for x in _flatten(options.items())])
        return Column.invoke_expression_over_column(col, expression.JSONFormat, options=options_col)
    return Column.invoke_expression_over_column(col, expression.JSONFormat)


@meta(unsupported_engines="*")
def schema_of_json(col: ColumnOrName, options: t.Optional[t.Dict[str, str]] = None) -> Column:
    if isinstance(col, str):
        col = lit(col)
    if options is not None:
        options_col = create_map([lit(str(x)) for x in _flatten(options.items())])
        return Column.invoke_anonymous_function(col, "SCHEMA_OF_JSON", options_col)
    return Column.invoke_anonymous_function(col, "SCHEMA_OF_JSON")


@meta(unsupported_engines="*")
def schema_of_csv(col: ColumnOrName, options: t.Optional[t.Dict[str, str]] = None) -> Column:
    if isinstance(col, str):
        col = lit(col)
    if options is not None:
        options_col = create_map([lit(str(x)) for x in _flatten(options.items())])
        return Column.invoke_anonymous_function(col, "SCHEMA_OF_CSV", options_col)
    return Column.invoke_anonymous_function(col, "SCHEMA_OF_CSV")


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def to_csv(col: ColumnOrName, options: t.Optional[t.Dict[str, str]] = None) -> Column:
    if options is not None:
        options_col = create_map([lit(str(x)) for x in _flatten(options.items())])
        return Column.invoke_anonymous_function(col, "TO_CSV", options_col)
    return Column.invoke_anonymous_function(col, "TO_CSV")


@meta()
def size(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.ArraySize)


@meta()
def array_min(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import (
        array_min_bgutil,
        array_min_from_sort,
        array_min_from_subquery,
    )

    session = _get_session()

    if session._is_bigquery:
        return array_min_bgutil(col)

    if session._is_duckdb:
        return array_min_from_sort(col)

    if session._is_postgres:
        return array_min_from_subquery(col)

    return Column.invoke_anonymous_function(col, "ARRAY_MIN")


@meta()
def array_max(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import (
        array_max_bgutil,
        array_max_from_sort,
        array_max_from_subquery,
    )

    session = _get_session()

    if session._is_bigquery:
        return array_max_bgutil(col)

    if session._is_duckdb:
        return array_max_from_sort(col)

    if session._is_postgres:
        return array_max_from_subquery(col)

    return Column.invoke_anonymous_function(col, "ARRAY_MAX")


@meta(unsupported_engines="postgres")
def sort_array(col: ColumnOrName, asc: t.Optional[bool] = None) -> Column:
    from sqlframe.base.function_alternatives import (
        sort_array_bgutil,
        sort_array_using_array_sort,
    )

    session = _get_session()

    if session._is_bigquery:
        return sort_array_bgutil(col, asc)

    if session._is_snowflake:
        return sort_array_using_array_sort(col, asc)

    if asc is not None:
        return Column.invoke_expression_over_column(col, expression.SortArray, asc=lit(asc))
    return Column.invoke_expression_over_column(col, expression.SortArray)


@meta(unsupported_engines="postgres")
def array_sort(
    col: ColumnOrName,
    comparator: t.Optional[t.Union[t.Callable[[Column, Column], Column]]] = None,
) -> Column:
    session = _get_session()
    sort_array_func = get_func_from_session("sort_array")

    if session._is_bigquery:
        return sort_array_func(col, comparator)

    if comparator is not None:
        f_expression = _get_lambda_from_func(comparator)
        return Column.invoke_expression_over_column(
            col, expression.ArraySort, expression=f_expression
        )
    return Column.invoke_expression_over_column(col, expression.ArraySort)


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def shuffle(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "SHUFFLE")


@meta(unsupported_engines="snowflake")
def reverse(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "REVERSE")


@meta(unsupported_engines=["bigquery", "postgres"])
def flatten(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import flatten_using_array_flatten

    session = _get_session()

    if session._is_snowflake:
        return flatten_using_array_flatten(col)

    return Column.invoke_expression_over_column(col, expression.Flatten)


@meta(unsupported_engines=["bigquery", "duckdb", "postgres"])
def map_keys(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "MAP_KEYS")


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def map_values(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "MAP_VALUES")


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def map_entries(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "MAP_ENTRIES")


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def map_from_entries(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.MapFromEntries)


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def array_repeat(col: ColumnOrName, count: t.Union[ColumnOrName, int]) -> Column:
    count_col = count if isinstance(count, Column) else lit(count)
    return Column.invoke_anonymous_function(col, "ARRAY_REPEAT", count_col)


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def arrays_zip(*cols: ColumnOrName) -> Column:
    if len(cols) == 1:
        return Column.invoke_anonymous_function(cols[0], "ARRAYS_ZIP")
    return Column.invoke_anonymous_function(cols[0], "ARRAYS_ZIP", *cols[1:])


@meta(unsupported_engines=["bigquery", "duckdb", "postgres"])
def map_concat(*cols: t.Union[ColumnOrName, t.Iterable[ColumnOrName]]) -> Column:
    from sqlframe.base.function_alternatives import map_concat_using_map_cat

    session = _get_session()

    if session._is_snowflake:
        return map_concat_using_map_cat(*cols)

    columns = list(flatten(cols)) if not isinstance(cols[0], (str, Column)) else cols  # type: ignore
    if len(columns) == 1:
        return Column.invoke_anonymous_function(columns[0], "MAP_CONCAT")  # type: ignore
    return Column.invoke_anonymous_function(columns[0], "MAP_CONCAT", *columns[1:])  # type: ignore


@meta(unsupported_engines="postgres")
def sequence(
    start: ColumnOrName, stop: ColumnOrName, step: t.Optional[ColumnOrName] = None
) -> Column:
    from sqlframe.base.function_alternatives import (
        sequence_from_array_generate_range,
        sequence_from_generate_array,
        sequence_from_generate_series,
    )

    session = _get_session()

    if session._is_bigquery:
        return sequence_from_generate_array(start, stop, step)

    if session._is_duckdb:
        return sequence_from_generate_series(start, stop, step)

    if session._is_snowflake:
        return sequence_from_array_generate_range(start, stop, step)

    return Column(
        expression.GenerateSeries(
            start=Column.ensure_col(start).column_expression,
            end=Column.ensure_col(stop).column_expression,
            step=Column.ensure_col(step).column_expression if step is not None else None,
        )
    )


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def from_csv(
    col: ColumnOrName,
    schema: t.Union[Column, str],
    options: t.Optional[t.Dict[str, str]] = None,
) -> Column:
    schema = schema if isinstance(schema, Column) else lit(schema)
    if options is not None:
        option_cols = create_map(
            [lit(str(x) if isinstance(x, bool) else x) for x in _flatten(options.items())]
        )
        return Column.invoke_anonymous_function(col, "FROM_CSV", schema, option_cols)
    return Column.invoke_anonymous_function(col, "FROM_CSV", schema)


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def aggregate(
    col: ColumnOrName,
    initialValue: ColumnOrName,
    merge: t.Callable[[Column, Column], Column],
    finish: t.Optional[t.Callable[[Column], Column]] = None,
) -> Column:
    merge_exp = _get_lambda_from_func(merge)
    if finish is not None:
        finish_exp = _get_lambda_from_func(finish)
        return Column.invoke_expression_over_column(
            col,
            expression.Reduce,
            initial=initialValue,
            merge=Column(merge_exp),
            finish=Column(finish_exp),
        )
    return Column.invoke_expression_over_column(
        col, expression.Reduce, initial=initialValue, merge=Column(merge_exp)
    )


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def transform(
    col: ColumnOrName,
    f: t.Union[t.Callable[[Column], Column], t.Callable[[Column, Column], Column]],
) -> Column:
    f_expression = _get_lambda_from_func(f)
    return Column.invoke_expression_over_column(
        col, expression.Transform, expression=Column(f_expression)
    )


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def exists(col: ColumnOrName, f: t.Callable[[Column], Column]) -> Column:
    f_expression = _get_lambda_from_func(f)
    return Column.invoke_anonymous_function(col, "EXISTS", Column(f_expression))


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def forall(col: ColumnOrName, f: t.Callable[[Column], Column]) -> Column:
    f_expression = _get_lambda_from_func(f)
    return Column.invoke_anonymous_function(col, "FORALL", Column(f_expression))


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def filter(
    col: ColumnOrName,
    f: t.Union[t.Callable[[Column], Column], t.Callable[[Column, Column], Column]],
) -> Column:
    f_expression = _get_lambda_from_func(f)
    return Column.invoke_expression_over_column(
        col, expression.ArrayFilter, expression=f_expression
    )


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def zip_with(
    left: ColumnOrName, right: ColumnOrName, f: t.Callable[[Column, Column], Column]
) -> Column:
    f_expression = _get_lambda_from_func(f)
    return Column.invoke_anonymous_function(left, "ZIP_WITH", right, Column(f_expression))


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def transform_keys(col: ColumnOrName, f: t.Union[t.Callable[[Column, Column], Column]]) -> Column:
    f_expression = _get_lambda_from_func(f)
    return Column.invoke_anonymous_function(col, "TRANSFORM_KEYS", Column(f_expression))


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def transform_values(col: ColumnOrName, f: t.Union[t.Callable[[Column, Column], Column]]) -> Column:
    f_expression = _get_lambda_from_func(f)
    return Column.invoke_anonymous_function(col, "TRANSFORM_VALUES", Column(f_expression))


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def map_filter(col: ColumnOrName, f: t.Union[t.Callable[[Column, Column], Column]]) -> Column:
    f_expression = _get_lambda_from_func(f)
    return Column.invoke_anonymous_function(col, "MAP_FILTER", Column(f_expression))


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def map_zip_with(
    col1: ColumnOrName,
    col2: ColumnOrName,
    f: t.Union[t.Callable[[Column, Column, Column], Column]],
) -> Column:
    f_expression = _get_lambda_from_func(f)
    return Column.invoke_anonymous_function(col1, "MAP_ZIP_WITH", col2, Column(f_expression))


@meta()
def typeof(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import (
        typeof_bgutil,
        typeof_from_variant,
        typeof_pg_typeof,
    )

    session = _get_session()

    if session._is_bigquery:
        return typeof_bgutil(col)

    if session._is_postgres:
        return typeof_pg_typeof(col)

    if session._is_snowflake:
        return typeof_from_variant(col)

    return Column.invoke_anonymous_function(col, "TYPEOF")


@meta()
def nullif(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col1, expression.Nullif, expression=col2)


@meta(unsupported_engines="*")
def stack(*cols: ColumnOrName) -> Column:
    columns = [Column.ensure_col(x) for x in cols]
    return Column.invoke_anonymous_function(
        columns[0], "STACK", *columns[1:] if len(columns) > 1 else []
    )


@meta(unsupported_engines="*")
def make_interval(
    years: t.Optional[ColumnOrName] = None,
    months: t.Optional[ColumnOrName] = None,
    weeks: t.Optional[ColumnOrName] = None,
    days: t.Optional[ColumnOrName] = None,
    hours: t.Optional[ColumnOrName] = None,
    mins: t.Optional[ColumnOrName] = None,
    secs: t.Optional[ColumnOrName] = None,
) -> Column:
    columns = _ensure_column_of_optionals([years, months, weeks, days, hours, mins, secs])
    if not columns:
        raise ValueError("At least one value must be provided")
    return Column.invoke_anonymous_function(columns[0], "MAKE_INTERVAL", *columns[1:])


@meta(unsupported_engines="*")
def try_add(left: ColumnOrName, right: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(left, "TRY_ADD", right)


@meta(unsupported_engines="*")
def try_avg(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "TRY_AVG")


@meta(unsupported_engines="*")
def try_divide(left: ColumnOrName, right: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(left, "TRY_DIVIDE", right)


@meta(unsupported_engines="*")
def try_multiply(left: ColumnOrName, right: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(left, "TRY_MULTIPLY", right)


@meta(unsupported_engines="*")
def try_subtract(left: ColumnOrName, right: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(left, "TRY_SUBTRACT", right)


@meta(unsupported_engines="*")
def try_sum(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "TRY_SUM")


@meta(unsupported_engines="*")
def try_to_binary(col: ColumnOrName, format: t.Optional[ColumnOrName] = None) -> Column:
    if format is not None:
        return Column.invoke_anonymous_function(col, "TRY_TO_BINARY", format)
    return Column.invoke_anonymous_function(col, "TRY_TO_BINARY")


@meta(unsupported_engines="*")
def try_to_number(col: ColumnOrName, format: t.Optional[ColumnOrName] = None) -> Column:
    if format is not None:
        return Column.invoke_anonymous_function(col, "TRY_TO_NUMBER", format)
    return Column.invoke_anonymous_function(col, "TRY_TO_NUMBER")


@meta(unsupported_engines="*")
def aes_decrypt(
    input: ColumnOrName,
    key: ColumnOrName,
    mode: t.Optional[ColumnOrName] = None,
    padding: t.Optional[ColumnOrName] = None,
    aad: t.Optional[ColumnOrName] = None,
) -> Column:
    columns = _ensure_column_of_optionals([key, mode, padding, aad])
    return Column.invoke_anonymous_function(input, "AES_DECRYPT", *columns)


@meta(unsupported_engines="*")
def aes_encrypt(
    input: ColumnOrName,
    key: ColumnOrName,
    mode: t.Optional[ColumnOrName] = None,
    padding: t.Optional[ColumnOrName] = None,
    iv: t.Optional[ColumnOrName] = None,
    aad: t.Optional[ColumnOrName] = None,
) -> Column:
    columns = _ensure_column_of_optionals([key, mode, padding, iv, aad])
    return Column.invoke_anonymous_function(input, "AES_ENCRYPT", *columns)


@meta(unsupported_engines="*")
def bitmap_bit_position(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "BITMAP_BIT_POSITION")


@meta(unsupported_engines="*")
def bitmap_bucket_number(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "BITMAP_BUCKET_NUMBER")


@meta(unsupported_engines="*")
def bitmap_construct_agg(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "BITMAP_CONSTRUCT_AGG")


@meta(unsupported_engines="*")
def bitmap_count(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "BITMAP_COUNT")


@meta(unsupported_engines="*")
def bitmap_or_agg(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "BITMAP_OR_AGG")


@meta(unsupported_engines="*")
def to_binary(col: ColumnOrName, format: t.Optional[ColumnOrName] = None) -> Column:
    if format is not None:
        return Column.invoke_anonymous_function(col, "TO_BINARY", format)
    return Column.invoke_anonymous_function(col, "TO_BINARY")


@meta()
def any_value(col: ColumnOrName, ignoreNulls: t.Optional[t.Union[bool, Column]] = None) -> Column:
    session = _get_session()

    if session._is_duckdb:
        if not ignoreNulls:
            logger.warning("Nulls are always ignored when using `ANY_VALUE` on this engine")
        ignoreNulls = None

    if session._is_bigquery or session._is_postgres or session._is_snowflake:
        if ignoreNulls:
            logger.warning("Ignoring nulls is not supported in this dialect")
        ignoreNulls = None

    column = Column.invoke_expression_over_column(col, expression.AnyValue)
    if ignoreNulls:
        return Column(expression.IgnoreNulls(this=column.column_expression))
    return column


@meta(unsupported_engines="*")
def approx_percentile(
    col: ColumnOrName,
    percentage: t.Union[Column, float, t.List[float], t.Tuple[float]],
    accuracy: t.Union[Column, float] = 10000,
) -> Column:
    percentage = lit(percentage) if not isinstance(accuracy, Column) else percentage
    accuracy = lit(accuracy) if not isinstance(accuracy, Column) else accuracy

    return Column.invoke_expression_over_column(
        col, expression.ApproxQuantile, quantile=percentage, accuracy=accuracy
    )


@meta()
def bool_and(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.LogicalAnd)


@meta()
def bool_or(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.LogicalOr)


@meta()
def btrim(str: ColumnOrName, trim: t.Optional[ColumnOrName] = None) -> Column:
    if trim is not None:
        return Column.invoke_expression_over_column(
            str, expression.Trim, expression=Column.ensure_col(trim).column_expression
        )
    else:
        return Column.invoke_expression_over_column(str, expression.Trim)


@meta(unsupported_engines="*")
def bucket(numBuckets: t.Union[Column, int], col: ColumnOrName) -> Column:
    numBuckets = lit(numBuckets) if isinstance(numBuckets, int) else numBuckets
    return Column.invoke_anonymous_function(numBuckets, "bucket", col)


@meta()
def call_function(funcName: str, *cols: ColumnOrName) -> Column:
    cols = ensure_list(cols)  # type: ignore
    if len(cols) > 1:
        return Column.invoke_anonymous_function(cols[0], funcName, *cols[1:])
    elif len(cols) == 1:
        return Column.invoke_anonymous_function(cols[0], funcName)
    return Column.invoke_anonymous_function(None, funcName)


# @meta(unsupported_engines="*")
# def call_udf(udfName: str, *cols: ColumnOrName) -> Column:
#     """
#     Call an user-defined function.
#
#     .. versionadded:: 3.4.0
#
#     Parameters
#     ----------
#     udfName : str
#         name of the user defined function (UDF)
#     cols : :class:`~pyspark.sql.Column` or str
#         column names or :class:`~pyspark.sql.Column`\\s to be used in the UDF
#
#     Returns
#     -------
#     :class:`~pyspark.sql.Column`
#         result of executed udf.
#
#     Examples
#     --------
#     >>> from pyspark.sql.functions import call_udf, col
#     >>> from pyspark.sql.types import IntegerType, StringType
#     >>> df = spark.createDataFrame([(1, "a"),(2, "b"), (3, "c")],["id", "name"])
#     >>> _ = spark.udf.register("intX2", lambda i: i * 2, IntegerType())
#     >>> df.select(call_udf("intX2", "id")).show()
#     +---------+
#     |intX2(id)|
#     +---------+
#     |        2|
#     |        4|
#     |        6|
#     +---------+
#     >>> _ = spark.udf.register("strX2", lambda s: s * 2, StringType())
#     >>> df.select(call_udf("strX2", col("name"))).show()
#     +-----------+
#     |strX2(name)|
#     +-----------+
#     |         aa|
#     |         bb|
#     |         cc|
#     +-----------+
#     """
#     sc = get_active_spark_context()
#     return _invoke_function("call_udf", udfName, _to_seq(sc, cols, _to_java_column))
#
#
# @pytest.mark.parametrize(
#     "expression, expected",
#     [
#         (SF.call_udf("cola"), "CALL_UDF(cola)"),
#         (SF.call_udf(SF.col("cola")), "CALL_UDF(cola)"),
#     ],
# )
# def test_call_udf(expression, expected):
#     assert expression.sql() == expected
#
# def test_call_udf(get_session_and_func, get_func):
#     session, call_udf = get_session_and_func("call_udf")
#         >>> from pyspark.sql.functions import call_udf, col
#     >>> from pyspark.sql.types import IntegerType, StringType
#     >>> df = spark.createDataFrame([(1, "a"),(2, "b"), (3, "c")],["id", "name"])
#     >>> _ = spark.udf.register("intX2", lambda i: i * 2, IntegerType())
#     >>> df.select(call_udf("intX2", "id")).show()
#     +---------+
#     |intX2(id)|
#     +---------+
#     |        2|
#     |        4|
#     |        6|
#     +---------+
#     >>> _ = spark.udf.register("strX2", lambda s: s * 2, StringType())
#     >>> df.select(call_udf("strX2", col("name"))).show()
#     +-----------+
#     |strX2(name)|
#     +-----------+
#     |         aa|
#     |         bb|
#     |         cc|
#     +-----------+


@meta(unsupported_engines="*")
def cardinality(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "cardinality")


@meta()
def char(col: ColumnOrName) -> Column:
    return Column(expression.Chr(expressions=Column.ensure_col(col).column_expression))


@meta()
def char_length(str: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(str, expression.Length)


@meta()
def character_length(str: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(str, expression.Length)


@meta(unsupported_engines=["bigquery", "postgres"])
def contains(left: ColumnOrName, right: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(
        left, expression.Contains, expression=Column.ensure_col(right).column_expression
    )


@meta(unsupported_engines=["bigquery", "postgres"])
def convert_timezone(
    sourceTz: t.Optional[Column], targetTz: Column, sourceTs: ColumnOrName
) -> Column:
    to_timestamp = get_func_from_session("to_timestamp")

    return Column(
        expression.ConvertTimezone(
            timestamp=to_timestamp(Column.ensure_col(sourceTs)).column_expression,
            source_tz=sourceTz.column_expression if sourceTz else None,
            target_tz=Column.ensure_col(targetTz).column_expression,
        )
    )


@meta(unsupported_engines="postgres")
def count_if(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.CountIf)


@meta(unsupported_engines="*")
def count_min_sketch(
    col: ColumnOrName,
    eps: ColumnOrName,
    confidence: ColumnOrName,
    seed: ColumnOrName,
) -> Column:
    eps = Column.ensure_col(eps).cast("double")
    confidence = Column.ensure_col(confidence).cast("double")
    return Column.invoke_anonymous_function(col, "count_min_sketch", eps, confidence, seed)


@meta(unsupported_engines="*")
def curdate() -> Column:
    """
    Returns the current date at the start of query evaluation as a :class:`DateType` column.
    All calls of current_date within the same query return the same value.

    .. versionadded:: 3.5.0

    Returns
    -------
    :class:`~pyspark.sql.Column`
        current date.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.range(1).select(sf.curdate()).show() # doctest: +SKIP
    +--------------+
    |current_date()|
    +--------------+
    |    2022-08-26|
    +--------------+
    """
    return Column.invoke_anonymous_function(None, "curdate")


@meta(unsupported_engines="*")
def current_catalog() -> Column:
    """Returns the current catalog.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> spark.range(1).select(current_catalog()).show()
    +-----------------+
    |current_catalog()|
    +-----------------+
    |    spark_catalog|
    +-----------------+
    """
    return Column.invoke_anonymous_function(None, "current_catalog")


@meta(unsupported_engines="*")
def current_database() -> Column:
    """Returns the current database.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> spark.range(1).select(current_database()).show()
    +------------------+
    |current_database()|
    +------------------+
    |           default|
    +------------------+
    """
    return Column.invoke_anonymous_function(None, "current_database")


current_schema = current_database


@meta(unsupported_engines=["*", "databricks"])
def current_timezone() -> Column:
    return Column.invoke_anonymous_function(None, "current_timezone")


@meta()
def current_user() -> Column:
    from sqlframe.base.function_alternatives import current_user_from_session_user

    session = _get_session()

    if session._is_bigquery:
        return current_user_from_session_user()

    return Column.invoke_expression_over_column(None, expression.CurrentUser)


@meta(unsupported_engines="*")
def date_from_unix_date(days: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(days, "date_from_unix_date")


@meta(unsupported_engines="*")
def date_part(field: ColumnOrName, source: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(field, "date_part", source)


dateadd = date_add
datediff = date_diff


@meta(unsupported_engines="*")
def datepart(field: ColumnOrName, source: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(field, "datepart", source)


@meta(unsupported_engines=["bigquery", "postgres", "snowflake"])
def day(col: ColumnOrName) -> Column:
    session = _get_session()

    if session._is_duckdb:
        try_to_timestamp = get_func_from_session("try_to_timestamp")
        to_date = get_func_from_session("to_date")
        _is_string = get_func_from_session("_is_string")
        coalesce = get_func_from_session("coalesce")
        col = coalesce(try_to_timestamp(Column.ensure_col(col).cast("VARCHAR")), to_date(col))

    return Column.invoke_expression_over_column(col, expression.Day)


@meta(unsupported_engines="*")
def days(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "days")


@meta(unsupported_engines="*")
def elt(*inputs: ColumnOrName) -> Column:
    inputs = ensure_list(inputs)  # type: ignore
    if len(inputs) > 1:
        return Column.invoke_anonymous_function(inputs[0], "elt", *inputs[1:])
    return Column.invoke_anonymous_function(inputs[0], "elt")


@meta()
def endswith(str: ColumnOrName, suffix: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import (
        endswith_using_like,
        endswith_with_underscore,
    )

    session = _get_session()

    if session._is_bigquery or session._is_duckdb:
        return endswith_with_underscore(str, suffix)

    if session._is_postgres:
        return endswith_using_like(str, suffix)

    return Column.invoke_anonymous_function(str, "endswith", suffix)


@meta(unsupported_engines="*")
def equal_null(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col1, "equal_null", col2)


@meta(unsupported_engines="*")
def every(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "every")


@meta()
def extract(field: ColumnOrName, source: ColumnOrName) -> Column:
    session = _get_session()

    if session._is_bigquery:
        field = expression.Var(this=Column.ensure_col(field).alias_or_name)  # type: ignore

    return Column.invoke_expression_over_column(field, expression.Extract, expression=source)


@meta(unsupported_engines="*")
def find_in_set(str: ColumnOrName, str_array: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(str, "find_in_set", str_array)


@meta(unsupported_engines="*")
def first_value(col: ColumnOrName, ignoreNulls: t.Optional[t.Union[bool, Column]] = None) -> Column:
    column = Column.invoke_expression_over_column(col, expression.FirstValue)

    if ignoreNulls:
        return Column(expression.IgnoreNulls(this=column.column_expression))
    return column


@meta(unsupported_engines="*")
def get(col: ColumnOrName, index: t.Union[ColumnOrName, int]) -> Column:
    index = lit(index) if isinstance(index, int) else index

    return Column.invoke_anonymous_function(col, "get", index)


@meta(unsupported_engines=["*", "databricks"])
def get_active_spark_context() -> SparkContext:
    """Raise RuntimeError if SparkContext is not initialized,
    otherwise, returns the active SparkContext."""
    from sqlframe.base.session import _BaseSession
    from sqlframe.spark.session import SparkSession

    session: _BaseSession = _BaseSession()
    if not isinstance(session, SparkSession):
        raise RuntimeError("This function is only available in SparkSession.")
    return session.spark_session.sparkContext


@meta(unsupported_engines="*")
def grouping(col: ColumnOrName) -> Column:
    """
    Aggregate function: indicates whether a specified column in a GROUP BY list is aggregated
    or not, returns 1 for aggregated or 0 for not aggregated in the result set.

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        column to check if it's aggregated.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        returns 1 for aggregated or 0 for not aggregated in the result set.

    Examples
    --------
    >>> df = spark.createDataFrame([("Alice", 2), ("Bob", 5)], ("name", "age"))
    >>> df.cube("name").agg(grouping("name"), sum("age")).orderBy("name").show()
    +-----+--------------+--------+
    | name|grouping(name)|sum(age)|
    +-----+--------------+--------+
    | NULL|             1|       7|
    |Alice|             0|       2|
    |  Bob|             0|       5|
    +-----+--------------+--------+
    """
    return Column.invoke_anonymous_function(col, "grouping")


@meta(unsupported_engines="*")
def histogram_numeric(col: ColumnOrName, nBins: ColumnOrName) -> Column:
    """Computes a histogram on numeric 'col' using nb bins.
    The return value is an array of (x,y) pairs representing the centers of the
    histogram's bins. As the value of 'nb' is increased, the histogram approximation
    gets finer-grained, but may yield artifacts around outliers. In practice, 20-40
    histogram bins appear to work well, with more bins being required for skewed or
    smaller datasets. Note that this function creates a histogram with non-uniform
    bin widths. It offers no guarantees in terms of the mean-squared-error of the
    histogram, but in practice is comparable to the histograms produced by the R/S-Plus
    statistical computing packages. Note: the output type of the 'x' field in the return value is
    propagated from the input value consumed in the aggregate function.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.
    nBins : :class:`~pyspark.sql.Column` or str
        number of Histogram columns.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        a histogram on numeric 'col' using nb bins.

    Examples
    --------
    >>> df = spark.createDataFrame([("a", 1),
    ...                             ("a", 2),
    ...                             ("a", 3),
    ...                             ("b", 8),
    ...                             ("b", 2)], ["c1", "c2"])
    >>> df.select(histogram_numeric('c2', lit(5))).show()
    +------------------------+
    |histogram_numeric(c2, 5)|
    +------------------------+
    |    [{1, 1.0}, {2, 1....|
    +------------------------+
    """
    return Column.invoke_anonymous_function(col, "histogram_numeric", nBins)


@meta(unsupported_engines="*")
def hll_sketch_agg(col: ColumnOrName, lgConfigK: t.Optional[t.Union[int, Column]] = None) -> Column:
    """
    Aggregate function: returns the updatable binary representation of the Datasketches
    HllSketch configured with lgConfigK arg.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str or int
    lgConfigK : int, optional
        The log-base-2 of K, where K is the number of buckets or slots for the HllSketch

    Returns
    -------
    :class:`~pyspark.sql.Column`
        The binary representation of the HllSketch.

    Examples
    --------
    >>> df = spark.createDataFrame([1,2,2,3], "INT")
    >>> df1 = df.agg(hll_sketch_estimate(hll_sketch_agg("value")).alias("distinct_cnt"))
    >>> df1.show()
    +------------+
    |distinct_cnt|
    +------------+
    |           3|
    +------------+
    >>> df2 = df.agg(hll_sketch_estimate(
    ...     hll_sketch_agg("value", lit(12))
    ... ).alias("distinct_cnt"))
    >>> df2.show()
    +------------+
    |distinct_cnt|
    +------------+
    |           3|
    +------------+
    >>> df3 = df.agg(hll_sketch_estimate(
    ...     hll_sketch_agg(col("value"), lit(12))).alias("distinct_cnt"))
    >>> df3.show()
    +------------+
    |distinct_cnt|
    +------------+
    |           3|
    +------------+
    """
    if lgConfigK is None:
        return Column.invoke_anonymous_function(col, "hll_sketch_agg")
    else:
        _lgConfigK = lit(lgConfigK) if isinstance(lgConfigK, int) else lgConfigK
        return Column.invoke_anonymous_function(col, "hll_sketch_agg", _lgConfigK)


@meta(unsupported_engines="*")
def hll_sketch_estimate(col: ColumnOrName) -> Column:
    """
    Returns the estimated number of unique values given the binary representation
    of a Datasketches HllSketch.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str

    Returns
    -------
    :class:`~pyspark.sql.Column`
        The estimated number of unique values for the HllSketch.

    Examples
    --------
    >>> df = spark.createDataFrame([1,2,2,3], "INT")
    >>> df = df.agg(hll_sketch_estimate(hll_sketch_agg("value")).alias("distinct_cnt"))
    >>> df.show()
    +------------+
    |distinct_cnt|
    +------------+
    |           3|
    +------------+
    """
    return Column.invoke_anonymous_function(col, "hll_sketch_estimate")


@meta(unsupported_engines="*")
def hll_union(
    col1: ColumnOrName, col2: ColumnOrName, allowDifferentLgConfigK: t.Optional[bool] = None
) -> Column:
    """
    Merges two binary representations of Datasketches HllSketch objects, using a
    Datasketches Union object.  Throws an exception if sketches have different
    lgConfigK values and allowDifferentLgConfigK is unset or set to false.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col1 : :class:`~pyspark.sql.Column` or str
    col2 : :class:`~pyspark.sql.Column` or str
    allowDifferentLgConfigK : bool, optional
        Allow sketches with different lgConfigK values to be merged (defaults to false).

    Returns
    -------
    :class:`~pyspark.sql.Column`
        The binary representation of the merged HllSketch.

    Examples
    --------
    >>> df = spark.createDataFrame([(1,4),(2,5),(2,5),(3,6)], "struct<v1:int,v2:int>")
    >>> df = df.agg(hll_sketch_agg("v1").alias("sketch1"), hll_sketch_agg("v2").alias("sketch2"))
    >>> df = df.withColumn("distinct_cnt", hll_sketch_estimate(hll_union("sketch1", "sketch2")))
    >>> df.drop("sketch1", "sketch2").show()
    +------------+
    |distinct_cnt|
    +------------+
    |           6|
    +------------+
    """
    if allowDifferentLgConfigK is not None:
        return Column.invoke_anonymous_function(
            col1, "hll_union", col2, lit(allowDifferentLgConfigK)
        )
    else:
        return Column.invoke_anonymous_function(col1, "hll_union", col2)


@meta(unsupported_engines="*")
def hll_union_agg(
    col: ColumnOrName, allowDifferentLgConfigK: t.Optional[t.Union[bool, Column]] = None
) -> Column:
    """
    Aggregate function: returns the updatable binary representation of the Datasketches
    HllSketch, generated by merging previously created Datasketches HllSketch instances
    via a Datasketches Union instance. Throws an exception if sketches have different
    lgConfigK values and allowDifferentLgConfigK is unset or set to false.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str or bool
    allowDifferentLgConfigK : bool, optional
        Allow sketches with different lgConfigK values to be merged (defaults to false).

    Returns
    -------
    :class:`~pyspark.sql.Column`
        The binary representation of the merged HllSketch.

    Examples
    --------
    >>> df1 = spark.createDataFrame([1,2,2,3], "INT")
    >>> df1 = df1.agg(hll_sketch_agg("value").alias("sketch"))
    >>> df2 = spark.createDataFrame([4,5,5,6], "INT")
    >>> df2 = df2.agg(hll_sketch_agg("value").alias("sketch"))
    >>> df3 = df1.union(df2).agg(hll_sketch_estimate(
    ...     hll_union_agg("sketch")
    ... ).alias("distinct_cnt"))
    >>> df3.drop("sketch").show()
    +------------+
    |distinct_cnt|
    +------------+
    |           6|
    +------------+
    >>> df4 = df1.union(df2).agg(hll_sketch_estimate(
    ...     hll_union_agg("sketch", lit(False))
    ... ).alias("distinct_cnt"))
    >>> df4.drop("sketch").show()
    +------------+
    |distinct_cnt|
    +------------+
    |           6|
    +------------+
    >>> df5 = df1.union(df2).agg(hll_sketch_estimate(
    ...     hll_union_agg(col("sketch"), lit(False))
    ... ).alias("distinct_cnt"))
    >>> df5.drop("sketch").show()
    +------------+
    |distinct_cnt|
    +------------+
    |           6|
    +------------+
    """
    if allowDifferentLgConfigK is None:
        return Column.invoke_anonymous_function(col, "hll_union_agg")
    else:
        _allowDifferentLgConfigK = (
            lit(allowDifferentLgConfigK)
            if isinstance(allowDifferentLgConfigK, bool)
            else allowDifferentLgConfigK
        )
        return Column.invoke_anonymous_function(col, "hll_union_agg", _allowDifferentLgConfigK)


@meta(unsupported_engines="*")
def hours(col: ColumnOrName) -> Column:
    """
    Partition transform function: A transform for timestamps
    to partition data into hours.

    .. versionadded:: 3.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target date or timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        data partitioned by hours.

    Examples
    --------
    >>> df.writeTo("catalog.db.table").partitionedBy(   # doctest: +SKIP
    ...     hours("ts")
    ... ).createOrReplace()

    Notes
    -----
    This function can be used only in combination with
    :py:meth:`~pyspark.sql.readwriter.DataFrameWriterV2.partitionedBy`
    method of the `DataFrameWriterV2`.

    """
    return Column.invoke_anonymous_function(col, "hours")


@meta()
def ifnull(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """
    Returns `col2` if `col1` is null, or `col1` otherwise.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col1 : :class:`~pyspark.sql.Column` or str
    col2 : :class:`~pyspark.sql.Column` or str

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> df = spark.createDataFrame([(None,), (1,)], ["e"])
    >>> df.select(sf.ifnull(df.e, sf.lit(8))).show()
    +------------+
    |ifnull(e, 8)|
    +------------+
    |           8|
    |           1|
    +------------+
    """
    return Column.invoke_expression_over_column(col1, expression.Coalesce, expressions=[col2])


@meta(unsupported_engines="*")
def ilike(
    str: ColumnOrName, pattern: ColumnOrName, escapeChar: t.Optional["Column"] = None
) -> Column:
    """
    Returns true if str matches `pattern` with `escape` case-insensitively,
    null if any arguments are null, false otherwise.
    The default escape character is the '\'.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        A string.
    pattern : :class:`~pyspark.sql.Column` or str
        A string. The pattern is a string which is matched literally, with
        exception to the following special symbols:
        _ matches any one character in the input (similar to . in posix regular expressions)
        % matches zero or more characters in the input (similar to .* in posix regular
        expressions)
        Since Spark 2.0, string literals are unescaped in our SQL parser. For example, in order
        to match "\abc", the pattern should be "\\abc".
        When SQL config 'spark.sql.parser.escapedStringLiterals' is enabled, it falls back
        to Spark 1.6 behavior regarding string literal parsing. For example, if the config is
        enabled, the pattern to match "\abc" should be "\abc".
    escape : :class:`~pyspark.sql.Column`
        An character added since Spark 3.0. The default escape character is the '\'.
        If an escape character precedes a special symbol or another escape character, the
        following character is matched literally. It is invalid to escape any other character.

    Examples
    --------
    >>> df = spark.createDataFrame([("Spark", "_park")], ['a', 'b'])
    >>> df.select(ilike(df.a, df.b).alias('r')).collect()
    [Row(r=True)]

    >>> df = spark.createDataFrame(
    ...     [("%SystemDrive%/Users/John", "/%SystemDrive/%//Users%")],
    ...     ['a', 'b']
    ... )
    >>> df.select(ilike(df.a, df.b, lit('/')).alias('r')).collect()
    [Row(r=True)]
    """
    column = Column.invoke_expression_over_column(str, expression.ILike, expression=pattern)
    if escapeChar is not None:
        return Column(
            expression.Escape(
                this=column.column_expression,
                expression=Column.ensure_col(escapeChar).column_expression,
            )
        )
    return column


@meta(unsupported_engines="*")
def inline(col: ColumnOrName) -> Column:
    """
    Explodes an array of structs into a table.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        input column of values to explode.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        generator expression with the inline exploded result.

    See Also
    --------
    :meth:`explode`

    Notes
    -----
    Supports Spark Connect.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([Row(structlist=[Row(a=1, b=2), Row(a=3, b=4)])])
    >>> df.select(inline(df.structlist)).show()
    +---+---+
    |  a|  b|
    +---+---+
    |  1|  2|
    |  3|  4|
    +---+---+
    """
    return Column.invoke_expression_over_column(col, expression.Inline)


@meta(unsupported_engines="*")
def inline_outer(col: ColumnOrName) -> Column:
    """
    Explodes an array of structs into a table.
    Unlike inline, if the array is null or empty then null is produced for each nested column.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        input column of values to explode.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        generator expression with the inline exploded result.

    See Also
    --------
    :meth:`explode_outer`
    :meth:`inline`

    Notes
    -----
    Supports Spark Connect.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([
    ...     Row(id=1, structlist=[Row(a=1, b=2), Row(a=3, b=4)]),
    ...     Row(id=2, structlist=[])
    ... ])
    >>> df.select('id', inline_outer(df.structlist)).show()
    +---+----+----+
    | id|   a|   b|
    +---+----+----+
    |  1|   1|   2|
    |  1|   3|   4|
    |  2|NULL|NULL|
    +---+----+----+
    """
    return Column.invoke_anonymous_function(col, "inline_outer")


@meta(unsupported_engines="*")
def isnotnull(col: ColumnOrName) -> Column:
    """
    Returns true if `col` is not null, or false otherwise.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str

    Examples
    --------
    >>> df = spark.createDataFrame([(None,), (1,)], ["e"])
    >>> df.select(isnotnull(df.e).alias('r')).collect()
    [Row(r=False), Row(r=True)]
    """
    return Column.invoke_anonymous_function(col, "isnotnull")


@meta(unsupported_engines=["*", "databricks"])
def java_method(*cols: ColumnOrName) -> Column:
    """
    Calls a method with reflection.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    cols : :class:`~pyspark.sql.Column` or str
        the first element should be a literal string for the class name,
        and the second element should be a literal string for the method name,
        and the remaining are input arguments to the Java method.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.range(1).select(
    ...     sf.java_method(
    ...         sf.lit("java.util.UUID"),
    ...         sf.lit("fromString"),
    ...         sf.lit("a5cf6c42-0c85-418f-af6c-3e4e5b1328f2")
    ...     )
    ... ).show(truncate=False)
    +-----------------------------------------------------------------------------+
    |java_method(java.util.UUID, fromString, a5cf6c42-0c85-418f-af6c-3e4e5b1328f2)|
    +-----------------------------------------------------------------------------+
    |a5cf6c42-0c85-418f-af6c-3e4e5b1328f2                                         |
    +-----------------------------------------------------------------------------+
    """
    cols = ensure_list(cols)  # type: ignore
    if len(cols) > 1:
        return Column.invoke_anonymous_function(cols[0], "java_method", *cols[1:])
    return Column.invoke_anonymous_function(cols[0], "java_method")


@meta(unsupported_engines="*")
def json_array_length(col: ColumnOrName) -> Column:
    """
    Returns the number of elements in the outermost JSON array. `NULL` is returned in case of
    any other valid JSON string, `NULL` or an invalid JSON.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col: :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        length of json array.

    Examples
    --------
    >>> df = spark.createDataFrame([(None,), ('[1, 2, 3]',), ('[]',)], ['data'])
    >>> df.select(json_array_length(df.data).alias('r')).collect()
    [Row(r=None), Row(r=3), Row(r=0)]
    """
    return Column.invoke_anonymous_function(col, "json_array_length")


@meta(unsupported_engines="*")
def json_object_keys(col: ColumnOrName) -> Column:
    """
    Returns all the keys of the outermost JSON object as an array. If a valid JSON object is
    given, all the keys of the outermost object will be returned as an array. If it is any
    other valid JSON string, an invalid JSON string or an empty string, the function returns null.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col: :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        all the keys of the outermost JSON object.

    Examples
    --------
    >>> df = spark.createDataFrame([(None,), ('{}',), ('{"key1":1, "key2":2}',)], ['data'])
    >>> df.select(json_object_keys(df.data).alias('r')).collect()
    [Row(r=None), Row(r=[]), Row(r=['key1', 'key2'])]
    """
    return Column.invoke_anonymous_function(col, "json_object_keys")


@meta(unsupported_engines="*")
def last_value(col: ColumnOrName, ignoreNulls: t.Optional[t.Union[bool, Column]] = None) -> Column:
    """Returns the last value of `col` for a group of rows. It will return the last non-null
    value it sees when `ignoreNulls` is set to true. If all values are null, then null is returned.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.
    ignorenulls : :class:`~pyspark.sql.Column` or bool
        if first value is null then look for first non-null value.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        some value of `col` for a group of rows.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("a", 1), ("a", 2), ("a", 3), ("b", 8), (None, 2)], ["a", "b"]
    ... ).select(sf.last_value('a'), sf.last_value('b')).show()
    +-------------+-------------+
    |last_value(a)|last_value(b)|
    +-------------+-------------+
    |         NULL|            2|
    +-------------+-------------+

    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("a", 1), ("a", 2), ("a", 3), ("b", 8), (None, 2)], ["a", "b"]
    ... ).select(sf.last_value('a', True), sf.last_value('b', True)).show()
    +-------------+-------------+
    |last_value(a)|last_value(b)|
    +-------------+-------------+
    |            b|            2|
    +-------------+-------------+
    """
    column = Column.invoke_expression_over_column(col, expression.LastValue)

    if ignoreNulls:
        return Column(expression.IgnoreNulls(this=column.column_expression))
    return column


@meta()
def lcase(str: ColumnOrName) -> Column:
    """
    Returns `str` with all characters changed to lowercase.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        Input column or strings.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.range(1).select(sf.lcase(sf.lit("Spark"))).show()
    +------------+
    |lcase(Spark)|
    +------------+
    |       spark|
    +------------+
    """
    return Column.invoke_expression_over_column(str, expression.Lower)


@meta()
def left(str: ColumnOrName, len: ColumnOrName) -> Column:
    """
    Returns the leftmost `len`(`len` can be string type) characters from the string `str`,
    if `len` is less or equal than 0 the result is an empty string.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        Input column or strings.
    len : :class:`~pyspark.sql.Column` or str
        Input column or strings, the leftmost `len`.

    Examples
    --------
    >>> df = spark.createDataFrame([("Spark SQL", 3,)], ['a', 'b'])
    >>> df.select(left(df.a, df.b).alias('r')).collect()
    [Row(r='Spa')]
    """
    session = _get_session()

    if session._is_postgres:
        len = Column.ensure_col(len).cast("integer")

    return Column.invoke_expression_over_column(str, expression.Left, expression=len)


@meta(unsupported_engines="*")
def like(
    str: ColumnOrName, pattern: ColumnOrName, escapeChar: t.Optional["Column"] = None
) -> Column:
    """
    Returns true if str matches `pattern` with `escape`,
    null if any arguments are null, false otherwise.
    The default escape character is the '\'.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        A string.
    pattern : :class:`~pyspark.sql.Column` or str
        A string. The pattern is a string which is matched literally, with
        exception to the following special symbols:
        _ matches any one character in the input (similar to . in posix regular expressions)
        % matches zero or more characters in the input (similar to .* in posix regular
        expressions)
        Since Spark 2.0, string literals are unescaped in our SQL parser. For example, in order
        to match "\abc", the pattern should be "\\abc".
        When SQL config 'spark.sql.parser.escapedStringLiterals' is enabled, it falls back
        to Spark 1.6 behavior regarding string literal parsing. For example, if the config is
        enabled, the pattern to match "\abc" should be "\abc".
    escape : :class:`~pyspark.sql.Column`
        An character added since Spark 3.0. The default escape character is the '\'.
        If an escape character precedes a special symbol or another escape character, the
        following character is matched literally. It is invalid to escape any other character.

    Examples
    --------
    >>> df = spark.createDataFrame([("Spark", "_park")], ['a', 'b'])
    >>> df.select(like(df.a, df.b).alias('r')).collect()
    [Row(r=True)]

    >>> df = spark.createDataFrame(
    ...     [("%SystemDrive%/Users/John", "/%SystemDrive/%//Users%")],
    ...     ['a', 'b']
    ... )
    >>> df.select(like(df.a, df.b, lit('/')).alias('r')).collect()
    [Row(r=True)]
    """
    column = Column.invoke_expression_over_column(str, expression.Like, expression=pattern)
    if escapeChar is not None:
        return Column(
            expression.Escape(
                this=column.column_expression,
                expression=Column.ensure_col(escapeChar).column_expression,
            )
        )
    return column


@meta()
def ln(col: ColumnOrName) -> Column:
    """Returns the natural logarithm of the argument.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        a column to calculate logariphm for.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        natural logarithm of given value.

    Examples
    --------
    >>> df = spark.createDataFrame([(4,)], ['a'])
    >>> df.select(ln('a')).show()
    +------------------+
    |             ln(a)|
    +------------------+
    |1.3862943611198906|
    +------------------+
    """
    return Column.invoke_expression_over_column(col, expression.Ln)


@meta(unsupported_engines=["*", "databricks"])
def localtimestamp() -> Column:
    """
    Returns the current timestamp without time zone at the start of query evaluation
    as a timestamp without time zone column. All calls of localtimestamp within the
    same query return the same value.

    .. versionadded:: 3.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        current local date and time.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(localtimestamp()).show(truncate=False) # doctest: +SKIP
    +-----------------------+
    |localtimestamp()       |
    +-----------------------+
    |2022-08-26 21:28:34.639|
    +-----------------------+
    """
    return Column.invoke_anonymous_function(None, "localtimestamp")


@meta(unsupported_engines=["*", "databricks"])
def make_dt_interval(
    days: t.Optional[ColumnOrName] = None,
    hours: t.Optional[ColumnOrName] = None,
    mins: t.Optional[ColumnOrName] = None,
    secs: t.Optional[ColumnOrName] = None,
) -> Column:
    """
    Make DayTimeIntervalType duration from days, hours, mins and secs.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    days : :class:`~pyspark.sql.Column` or str
        the number of days, positive or negative
    hours : :class:`~pyspark.sql.Column` or str
        the number of hours, positive or negative
    mins : :class:`~pyspark.sql.Column` or str
        the number of minutes, positive or negative
    secs : :class:`~pyspark.sql.Column` or str
        the number of seconds with the fractional part in microsecond precision.

    Examples
    --------
    >>> df = spark.createDataFrame([[1, 12, 30, 01.001001]],
    ...     ["day", "hour", "min", "sec"])
    >>> df.select(make_dt_interval(
    ...     df.day, df.hour, df.min, df.sec).alias('r')
    ... ).show(truncate=False)
    +------------------------------------------+
    |r                                         |
    +------------------------------------------+
    |INTERVAL '1 12:30:01.001001' DAY TO SECOND|
    +------------------------------------------+

    >>> df.select(make_dt_interval(
    ...     df.day, df.hour, df.min).alias('r')
    ... ).show(truncate=False)
    +-----------------------------------+
    |r                                  |
    +-----------------------------------+
    |INTERVAL '1 12:30:00' DAY TO SECOND|
    +-----------------------------------+

    >>> df.select(make_dt_interval(
    ...     df.day, df.hour).alias('r')
    ... ).show(truncate=False)
    +-----------------------------------+
    |r                                  |
    +-----------------------------------+
    |INTERVAL '1 12:00:00' DAY TO SECOND|
    +-----------------------------------+

    >>> df.select(make_dt_interval(df.day).alias('r')).show(truncate=False)
    +-----------------------------------+
    |r                                  |
    +-----------------------------------+
    |INTERVAL '1 00:00:00' DAY TO SECOND|
    +-----------------------------------+

    >>> df.select(make_dt_interval().alias('r')).show(truncate=False)
    +-----------------------------------+
    |r                                  |
    +-----------------------------------+
    |INTERVAL '0 00:00:00' DAY TO SECOND|
    +-----------------------------------+
    """
    _days = lit(0) if days is None else days
    _hours = lit(0) if hours is None else hours
    _mins = lit(0) if mins is None else mins
    _secs = lit(decimal.Decimal(0)) if secs is None else secs
    return Column.invoke_anonymous_function(_days, "make_dt_interval", _hours, _mins, _secs)


@meta(unsupported_engines="*")
def make_timestamp(
    years: ColumnOrName,
    months: ColumnOrName,
    days: ColumnOrName,
    hours: ColumnOrName,
    mins: ColumnOrName,
    secs: ColumnOrName,
    timezone: t.Optional[ColumnOrName] = None,
) -> Column:
    """
    Create timestamp from years, months, days, hours, mins, secs and timezone fields.
    The result data type is consistent with the value of configuration `spark.sql.timestampType`.
    If the configuration `spark.sql.ansi.enabled` is false, the function returns NULL
    on invalid inputs. Otherwise, it will throw an error instead.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    years : :class:`~pyspark.sql.Column` or str
        the year to represent, from 1 to 9999
    months : :class:`~pyspark.sql.Column` or str
        the month-of-year to represent, from 1 (January) to 12 (December)
    days : :class:`~pyspark.sql.Column` or str
        the day-of-month to represent, from 1 to 31
    hours : :class:`~pyspark.sql.Column` or str
        the hour-of-day to represent, from 0 to 23
    mins : :class:`~pyspark.sql.Column` or str
        the minute-of-hour to represent, from 0 to 59
    secs : :class:`~pyspark.sql.Column` or str
        the second-of-minute and its micro-fraction to represent, from 0 to 60.
        The value can be either an integer like 13 , or a fraction like 13.123.
        If the sec argument equals to 60, the seconds field is set
        to 0 and 1 minute is added to the final timestamp.
    timezone : :class:`~pyspark.sql.Column` or str
        the time zone identifier. For example, CET, UTC and etc.

    Examples
    --------
    >>> spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    >>> df = spark.createDataFrame([[2014, 12, 28, 6, 30, 45.887, 'CET']],
    ...     ["year", "month", "day", "hour", "min", "sec", "timezone"])
    >>> df.select(make_timestamp(
    ...     df.year, df.month, df.day, df.hour, df.min, df.sec, df.timezone).alias('r')
    ... ).show(truncate=False)
    +-----------------------+
    |r                      |
    +-----------------------+
    |2014-12-27 21:30:45.887|
    +-----------------------+

    >>> df.select(make_timestamp(
    ...     df.year, df.month, df.day, df.hour, df.min, df.sec).alias('r')
    ... ).show(truncate=False)
    +-----------------------+
    |r                      |
    +-----------------------+
    |2014-12-28 06:30:45.887|
    +-----------------------+
    >>> spark.conf.unset("spark.sql.session.timeZone")
    """
    if timezone is not None:
        return Column.invoke_anonymous_function(
            years, "make_timestamp", months, days, hours, mins, secs, timezone
        )
    else:
        return Column.invoke_anonymous_function(
            years, "make_timestamp", months, days, hours, mins, secs
        )


@meta(unsupported_engines=["*", "databricks"])
def make_timestamp_ltz(
    years: ColumnOrName,
    months: ColumnOrName,
    days: ColumnOrName,
    hours: ColumnOrName,
    mins: ColumnOrName,
    secs: ColumnOrName,
    timezone: t.Optional[ColumnOrName] = None,
) -> Column:
    """
    Create the current timestamp with local time zone from years, months, days, hours, mins,
    secs and timezone fields. If the configuration `spark.sql.ansi.enabled` is false,
    the function returns NULL on invalid inputs. Otherwise, it will throw an error instead.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    years : :class:`~pyspark.sql.Column` or str
        the year to represent, from 1 to 9999
    months : :class:`~pyspark.sql.Column` or str
        the month-of-year to represent, from 1 (January) to 12 (December)
    days : :class:`~pyspark.sql.Column` or str
        the day-of-month to represent, from 1 to 31
    hours : :class:`~pyspark.sql.Column` or str
        the hour-of-day to represent, from 0 to 23
    mins : :class:`~pyspark.sql.Column` or str
        the minute-of-hour to represent, from 0 to 59
    secs : :class:`~pyspark.sql.Column` or str
        the second-of-minute and its micro-fraction to represent, from 0 to 60.
        The value can be either an integer like 13 , or a fraction like 13.123.
        If the sec argument equals to 60, the seconds field is set
        to 0 and 1 minute is added to the final timestamp.
    timezone : :class:`~pyspark.sql.Column` or str
        the time zone identifier. For example, CET, UTC and etc.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    >>> df = spark.createDataFrame([[2014, 12, 28, 6, 30, 45.887, 'CET']],
    ...     ["year", "month", "day", "hour", "min", "sec", "timezone"])
    >>> df.select(sf.make_timestamp_ltz(
    ...     df.year, df.month, df.day, df.hour, df.min, df.sec, df.timezone)
    ... ).show(truncate=False)
    +--------------------------------------------------------------+
    |make_timestamp_ltz(year, month, day, hour, min, sec, timezone)|
    +--------------------------------------------------------------+
    |2014-12-27 21:30:45.887                                       |
    +--------------------------------------------------------------+

    >>> df.select(sf.make_timestamp_ltz(
    ...     df.year, df.month, df.day, df.hour, df.min, df.sec)
    ... ).show(truncate=False)
    +----------------------------------------------------+
    |make_timestamp_ltz(year, month, day, hour, min, sec)|
    +----------------------------------------------------+
    |2014-12-28 06:30:45.887                             |
    +----------------------------------------------------+
    >>> spark.conf.unset("spark.sql.session.timeZone")
    """
    if timezone is not None:
        return Column.invoke_anonymous_function(
            years, "make_timestamp_ltz", months, days, hours, mins, secs, timezone
        )
    else:
        return Column.invoke_anonymous_function(
            years, "make_timestamp_ltz", months, days, hours, mins, secs
        )


@meta(unsupported_engines="*")
def make_timestamp_ntz(
    years: ColumnOrName,
    months: ColumnOrName,
    days: ColumnOrName,
    hours: ColumnOrName,
    mins: ColumnOrName,
    secs: ColumnOrName,
) -> Column:
    """
    Create local date-time from years, months, days, hours, mins, secs fields.
    If the configuration `spark.sql.ansi.enabled` is false, the function returns NULL
    on invalid inputs. Otherwise, it will throw an error instead.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    years : :class:`~pyspark.sql.Column` or str
        the year to represent, from 1 to 9999
    months : :class:`~pyspark.sql.Column` or str
        the month-of-year to represent, from 1 (January) to 12 (December)
    days : :class:`~pyspark.sql.Column` or str
        the day-of-month to represent, from 1 to 31
    hours : :class:`~pyspark.sql.Column` or str
        the hour-of-day to represent, from 0 to 23
    mins : :class:`~pyspark.sql.Column` or str
        the minute-of-hour to represent, from 0 to 59
    secs : :class:`~pyspark.sql.Column` or str
        the second-of-minute and its micro-fraction to represent, from 0 to 60.
        The value can be either an integer like 13 , or a fraction like 13.123.
        If the sec argument equals to 60, the seconds field is set
        to 0 and 1 minute is added to the final timestamp.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    >>> df = spark.createDataFrame([[2014, 12, 28, 6, 30, 45.887]],
    ...     ["year", "month", "day", "hour", "min", "sec"])
    >>> df.select(sf.make_timestamp_ntz(
    ...     df.year, df.month, df.day, df.hour, df.min, df.sec)
    ... ).show(truncate=False)
    +----------------------------------------------------+
    |make_timestamp_ntz(year, month, day, hour, min, sec)|
    +----------------------------------------------------+
    |2014-12-28 06:30:45.887                             |
    +----------------------------------------------------+
    >>> spark.conf.unset("spark.sql.session.timeZone")
    """
    return Column.invoke_anonymous_function(
        years, "make_timestamp_ntz", months, days, hours, mins, secs
    )


@meta(unsupported_engines=["*", "databricks"])
def make_ym_interval(
    years: t.Optional[ColumnOrName] = None,
    months: t.Optional[ColumnOrName] = None,
) -> Column:
    """
    Make year-month interval from years, months.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    years : :class:`~pyspark.sql.Column` or str
        the number of years, positive or negative
    months : :class:`~pyspark.sql.Column` or str
        the number of months, positive or negative

    Examples
    --------
    >>> spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    >>> df = spark.createDataFrame([[2014, 12]], ["year", "month"])
    >>> df.select(make_ym_interval(df.year, df.month).alias('r')).show(truncate=False)
    +-------------------------------+
    |r                              |
    +-------------------------------+
    |INTERVAL '2015-0' YEAR TO MONTH|
    +-------------------------------+
    >>> spark.conf.unset("spark.sql.session.timeZone")
    """
    _years = lit(0) if years is None else years
    _months = lit(0) if months is None else months
    return Column.invoke_anonymous_function(_years, "make_ym_interval", _months)


@meta(unsupported_engines="*")
def map_contains_key(col: ColumnOrName, value: t.Any) -> Column:
    """
    Returns true if the map contains the key.

    .. versionadded:: 3.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        name of column or expression
    value :
        a literal value

    Returns
    -------
    :class:`~pyspark.sql.Column`
        True if key is in the map and False otherwise.

    Examples
    --------
    >>> from pyspark.sql.functions import map_contains_key
    >>> df = spark.sql("SELECT map(1, 'a', 2, 'b') as data")
    >>> df.select(map_contains_key("data", 1)).show()
    +---------------------------------+
    |array_contains(map_keys(data), 1)|
    +---------------------------------+
    |                             true|
    +---------------------------------+
    >>> df.select(map_contains_key("data", -1)).show()
    +----------------------------------+
    |array_contains(map_keys(data), -1)|
    +----------------------------------+
    |                             false|
    +----------------------------------+
    """
    value = lit(value) if not isinstance(value, Column) else value
    return Column.invoke_anonymous_function(col, "map_contains_key", value)


@meta(unsupported_engines="*")
def mask(
    col: ColumnOrName,
    upperChar: t.Optional[ColumnOrName] = None,
    lowerChar: t.Optional[ColumnOrName] = None,
    digitChar: t.Optional[ColumnOrName] = None,
    otherChar: t.Optional[ColumnOrName] = None,
) -> Column:
    """
    Masks the given string value. This can be useful for creating copies of tables with sensitive
    information removed.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col: :class:`~pyspark.sql.Column` or str
        target column to compute on.
    upperChar: :class:`~pyspark.sql.Column` or str
        character to replace upper-case characters with. Specify NULL to retain original character.
    lowerChar: :class:`~pyspark.sql.Column` or str
        character to replace lower-case characters with. Specify NULL to retain original character.
    digitChar: :class:`~pyspark.sql.Column` or str
        character to replace digit characters with. Specify NULL to retain original character.
    otherChar: :class:`~pyspark.sql.Column` or str
        character to replace all other characters with. Specify NULL to retain original character.

    Returns
    -------
    :class:`~pyspark.sql.Column`

    Examples
    --------
    >>> df = spark.createDataFrame([("AbCD123-@$#",), ("abcd-EFGH-8765-4321",)], ['data'])
    >>> df.select(mask(df.data).alias('r')).collect()
    [Row(r='XxXXnnn-@$#'), Row(r='xxxx-XXXX-nnnn-nnnn')]
    >>> df.select(mask(df.data, lit('Y')).alias('r')).collect()
    [Row(r='YxYYnnn-@$#'), Row(r='xxxx-YYYY-nnnn-nnnn')]
    >>> df.select(mask(df.data, lit('Y'), lit('y')).alias('r')).collect()
    [Row(r='YyYYnnn-@$#'), Row(r='yyyy-YYYY-nnnn-nnnn')]
    >>> df.select(mask(df.data, lit('Y'), lit('y'), lit('d')).alias('r')).collect()
    [Row(r='YyYYddd-@$#'), Row(r='yyyy-YYYY-dddd-dddd')]
    >>> df.select(mask(df.data, lit('Y'), lit('y'), lit('d'), lit('*')).alias('r')).collect()
    [Row(r='YyYYddd****'), Row(r='yyyy*YYYY*dddd*dddd')]
    """

    _upperChar = lit("X") if upperChar is None else upperChar
    _lowerChar = lit("x") if lowerChar is None else lowerChar
    _digitChar = lit("n") if digitChar is None else digitChar
    _otherChar = lit(None) if otherChar is None else otherChar
    return Column.invoke_anonymous_function(
        col, "mask", _upperChar, _lowerChar, _digitChar, _otherChar
    )


@meta(unsupported_engines=["bigquery"])
def median(col: ColumnOrName) -> Column:
    """
    Returns the median of the values in a group.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the median of the values in a group.

    Notes
    -----
    Supports Spark Connect.

    Examples
    --------
    >>> df = spark.createDataFrame([
    ...     ("Java", 2012, 20000), ("dotNET", 2012, 5000),
    ...     ("Java", 2012, 22000), ("dotNET", 2012, 10000),
    ...     ("dotNET", 2013, 48000), ("Java", 2013, 30000)],
    ...     schema=("course", "year", "earnings"))
    >>> df.groupby("course").agg(median("earnings")).show()
    +------+----------------+
    |course|median(earnings)|
    +------+----------------+
    |  Java|         22000.0|
    |dotNET|         10000.0|
    +------+----------------+
    """
    return Column.invoke_expression_over_column(col, expression.Median)


@meta(unsupported_engines=["bigquery", "postgres"])
def mode(col: ColumnOrName) -> Column:
    """
    Returns the most frequent value in a group.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the most frequent value in a group.

    Notes
    -----
    Supports Spark Connect.

    Examples
    --------
    >>> df = spark.createDataFrame([
    ...     ("Java", 2012, 20000), ("dotNET", 2012, 5000),
    ...     ("Java", 2012, 20000), ("dotNET", 2012, 5000),
    ...     ("dotNET", 2013, 48000), ("Java", 2013, 30000)],
    ...     schema=("course", "year", "earnings"))
    >>> df.groupby("course").agg(mode("year")).show()
    +------+----------+
    |course|mode(year)|
    +------+----------+
    |  Java|      2012|
    |dotNET|      2012|
    +------+----------+
    """

    return Column.invoke_anonymous_function(col, "mode")


@meta(unsupported_engines="*")
def months(col: ColumnOrName) -> Column:
    """
    Partition transform function: A transform for timestamps and dates
    to partition data into months.

    .. versionadded:: 3.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target date or timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        data partitioned by months.

    Examples
    --------
    >>> df.writeTo("catalog.db.table").partitionedBy(
    ...     months("ts")
    ... ).createOrReplace()  # doctest: +SKIP

    Notes
    -----
    This function can be used only in combination with
    :py:meth:`~pyspark.sql.readwriter.DataFrameWriterV2.partitionedBy`
    method of the `DataFrameWriterV2`.

    """
    return Column.invoke_anonymous_function(col, "months")


@meta(unsupported_engines="*")
def named_struct(*cols: ColumnOrName) -> Column:
    """
    Creates a struct with the given field names and values.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    cols : :class:`~pyspark.sql.Column` or str
        list of columns to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`

    Examples
    --------
    >>> df = spark.createDataFrame([(1, 2, 3)], ['a', 'b', 'c'])
    >>> df.select(named_struct(lit('x'), df.a, lit('y'), df.b).alias('r')).collect()
    [Row(r=Row(x=1, y=2))]
    """
    cols = ensure_list(cols)  # type: ignore
    if len(cols) > 1:
        return Column.invoke_anonymous_function(cols[0], "named_struct", *cols[1:])
    return Column.invoke_anonymous_function(cols[0], "named_struct")


@meta(unsupported_engines="*")
def negative(col: ColumnOrName) -> Column:
    """
    Returns the negative value.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        column to calculate negative value for.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        negative value.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.range(3).select(sf.negative("id")).show()
    +------------+
    |negative(id)|
    +------------+
    |           0|
    |          -1|
    |          -2|
    +------------+
    """
    return Column.invoke_anonymous_function(col, "negative")


negate = negative
now = current_timestamp


@meta()
def nvl(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """
    Returns `col2` if `col1` is null, or `col1` otherwise.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col1 : :class:`~pyspark.sql.Column` or str
    col2 : :class:`~pyspark.sql.Column` or str

    Examples
    --------
    >>> df = spark.createDataFrame([(None, 8,), (1, 9,)], ["a", "b"])
    >>> df.select(nvl(df.a, df.b).alias('r')).collect()
    [Row(r=8), Row(r=1)]
    """
    return Column.invoke_expression_over_column(col1, expression.Coalesce, expressions=[col2])


@meta()
def nvl2(col1: ColumnOrName, col2: ColumnOrName, col3: ColumnOrName) -> Column:
    """
    Returns `col2` if `col1` is not null, or `col3` otherwise.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col1 : :class:`~pyspark.sql.Column` or str
    col2 : :class:`~pyspark.sql.Column` or str
    col3 : :class:`~pyspark.sql.Column` or str

    Examples
    --------
    >>> df = spark.createDataFrame([(None, 8, 6,), (1, 9, 9,)], ["a", "b", "c"])
    >>> df.select(nvl2(df.a, df.b, df.c).alias('r')).collect()
    [Row(r=6), Row(r=9)]
    """
    return Column.invoke_expression_over_column(col1, expression.Nvl2, true=col2, false=col3)


@meta(unsupported_engines="*")
def parse_url(
    url: ColumnOrName, partToExtract: ColumnOrName, key: t.Optional[ColumnOrName] = None
) -> Column:
    """
    Extracts a part from a URL.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    url : :class:`~pyspark.sql.Column` or str
        A column of string.
    partToExtract : :class:`~pyspark.sql.Column` or str
        A column of string, the path.
    key : :class:`~pyspark.sql.Column` or str, optional
        A column of string, the key.

    Examples
    --------
    >>> df = spark.createDataFrame(
    ...     [("http://spark.apache.org/path?query=1", "QUERY", "query",)],
    ...     ["a", "b", "c"]
    ... )
    >>> df.select(parse_url(df.a, df.b, df.c).alias('r')).collect()
    [Row(r='1')]

    >>> df.select(parse_url(df.a, df.b).alias('r')).collect()
    [Row(r='query=1')]
    """
    if key is not None:
        return Column.invoke_anonymous_function(url, "parse_url", partToExtract, key)
    else:
        return Column.invoke_anonymous_function(url, "parse_url", partToExtract)


@meta(unsupported_engines="*")
def pi() -> Column:
    """Returns Pi.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> spark.range(1).select(pi()).show()
    +-----------------+
    |             PI()|
    +-----------------+
    |3.141592653589793|
    +-----------------+
    """
    return Column.invoke_anonymous_function(None, "pi")


@meta(unsupported_engines="*")
def pmod(dividend: t.Union[ColumnOrName, float], divisor: t.Union[ColumnOrName, float]) -> Column:
    """
    Returns the positive value of dividend mod divisor.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    dividend : str, :class:`~pyspark.sql.Column` or float
        the column that contains dividend, or the specified dividend value
    divisor : str, :class:`~pyspark.sql.Column` or float
        the column that contains divisor, or the specified divisor value

    Returns
    -------
    :class:`~pyspark.sql.Column`
        positive value of dividend mod divisor.

    Notes
    -----
    Supports Spark Connect.

    Examples
    --------
    >>> from pyspark.sql.functions import pmod
    >>> df = spark.createDataFrame([
    ...     (1.0, float('nan')), (float('nan'), 2.0), (10.0, 3.0),
    ...     (float('nan'), float('nan')), (-3.0, 4.0), (-10.0, 3.0),
    ...     (-5.0, -6.0), (7.0, -8.0), (1.0, 2.0)],
    ...     ("a", "b"))
    >>> df.select(pmod("a", "b")).show()
    +----------+
    |pmod(a, b)|
    +----------+
    |       NaN|
    |       NaN|
    |       1.0|
    |       NaN|
    |       1.0|
    |       2.0|
    |      -5.0|
    |       7.0|
    |       1.0|
    +----------+
    """
    dividend = lit(dividend) if isinstance(dividend, float) else dividend
    divisor = lit(divisor) if isinstance(divisor, float) else divisor
    return Column.invoke_anonymous_function(dividend, "pmod", divisor)


@meta()
def position(
    substr: ColumnOrName, str: ColumnOrName, start: t.Optional[ColumnOrName] = None
) -> Column:
    """
    Returns the position of the first occurrence of `substr` in `str` after position `start`.
    The given `start` and return value are 1-based.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    substr : :class:`~pyspark.sql.Column` or str
        A column of string, substring.
    str : :class:`~pyspark.sql.Column` or str
        A column of string.
    start : :class:`~pyspark.sql.Column` or str, optional
        A column of string, start position.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("bar", "foobarbar", 5,)], ["a", "b", "c"]
    ... ).select(sf.position("a", "b", "c")).show()
    +-----------------+
    |position(a, b, c)|
    +-----------------+
    |                7|
    +-----------------+

    >>> spark.createDataFrame(
    ...     [("bar", "foobarbar", 5,)], ["a", "b", "c"]
    ... ).select(sf.position("a", "b")).show()
    +-----------------+
    |position(a, b, 1)|
    +-----------------+
    |                4|
    +-----------------+
    """
    from sqlframe.base.function_alternatives import position_as_strpos

    session = _get_session()

    if session._is_bigquery:
        return position_as_strpos(substr, str, start)

    if session._is_postgres:
        start = Column.ensure_col(start).cast("integer") if start else None

    if start is not None:
        return Column.invoke_expression_over_column(
            str, expression.StrPosition, substr=substr, position=start
        )
    else:
        return Column.invoke_expression_over_column(str, expression.StrPosition, substr=substr)


@meta(unsupported_engines="*")
def positive(col: ColumnOrName) -> Column:
    """
    Returns the value.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        input value column.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        value.

    Examples
    --------
    >>> df = spark.createDataFrame([(-1,), (0,), (1,)], ['v'])
    >>> df.select(positive("v").alias("p")).show()
    +---+
    |  p|
    +---+
    | -1|
    |  0|
    |  1|
    +---+
    """
    return Column.invoke_anonymous_function(col, "positive")


@meta(unsupported_engines="*")
def printf(format: ColumnOrName, *cols: ColumnOrName) -> Column:
    """
    Formats the arguments in printf-style and returns the result as a string column.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    format : :class:`~pyspark.sql.Column` or str
        string that can contain embedded format tags and used as result column's value
    cols : :class:`~pyspark.sql.Column` or str
        column names or :class:`~pyspark.sql.Column`\\s to be used in formatting

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("aa%d%s", 123, "cc",)], ["a", "b", "c"]
    ... ).select(sf.printf("a", "b", "c")).show()
    +---------------+
    |printf(a, b, c)|
    +---------------+
    |        aa123cc|
    +---------------+
    """
    return Column.invoke_anonymous_function(format, "printf", *cols)


@meta(unsupported_engines=["bigquery", "postgres", "redshift", "snowflake", "spark", "databricks"])
def product(col: ColumnOrName) -> Column:
    """
    Aggregate function: returns the product of the values in a group.

    .. versionadded:: 3.2.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : str, :class:`Column`
        column containing values to be multiplied together

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(1, 10).toDF('x').withColumn('mod3', col('x') % 3)
    >>> prods = df.groupBy('mod3').agg(product('x').alias('product'))
    >>> prods.orderBy('mod3').show()
    +----+-------+
    |mod3|product|
    +----+-------+
    |   0|  162.0|
    |   1|   28.0|
    |   2|   80.0|
    +----+-------+
    """
    return Column.invoke_anonymous_function(col, "product")


reduce = aggregate


@meta(unsupported_engines=["*", "databricks"])
def reflect(*cols: ColumnOrName) -> Column:
    """
    Calls a method with reflection.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    cols : :class:`~pyspark.sql.Column` or str
        the first element should be a literal string for the class name,
        and the second element should be a literal string for the method name,
        and the remaining are input arguments to the Java method.

    Examples
    --------
    >>> df = spark.createDataFrame([("a5cf6c42-0c85-418f-af6c-3e4e5b1328f2",)], ["a"])
    >>> df.select(
    ...     reflect(lit("java.util.UUID"), lit("fromString"), df.a).alias('r')
    ... ).collect()
    [Row(r='a5cf6c42-0c85-418f-af6c-3e4e5b1328f2')]
    """
    if len(cols) > 1:
        return Column.invoke_anonymous_function(cols[0], "reflect", *cols[1:])
    return Column.invoke_anonymous_function(cols[0], "reflect")


@meta(unsupported_engines="snowflake")
def regexp(str: ColumnOrName, regexp: ColumnOrName) -> Column:
    r"""Returns true if `str` matches the Java regex `regexp`, or false otherwise.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        target column to work on.
    regexp : :class:`~pyspark.sql.Column` or str
        regex pattern to apply.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        true if `str` matches a Java regex, or false otherwise.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("1a 2b 14m", r"(\d+)")], ["str", "regexp"]
    ... ).select(sf.regexp('str', sf.lit(r'(\d+)'))).show()
    +------------------+
    |REGEXP(str, (\d+))|
    +------------------+
    |              true|
    +------------------+

    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("1a 2b 14m", r"(\d+)")], ["str", "regexp"]
    ... ).select(sf.regexp('str', sf.lit(r'\d{2}b'))).show()
    +-------------------+
    |REGEXP(str, \d{2}b)|
    +-------------------+
    |              false|
    +-------------------+

    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("1a 2b 14m", r"(\d+)")], ["str", "regexp"]
    ... ).select(sf.regexp('str', sf.col("regexp"))).show()
    +-------------------+
    |REGEXP(str, regexp)|
    +-------------------+
    |               true|
    +-------------------+
    """
    from sqlframe.base.function_alternatives import (
        regexp_with_contains,
        regexp_with_matches,
    )

    session = _get_session()

    if session._is_duckdb:
        return regexp_with_matches(str, regexp)

    if session._is_postgres:
        return Column.invoke_expression_over_column(str, expression.RegexpILike, expression=regexp)

    if session._is_bigquery:
        return regexp_with_contains(str, regexp)

    return Column.invoke_anonymous_function(str, "regexp", regexp)


@meta(unsupported_engines="*")
def regexp_count(str: ColumnOrName, regexp: ColumnOrName) -> Column:
    r"""Returns a count of the number of times that the Java regex pattern `regexp` is matched
    in the string `str`.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        target column to work on.
    regexp : :class:`~pyspark.sql.Column` or str
        regex pattern to apply.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the number of times that a Java regex pattern is matched in the string.

    Examples
    --------
    >>> df = spark.createDataFrame([("1a 2b 14m", r"\d+")], ["str", "regexp"])
    >>> df.select(regexp_count('str', lit(r'\d+')).alias('d')).collect()
    [Row(d=3)]
    >>> df.select(regexp_count('str', lit(r'mmm')).alias('d')).collect()
    [Row(d=0)]
    >>> df.select(regexp_count("str", col("regexp")).alias('d')).collect()
    [Row(d=3)]
    """
    return Column.invoke_anonymous_function(str, "regexp_count", regexp)


@meta(unsupported_engines="*")
def regexp_extract_all(
    str: ColumnOrName, regexp: ColumnOrName, idx: t.Optional[t.Union[int, Column]] = None
) -> Column:
    r"""Extract all strings in the `str` that match the Java regex `regexp`
    and corresponding to the regex group index.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        target column to work on.
    regexp : :class:`~pyspark.sql.Column` or str
        regex pattern to apply.
    idx : int
        matched group id.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        all strings in the `str` that match a Java regex and corresponding to the regex group index.

    Examples
    --------
    >>> df = spark.createDataFrame([("100-200, 300-400", r"(\d+)-(\d+)")], ["str", "regexp"])
    >>> df.select(regexp_extract_all('str', lit(r'(\d+)-(\d+)')).alias('d')).collect()
    [Row(d=['100', '300'])]
    >>> df.select(regexp_extract_all('str', lit(r'(\d+)-(\d+)'), 1).alias('d')).collect()
    [Row(d=['100', '300'])]
    >>> df.select(regexp_extract_all('str', lit(r'(\d+)-(\d+)'), 2).alias('d')).collect()
    [Row(d=['200', '400'])]
    >>> df.select(regexp_extract_all('str', col("regexp")).alias('d')).collect()
    [Row(d=['100', '300'])]
    """
    return Column.invoke_expression_over_column(
        str, expression.RegexpExtractAll, expression=regexp, group=idx
    )


@meta(unsupported_engines="*")
def regexp_instr(
    str: ColumnOrName, regexp: ColumnOrName, idx: t.Optional[t.Union[int, Column]] = None
) -> Column:
    r"""Extract all strings in the `str` that match the Java regex `regexp`
    and corresponding to the regex group index.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        target column to work on.
    regexp : :class:`~pyspark.sql.Column` or str
        regex pattern to apply.
    idx : int
        matched group id.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        all strings in the `str` that match a Java regex and corresponding to the regex group index.

    Examples
    --------
    >>> df = spark.createDataFrame([("1a 2b 14m", r"\d+(a|b|m)")], ["str", "regexp"])
    >>> df.select(regexp_instr('str', lit(r'\d+(a|b|m)')).alias('d')).collect()
    [Row(d=1)]
    >>> df.select(regexp_instr('str', lit(r'\d+(a|b|m)'), 1).alias('d')).collect()
    [Row(d=1)]
    >>> df.select(regexp_instr('str', lit(r'\d+(a|b|m)'), 2).alias('d')).collect()
    [Row(d=1)]
    >>> df.select(regexp_instr('str', col("regexp")).alias('d')).collect()
    [Row(d=1)]
    """
    if idx is None:
        return Column.invoke_anonymous_function(str, "regexp_instr", regexp)
    else:
        idx = lit(idx) if isinstance(idx, int) else idx
        return Column.invoke_anonymous_function(str, "regexp_instr", regexp, idx)


@meta(unsupported_engines="snowflake")
def regexp_like(str: ColumnOrName, regexp: ColumnOrName) -> Column:
    r"""Returns true if `str` matches the Java regex `regexp`, or false otherwise.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        target column to work on.
    regexp : :class:`~pyspark.sql.Column` or str
        regex pattern to apply.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        true if `str` matches a Java regex, or false otherwise.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("1a 2b 14m", r"(\d+)")], ["str", "regexp"]
    ... ).select(sf.regexp_like('str', sf.lit(r'(\d+)'))).show()
    +-----------------------+
    |REGEXP_LIKE(str, (\d+))|
    +-----------------------+
    |                   true|
    +-----------------------+

    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("1a 2b 14m", r"(\d+)")], ["str", "regexp"]
    ... ).select(sf.regexp_like('str', sf.lit(r'\d{2}b'))).show()
    +------------------------+
    |REGEXP_LIKE(str, \d{2}b)|
    +------------------------+
    |                   false|
    +------------------------+

    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("1a 2b 14m", r"(\d+)")], ["str", "regexp"]
    ... ).select(sf.regexp_like('str', sf.col("regexp"))).show()
    +------------------------+
    |REGEXP_LIKE(str, regexp)|
    +------------------------+
    |                    true|
    +------------------------+
    """
    return Column.invoke_expression_over_column(str, expression.RegexpLike, expression=regexp)


@meta(unsupported_engines="*")
def regexp_substr(str: ColumnOrName, regexp: ColumnOrName) -> Column:
    r"""Returns the substring that matches the Java regex `regexp` within the string `str`.
    If the regular expression is not found, the result is null.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        target column to work on.
    regexp : :class:`~pyspark.sql.Column` or str
        regex pattern to apply.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the substring that matches a Java regex within the string `str`.

    Examples
    --------
    >>> df = spark.createDataFrame([("1a 2b 14m", r"\d+")], ["str", "regexp"])
    >>> df.select(regexp_substr('str', lit(r'\d+')).alias('d')).collect()
    [Row(d='1')]
    >>> df.select(regexp_substr('str', lit(r'mmm')).alias('d')).collect()
    [Row(d=None)]
    >>> df.select(regexp_substr("str", col("regexp")).alias('d')).collect()
    [Row(d='1')]
    """
    return Column.invoke_anonymous_function(str, "regexp_substr", regexp)


@meta(unsupported_engines="*")
def regr_avgx(y: ColumnOrName, x: ColumnOrName) -> Column:
    """
    Aggregate function: returns the average of the independent variable for non-null pairs
    in a group, where `y` is the dependent variable and `x` is the independent variable.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    y : :class:`~pyspark.sql.Column` or str
        the dependent variable.
    x : :class:`~pyspark.sql.Column` or str
        the independent variable.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the average of the independent variable for non-null pairs in a group.

    Examples
    --------
    >>> x = (col("id") % 3).alias("x")
    >>> y = (randn(42) + x * 10).alias("y")
    >>> df = spark.range(0, 1000, 1, 1).select(x, y)
    >>> df.select(regr_avgx("y", "x")).first()
    Row(regr_avgx(y, x)=0.999)
    """
    return Column.invoke_anonymous_function(y, "regr_avgx", x)


@meta(unsupported_engines="*")
def regr_avgy(y: ColumnOrName, x: ColumnOrName) -> Column:
    """
    Aggregate function: returns the average of the dependent variable for non-null pairs
    in a group, where `y` is the dependent variable and `x` is the independent variable.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    y : :class:`~pyspark.sql.Column` or str
        the dependent variable.
    x : :class:`~pyspark.sql.Column` or str
        the independent variable.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the average of the dependent variable for non-null pairs in a group.

    Examples
    --------
    >>> x = (col("id") % 3).alias("x")
    >>> y = (randn(42) + x * 10).alias("y")
    >>> df = spark.range(0, 1000, 1, 1).select(x, y)
    >>> df.select(regr_avgy("y", "x")).first()
    Row(regr_avgy(y, x)=9.980732994136464)
    """
    return Column.invoke_anonymous_function(y, "regr_avgy", x)


@meta(unsupported_engines="*")
def regr_count(y: ColumnOrName, x: ColumnOrName) -> Column:
    """
    Aggregate function: returns the number of non-null number pairs
    in a group, where `y` is the dependent variable and `x` is the independent variable.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    y : :class:`~pyspark.sql.Column` or str
        the dependent variable.
    x : :class:`~pyspark.sql.Column` or str
        the independent variable.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the number of non-null number pairs in a group.

    Examples
    --------
    >>> x = (col("id") % 3).alias("x")
    >>> y = (randn(42) + x * 10).alias("y")
    >>> df = spark.range(0, 1000, 1, 1).select(x, y)
    >>> df.select(regr_count("y", "x")).first()
    Row(regr_count(y, x)=1000)
    """
    return Column.invoke_anonymous_function(y, "regr_count", x)


@meta(unsupported_engines="*")
def regr_intercept(y: ColumnOrName, x: ColumnOrName) -> Column:
    """
    Aggregate function: returns the intercept of the univariate linear regression line
    for non-null pairs in a group, where `y` is the dependent variable and
    `x` is the independent variable.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    y : :class:`~pyspark.sql.Column` or str
        the dependent variable.
    x : :class:`~pyspark.sql.Column` or str
        the independent variable.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the intercept of the univariate linear regression line for non-null pairs in a group.

    Examples
    --------
    >>> x = (col("id") % 3).alias("x")
    >>> y = (randn(42) + x * 10).alias("y")
    >>> df = spark.range(0, 1000, 1, 1).select(x, y)
    >>> df.select(regr_intercept("y", "x")).first()
    Row(regr_intercept(y, x)=-0.04961745990969568)
    """
    return Column.invoke_anonymous_function(y, "regr_intercept", x)


@meta(unsupported_engines="*")
def regr_r2(y: ColumnOrName, x: ColumnOrName) -> Column:
    """
    Aggregate function: returns the coefficient of determination for non-null pairs
    in a group, where `y` is the dependent variable and `x` is the independent variable.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    y : :class:`~pyspark.sql.Column` or str
        the dependent variable.
    x : :class:`~pyspark.sql.Column` or str
        the independent variable.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the coefficient of determination for non-null pairs in a group.

    Examples
    --------
    >>> x = (col("id") % 3).alias("x")
    >>> y = (randn(42) + x * 10).alias("y")
    >>> df = spark.range(0, 1000, 1, 1).select(x, y)
    >>> df.select(regr_r2("y", "x")).first()
    Row(regr_r2(y, x)=0.9851908293645436)
    """
    return Column.invoke_anonymous_function(y, "regr_r2", x)


@meta(unsupported_engines="*")
def regr_slope(y: ColumnOrName, x: ColumnOrName) -> Column:
    """
    Aggregate function: returns the slope of the linear regression line for non-null pairs
    in a group, where `y` is the dependent variable and `x` is the independent variable.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    y : :class:`~pyspark.sql.Column` or str
        the dependent variable.
    x : :class:`~pyspark.sql.Column` or str
        the independent variable.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the slope of the linear regression line for non-null pairs in a group.

    Examples
    --------
    >>> x = (col("id") % 3).alias("x")
    >>> y = (randn(42) + x * 10).alias("y")
    >>> df = spark.range(0, 1000, 1, 1).select(x, y)
    >>> df.select(regr_slope("y", "x")).first()
    Row(regr_slope(y, x)=10.040390844891048)
    """
    return Column.invoke_anonymous_function(y, "regr_slope", x)


@meta(unsupported_engines="*")
def regr_sxx(y: ColumnOrName, x: ColumnOrName) -> Column:
    """
    Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(x) for non-null pairs
    in a group, where `y` is the dependent variable and `x` is the independent variable.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    y : :class:`~pyspark.sql.Column` or str
        the dependent variable.
    x : :class:`~pyspark.sql.Column` or str
        the independent variable.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        REGR_COUNT(y, x) * VAR_POP(x) for non-null pairs in a group.

    Examples
    --------
    >>> x = (col("id") % 3).alias("x")
    >>> y = (randn(42) + x * 10).alias("y")
    >>> df = spark.range(0, 1000, 1, 1).select(x, y)
    >>> df.select(regr_sxx("y", "x")).first()
    Row(regr_sxx(y, x)=666.9989999999996)
    """
    return Column.invoke_anonymous_function(y, "regr_sxx", x)


@meta(unsupported_engines="*")
def regr_sxy(y: ColumnOrName, x: ColumnOrName) -> Column:
    """
    Aggregate function: returns REGR_COUNT(y, x) * COVAR_POP(y, x) for non-null pairs
    in a group, where `y` is the dependent variable and `x` is the independent variable.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    y : :class:`~pyspark.sql.Column` or str
        the dependent variable.
    x : :class:`~pyspark.sql.Column` or str
        the independent variable.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        REGR_COUNT(y, x) * COVAR_POP(y, x) for non-null pairs in a group.

    Examples
    --------
    >>> x = (col("id") % 3).alias("x")
    >>> y = (randn(42) + x * 10).alias("y")
    >>> df = spark.range(0, 1000, 1, 1).select(x, y)
    >>> df.select(regr_sxy("y", "x")).first()
    Row(regr_sxy(y, x)=6696.93065315148)
    """
    return Column.invoke_anonymous_function(y, "regr_sxy", x)


@meta(unsupported_engines="*")
def regr_syy(y: ColumnOrName, x: ColumnOrName) -> Column:
    """
    Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(y) for non-null pairs
    in a group, where `y` is the dependent variable and `x` is the independent variable.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    y : :class:`~pyspark.sql.Column` or str
        the dependent variable.
    x : :class:`~pyspark.sql.Column` or str
        the independent variable.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        REGR_COUNT(y, x) * VAR_POP(y) for non-null pairs in a group.

    Examples
    --------
    >>> x = (col("id") % 3).alias("x")
    >>> y = (randn(42) + x * 10).alias("y")
    >>> df = spark.range(0, 1000, 1, 1).select(x, y)
    >>> df.select(regr_syy("y", "x")).first()
    Row(regr_syy(y, x)=68250.53503811295)
    """
    return Column.invoke_anonymous_function(y, "regr_syy", x)


@meta()
def replace(
    src: ColumnOrName, search: ColumnOrName, replace: t.Optional[ColumnOrName] = None
) -> Column:
    """
    Replaces all occurrences of `search` with `replace`.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    src : :class:`~pyspark.sql.Column` or str
        A column of string to be replaced.
    search : :class:`~pyspark.sql.Column` or str
        A column of string, If `search` is not found in `str`, `str` is returned unchanged.
    replace : :class:`~pyspark.sql.Column` or str, optional
        A column of string, If `replace` is not specified or is an empty string,
        nothing replaces the string that is removed from `str`.

    Examples
    --------
    >>> df = spark.createDataFrame([("ABCabc", "abc", "DEF",)], ["a", "b", "c"])
    >>> df.select(replace(df.a, df.b, df.c).alias('r')).collect()
    [Row(r='ABCDEF')]

    >>> df.select(replace(df.a, df.b).alias('r')).collect()
    [Row(r='ABC')]
    """
    if replace is None and (
        _get_session()._is_duckdb or _get_session()._is_postgres or _get_session()._is_bigquery
    ):
        replace = expression.Literal.string("")  # type: ignore

    if replace is not None:
        return Column.invoke_anonymous_function(src, "replace", search, replace)
    else:
        return Column.invoke_anonymous_function(src, "replace", search)


@meta()
def right(str: ColumnOrName, len: ColumnOrName) -> Column:
    """
    Returns the rightmost `len`(`len` can be string type) characters from the string `str`,
    if `len` is less or equal than 0 the result is an empty string.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        Input column or strings.
    len : :class:`~pyspark.sql.Column` or str
        Input column or strings, the rightmost `len`.

    Examples
    --------
    >>> df = spark.createDataFrame([("Spark SQL", 3,)], ['a', 'b'])
    >>> df.select(right(df.a, df.b).alias('r')).collect()
    [Row(r='SQL')]
    """
    session = _get_session()

    if session._is_postgres:
        len = Column.ensure_col(len).cast("integer")

    return Column.invoke_expression_over_column(str, expression.Right, expression=len)


rlike = regexp_like
sha = sha1


@meta()
def sign(col: ColumnOrName) -> Column:
    """
    Computes the signum of the given value.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.range(1).select(
    ...     sf.sign(sf.lit(-5)),
    ...     sf.sign(sf.lit(6))
    ... ).show()
    +--------+-------+
    |sign(-5)|sign(6)|
    +--------+-------+
    |    -1.0|    1.0|
    +--------+-------+
    """
    return Column.invoke_expression_over_column(col, expression.Sign)


@meta(unsupported_engines="*")
def some(col: ColumnOrName) -> Column:
    """
    Aggregate function: returns true if at least one value of `col` is true.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        column to check if at least one value is true.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        true if at least one value of `col` is true, false otherwise.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [[True], [True], [True]], ["flag"]
    ... ).select(sf.some("flag")).show()
    +----------+
    |some(flag)|
    +----------+
    |      true|
    +----------+

    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [[True], [False], [True]], ["flag"]
    ... ).select(sf.some("flag")).show()
    +----------+
    |some(flag)|
    +----------+
    |      true|
    +----------+

    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [[False], [False], [False]], ["flag"]
    ... ).select(sf.some("flag")).show()
    +----------+
    |some(flag)|
    +----------+
    |     false|
    +----------+
    """
    return Column.invoke_anonymous_function(col, "some")


@meta(unsupported_engines="*")
def spark_partition_id() -> Column:
    """A column for partition ID.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    This is non deterministic because it depends on data partitioning and task scheduling.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        partition id the record belongs to.

    Examples
    --------
    >>> df = spark.range(2)
    >>> df.repartition(1).select(spark_partition_id().alias("pid")).collect()
    [Row(pid=0), Row(pid=0)]
    """
    return Column.invoke_anonymous_function(None, "spark_partition_id")


@meta(unsupported_engines=["bigquery", "postgres"])
def split_part(src: ColumnOrName, delimiter: ColumnOrName, partNum: ColumnOrName) -> Column:
    """
    Splits `str` by delimiter and return requested part of the split (1-based).
    If any input is null, returns null. if `partNum` is out of range of split parts,
    returns empty string. If `partNum` is 0, throws an error. If `partNum` is negative,
    the parts are counted backward from the end of the string.
    If the `delimiter` is an empty string, the `str` is not split.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    src : :class:`~pyspark.sql.Column` or str
        A column of string to be splited.
    delimiter : :class:`~pyspark.sql.Column` or str
        A column of string, the delimiter used for split.
    partNum : :class:`~pyspark.sql.Column` or str
        A column of string, requested part of the split (1-based).

    Examples
    --------
    >>> df = spark.createDataFrame([("11.12.13", ".", 3,)], ["a", "b", "c"])
    >>> df.select(split_part(df.a, df.b, df.c).alias('r')).collect()
    [Row(r='13')]
    """
    return Column.invoke_expression_over_column(
        src, expression.SplitPart, delimiter=delimiter, part_index=partNum
    )


@meta()
def startswith(str: ColumnOrName, prefix: ColumnOrName) -> Column:
    """
    Returns a boolean. The value is True if str starts with prefix.
    Returns NULL if either input expression is NULL. Otherwise, returns False.
    Both str or prefix must be of STRING or BINARY type.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        A column of string.
    prefix : :class:`~pyspark.sql.Column` or str
        A column of string, the prefix.

    Examples
    --------
    >>> df = spark.createDataFrame([("Spark SQL", "Spark",)], ["a", "b"])
    >>> df.select(startswith(df.a, df.b).alias('r')).collect()
    [Row(r=True)]

    >>> df = spark.createDataFrame([("414243", "4142",)], ["e", "f"])
    >>> df = df.select(to_binary("e").alias("e"), to_binary("f").alias("f"))
    >>> df.printSchema()
    root
     |-- e: binary (nullable = true)
     |-- f: binary (nullable = true)
    >>> df.select(startswith("e", "f"), startswith("f", "e")).show()
    +----------------+----------------+
    |startswith(e, f)|startswith(f, e)|
    +----------------+----------------+
    |            true|           false|
    +----------------+----------------+
    """
    return Column.invoke_expression_over_column(str, expression.StartsWith, expression=prefix)


@meta(unsupported_engines="*")
def std(col: ColumnOrName) -> Column:
    """
    Aggregate function: alias for stddev_samp.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        standard deviation of given column.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.range(6).select(sf.std("id")).show()
    +------------------+
    |           std(id)|
    +------------------+
    |1.8708286933869...|
    +------------------+
    """
    return Column.invoke_anonymous_function(col, "std")


@meta(unsupported_engines="*")
def str_to_map(
    text: ColumnOrName,
    pairDelim: t.Optional[ColumnOrName] = None,
    keyValueDelim: t.Optional[ColumnOrName] = None,
) -> Column:
    """
    Creates a map after splitting the text into key/value pairs using delimiters.
    Both `pairDelim` and `keyValueDelim` are treated as regular expressions.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    text : :class:`~pyspark.sql.Column` or str
        Input column or strings.
    pairDelim : :class:`~pyspark.sql.Column` or str, optional
        delimiter to use to split pair.
    keyValueDelim : :class:`~pyspark.sql.Column` or str, optional
        delimiter to use to split key/value.

    Examples
    --------
    >>> df = spark.createDataFrame([("a:1,b:2,c:3",)], ["e"])
    >>> df.select(str_to_map(df.e, lit(","), lit(":")).alias('r')).collect()
    [Row(r={'a': '1', 'b': '2', 'c': '3'})]

    >>> df = spark.createDataFrame([("a:1,b:2,c:3",)], ["e"])
    >>> df.select(str_to_map(df.e, lit(",")).alias('r')).collect()
    [Row(r={'a': '1', 'b': '2', 'c': '3'})]

    >>> df = spark.createDataFrame([("a:1,b:2,c:3",)], ["e"])
    >>> df.select(str_to_map(df.e).alias('r')).collect()
    [Row(r={'a': '1', 'b': '2', 'c': '3'})]
    """
    if pairDelim is None:
        pairDelim = lit(",")
    if keyValueDelim is None:
        keyValueDelim = lit(":")
    return Column.invoke_expression_over_column(
        text, expression.StrToMap, pair_delim=pairDelim, key_value_delim=keyValueDelim
    )


@meta(unsupported_engines="postgres")
def substr(str: ColumnOrName, pos: ColumnOrName, len: t.Optional[ColumnOrName] = None) -> Column:
    """
    Returns the substring of `str` that starts at `pos` and is of length `len`,
    or the slice of byte array that starts at `pos` and is of length `len`.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    src : :class:`~pyspark.sql.Column` or str
        A column of string.
    pos : :class:`~pyspark.sql.Column` or str
        A column of string, the substring of `str` that starts at `pos`.
    len : :class:`~pyspark.sql.Column` or str, optional
        A column of string, the substring of `str` is of length `len`.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("Spark SQL", 5, 1,)], ["a", "b", "c"]
    ... ).select(sf.substr("a", "b", "c")).show()
    +---------------+
    |substr(a, b, c)|
    +---------------+
    |              k|
    +---------------+

    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("Spark SQL", 5, 1,)], ["a", "b", "c"]
    ... ).select(sf.substr("a", "b")).show()
    +------------------------+
    |substr(a, b, 2147483647)|
    +------------------------+
    |                   k SQL|
    +------------------------+
    """
    return Column.invoke_expression_over_column(str, expression.Substring, start=pos, length=len)


@meta(unsupported_engines="*")
def timestamp_micros(col: ColumnOrName) -> Column:
    """
    Creates timestamp from the number of microseconds since UTC epoch.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        unix time values.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        converted timestamp value.

    Examples
    --------
    >>> spark.conf.set("spark.sql.session.timeZone", "UTC")
    >>> time_df = spark.createDataFrame([(1230219000,)], ['unix_time'])
    >>> time_df.select(timestamp_micros(time_df.unix_time).alias('ts')).show()
    +--------------------+
    |                  ts|
    +--------------------+
    |1970-01-01 00:20:...|
    +--------------------+
    >>> time_df.select(timestamp_micros('unix_time').alias('ts')).printSchema()
    root
     |-- ts: timestamp (nullable = true)
    >>> spark.conf.unset("spark.sql.session.timeZone")
    """
    return Column.invoke_anonymous_function(col, "timestamp_micros")


@meta(unsupported_engines="*")
def timestamp_millis(col: ColumnOrName) -> Column:
    """
    Creates timestamp from the number of milliseconds since UTC epoch.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        unix time values.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        converted timestamp value.

    Examples
    --------
    >>> spark.conf.set("spark.sql.session.timeZone", "UTC")
    >>> time_df = spark.createDataFrame([(1230219000,)], ['unix_time'])
    >>> time_df.select(timestamp_millis(time_df.unix_time).alias('ts')).show()
    +-------------------+
    |                 ts|
    +-------------------+
    |1970-01-15 05:43:39|
    +-------------------+
    >>> time_df.select(timestamp_millis('unix_time').alias('ts')).printSchema()
    root
     |-- ts: timestamp (nullable = true)
    >>> spark.conf.unset("spark.sql.session.timeZone")
    """
    return Column.invoke_anonymous_function(col, "timestamp_millis")


@meta(unsupported_engines="*")
def to_char(col: ColumnOrName, format: ColumnOrName) -> Column:
    """
    Convert `col` to a string based on the `format`.
    Throws an exception if the conversion fails. The format can consist of the following
    characters, case insensitive:
    '0' or '9': Specifies an expected digit between 0 and 9. A sequence of 0 or 9 in the
    format string matches a sequence of digits in the input value, generating a result
    string of the same length as the corresponding sequence in the format string.
    The result string is left-padded with zeros if the 0/9 sequence comprises more digits
    than the matching part of the decimal value, starts with 0, and is before the decimal
    point. Otherwise, it is padded with spaces.
    '.' or 'D': Specifies the position of the decimal point (optional, only allowed once).
    ',' or 'G': Specifies the position of the grouping (thousands) separator (,).
    There must be a 0 or 9 to the left and right of each grouping separator.
    '$': Specifies the location of the $ currency sign. This character may only be specified once.
    'S' or 'MI': Specifies the position of a '-' or '+' sign (optional, only allowed once at
    the beginning or end of the format string). Note that 'S' prints '+' for positive
    values but 'MI' prints a space.
    'PR': Only allowed at the end of the format string; specifies that the result string
    will be wrapped by angle brackets if the input value is negative.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        Input column or strings.
    format : :class:`~pyspark.sql.Column` or str, optional
        format to use to convert char values.

    Examples
    --------
    >>> df = spark.createDataFrame([(78.12,)], ["e"])
    >>> df.select(to_char(df.e, lit("$99.99")).alias('r')).collect()
    [Row(r='$78.12')]
    """
    return Column.invoke_anonymous_function(col, "to_char", format)


@meta(unsupported_engines=["bigquery", "duckdb"])
def to_number(col: ColumnOrName, format: ColumnOrName) -> Column:
    """
    Convert string 'col' to a number based on the string format 'format'.
    Throws an exception if the conversion fails. The format can consist of the following
    characters, case insensitive:
    '0' or '9': Specifies an expected digit between 0 and 9. A sequence of 0 or 9 in the
    format string matches a sequence of digits in the input string. If the 0/9
    sequence starts with 0 and is before the decimal point, it can only match a digit
    sequence of the same size. Otherwise, if the sequence starts with 9 or is after
    the decimal point, it can match a digit sequence that has the same or smaller size.
    '.' or 'D': Specifies the position of the decimal point (optional, only allowed once).
    ',' or 'G': Specifies the position of the grouping (thousands) separator (,).
    There must be a 0 or 9 to the left and right of each grouping separator.
    'col' must match the grouping separator relevant for the size of the number.
    '$': Specifies the location of the $ currency sign. This character may only be
    specified once.
    'S' or 'MI': Specifies the position of a '-' or '+' sign (optional, only allowed
    once at the beginning or end of the format string). Note that 'S' allows '-'
    but 'MI' does not.
    'PR': Only allowed at the end of the format string; specifies that 'col' indicates a
    negative number with wrapping angled brackets.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        Input column or strings.
    format : :class:`~pyspark.sql.Column` or str, optional
        format to use to convert number values.

    Examples
    --------
    >>> df = spark.createDataFrame([("$78.12",)], ["e"])
    >>> df.select(to_number(df.e, lit("$99.99")).alias('r')).collect()
    [Row(r=Decimal('78.12'))]
    """
    from sqlframe.base.function_alternatives import to_number_using_to_double

    session = _get_session()

    if session._is_snowflake:
        return to_number_using_to_double(col, format)

    return Column.invoke_expression_over_column(col, expression.ToNumber, format=format)


def to_str(value: t.Any) -> t.Optional[str]:
    """
    A wrapper over str(), but converts bool values to lower case strings.
    If None is given, just returns None, instead of converting it to string "None".
    """
    if isinstance(value, bool):
        return str(value).lower()
    elif value is None:
        return value
    else:
        return str(value)


@meta(unsupported_engines=["*", "databricks"])
def to_timestamp_ltz(
    timestamp: ColumnOrName,
    format: t.Optional[ColumnOrName] = None,
) -> Column:
    """
    Parses the `timestamp` with the `format` to a timestamp without time zone.
    Returns null with invalid input.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    timestamp : :class:`~pyspark.sql.Column` or str
        Input column or strings.
    format : :class:`~pyspark.sql.Column` or str, optional
        format to use to convert type `TimestampType` timestamp values.

    Examples
    --------
    >>> df = spark.createDataFrame([("2016-12-31",)], ["e"])
    >>> df.select(to_timestamp_ltz(df.e, lit("yyyy-MM-dd")).alias('r')).collect()
    ... # doctest: +SKIP
    [Row(r=datetime.datetime(2016, 12, 31, 0, 0))]

    >>> df = spark.createDataFrame([("2016-12-31",)], ["e"])
    >>> df.select(to_timestamp_ltz(df.e).alias('r')).collect()
    ... # doctest: +SKIP
    [Row(r=datetime.datetime(2016, 12, 31, 0, 0))]
    """
    if format is not None:
        return Column.invoke_anonymous_function(timestamp, "to_timestamp_ltz", format)
    else:
        return Column.invoke_anonymous_function(timestamp, "to_timestamp_ltz")


@meta()
def to_timestamp_ntz(
    timestamp: ColumnOrName,
    format: t.Optional[ColumnOrName] = None,
) -> Column:
    """
    Parses the `timestamp` with the `format` to a timestamp without time zone.
    Returns null with invalid input.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    timestamp : :class:`~pyspark.sql.Column` or str
        Input column or strings.
    format : :class:`~pyspark.sql.Column` or str, optional
        format to use to convert type `TimestampNTZType` timestamp values.

    Examples
    --------
    >>> df = spark.createDataFrame([("2016-04-08",)], ["e"])
    >>> df.select(to_timestamp_ntz(df.e, lit("yyyy-MM-dd")).alias('r')).collect()
    ... # doctest: +SKIP
    [Row(r=datetime.datetime(2016, 4, 8, 0, 0))]

    >>> df = spark.createDataFrame([("2016-04-08",)], ["e"])
    >>> df.select(to_timestamp_ntz(df.e).alias('r')).collect()
    ... # doctest: +SKIP
    [Row(r=datetime.datetime(2016, 4, 8, 0, 0))]
    """
    session = _get_session()

    if session._is_duckdb:
        to_timestamp_func = get_func_from_session("to_timestamp")
        return to_timestamp_func(timestamp, format)

    if session._is_bigquery:
        if format is not None:
            return Column.invoke_anonymous_function(
                session.format_execution_time(format),  # type: ignore
                "parse_datetime",
                timestamp,
            )
        else:
            return Column.ensure_col(timestamp).cast("datetime", dialect="bigquery")

    if session._is_postgres:
        if format is not None:
            return Column.invoke_anonymous_function(
                timestamp,
                "to_timestamp",
                session.format_execution_time(format),  # type: ignore
            )
        else:
            return Column.ensure_col(timestamp).cast("timestamp", dialect="postgres")

    if format is not None:
        return Column.invoke_anonymous_function(timestamp, "to_timestamp_ntz", format)
    else:
        return Column.invoke_anonymous_function(timestamp, "to_timestamp_ntz")


@meta(unsupported_engines=["bigquery", "postgres", "snowflake"])
def to_unix_timestamp(
    timestamp: ColumnOrName,
    format: t.Optional[ColumnOrName] = None,
) -> Column:
    """
    Returns the UNIX timestamp of the given time.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    timestamp : :class:`~pyspark.sql.Column` or str
        Input column or strings.
    format : :class:`~pyspark.sql.Column` or str, optional
        format to use to convert UNIX timestamp values.

    Examples
    --------
    >>> spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    >>> df = spark.createDataFrame([("2016-04-08",)], ["e"])
    >>> df.select(to_unix_timestamp(df.e, lit("yyyy-MM-dd")).alias('r')).collect()
    [Row(r=1460098800)]
    >>> spark.conf.unset("spark.sql.session.timeZone")

    >>> spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    >>> df = spark.createDataFrame([("2016-04-08",)], ["e"])
    >>> df.select(to_unix_timestamp(df.e).alias('r')).collect()
    [Row(r=None)]
    >>> spark.conf.unset("spark.sql.session.timeZone")
    """
    session = _get_session()

    if session._is_duckdb:
        format = format or _BaseSession().default_time_format

    if format is not None:
        return Column.invoke_expression_over_column(
            timestamp, expression.StrToUnix, format=session.format_time(format)
        )
    else:
        return Column.invoke_expression_over_column(timestamp, expression.StrToUnix)


@meta(unsupported_engines="*")
def to_varchar(col: ColumnOrName, format: ColumnOrName) -> Column:
    """
    Convert `col` to a string based on the `format`.
    Throws an exception if the conversion fails. The format can consist of the following
    characters, case insensitive:
    '0' or '9': Specifies an expected digit between 0 and 9. A sequence of 0 or 9 in the
    format string matches a sequence of digits in the input value, generating a result
    string of the same length as the corresponding sequence in the format string.
    The result string is left-padded with zeros if the 0/9 sequence comprises more digits
    than the matching part of the decimal value, starts with 0, and is before the decimal
    point. Otherwise, it is padded with spaces.
    '.' or 'D': Specifies the position of the decimal point (optional, only allowed once).
    ',' or 'G': Specifies the position of the grouping (thousands) separator (,).
    There must be a 0 or 9 to the left and right of each grouping separator.
    '$': Specifies the location of the $ currency sign. This character may only be specified once.
    'S' or 'MI': Specifies the position of a '-' or '+' sign (optional, only allowed once at
    the beginning or end of the format string). Note that 'S' prints '+' for positive
    values but 'MI' prints a space.
    'PR': Only allowed at the end of the format string; specifies that the result string
    will be wrapped by angle brackets if the input value is negative.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        Input column or strings.
    format : :class:`~pyspark.sql.Column` or str, optional
        format to use to convert char values.

    Examples
    --------
    >>> df = spark.createDataFrame([(78.12,)], ["e"])
    >>> df.select(to_varchar(df.e, lit("$99.99")).alias('r')).collect()
    [Row(r='$78.12')]
    """
    return Column.invoke_anonymous_function(col, "to_varchar", format)


@meta(unsupported_engines="*")
def try_aes_decrypt(
    input: ColumnOrName,
    key: ColumnOrName,
    mode: t.Optional[ColumnOrName] = None,
    padding: t.Optional[ColumnOrName] = None,
    aad: t.Optional[ColumnOrName] = None,
) -> Column:
    """
    This is a special version of `aes_decrypt` that performs the same operation,
    but returns a NULL value instead of raising an error if the decryption cannot be performed.
    Returns a decrypted value of `input` using AES in `mode` with `padding`. Key lengths of 16,
    24 and 32 bits are supported. Supported combinations of (`mode`, `padding`) are ('ECB',
    'PKCS'), ('GCM', 'NONE') and ('CBC', 'PKCS'). t.Optional additional authenticated data (AAD) is
    only supported for GCM. If provided for encryption, the identical AAD value must be provided
    for decryption. The default mode is GCM.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    input : :class:`~pyspark.sql.Column` or str
        The binary value to decrypt.
    key : :class:`~pyspark.sql.Column` or str
        The passphrase to use to decrypt the data.
    mode : :class:`~pyspark.sql.Column` or str, optional
        Specifies which block cipher mode should be used to decrypt messages. Valid modes: ECB,
        GCM, CBC.
    padding : :class:`~pyspark.sql.Column` or str, optional
        Specifies how to pad messages whose length is not a multiple of the block size. Valid
        values: PKCS, NONE, DEFAULT. The DEFAULT padding means PKCS for ECB, NONE for GCM and PKCS
        for CBC.
    aad : :class:`~pyspark.sql.Column` or str, optional
        t.Optional additional authenticated data. Only supported for GCM mode. This can be any
        free-form input and must be provided for both encryption and decryption.

    Examples
    --------
    >>> df = spark.createDataFrame([(
    ...     "AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4",
    ...     "abcdefghijklmnop12345678ABCDEFGH", "GCM", "DEFAULT",
    ...     "This is an AAD mixed into the input",)],
    ...     ["input", "key", "mode", "padding", "aad"]
    ... )
    >>> df.select(try_aes_decrypt(
    ...     unbase64(df.input), df.key, df.mode, df.padding, df.aad).alias('r')
    ... ).collect()
    [Row(r=bytearray(b'Spark'))]

    >>> df = spark.createDataFrame([(
    ...     "AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg=",
    ...     "abcdefghijklmnop12345678ABCDEFGH", "CBC", "DEFAULT",)],
    ...     ["input", "key", "mode", "padding"]
    ... )
    >>> df.select(try_aes_decrypt(
    ...     unbase64(df.input), df.key, df.mode, df.padding).alias('r')
    ... ).collect()
    [Row(r=bytearray(b'Spark'))]

    >>> df.select(try_aes_decrypt(unbase64(df.input), df.key, df.mode).alias('r')).collect()
    [Row(r=bytearray(b'Spark'))]

    >>> df = spark.createDataFrame([(
    ...     "83F16B2AA704794132802D248E6BFD4E380078182D1544813898AC97E709B28A94",
    ...     "0000111122223333",)],
    ...     ["input", "key"]
    ... )
    >>> df.select(try_aes_decrypt(unhex(df.input), df.key).alias('r')).collect()
    [Row(r=bytearray(b'Spark'))]
    """
    _mode = lit("GCM") if mode is None else mode
    _padding = lit("DEFAULT") if padding is None else padding
    _aad = lit("") if aad is None else aad
    return Column.invoke_anonymous_function(input, "try_aes_decrypt", key, _mode, _padding, _aad)


@meta(unsupported_engines=["bigquery", "snowflake"])
def try_element_at(col: ColumnOrName, extraction: ColumnOrName) -> Column:
    """
    (array, index) - Returns element of array at given (1-based) index. If Index is 0, Spark will
    throw an error. If index < 0, accesses elements from the last to the first. The function
    always returns NULL if the index exceeds the length of the array.

    (map, key) - Returns value for given key. The function always returns NULL if the key is not
    contained in the map.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        name of column containing array or map
    extraction :
        index to check for in array or key to check for in map

    Examples
    --------
    >>> df = spark.createDataFrame([(["a", "b", "c"],)], ['data'])
    >>> df.select(try_element_at(df.data, lit(1)).alias('r')).collect()
    [Row(r='a')]
    >>> df.select(try_element_at(df.data, lit(-1)).alias('r')).collect()
    [Row(r='c')]

    >>> df = spark.createDataFrame([({"a": 1.0, "b": 2.0},)], ['data'])
    >>> df.select(try_element_at(df.data, lit("a")).alias('r')).collect()
    [Row(r=1.0)]
    """
    session = _get_session()

    if session._is_databricks or session._is_duckdb or session._is_postgres or session._is_spark:
        lit = get_func_from_session("lit")
        extraction = Column.ensure_col(extraction)
        if (
            isinstance(extraction.column_expression, expression.Literal)
            and extraction.column_expression.is_number
        ):
            extraction = extraction - lit(1)

    return Column(
        expression.Bracket(
            this=Column.ensure_col(col).column_expression,
            expressions=[Column.ensure_col(extraction).column_expression],
            safe=True,
        )
    )


@meta()
def try_to_timestamp(col: ColumnOrName, format: t.Optional[ColumnOrName] = None) -> Column:
    """
    Parses the `col` with the `format` to a timestamp. The function always
    returns null on an invalid input with/without ANSI SQL mode enabled. The result data type is
    consistent with the value of configuration `spark.sql.timestampType`.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        column values to convert.
    format: str, optional
        format to use to convert timestamp values.

    Examples
    --------
    >>> df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
    >>> df.select(try_to_timestamp(df.t).alias('dt')).collect()
    [Row(dt=datetime.datetime(1997, 2, 28, 10, 30))]

    >>> df.select(try_to_timestamp(df.t, lit('yyyy-MM-dd HH:mm:ss')).alias('dt')).collect()
    [Row(dt=datetime.datetime(1997, 2, 28, 10, 30))]
    """
    from sqlframe.base.function_alternatives import (
        try_to_timestamp_pgtemp,
        try_to_timestamp_safe,
        try_to_timestamp_strptime,
    )

    session = _get_session()

    if session._is_bigquery:
        return try_to_timestamp_safe(col, format)

    if session._is_duckdb:
        return try_to_timestamp_strptime(col, format)

    if session._is_postgres:
        return try_to_timestamp_pgtemp(col, format)

    return Column.invoke_anonymous_function(
        col,
        "try_to_timestamp",
        session.format_execution_time(format),  # type: ignore
    )


@meta()
def ucase(str: ColumnOrName) -> Column:
    """
    Returns `str` with all characters changed to uppercase.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        Input column or strings.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.range(1).select(sf.ucase(sf.lit("Spark"))).show()
    +------------+
    |ucase(Spark)|
    +------------+
    |       SPARK|
    +------------+
    """
    return Column.invoke_expression_over_column(str, expression.Upper)


@meta(unsupported_engines=["bigquery", "snowflake"])
def unix_date(col: ColumnOrName) -> Column:
    """Returns the number of days since 1970-01-01.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    >>> df = spark.createDataFrame([('1970-01-02',)], ['t'])
    >>> df.select(unix_date(to_date(df.t)).alias('n')).collect()
    [Row(n=1)]
    >>> spark.conf.unset("spark.sql.session.timeZone")
    """
    return Column.invoke_expression_over_column(col, expression.UnixDate)


@meta()
def unix_micros(col: ColumnOrName) -> Column:
    """Returns the number of microseconds since 1970-01-01 00:00:00 UTC.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    >>> df = spark.createDataFrame([('2015-07-22 10:00:00',)], ['t'])
    >>> df.select(unix_micros(to_timestamp(df.t)).alias('n')).collect()
    [Row(n=1437584400000000)]
    >>> spark.conf.unset("spark.sql.session.timeZone")
    """
    from sqlframe.base.function_alternatives import unix_micros_multiply_epoch

    if _get_session()._is_duckdb:
        return Column.invoke_anonymous_function(col, "epoch_us")

    if _get_session()._is_bigquery:
        return Column(
            expression.Anonymous(
                this="UNIX_MICROS",
                expressions=[
                    expression.Anonymous(
                        this="TIMESTAMP",
                        expressions=[
                            Column.ensure_col(col).column_expression,
                        ],
                    )
                ],
            )
        )

    if _get_session()._is_postgres or _get_session()._is_snowflake:
        return unix_micros_multiply_epoch(col)

    return Column.invoke_anonymous_function(col, "unix_micros")


@meta()
def unix_millis(col: ColumnOrName) -> Column:
    """Returns the number of milliseconds since 1970-01-01 00:00:00 UTC.
    Truncates higher levels of precision.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    >>> df = spark.createDataFrame([('2015-07-22 10:00:00',)], ['t'])
    >>> df.select(unix_millis(to_timestamp(df.t)).alias('n')).collect()
    [Row(n=1437584400000)]
    >>> spark.conf.unset("spark.sql.session.timeZone")
    """
    from sqlframe.base.function_alternatives import unix_millis_multiply_epoch

    if (
        _get_session()._is_bigquery
        or _get_session()._is_duckdb
        or _get_session()._is_postgres
        or _get_session()._is_snowflake
    ):
        return unix_millis_multiply_epoch(col)

    return Column.invoke_anonymous_function(col, "unix_millis")


@meta()
def unix_seconds(col: ColumnOrName) -> Column:
    """Returns the number of seconds since 1970-01-01 00:00:00 UTC.
    Truncates higher levels of precision.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    >>> df = spark.createDataFrame([('2015-07-22 10:00:00',)], ['t'])
    >>> df.select(unix_seconds(to_timestamp(df.t)).alias('n')).collect()
    [Row(n=1437584400)]
    >>> spark.conf.unset("spark.sql.session.timeZone")
    """
    from sqlframe.base.function_alternatives import unix_seconds_extract_epoch

    if _get_session()._is_postgres:
        return unix_seconds_extract_epoch(col)

    if _get_session()._is_bigquery:
        return Column(
            expression.Anonymous(
                this="UNIX_SECONDS",
                expressions=[
                    expression.Anonymous(
                        this="TIMESTAMP",
                        expressions=[
                            Column.ensure_col(col).column_expression,
                            expression.Literal.string("UTC"),
                        ],
                    )
                ],
            )
        )

    return Column.invoke_expression_over_column(col, expression.UnixSeconds)


@meta(unsupported_engines="*")
def url_decode(str: ColumnOrName) -> Column:
    """
    Decodes a `str` in 'application/x-www-form-urlencoded' format
    using a specific encoding scheme.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        A column of string to decode.

    Examples
    --------
    >>> df = spark.createDataFrame([("https%3A%2F%2Fspark.apache.org",)], ["a"])
    >>> df.select(url_decode(df.a).alias('r')).collect()
    [Row(r='https://spark.apache.org')]
    """
    return Column.invoke_anonymous_function(str, "url_decode")


@meta(unsupported_engines="*")
def url_encode(str: ColumnOrName) -> Column:
    """
    Translates a string into 'application/x-www-form-urlencoded' format
    using a specific encoding scheme.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        A column of string to encode.

    Examples
    --------
    >>> df = spark.createDataFrame([("https://spark.apache.org",)], ["a"])
    >>> df.select(url_encode(df.a).alias('r')).collect()
    [Row(r='https%3A%2F%2Fspark.apache.org')]
    """
    return Column.invoke_anonymous_function(str, "url_encode")


user = current_user


@meta(unsupported_engines="*")
def version() -> Column:
    """
    Returns the Spark version. The string contains 2 fields, the first being a release version
    and the second being a git revision.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(version()).show(truncate=False) # doctest: +SKIP
    +----------------------------------------------+
    |version()                                     |
    +----------------------------------------------+
    |3.5.0 cafbea5b13623276517a9d716f75745eff91f616|
    +----------------------------------------------+
    """
    return Column.invoke_anonymous_function(None, "version")


@meta(unsupported_engines="*")
def weekday(col: ColumnOrName) -> Column:
    """
    Returns the day of the week for date/timestamp (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target date/timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the day of the week for date/timestamp (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> df.select(weekday('dt').alias('day')).show()
    +---+
    |day|
    +---+
    |  2|
    +---+
    """
    return Column.invoke_anonymous_function(col, "weekday")


@meta(unsupported_engines="*")
def width_bucket(
    v: ColumnOrName,
    min: ColumnOrName,
    max: ColumnOrName,
    numBucket: t.Union[ColumnOrName, int],
) -> Column:
    """
    Returns the bucket number into which the value of this expression would fall
    after being evaluated. Note that input arguments must follow conditions listed below;
    otherwise, the method will return null.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    v : str or :class:`~pyspark.sql.Column`
        value to compute a bucket number in the histogram
    min : str or :class:`~pyspark.sql.Column`
        minimum value of the histogram
    max : str or :class:`~pyspark.sql.Column`
        maximum value of the histogram
    numBucket : str, :class:`~pyspark.sql.Column` or int
        the number of buckets

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the bucket number into which the value would fall after being evaluated

    Examples
    --------
    >>> df = spark.createDataFrame([
    ...     (5.3, 0.2, 10.6, 5),
    ...     (-2.1, 1.3, 3.4, 3),
    ...     (8.1, 0.0, 5.7, 4),
    ...     (-0.9, 5.2, 0.5, 2)],
    ...     ['v', 'min', 'max', 'n'])
    >>> df.select(width_bucket('v', 'min', 'max', 'n')).show()
    +----------------------------+
    |width_bucket(v, min, max, n)|
    +----------------------------+
    |                           3|
    |                           0|
    |                           5|
    |                           3|
    +----------------------------+
    """
    numBucket = lit(numBucket) if isinstance(numBucket, int) else numBucket
    return Column.invoke_anonymous_function(v, "width_bucket", min, max, numBucket)


@meta(unsupported_engines=["*", "spark"])
def window_time(
    windowColumn: ColumnOrName,
) -> Column:
    """Computes the event time from a window column. The column window values are produced
    by window aggregating operators and are of type `STRUCT<start: TIMESTAMP, end: TIMESTAMP>`
    where start is inclusive and end is exclusive. The event time of records produced by window
    aggregating operators can be computed as ``window_time(window)`` and are
    ``window.end - lit(1).alias("microsecond")`` (as microsecond is the minimal supported event
    time precision). The window column must be one produced by a window aggregating operator.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    windowColumn : :class:`~pyspark.sql.Column`
        The window column of a window aggregate records.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Notes
    -----
    Supports Spark Connect.

    Examples
    --------
    >>> import datetime
    >>> df = spark.createDataFrame(
    ...     [(datetime.datetime(2016, 3, 11, 9, 0, 7), 1)],
    ... ).toDF("date", "val")

    Group the data into 5 second time windows and aggregate as sum.

    >>> w = df.groupBy(window("date", "5 seconds")).agg(sum("val").alias("sum"))

    Extract the window event time using the window_time function.

    >>> w.select(
    ...     w.window.end.cast("string").alias("end"),
    ...     window_time(w.window).cast("string").alias("window_time"),
    ...     "sum"
    ... ).collect()
    [Row(end='2016-03-11 09:00:10', window_time='2016-03-11 09:00:09.999999', sum=1)]
    """
    return Column.invoke_anonymous_function(windowColumn, "window_time")


@meta(unsupported_engines="*")
def xpath(xml: ColumnOrName, path: ColumnOrName) -> Column:
    """
    Returns a string array of values within the nodes of xml that match the XPath expression.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> df = spark.createDataFrame(
    ...     [('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>',)], ['x'])
    >>> df.select(xpath(df.x, lit('a/b/text()')).alias('r')).collect()
    [Row(r=['b1', 'b2', 'b3'])]
    """
    return Column.invoke_anonymous_function(xml, "xpath", path)


@meta(unsupported_engines="*")
def xpath_boolean(xml: ColumnOrName, path: ColumnOrName) -> Column:
    """
    Returns true if the XPath expression evaluates to true, or if a matching node is found.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> df = spark.createDataFrame([('<a><b>1</b></a>',)], ['x'])
    >>> df.select(xpath_boolean(df.x, lit('a/b')).alias('r')).collect()
    [Row(r=True)]
    """
    return Column.invoke_anonymous_function(xml, "xpath_boolean", path)


@meta(unsupported_engines="*")
def xpath_double(xml: ColumnOrName, path: ColumnOrName) -> Column:
    """
    Returns a double value, the value zero if no match is found,
    or NaN if a match is found but the value is non-numeric.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> df = spark.createDataFrame([('<a><b>1</b><b>2</b></a>',)], ['x'])
    >>> df.select(xpath_double(df.x, lit('sum(a/b)')).alias('r')).collect()
    [Row(r=3.0)]
    """
    return Column.invoke_anonymous_function(xml, "xpath_double", path)


@meta(unsupported_engines="*")
def xpath_float(xml: ColumnOrName, path: ColumnOrName) -> Column:
    """
    Returns a float value, the value zero if no match is found,
    or NaN if a match is found but the value is non-numeric.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> df = spark.createDataFrame([('<a><b>1</b><b>2</b></a>',)], ['x'])
    >>> df.select(xpath_float(df.x, lit('sum(a/b)')).alias('r')).collect()
    [Row(r=3.0)]
    """
    return Column.invoke_anonymous_function(xml, "xpath_float", path)


@meta(unsupported_engines="*")
def xpath_int(xml: ColumnOrName, path: ColumnOrName) -> Column:
    """
    Returns an integer value, or the value zero if no match is found,
    or a match is found but the value is non-numeric.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> df = spark.createDataFrame([('<a><b>1</b><b>2</b></a>',)], ['x'])
    >>> df.select(xpath_int(df.x, lit('sum(a/b)')).alias('r')).collect()
    [Row(r=3)]
    """
    return Column.invoke_anonymous_function(xml, "xpath_int", path)


@meta(unsupported_engines="*")
def xpath_long(xml: ColumnOrName, path: ColumnOrName) -> Column:
    """
    Returns a long integer value, or the value zero if no match is found,
    or a match is found but the value is non-numeric.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> df = spark.createDataFrame([('<a><b>1</b><b>2</b></a>',)], ['x'])
    >>> df.select(xpath_long(df.x, lit('sum(a/b)')).alias('r')).collect()
    [Row(r=3)]
    """
    return Column.invoke_anonymous_function(xml, "xpath_long", path)


@meta(unsupported_engines="*")
def xpath_number(xml: ColumnOrName, path: ColumnOrName) -> Column:
    """
    Returns a double value, the value zero if no match is found,
    or NaN if a match is found but the value is non-numeric.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [('<a><b>1</b><b>2</b></a>',)], ['x']
    ... ).select(sf.xpath_number('x', sf.lit('sum(a/b)'))).show()
    +-------------------------+
    |xpath_number(x, sum(a/b))|
    +-------------------------+
    |                      3.0|
    +-------------------------+
    """
    return Column.invoke_anonymous_function(xml, "xpath_number", path)


@meta(unsupported_engines="*")
def xpath_short(xml: ColumnOrName, path: ColumnOrName) -> Column:
    """
    Returns a short integer value, or the value zero if no match is found,
    or a match is found but the value is non-numeric.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> df = spark.createDataFrame([('<a><b>1</b><b>2</b></a>',)], ['x'])
    >>> df.select(xpath_short(df.x, lit('sum(a/b)')).alias('r')).collect()
    [Row(r=3)]
    """
    return Column.invoke_anonymous_function(xml, "xpath_short", path)


@meta(unsupported_engines="*")
def xpath_string(xml: ColumnOrName, path: ColumnOrName) -> Column:
    """
    Returns the text contents of the first xml node that matches the XPath expression.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> df = spark.createDataFrame([('<a><b>b</b><c>cc</c></a>',)], ['x'])
    >>> df.select(xpath_string(df.x, lit('a/c')).alias('r')).collect()
    [Row(r='cc')]
    """
    return Column.invoke_anonymous_function(xml, "xpath_string", path)


@meta(unsupported_engines="*")
def years(col: ColumnOrName) -> Column:
    """
    Partition transform function: A transform for timestamps and dates
    to partition data into years.

    .. versionadded:: 3.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target date or timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        data partitioned by years.

    Examples
    --------
    >>> df.writeTo("catalog.db.table").partitionedBy(  # doctest: +SKIP
    ...     years("ts")
    ... ).createOrReplace()

    Notes
    -----
    This function can be used only in combination with
    :py:meth:`~pyspark.sql.readwriter.DataFrameWriterV2.partitionedBy`
    method of the `DataFrameWriterV2`.

    """
    return Column.invoke_anonymous_function(col, "years")


# SQLFrame specific
@meta()
def _is_string(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import (
        _is_string_using_typeof_char_varying,
        _is_string_using_typeof_string,
        _is_string_using_typeof_string_lcase,
        _is_string_using_typeof_varchar,
    )

    session = _get_session()

    if session._is_bigquery:
        return _is_string_using_typeof_string(col)

    if session._is_duckdb:
        return _is_string_using_typeof_varchar(col)

    if session._is_postgres:
        return _is_string_using_typeof_char_varying(col)

    if session._is_databricks or session._is_spark:
        return _is_string_using_typeof_string_lcase(col)

    col = Column.invoke_anonymous_function(col, "TO_VARIANT")
    return Column.invoke_anonymous_function(col, "IS_VARCHAR")


@meta()
def _is_date(col: ColumnOrName) -> Column:
    typeof = get_func_from_session("typeof")
    upper = get_func_from_session("upper")
    return lit(upper(typeof(col)) == lit("DATE"))


@meta()
def _is_array(col: ColumnOrName) -> Column:
    typeof = get_func_from_session("typeof")
    upper = get_func_from_session("upper")
    startswith = get_func_from_session("startswith")
    endswith = get_func_from_session("endswith")
    return lit(
        (startswith(upper(typeof(col)), lit("ARRAY"))) | (endswith(upper(typeof(col)), lit("[]")))
    )


@meta()
def _is_int_variant(col: ColumnOrName) -> Column:
    typeof = get_func_from_session("typeof")
    upper = get_func_from_session("upper")
    return lit(
        (upper(typeof(col)) == lit("INTEGER"))
        | (upper(typeof(col)) == lit("BIGINT"))
        | (upper(typeof(col)) == lit("SMALLINT"))
        | (upper(typeof(col)) == lit("TINYINT"))
        | (upper(typeof(col)) == lit("INT"))
        | (upper(typeof(col)) == lit("INT64"))
    )


@meta()
def _lambda_quoted(value: str) -> t.Optional[bool]:
    return False if value == "_" else None


@meta()
def _get_lambda_from_func(lambda_expression: t.Callable):
    variables = [
        expression.to_identifier(x, quoted=_lambda_quoted(x))
        for x in lambda_expression.__code__.co_varnames
    ]
    return expression.Lambda(
        this=lambda_expression(*[Column(x) for x in variables]).column_expression,
        expressions=variables,
    )


def _ensure_column_of_optionals(optionals: t.List[t.Optional[ColumnOrName]]) -> t.List[Column]:
    for value in reversed(optionals.copy()):
        if value is not None:
            break
        optionals = optionals[:-1]
    return [Column.ensure_col(x) if x is not None else lit(None) for x in optionals]
