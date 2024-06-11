# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import logging
import typing as t

from sqlglot import exp as expression
from sqlglot.helper import ensure_list
from sqlglot.helper import flatten as _flatten

from sqlframe.base.column import Column
from sqlframe.base.decorators import func_metadata as meta

if t.TYPE_CHECKING:
    from sqlframe.base._typing import ColumnOrLiteral, ColumnOrName
    from sqlframe.base.session import DF
    from sqlframe.base.types import ArrayType, StructType

logger = logging.getLogger(__name__)


@meta()
def col(column_name: t.Union[ColumnOrName, t.Any]) -> Column:
    from sqlframe.base.session import _BaseSession

    dialect = _BaseSession().input_dialect
    if isinstance(column_name, str):
        return Column(
            expression.to_column(column_name, dialect=dialect).transform(
                dialect.normalize_identifier
            )
        )
    return Column(column_name)


@meta()
def lit(value: t.Optional[t.Any] = None) -> Column:
    if isinstance(value, str):
        return Column(expression.Literal.string(value))
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
        expression.Count(this=expression.Distinct(expressions=[x.expression for x in columns]))
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
        expression.Sum(this=expression.Distinct(expressions=[Column.ensure_col(col).expression]))
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
    return Column(expression.Anonymous(this="e"))


@meta()
def exp(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Exp)


@meta()
def expm1(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "EXPM1")


@meta()
def factorial(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "FACTORIAL")


@meta()
def floor(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Floor)


@meta()
def log10(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(lit(10), expression.Log, expression=col)


@meta()
def log1p(col: ColumnOrName) -> Column:
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
    return Column.invoke_anonymous_function(col, "DEGREES")


toDegrees = degrees


@meta()
def radians(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "RADIANS")


toRadians = radians


@meta()
def bitwiseNOT(col: ColumnOrName) -> Column:
    return bitwise_not(col)


@meta()
def bitwise_not(col: ColumnOrName) -> Column:
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
    return Column.invoke_anonymous_function(col, "SKEWNESS")


@meta(unsupported_engines=["bigquery", "postgres"])
def kurtosis(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "KURTOSIS")


@meta()
def collect_list(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.ArrayAgg)


@meta()
def collect_set(col: ColumnOrName) -> Column:
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
    return Column.invoke_expression_over_column(col, expression.IsNan)


@meta()
def isnull(col: ColumnOrName) -> Column:
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
    return Column.invoke_anonymous_function(col1, "NANVL", col2)


@meta(unsupported_engines="postgres")
def percentile_approx(
    col: ColumnOrName,
    percentage: t.Union[ColumnOrLiteral, t.List[float], t.Tuple[float]],
    accuracy: t.Optional[t.Union[ColumnOrLiteral, int]] = None,
) -> Column:
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
    if frequency:
        logger.warning("Frequency is not supported in all engines")
    return Column.invoke_expression_over_column(
        col, expression.PercentileDisc, expression=lit(percentage)
    )


@meta()
def rand(seed: t.Optional[int] = None) -> Column:
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
    if scale is not None:
        return Column.invoke_expression_over_column(col, expression.Round, decimals=scale)
    return Column.invoke_expression_over_column(col, expression.Round)


@meta(unsupported_engines=["duckdb", "postgres"])
def bround(col: ColumnOrName, scale: t.Optional[int] = None) -> Column:
    if scale is not None:
        return Column.invoke_anonymous_function(col, "BROUND", lit(scale))
    return Column.invoke_anonymous_function(col, "BROUND")


@meta()
def shiftleft(col: ColumnOrName, numBits: int) -> Column:
    return Column.invoke_expression_over_column(
        col, expression.BitwiseLeftShift, expression=lit(numBits)
    )


shiftLeft = shiftleft


@meta()
def shiftright(col: ColumnOrName, numBits: int) -> Column:
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
    columns = ensure_list(col) + list(cols)
    return Column.invoke_expression_over_column(None, expression.Struct, expressions=columns)


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
    return Column.invoke_expression_over_column(
        Column(expression.TimeStrToTime(this=Column.ensure_col(col).expression)),
        expression.TimeToStr,
        format=lit(format),
    )


@meta()
def year(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(
        Column(expression.TsOrDsToDate(this=Column.ensure_col(col).expression)), expression.Year
    )


@meta()
def quarter(col: ColumnOrName) -> Column:
    return Column(
        expression.Anonymous(
            this="QUARTER",
            expressions=[expression.TsOrDsToDate(this=Column.ensure_col(col).expression)],
        )
    )


@meta()
def month(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(
        Column(expression.TsOrDsToDate(this=Column.ensure_col(col).expression)), expression.Month
    )


@meta()
def dayofweek(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(
        Column(expression.TsOrDsToDate(this=Column.ensure_col(col).expression)),
        expression.DayOfWeek,
    )


@meta()
def dayofmonth(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(
        Column(expression.TsOrDsToDate(this=Column.ensure_col(col).expression)),
        expression.DayOfMonth,
    )


@meta()
def dayofyear(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(
        Column(expression.TsOrDsToDate(this=Column.ensure_col(col).expression)),
        expression.DayOfYear,
    )


@meta()
def hour(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "HOUR")


@meta()
def minute(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "MINUTE")


@meta()
def second(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "SECOND")


@meta()
def weekofyear(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(
        Column(expression.TsOrDsToDate(this=Column.ensure_col(col).expression)),
        expression.WeekOfYear,
    )


@meta()
def make_date(year: ColumnOrName, month: ColumnOrName, day: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(year, "MAKE_DATE", month, day)


@meta()
def date_add(
    col: ColumnOrName, days: t.Union[ColumnOrName, int], cast_as_date: bool = True
) -> Column:
    if isinstance(days, int):
        if days < 0:
            return date_sub(col, days * -1)
        days = lit(days)
    result = Column.invoke_expression_over_column(
        Column.ensure_col(col).cast("date"),
        expression.DateAdd,
        expression=days,
        unit=expression.Var(this="DAY"),
    )
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
    if isinstance(days, int):
        if days < 0:
            return date_add(col, days * -1)
        days = lit(days)
    result = Column.invoke_expression_over_column(
        Column.ensure_col(col).cast("date"),
        expression.DateSub,
        expression=days,
        unit=expression.Var(this="DAY"),
    )
    if cast_as_date:
        return result.cast("date")
    return result


@meta()
def date_diff(end: ColumnOrName, start: ColumnOrName) -> Column:
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
    start_col = Column(start).cast("date")

    if isinstance(months, int):
        if months < 0:
            end_col = Column(
                expression.Interval(
                    this=lit(months * -1).expression, unit=expression.Var(this="MONTH")
                )
            )
            result = start_col - end_col
        else:
            end_col = Column(
                expression.Interval(this=lit(months).expression, unit=expression.Var(this="MONTH"))
            )
            result = start_col + end_col
    else:
        end_col = Column(
            expression.Interval(
                this=Column.ensure_col(months).expression, unit=expression.Var(this="MONTH")
            )
        )
        result = start_col + end_col
    if cast_as_date:
        return result.cast("date")
    return result


@meta()
def months_between(
    date1: ColumnOrName, date2: ColumnOrName, roundOff: t.Optional[bool] = None
) -> Column:
    if roundOff is None:
        return Column.invoke_expression_over_column(
            date1, expression.MonthsBetween, expression=date2
        )

    return Column.invoke_expression_over_column(
        date1, expression.MonthsBetween, expression=date2, roundoff=lit(roundOff)
    )


@meta()
def to_date(col: ColumnOrName, format: t.Optional[str] = None) -> Column:
    if format is not None:
        return Column.invoke_expression_over_column(
            col, expression.TsOrDsToDate, format=lit(format)
        )
    return Column.invoke_expression_over_column(col, expression.TsOrDsToDate)


@meta()
def to_timestamp(col: ColumnOrName, format: t.Optional[str] = None) -> Column:
    if format is not None:
        return Column.invoke_expression_over_column(col, expression.StrToTime, format=lit(format))

    return Column.ensure_col(col).cast("timestamp")


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
    return Column.invoke_anonymous_function(col, "NEXT_DAY", lit(dayOfWeek))


@meta(unsupported_engines=["duckdb", "postgres"])
def last_day(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.LastDay)


@meta()
def from_unixtime(col: ColumnOrName, format: t.Optional[str] = None) -> Column:
    from sqlframe.base.session import _BaseSession

    if format is None:
        format = _BaseSession().DEFAULT_TIME_FORMAT
    return Column.invoke_expression_over_column(col, expression.UnixToStr, format=lit(format))


@meta()
def unix_timestamp(
    timestamp: t.Optional[ColumnOrName] = None, format: t.Optional[str] = None
) -> Column:
    from sqlframe.base.session import _BaseSession

    if format is None:
        format = _BaseSession().DEFAULT_TIME_FORMAT
    return Column.invoke_expression_over_column(
        timestamp, expression.StrToUnix, format=lit(format)
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


@meta(unsupported_engines=["duckdb", "postgres", "bigquery", "redshift", "snowflake", "spark"])
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


@meta(unsupported_engines=["duckdb", "postgres"])
def sha1(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.SHA)


@meta(unsupported_engines=["bigquery", "duckdb", "postgres"])
def sha2(col: ColumnOrName, numBits: int) -> Column:
    return Column.invoke_expression_over_column(col, expression.SHA2, length=lit(numBits))


@meta(unsupported_engines=["postgres"])
def hash(*cols: ColumnOrName) -> Column:
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
    return Column.invoke_expression_over_column(col, expression.ToBase64)


@meta()
def unbase64(col: ColumnOrName) -> Column:
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
    return Column.invoke_expression_over_column(
        None, expression.ConcatWs, expressions=[lit(sep)] + list(cols)
    )


@meta(unsupported_engines=["bigquery", "snowflake"])
def decode(col: ColumnOrName, charset: str) -> Column:
    return Column.invoke_expression_over_column(
        col, expression.Decode, charset=expression.Literal.string(charset)
    )


@meta(unsupported_engines=["bigquery", "snowflake"])
def encode(col: ColumnOrName, charset: str) -> Column:
    return Column.invoke_expression_over_column(
        col, expression.Encode, charset=expression.Literal.string(charset)
    )


@meta(unsupported_engines="duckdb")
def format_number(col: ColumnOrName, d: int) -> Column:
    return Column.invoke_anonymous_function(col, "FORMAT_NUMBER", lit(d))


@meta(unsupported_engines="snowflake")
def format_string(format: str, *cols: ColumnOrName) -> Column:
    format_col = lit(format)
    columns = [Column.ensure_col(x) for x in cols]
    return Column.invoke_anonymous_function(format_col, "FORMAT_STRING", *columns)


@meta()
def instr(col: ColumnOrName, substr: str) -> Column:
    return Column.invoke_expression_over_column(col, expression.StrPosition, substr=lit(substr))


@meta()
def overlay(
    src: ColumnOrName,
    replace: ColumnOrName,
    pos: t.Union[ColumnOrName, int],
    len: t.Optional[t.Union[ColumnOrName, int]] = None,
) -> Column:
    pos_value = lit(pos) if isinstance(pos, int) else pos
    if len is not None:
        len_value = lit(len) if isinstance(len, int) else len
        return Column.invoke_anonymous_function(src, "OVERLAY", replace, pos_value, len_value)
    return Column.invoke_anonymous_function(src, "OVERLAY", replace, pos_value)


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
    return Column.invoke_anonymous_function(str, "SUBSTRING_INDEX", lit(delim), lit(count))


@meta(unsupported_engines="bigquery")
def levenshtein(
    left: ColumnOrName, right: ColumnOrName, threshold: t.Optional[int] = None
) -> Column:
    value: t.Union[expression.Case, expression.Levenshtein] = expression.Levenshtein(
        this=Column.ensure_col(left).expression,
        expression=Column.ensure_col(right).expression,
    )
    if threshold is not None:
        value = (
            expression.case()
            .when(expression.LTE(this=value, expression=lit(threshold).expression), value)
            .else_(lit(-1).expression)
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
    return Column.invoke_anonymous_function(col, "LPAD", lit(len), lit(pad))


@meta()
def rpad(col: ColumnOrName, len: int, pad: str) -> Column:
    return Column.invoke_anonymous_function(col, "RPAD", lit(len), lit(pad))


@meta()
def repeat(col: ColumnOrName, n: int) -> Column:
    return Column.invoke_expression_over_column(col, expression.Repeat, times=lit(n))


@meta()
def split(str: ColumnOrName, pattern: str, limit: t.Optional[int] = None) -> Column:
    if limit is not None:
        return Column.invoke_expression_over_column(
            str, expression.RegexpSplit, expression=lit(pattern), limit=lit(limit)
        )
    return Column.invoke_expression_over_column(
        str, expression.RegexpSplit, expression=lit(pattern)
    )


@meta(unsupported_engines="postgres")
def regexp_extract(str: ColumnOrName, pattern: str, idx: t.Optional[int] = None) -> Column:
    if idx is not None:
        return Column.invoke_expression_over_column(
            str,
            expression.RegexpExtract,
            expression=lit(pattern),
            group=lit(idx),
        )
    return Column.invoke_expression_over_column(
        str, expression.RegexpExtract, expression=lit(pattern)
    )


@meta()
def regexp_replace(
    str: ColumnOrName, pattern: str, replacement: str, position: t.Optional[int] = None
) -> Column:
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
    return Column.invoke_anonymous_function(col, "BIN")


@meta(unsupported_engines="postgres")
def hex(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Hex)


@meta(unsupported_engines="postgres")
def unhex(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Unhex)


@meta()
def length(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.Length)


@meta(unsupported_engines="duckdb")
def octet_length(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "OCTET_LENGTH")


@meta()
def bit_length(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "BIT_LENGTH")


@meta()
def translate(srcCol: ColumnOrName, matching: str, replace: str) -> Column:
    return Column.invoke_anonymous_function(srcCol, "TRANSLATE", lit(matching), lit(replace))


@meta()
def array(*cols: t.Union[ColumnOrName, t.Iterable[ColumnOrName]]) -> Column:
    columns = _flatten(cols) if not isinstance(cols[0], (str, Column)) else cols
    return Column.invoke_expression_over_column(None, expression.Array, expressions=columns)


@meta(unsupported_engines=["bigquery", "postgres"])
def create_map(*cols: t.Union[ColumnOrName, t.Iterable[ColumnOrName]]) -> Column:
    cols = list(_flatten(cols)) if not isinstance(cols[0], (str, Column)) else cols  # type: ignore
    return Column.invoke_expression_over_column(
        None,
        expression.VarMap,
        keys=array(*cols[::2]).expression,
        values=array(*cols[1::2]).expression,
    )


@meta(unsupported_engines=["bigquery", "postgres", "snowflake"])
def map_from_arrays(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(None, expression.Map, keys=col1, values=col2)


@meta()
def array_contains(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    value_col = value if isinstance(value, Column) else lit(value)
    return Column.invoke_expression_over_column(
        col, expression.ArrayContains, expression=value_col.expression
    )


@meta(unsupported_engines="bigquery")
def arrays_overlap(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col1, expression.ArrayOverlaps, expression=col2)


@meta()
def slice(
    x: ColumnOrName, start: t.Union[ColumnOrName, int], length: t.Union[ColumnOrName, int]
) -> Column:
    start_col = lit(start) if isinstance(start, int) else start
    length_col = lit(length) if isinstance(length, int) else length
    return Column.invoke_anonymous_function(x, "SLICE", start_col, length_col)


@meta()
def array_join(
    col: ColumnOrName, delimiter: str, null_replacement: t.Optional[str] = None
) -> Column:
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
    value_col = value if isinstance(value, Column) else lit(value)
    # Some engines return NULL if item is not found but Spark expects 0 so we coalesce to 0
    return coalesce(Column.invoke_anonymous_function(col, "ARRAY_POSITION", value_col), lit(0))


@meta()
def element_at(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    value_col = value if isinstance(value, Column) else lit(value)
    return Column.invoke_anonymous_function(col, "ELEMENT_AT", value_col)


@meta()
def array_remove(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    value_col = value if isinstance(value, Column) else lit(value)
    return Column.invoke_anonymous_function(col, "ARRAY_REMOVE", value_col)


@meta(unsupported_engines="postgres")
def array_distinct(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ARRAY_DISTINCT")


@meta(unsupported_engines=["bigquery", "postgres"])
def array_intersect(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col1, "ARRAY_INTERSECT", Column.ensure_col(col2))


@meta(unsupported_engines=["postgres"])
def array_union(col1: ColumnOrName, col2: ColumnOrName) -> Column:
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
        options_col = create_map([lit(x) for x in _flatten(options.items())])
        return Column.invoke_anonymous_function(col, "FROM_JSON", schema, options_col)
    return Column.invoke_anonymous_function(col, "FROM_JSON", schema)


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def to_json(col: ColumnOrName, options: t.Optional[t.Dict[str, str]] = None) -> Column:
    if options is not None:
        options_col = create_map([lit(x) for x in _flatten(options.items())])
        return Column.invoke_expression_over_column(col, expression.JSONFormat, options=options_col)
    return Column.invoke_expression_over_column(col, expression.JSONFormat)


@meta(unsupported_engines="*")
def schema_of_json(col: ColumnOrName, options: t.Optional[t.Dict[str, str]] = None) -> Column:
    if isinstance(col, str):
        col = lit(col)
    if options is not None:
        options_col = create_map([lit(x) for x in _flatten(options.items())])
        return Column.invoke_anonymous_function(col, "SCHEMA_OF_JSON", options_col)
    return Column.invoke_anonymous_function(col, "SCHEMA_OF_JSON")


@meta(unsupported_engines="*")
def schema_of_csv(col: ColumnOrName, options: t.Optional[t.Dict[str, str]] = None) -> Column:
    if isinstance(col, str):
        col = lit(col)
    if options is not None:
        options_col = create_map([lit(x) for x in _flatten(options.items())])
        return Column.invoke_anonymous_function(col, "SCHEMA_OF_CSV", options_col)
    return Column.invoke_anonymous_function(col, "SCHEMA_OF_CSV")


@meta(unsupported_engines=["bigquery", "duckdb", "postgres", "snowflake"])
def to_csv(col: ColumnOrName, options: t.Optional[t.Dict[str, str]] = None) -> Column:
    if options is not None:
        options_col = create_map([lit(x) for x in _flatten(options.items())])
        return Column.invoke_anonymous_function(col, "TO_CSV", options_col)
    return Column.invoke_anonymous_function(col, "TO_CSV")


@meta()
def size(col: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(col, expression.ArraySize)


@meta()
def array_min(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ARRAY_MIN")


@meta()
def array_max(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "ARRAY_MAX")


@meta(unsupported_engines="postgres")
def sort_array(col: ColumnOrName, asc: t.Optional[bool] = None) -> Column:
    if asc is not None:
        return Column.invoke_expression_over_column(col, expression.SortArray, asc=lit(asc))
    return Column.invoke_expression_over_column(col, expression.SortArray)


@meta(unsupported_engines="postgres")
def array_sort(
    col: ColumnOrName,
    comparator: t.Optional[t.Union[t.Callable[[Column, Column], Column]]] = None,
) -> Column:
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
    columns = list(flatten(cols)) if not isinstance(cols[0], (str, Column)) else cols  # type: ignore
    if len(columns) == 1:
        return Column.invoke_anonymous_function(columns[0], "MAP_CONCAT")
    return Column.invoke_anonymous_function(columns[0], "MAP_CONCAT", *columns[1:])


@meta(unsupported_engines="postgres")
def sequence(
    start: ColumnOrName, stop: ColumnOrName, step: t.Optional[ColumnOrName] = None
) -> Column:
    if step is not None:
        return Column.invoke_anonymous_function(start, "SEQUENCE", stop, step)
    return Column.invoke_anonymous_function(start, "SEQUENCE", stop)


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


@meta(unsupported_engines=["postgres", "snowflake"])
def typeof(col: ColumnOrName) -> Column:
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
        this=lambda_expression(*[Column(x) for x in variables]).expression,
        expressions=variables,
    )
