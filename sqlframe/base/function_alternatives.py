from __future__ import annotations

import logging
import math
import re
import typing as t

from sqlglot import exp as expression
from sqlglot.helper import ensure_list

from sqlframe.base.column import Column
from sqlframe.base.util import (
    get_func_from_session,
)

if t.TYPE_CHECKING:
    from sqlframe.base._typing import ColumnOrLiteral, ColumnOrName

logger = logging.getLogger(__name__)


def e_literal() -> Column:
    lit = get_func_from_session("lit")

    return lit(math.e)


def expm1_from_exp(col: ColumnOrName) -> Column:
    exp = get_func_from_session("exp")
    lit = get_func_from_session("lit")
    return exp(col) - lit(1)


def log1p_from_log(col: ColumnOrName) -> Column:
    from sqlframe.base.session import _BaseSession

    session: _BaseSession = _BaseSession()
    log = get_func_from_session("log", session)
    lit = get_func_from_session("lit", session)
    return log(col + lit(1))


def rint_from_round(col: ColumnOrName) -> Column:
    from sqlframe.base.session import _BaseSession

    round = get_func_from_session("round", _BaseSession())
    return round(col, 0)


def kurtosis_from_kurtosis_pop(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "KURTOSIS_POP")


def collect_set_from_list_distinct(col: ColumnOrName) -> Column:
    collect_list = get_func_from_session("collect_list")
    return collect_list(Column(expression.Distinct(expressions=[Column(col).column_expression])))


def to_timestamp_with_time_zone(col: ColumnOrName, format: t.Optional[str] = None) -> Column:
    from sqlframe.base.session import _BaseSession

    if format is not None:
        return Column.invoke_expression_over_column(
            col, expression.StrToTime, format=_BaseSession().format_time(format)
        )

    return Column.ensure_col(col).cast("timestamp with time zone", dialect="postgres")


def to_timestamp_tz(col: ColumnOrName, format: t.Optional[str] = None) -> Column:
    from sqlframe.base.session import _BaseSession

    if format is not None:
        return Column.invoke_expression_over_column(
            col, expression.StrToTime, format=_BaseSession().format_time(format)
        )

    return Column.ensure_col(col).cast("timestamptz", dialect="duckdb")


def to_timestamp_just_timestamp(col: ColumnOrName, format: t.Optional[str] = None) -> Column:
    from sqlframe.base.session import _BaseSession

    if format is not None:
        return Column.invoke_expression_over_column(
            col, expression.StrToTime, format=_BaseSession().format_time(format)
        )

    return Column.ensure_col(col).cast("datetime", dialect="bigquery")


def bitwise_not_from_bitnot(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "BITNOT")


def factorial_from_case_statement(col: ColumnOrName) -> Column:
    from sqlframe.base.session import _BaseSession

    session: _BaseSession = _BaseSession()
    when = get_func_from_session("when", session)
    col_func = get_func_from_session("col", session)
    lit = get_func_from_session("lit", session)
    return (
        when(
            col_func(col) == lit(1),
            lit(1),
        )
        .when(
            col_func(col) == lit(2),
            lit(2),
        )
        .when(
            col_func(col) == lit(3),
            lit(6),
        )
        .when(
            col_func(col) == lit(4),
            lit(24),
        )
        .when(
            col_func(col) == lit(5),
            lit(120),
        )
        .when(
            col_func(col) == lit(6),
            lit(720),
        )
        .when(
            col_func(col) == lit(7),
            lit(5040),
        )
        .when(
            col_func(col) == lit(8),
            lit(40320),
        )
        .when(
            col_func(col) == lit(9),
            lit(362880),
        )
        .when(
            col_func(col) == lit(10),
            lit(3628800),
        )
        .when(
            col_func(col) == lit(11),
            lit(39916800),
        )
        .when(
            col_func(col) == lit(12),
            lit(479001600),
        )
        .when(
            col_func(col) == lit(13),
            lit(6227020800),
        )
        .when(
            col_func(col) == lit(14),
            lit(87178291200),
        )
        .when(
            col_func(col) == lit(15),
            lit(1307674368000),
        )
        .when(
            col_func(col) == lit(16),
            lit(20922789888000),
        )
        .when(
            col_func(col) == lit(17),
            lit(355687428096000),
        )
        .when(
            col_func(col) == lit(18),
            lit(6402373705728000),
        )
        .when(
            col_func(col) == lit(19),
            lit(121645100408832000),
        )
        .when(
            col_func(col) == lit(20),
            lit(2432902008176640000),
        )
        .otherwise(
            lit(None),
        )
    )


def factorial_ensure_int(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column.invoke_anonymous_function(col_func(col).cast("integer"), "FACTORIAL")


def isnan_using_equal(col: ColumnOrName) -> Column:
    lit = get_func_from_session("lit")
    return Column(
        expression.EQ(
            this=Column(col).column_expression, expression=lit(float("nan")).column_expression
        )
    )


def isnull_using_equal(col: ColumnOrName) -> Column:
    lit = get_func_from_session("lit")
    col_func = get_func_from_session("col")
    return Column(
        expression.Is(this=col_func(col).column_expression, expression=lit(None).column_expression)
    )


def nanvl_as_case(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    when = get_func_from_session("when")
    isnan = get_func_from_session("isnan")
    col = get_func_from_session("col")
    return when(~isnan(col1), col(col1)).otherwise(col(col2))


def percentile_approx_without_accuracy_and_plural(
    col: ColumnOrName,
    percentage: t.Union[ColumnOrLiteral, t.List[float], t.Tuple[float]],
    accuracy: t.Optional[float] = None,
) -> Column:
    lit = get_func_from_session("lit")
    array = get_func_from_session("array")
    col_func = get_func_from_session("col")

    def make_bracket_approx_percentile(percentage: float) -> expression.Bracket:
        return expression.Bracket(
            this=expression.Anonymous(
                this="APPROX_QUANTILES",
                expressions=[col_func(col).column_expression, lit(100).column_expression],
            ),
            expressions=[lit(int(percentage * 100)).cast("int").column_expression],
            offset=0,
            safe=False,
        )

    if accuracy:
        logger.warning("Accuracy is ignored since it is not supported in this dialect")
    if isinstance(percentage, (list, tuple)):
        return array(*[make_bracket_approx_percentile(p) for p in percentage])
    return Column(make_bracket_approx_percentile(percentage))  # type: ignore


def percentile_approx_without_accuracy_and_max_array(
    col: ColumnOrName,
    percentage: t.Union[ColumnOrLiteral, t.List[float], t.Tuple[float]],
    accuracy: t.Optional[float] = None,
) -> Column:
    lit = get_func_from_session("lit")
    array = get_func_from_session("array")
    col_func = get_func_from_session("col")

    def make_approx_percentile(percentage: float) -> expression.Anonymous:
        return expression.Anonymous(
            this="APPROX_PERCENTILE",
            expressions=[col_func(col).column_expression, lit(percentage).column_expression],
        )

    if accuracy:
        logger.warning("Accuracy is ignored since it is not supported in this dialect")

    return array(*[make_approx_percentile(p) for p in percentage])  # type: ignore


def percentile_without_disc(
    col: ColumnOrName,
    percentage: t.Union[ColumnOrLiteral, t.List[float], t.Tuple[float]],
    frequency: t.Optional[ColumnOrLiteral] = None,
) -> Column:
    lit = get_func_from_session("lit")
    col_func = get_func_from_session("col")

    percentage_col = percentage if isinstance(percentage, Column) else lit(percentage)
    func_expressions = [
        col_func(col).column_expression,
        percentage_col.column_expression,
    ]
    if frequency:
        func_expressions.append(frequency if isinstance(frequency, Column) else lit(frequency))
    return Column(
        expression.Anonymous(
            this="PERCENTILE",
            expressions=func_expressions,
        )
    )


def bround_using_half_even(col: ColumnOrName, scale: t.Optional[int] = None) -> Column:
    lit_func = get_func_from_session("lit")

    return Column.invoke_anonymous_function(col, "ROUND", scale, lit_func("HALF_TO_EVEN"))  # type: ignore


def shiftleft_from_bitshiftleft(col: ColumnOrName, numBits: int) -> Column:
    col_func = get_func_from_session("col")
    lit = get_func_from_session("lit")

    return Column(
        expression.Anonymous(
            this="BITSHIFTLEFT",
            expressions=[col_func(col).column_expression, lit(numBits).column_expression],
        )
    )


def shiftright_from_bitshiftright(col: ColumnOrName, numBits: int) -> Column:
    col_func = get_func_from_session("col")
    lit = get_func_from_session("lit")

    return Column(
        expression.Anonymous(
            this="BITSHIFTRIGHT",
            expressions=[col_func(col).column_expression, lit(numBits).column_expression],
        )
    )


def struct_with_eq(
    col: t.Union[ColumnOrName, t.Iterable[ColumnOrName]], *cols: ColumnOrName
) -> Column:
    from sqlframe.base.session import _BaseSession

    col_func = get_func_from_session("col")

    columns = [col_func(x) for x in ensure_list(col) + list(cols)]
    expressions = []
    for column in columns:
        expressions.append(
            expression.PropertyEQ(
                this=expression.parse_identifier(
                    column.alias_or_name, dialect=_BaseSession().input_dialect
                ),
                expression=column.column_expression,
            )
        )
    return Column(expression.Struct(expressions=expressions))


def year_from_extract(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Extract(
            this=expression.Var(this="year"),
            expression=col_func(col).cast("date").column_expression,
        )
    )


def quarter_from_extract(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Extract(
            this=expression.Var(this="quarter"),
            expression=col_func(col).cast("date").column_expression,
        )
    )


def month_from_extract(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Extract(
            this=expression.Var(this="month"),
            expression=col_func(col).cast("date").column_expression,
        )
    )


def dayofweek_from_extract(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Extract(
            this=expression.Var(this="dayofweek"),
            expression=col_func(col).cast("date").column_expression,
        )
    )


def dayofweek_from_extract_with_isodow(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Extract(
            this=expression.Var(this="isodow"),
            expression=col_func(col).cast("date").column_expression,
        )
    )


def dayofmonth_from_extract_with_day(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Extract(
            this=expression.Var(this="day"), expression=col_func(col).cast("date").column_expression
        )
    )


def dayofyear_from_extract(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Extract(
            this=expression.Var(this="dayofyear"),
            expression=col_func(col).cast("date").column_expression,
        )
    )


def dayofyear_from_extract_doy(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Extract(
            this=expression.Var(this="doy"), expression=col_func(col).cast("date").column_expression
        )
    )


def hour_from_extract(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Extract(
            this=expression.Var(this="hour"), expression=col_func(col).column_expression
        )
    )


def minute_from_extract(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Extract(
            this=expression.Var(this="minute"), expression=col_func(col).column_expression
        )
    )


def second_from_extract(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Extract(
            this=expression.Var(this="second"), expression=col_func(col).column_expression
        )
    )


def weekofyear_from_extract_as_week(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Extract(
            this=expression.Var(this="week"),
            expression=col_func(col).cast("date").column_expression,
        )
    )


def weekofyear_from_extract_as_isoweek(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Extract(
            this=expression.Var(this="ISOWEEK"),
            expression=col_func(col).cast("date").column_expression,
        )
    )


def make_date_from_date_func(year: ColumnOrName, month: ColumnOrName, day: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Anonymous(
            this="DATE",
            expressions=[
                col_func(year).cast("integer").column_expression,
                col_func(month).cast("integer").column_expression,
                col_func(day).cast("integer").column_expression,
            ],
        )
    )


def make_date_date_from_parts(year: ColumnOrName, month: ColumnOrName, day: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Anonymous(
            this="DATE_FROM_PARTS",
            expressions=[
                col_func(year).cast("integer").column_expression,
                col_func(month).cast("integer").column_expression,
                col_func(day).cast("integer").column_expression,
            ],
        )
    )


def date_add_no_date_sub(
    col: ColumnOrName, days: t.Union[ColumnOrName, int], cast_as_date: bool = True
) -> Column:
    lit_func = get_func_from_session("col")

    if isinstance(days, int):
        days = lit_func(days)

    result = Column.invoke_expression_over_column(
        Column.ensure_col(col).cast("date"),
        expression.DateAdd,
        expression=days,
        unit=expression.Var(this="DAY"),
    )
    if cast_as_date:
        return result.cast("date")
    return result


def date_sub_by_date_add(
    col: ColumnOrName, days: t.Union[ColumnOrName, int], cast_as_date: bool = True
) -> Column:
    lit_func = get_func_from_session("col")
    date_add_func = get_func_from_session("date_add")

    return date_add_func(col, days * lit_func(-1), cast_as_date)


def sha1_force_sha1_and_to_hex(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Anonymous(
            this="TO_HEX",
            expressions=[
                expression.Anonymous(
                    this="SHA1",
                    expressions=[col_func(col).column_expression],
                )
            ],
        )
    )


def sha2_sha265(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Anonymous(
            this="SHA256",
            expressions=[col_func(col).column_expression],
        )
    )


def hash_from_farm_fingerprint(*cols: ColumnOrName) -> Column:
    if len(cols) > 1:
        raise ValueError("This dialect only supports a single column for calculating hash")

    col_func = get_func_from_session("col")

    return Column(
        expression.Anonymous(
            this="FARM_FINGERPRINT",
            expressions=[col_func(cols[0]).column_expression],
        )
    )


def date_diff_with_subtraction(end: ColumnOrName, start: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return col_func(end).cast("date") - col_func(start).cast("date")


def add_months_using_func(
    start: ColumnOrName, months: t.Union[ColumnOrName, int], cast_as_date: bool = True
) -> Column:
    from sqlframe.base.functions import add_months

    if isinstance(months, int):
        months = get_func_from_session("lit")(months)
    else:
        months = Column.ensure_col(months)

    value = Column(
        expression.Anonymous(
            this="ADD_MONTHS",
            expressions=[
                Column.ensure_col(start).column_expression,
                months.column_expression,  # type: ignore
            ],
        )
    )

    if cast_as_date:
        return value.cast("date")
    return value


def months_between_from_age_and_extract(
    date1: ColumnOrName, date2: ColumnOrName, roundOff: t.Optional[bool] = None
) -> Column:
    lit = get_func_from_session("lit")
    col_func = get_func_from_session("col")

    if roundOff:
        logger.warning("Round off is ignored since it is not supported in this dialect")
    age_expression = expression.Anonymous(
        this="AGE",
        expressions=[
            col_func(date1).cast("date").column_expression,
            col_func(date2).cast("date").column_expression,
        ],
    )
    return (
        Column(
            expression.Extract(this=expression.Var(this="year"), expression=age_expression)
            * expression.Literal.number(12)
        )
        + Column(expression.Extract(this=expression.Var(this="month"), expression=age_expression))
        + lit(1)
    ).cast("bigint")


def from_unixtime_from_timestamp(col: ColumnOrName, format: t.Optional[str] = None) -> Column:
    from sqlframe.base.session import _BaseSession

    col_func = get_func_from_session("col")

    return Column.invoke_expression_over_column(
        Column(
            expression.Anonymous(
                this="TO_TIMESTAMP",
                expressions=[col_func(col).column_expression],
            )
        ),
        expression.TimeToStr,
        format=_BaseSession().format_time(format),
    )


def unix_timestamp_from_extract(
    timestamp: t.Optional[ColumnOrName] = None, format: t.Optional[str] = None
) -> Column:
    to_timestamp = get_func_from_session("to_timestamp")

    return Column(
        expression.Extract(
            this=expression.Var(this="epoch"),
            expression=to_timestamp(timestamp, format).column_expression,
        )
    ).cast("bigint")


def base64_from_blob(col: ColumnOrLiteral) -> Column:
    return Column.invoke_expression_over_column(Column(col).cast("blob"), expression.ToBase64)


def base64_from_encode(col: ColumnOrLiteral) -> Column:
    return Column(
        expression.Encode(
            this=Column(col).cast("bytea").column_expression,
            charset=expression.Literal.string("base64"),
        )
    )


def base64_from_base64_encode(col: ColumnOrLiteral) -> Column:
    return Column(
        expression.Anonymous(
            this="BASE64_ENCODE",
            expressions=[Column(col).column_expression],
        )
    )


def unbase64_from_decode(col: ColumnOrLiteral) -> Column:
    return Column(
        expression.Decode(
            this=Column(col).column_expression, charset=expression.Literal.string("base64")
        )
    )


def unbase64_from_base64_decode_string(col: ColumnOrLiteral) -> Column:
    return Column(
        expression.Anonymous(
            this="BASE64_DECODE_STRING",
            expressions=[Column(col).column_expression],
        )
    )


def decode_from_blob(col: ColumnOrLiteral, charset: str) -> Column:
    return Column(
        expression.Decode(
            this=Column(col).cast("blob").column_expression,
            charset=expression.Literal.string(charset),
        )
    )


def decode_from_convert_from(col: ColumnOrLiteral, charset: str) -> Column:
    return Column(
        expression.Anonymous(
            this="CONVERT_FROM",
            expressions=[
                Column(col).cast("bytea").column_expression,
                expression.Literal.string(charset),
            ],
        )
    )


def encode_from_convert_to(col: ColumnOrName, charset: str) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Anonymous(
            this="CONVERT_TO",
            expressions=[col_func(col).column_expression, expression.Literal.string(charset)],
        )
    )


def concat_ws_from_array_to_string(sep: str, *cols: ColumnOrName) -> Column:
    array = get_func_from_session("array")
    lit = get_func_from_session("lit")

    return Column(
        expression.Anonymous(
            this="ARRAY_TO_STRING",
            expressions=[array(*cols).column_expression, lit(sep).column_expression],
        )
    )


def format_number_from_to_char(col: ColumnOrName, d: int) -> Column:
    round = get_func_from_session("round")
    format = "FM" + ("999," * 5) + "990" + "D" + ("0" * d)

    return Column(
        expression.ToChar(
            this=round(col, d).column_expression, format=expression.Literal.string(format)
        )
    )


def format_string_with_format(format: str, *cols: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Anonymous(
            this="FORMAT",
            expressions=[
                expression.Literal.string(format.replace("%d", "%s")),
                *[col_func(x).cast("string").column_expression for x in ensure_list(cols)],
            ],
        )
    )


def format_string_with_pipes(format: str, *cols: ColumnOrName) -> Column:
    lit = get_func_from_session("lit")
    col_func = get_func_from_session("col")

    values = format.replace("%d", "%s").split("%s")
    if len(values) != len(cols) + 1:
        raise ValueError("Number of values and columns do not match")
    result = expression.DPipe(
        this=lit(values[0]).column_expression, expression=col_func(cols[0]).column_expression
    )
    for i, value in enumerate(values[1:], start=1):
        if i == len(cols):
            result = expression.DPipe(this=result, expression=lit(value).column_expression)
        else:
            result = expression.DPipe(
                this=expression.DPipe(this=result, expression=lit(value).column_expression),
                expression=col_func(cols[i]).column_expression,
            )
    return Column(result)


def instr_using_strpos(col: ColumnOrName, substr: str) -> Column:
    lit = get_func_from_session("lit")
    col_func = get_func_from_session("col")

    return Column(
        expression.Anonymous(
            this="STRPOS",
            expressions=[col_func(col).column_expression, lit(substr).column_expression],
        )
    )


def overlay_from_substr(
    src: ColumnOrName,
    replace: ColumnOrName,
    pos: t.Union[ColumnOrName, int],
    len: t.Optional[t.Union[ColumnOrName, int]] = None,
) -> Column:
    col_func = get_func_from_session("col")
    lit = get_func_from_session("lit")
    substring = get_func_from_session("substring")
    length_func = get_func_from_session("length")
    length_value = len if len is not None else length_func(replace)
    return Column(
        expression.Concat(
            expressions=[
                substring(col_func(src), 1, col_func(pos) - lit(1)).column_expression,
                col_func(replace).column_expression,
                substring(
                    col_func(src), col_func(pos) + col_func(length_value), length_func(src)
                ).column_expression,
            ]
        )
    )


def levenshtein_edit_distance(
    left: ColumnOrName, right: ColumnOrName, threshold: t.Optional[int] = None
) -> Column:
    if threshold is not None:
        logger.warning("Threshold is ignored since it is not supported in this dialect")
    return Column(
        expression.Anonymous(
            this="EDITDISTANCE",
            expressions=[
                Column.ensure_col(left).column_expression,
                Column.ensure_col(right).column_expression,
            ],
        )
    )


def split_from_regex_split_to_array(
    str: ColumnOrName, pattern: str, limit: t.Optional[int] = None
) -> Column:
    col_func = get_func_from_session("col")

    if limit is not None:
        logger.warning("Limit is ignored since it is not supported in this dialect")
    return Column(
        expression.Anonymous(
            this="REGEXP_SPLIT_TO_ARRAY",
            expressions=[
                col_func(str).column_expression,
                expression.Literal.string(pattern),
            ],
        )
    )


def split_with_split(str: ColumnOrName, pattern: str, limit: t.Optional[int] = None) -> Column:
    lit = get_func_from_session("lit")
    col_func = get_func_from_session("col")

    if limit is not None:
        logger.warning("Limit is ignored since it is not supported in this dialect")
    return Column(
        expression.Anonymous(
            this="SPLIT",
            expressions=[col_func(str).column_expression, lit(pattern).column_expression],
        )
    )


def array_contains_any(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    lit = get_func_from_session("lit")
    value_col = value if isinstance(value, Column) else lit(value)
    col_func = get_func_from_session("col")

    return Column(
        expression.EQ(
            this=value_col.column_expression,
            expression=expression.Anonymous(
                this="ANY", expressions=[col_func(col).column_expression]
            ),
        )
    )


def arrays_overlap_using_intersect(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.GT(
            this=expression.ArraySize(
                this=expression.Anonymous(
                    this="ARRAY_INTERSECT",
                    expressions=[
                        col_func(col1).column_expression,
                        col_func(col2).column_expression,
                    ],
                )
            ),
            expression=expression.Literal.number(0),
        )
    )


def arrays_overlap_renamed(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Anonymous(
            this="ARRAYS_OVERLAP",
            expressions=[col_func(col1).column_expression, col_func(col2).column_expression],
        )
    )


def slice_as_list_slice(
    x: ColumnOrName, start: t.Union[ColumnOrName, int], length: t.Union[ColumnOrName, int]
) -> Column:
    lit = get_func_from_session("lit")

    start_col = start if isinstance(start, Column) else lit(start)
    length_col = length if isinstance(length, Column) else lit(length)
    return Column.invoke_anonymous_function(x, "LIST_SLICE", start_col, start_col + length_col)


def slice_with_brackets(
    x: ColumnOrName, start: t.Union[ColumnOrName, int], length: t.Union[ColumnOrName, int]
) -> Column:
    lit = get_func_from_session("lit")

    start_col = start if isinstance(start, Column) else lit(start)
    length_col = length if isinstance(length, Column) else lit(length)
    col_func = get_func_from_session("col")

    return Column(
        expression.Bracket(
            this=col_func(x).column_expression,
            expressions=[
                expression.Slice(
                    this=start_col.column_expression,
                    expression=(start_col + length_col).column_expression,
                )
            ],
        )
    )


def arrays_overlap_as_plural(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Anonymous(
            this="ARRAYS_OVERLAP",
            expressions=[col_func(col1).column_expression, col_func(col2).column_expression],
        )
    )


def slice_as_array_slice(
    x: ColumnOrName, start: t.Union[ColumnOrName, int], length: t.Union[ColumnOrName, int]
) -> Column:
    lit = get_func_from_session("lit")

    start_col = start if isinstance(start, Column) else lit(start)
    length_col = length if isinstance(length, Column) else lit(length)
    return Column.invoke_anonymous_function(
        x, "ARRAY_SLICE", start_col - lit(1), start_col + length_col
    )


def array_position_cast_variant_and_flip(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    when = get_func_from_session("when")
    lit = get_func_from_session("lit")
    value_col = value if isinstance(value, Column) else lit(value)
    # Some engines return NULL if item is not found but Spark expects 0 so we coalesce to 0
    resp = Column.invoke_anonymous_function(value_col.cast("variant"), "ARRAY_POSITION", col)
    return when(resp.isNotNull(), resp + lit(1)).otherwise(lit(0))


def array_intersect_using_intersection(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Anonymous(
            this="ARRAY_INTERSECTION",
            expressions=[col_func(col1).column_expression, col_func(col2).column_expression],
        )
    )


def element_at_using_brackets(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    col_func = get_func_from_session("col")
    lit = get_func_from_session("lit")
    # SQLGlot will auto add 1 to whatever we pass in for the brackets even though the value is already 1 based.
    value = value if isinstance(value, Column) else lit(value)
    if [x for x in value.column_expression.find_all(expression.Literal) if x.is_number]:
        value = value - lit(1)
    return Column(
        expression.Bracket(
            this=col_func(col).column_expression,
            expressions=[value.column_expression],  # type: ignore
        )
    )


def array_remove_using_filter(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    lit = get_func_from_session("lit")
    col_func = get_func_from_session("col")

    value = value if isinstance(value, Column) else lit(value)
    return Column(
        expression.Anonymous(
            this="LIST_FILTER",
            expressions=[
                col_func(col).column_expression,
                expression.Lambda(
                    this=expression.NEQ(
                        this=expression.Identifier(this="x"), expression=value.column_expression
                    ),
                    expressions=[expression.Identifier(this="x")],
                ),
            ],
        )
    )


def array_union_using_list_concat(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Anonymous(
            this="LIST_DISTINCT",
            expressions=[
                expression.Anonymous(
                    this="LIST_CONCAT",
                    expressions=[
                        col_func(col1).column_expression,
                        col_func(col2).column_expression,
                    ],
                )
            ],
        )
    )


def array_union_using_array_concat(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    array_distinct = get_func_from_session("array_distinct")
    col_func = get_func_from_session("col")

    return array_distinct(
        expression.ArrayConcat(
            this=col_func(col1).column_expression, expressions=[col_func(col2).column_expression]
        )
    )


def get_json_object_using_arrow_op(col: ColumnOrName, path: str) -> Column:
    col_func = get_func_from_session("col")
    path = path.replace("$.", "")
    return Column(
        expression.JSONExtract(
            this=expression.Cast(
                this=col_func(col).column_expression, to=expression.DataType.build("JSON")
            ),
            expression=expression.JSONPath(
                expressions=[expression.JSONPathRoot(), expression.JSONPathKey(this=path)]
            ),
            only_json_types=True,
        )
    )


def get_json_object_using_function(col: ColumnOrName, path: str) -> Column:
    lit = get_func_from_session("lit")
    return Column.invoke_anonymous_function(col, "GET_JSON_OBJECT", lit(path))


def array_min_from_sort(col: ColumnOrName) -> Column:
    element_at = get_func_from_session("element_at")
    array_sort = get_func_from_session("array_sort")

    return element_at(array_sort(col), 1)


def array_min_from_subquery(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    explode = get_func_from_session("explode")
    select = expression.Select(
        expressions=[
            expression.Min(
                this=col_func("x").column_expression,
            )
        ],
    )
    select.set(
        "from",
        expression.From(
            this=explode(col).alias("x").expression,
        ),
    )

    return Column(expression.Subquery(this=select)).alias(col_func(col).alias_or_name)


def array_max_from_sort(col: ColumnOrName) -> Column:
    element_at = get_func_from_session("element_at")
    array_sort = get_func_from_session("array_sort")

    return element_at(array_sort(col), -1)


def array_max_from_subquery(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    explode = get_func_from_session("explode")
    select = expression.Select(
        expressions=[
            expression.Max(
                this=col_func("x").column_expression,
            )
        ],
    )
    select.set(
        "from",
        expression.From(
            this=explode(col).alias("x").expression,
        ),
    )

    return Column(expression.Subquery(this=select)).alias(col_func(col).alias_or_name)


def sort_array_using_array_sort(col: ColumnOrName, asc: t.Optional[bool] = None) -> Column:
    col_func = get_func_from_session("col")
    lit_func = get_func_from_session("lit")
    expressions = [col_func(col).column_expression]
    asc = asc if asc is not None else True
    expressions.append(lit_func(asc).column_expression)
    if asc:
        expressions.append(lit_func(True).column_expression)
    else:
        expressions.append(lit_func(False).column_expression)

    return Column(
        expression.Anonymous(
            this="ARRAY_SORT",
            expressions=expressions,
        )
    )


def flatten_using_array_flatten(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Anonymous(
            this="ARRAY_FLATTEN",
            expressions=[col_func(col).column_expression],
        )
    )


def map_concat_using_map_cat(*cols: t.Union[ColumnOrName, t.Iterable[ColumnOrName]]) -> Column:
    columns = list(flatten(cols)) if not isinstance(cols[0], (str, Column)) else cols  # type: ignore
    if len(columns) == 1:
        return Column.invoke_anonymous_function(columns[0], "MAP_CAT")  # type: ignore
    return Column.invoke_anonymous_function(columns[0], "MAP_CAT", *columns[1:])  # type: ignore


def sequence_from_generate_series(
    start: ColumnOrName, stop: ColumnOrName, step: t.Optional[ColumnOrName] = None
) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Anonymous(
            this="GENERATE_SERIES",
            expressions=[
                col_func(start).column_expression,
                col_func(stop).column_expression,
                col_func(step).column_expression if step else expression.Literal.number(1),
            ],
        )
    )


def sequence_from_generate_array(
    start: ColumnOrName, stop: ColumnOrName, step: t.Optional[ColumnOrName] = None
) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Anonymous(
            this="GENERATE_ARRAY",
            expressions=[
                col_func(start).column_expression,
                col_func(stop).column_expression,
                col_func(step).column_expression if step else expression.Literal.number(1),
            ],
        )
    )


def sequence_from_array_generate_range(
    start: ColumnOrName, stop: ColumnOrName, step: t.Optional[ColumnOrName] = None
) -> Column:
    col_func = get_func_from_session("col")
    when = get_func_from_session("when")
    lit = get_func_from_session("lit")

    return Column(
        expression.Anonymous(
            this="ARRAY_GENERATE_RANGE",
            expressions=[
                col_func(start).column_expression,
                (
                    col_func(stop) + when(col_func(stop) > lit(0), lit(1)).otherwise(lit(-1))
                ).column_expression,
                col_func(step).column_expression if step else lit(1).column_expression,
            ],
        )
    )


def regexp_extract_only_one_group(
    str: ColumnOrName, pattern: str, idx: t.Optional[int] = None
) -> Column:
    from sqlframe.base.functions import regexp_extract

    if re.compile(pattern).groups > 1 or (idx is not None and idx > 1):
        raise ValueError("This dialect only supports regular expressions with a single group")

    return regexp_extract(str, pattern, 1)


def hex_casted_as_bytes(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Anonymous(
            this="TO_HEX",
            expressions=[col_func(col).cast("bytes", dialect="bigquery").column_expression],
        )
    )


def hex_using_encode(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Anonymous(
            this="HEX_ENCODE",
            expressions=[col_func(col).column_expression],
        )
    )


def unhex_hex_decode_str(col: ColumnOrName) -> Column:
    col_func = get_func_from_session("col")

    return Column(
        expression.Anonymous(
            this="HEX_DECODE_STRING",
            expressions=[col_func(col).column_expression],
        )
    )


def bit_length_from_length(col: ColumnOrName) -> Column:
    lit = get_func_from_session("lit")
    col_func = get_func_from_session("col")

    return Column(expression.Length(this=col_func(col).column_expression)) * lit(8)


def current_user_from_session_user() -> Column:
    return Column(expression.Anonymous(this="SESSION_USER"))


def position_as_strpos(
    substr: ColumnOrName, str: ColumnOrName, start: t.Optional[ColumnOrName] = None
) -> Column:
    substr_func = get_func_from_session("substr")
    lit = get_func_from_session("lit")

    if start:
        str = substr_func(str, start)
    column = Column.invoke_anonymous_function(str, "STRPOS", substr)
    if start:
        return column + Column(start) - lit(1)
    return column


def to_number_using_to_double(col: ColumnOrName, format: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "TO_DOUBLE", format)


def array_append_list_append(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    lit = get_func_from_session("lit")
    value = value if isinstance(value, Column) else lit(value)
    return Column.invoke_anonymous_function(col, "LIST_APPEND", value)


def array_append_using_array_cat(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    lit = get_func_from_session("lit")
    array = get_func_from_session("array")
    value = value if isinstance(value, Column) else lit(value)
    return Column.invoke_anonymous_function(col, "ARRAY_CONCAT", array(value))


def day_with_try_to_timestamp(col: ColumnOrName) -> Column:
    from sqlframe.base.functions import day

    try_to_timestamp = get_func_from_session("try_to_timestamp")
    to_date = get_func_from_session("to_date")
    when = get_func_from_session("when")
    _is_string = get_func_from_session("_is_string")
    coalesce = get_func_from_session("coalesce")
    return day(
        when(
            _is_string(col),
            coalesce(try_to_timestamp(col), to_date(col)),
        ).otherwise(col)
    )


def endswith_with_underscore(str: ColumnOrName, suffix: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(str, "ENDS_WITH", suffix)


def endswith_using_like(str: ColumnOrName, suffix: ColumnOrName) -> Column:
    concat = get_func_from_session("concat")
    lit = get_func_from_session("lit")

    return Column.invoke_expression_over_column(
        str, expression.Like, expression=concat(lit("%"), suffix)
    )


def try_to_timestamp_strptime(col: ColumnOrName, format: t.Optional[ColumnOrName] = None) -> Column:
    from sqlframe.base.session import _BaseSession

    return Column.invoke_anonymous_function(col, "TRY_STRPTIME", _BaseSession().format_time(format))  # type: ignore


def try_to_timestamp_safe(col: ColumnOrName, format: t.Optional[ColumnOrName] = None) -> Column:
    from sqlframe.base.session import _BaseSession

    return Column.invoke_anonymous_function(
        _BaseSession().format_time(format),  # type: ignore
        "SAFE.PARSE_TIMESTAMP",
        col,  # type: ignore
    )


def try_to_timestamp_pgtemp(col: ColumnOrName, format: t.Optional[ColumnOrName] = None) -> Column:
    from sqlframe.base.session import _BaseSession

    return Column.invoke_anonymous_function(
        col,
        "pg_temp.TRY_TO_TIMESTAMP",
        _BaseSession().format_execution_time(format),  # type: ignore
    )


def typeof_pg_typeof(col: ColumnOrName) -> Column:
    return (
        Column.invoke_anonymous_function(col, "pg_typeof")
        .cast(expression.DataType(this=expression.DataType.Type.USERDEFINED, kind="regtype"))
        .cast("text")
    )


def typeof_from_variant(col: ColumnOrName) -> Column:
    col = Column.invoke_anonymous_function(col, "TO_VARIANT")
    return Column.invoke_anonymous_function(col, "TYPEOF")


def typeof_bgutil(col: ColumnOrName) -> Column:
    return Column(
        expression.Anonymous(
            this="bqutil.fn.typeof", expressions=[Column.ensure_col(col).column_expression]
        )
    )


def regexp_replace_global_option(
    str: ColumnOrName, pattern: str, replacement: str, position: t.Optional[int] = None
) -> Column:
    lit = get_func_from_session("lit")

    if position is not None:
        return Column.invoke_expression_over_column(
            str,
            expression.RegexpReplace,
            expression=lit(pattern),
            replacement=lit(replacement),
            position=lit(position),
            modifiers=lit("g"),
        )
    return Column.invoke_expression_over_column(
        str,
        expression.RegexpReplace,
        expression=lit(pattern),
        replacement=lit(replacement),
        modifiers=lit("g"),
    )


def regexp_with_matches(str: ColumnOrName, regexp: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(str, "REGEXP_MATCHES", regexp)


def regexp_with_contains(str: ColumnOrName, regexp: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(str, "REGEXP_CONTAINS", regexp)


def degrees_bgutil(col: ColumnOrName) -> Column:
    return Column(
        expression.Anonymous(
            this="bqutil.fn.degrees", expressions=[Column.ensure_col(col).column_expression]
        )
    )


def radians_bgutil(col: ColumnOrName) -> Column:
    return Column(
        expression.Anonymous(
            this="bqutil.fn.radians", expressions=[Column.ensure_col(col).column_expression]
        )
    )


def bround_bgutil(col: ColumnOrName, scale: t.Optional[int] = None) -> Column:
    from sqlframe.base.session import _BaseSession

    lit = get_func_from_session("lit", _BaseSession())

    expressions = [Column.ensure_col(col).cast("bignumeric").column_expression]
    if scale is not None:
        expressions.append(lit(scale).column_expression)
    return Column(
        expression.Anonymous(
            this="bqutil.fn.cw_round_half_even",
            expressions=expressions,
        )
    )


def months_between_bgutils(
    date1: ColumnOrName, date2: ColumnOrName, roundOff: t.Optional[bool] = None
) -> Column:
    roundOff = True if roundOff is None else roundOff
    round = get_func_from_session("round")
    lit = get_func_from_session("lit")

    value = Column(
        expression.Anonymous(
            this="bqutil.fn.cw_months_between",
            expressions=[
                Column.ensure_col(date1).cast("datetime").column_expression,
                Column.ensure_col(date2).cast("datetime").column_expression,
            ],
        )
    )
    if roundOff:
        value = round(value, lit(8))
    return value


def next_day_bgutil(col: ColumnOrName, dayOfWeek: str) -> Column:
    lit = get_func_from_session("lit")

    return Column(
        expression.Anonymous(
            this="bqutil.fn.cw_next_day",
            expressions=[
                Column.ensure_col(col).cast("date").column_expression,
                lit(dayOfWeek).column_expression,
            ],
        )
    )


def from_unixtime_bigutil(col: ColumnOrName, format: t.Optional[str] = None) -> Column:
    from sqlframe.base.session import _BaseSession

    session: _BaseSession = _BaseSession()

    expressions = [Column.ensure_col(col).column_expression]
    return Column(
        expression.Anonymous(
            this="FORMAT_TIMESTAMP",
            expressions=[
                session.format_time(format),
                Column(
                    expression.Anonymous(this="TIMESTAMP_SECONDS", expressions=expressions)
                ).column_expression,
            ],
        )
    )


def unix_timestamp_bgutil(
    timestamp: t.Optional[ColumnOrName] = None, format: t.Optional[str] = None
) -> Column:
    from sqlframe.base.session import _BaseSession

    lit = get_func_from_session("lit")
    return Column(
        expression.Anonymous(
            this="UNIX_SECONDS",
            expressions=[
                expression.Anonymous(
                    this="PARSE_TIMESTAMP",
                    expressions=[
                        _BaseSession().format_time(format),
                        Column.ensure_col(timestamp).column_expression,
                        lit("UTC").column_expression,
                    ],
                )
            ],
        )
    )


def unix_seconds_extract_epoch(col: ColumnOrName) -> Column:
    return Column(
        expression.Extract(
            this=expression.Var(this="EPOCH"),
            expression=Column.ensure_col(col).column_expression,
        )
    )


def unix_millis_multiply_epoch(col: ColumnOrName) -> Column:
    unix_seconds = get_func_from_session("unix_seconds")

    return Column(
        expression.Cast(
            this=expression.Mul(
                this=unix_seconds(col).column_expression,
                expression=expression.Literal.number(1000),
            ),
            to=expression.DataType.build("bigint"),
        )
    )


def unix_micros_multiply_epoch(col: ColumnOrName) -> Column:
    unix_seconds = get_func_from_session("unix_seconds")

    return Column(
        expression.Cast(
            this=expression.Mul(
                this=unix_seconds(col).column_expression,
                expression=expression.Literal.number(1000000),
            ),
            to=expression.DataType.build("bigint"),
        )
    )


def format_number_bgutil(col: ColumnOrName, d: int) -> Column:
    round = get_func_from_session("round")
    lit = get_func_from_session("lit")

    return Column(
        expression.Anonymous(
            this="FORMAT",
            expressions=[
                lit(f"%'.{d}f").column_expression,
                round(Column.ensure_col(col).cast("float"), d).column_expression,
            ],
        )
    )


def substring_index_bgutil(str: ColumnOrName, delim: str, count: int) -> Column:
    lit = get_func_from_session("lit")

    return Column(
        expression.Anonymous(
            this="bqutil.fn.cw_substring_index",
            expressions=[
                Column.ensure_col(str).column_expression,
                lit(delim).column_expression,
                lit(count).column_expression,
            ],
        )
    )


def bin_bgutil(col: ColumnOrName) -> Column:
    return (
        Column(
            expression.Anonymous(
                this="bqutil.fn.to_binary",
                expressions=[Column.ensure_col(col).column_expression],
            )
        )
        .cast("int")
        .cast("string")
    )


def slice_bgutil(
    x: ColumnOrName, start: t.Union[ColumnOrName, int], length: t.Union[ColumnOrName, int]
) -> Column:
    lit = get_func_from_session("lit")

    start_col = start if isinstance(start, Column) else lit(start)
    length_col = length if isinstance(length, Column) else lit(length)

    subquery = (
        expression.select(
            expression.column("x"),
        )
        .from_(
            expression.Unnest(
                expressions=[Column.ensure_col(x).column_expression],
                alias=expression.TableAlias(
                    columns=[expression.to_identifier("x")],
                ),
                offset=expression.to_identifier("offset"),
            )
        )
        .where(
            expression.Between(
                this=expression.column("offset"),
                low=(start_col - lit(1)).column_expression,
                high=(start_col + length_col).column_expression,
            )
        )
    )

    return Column(
        expression.Anonymous(
            this="ARRAY",
            expressions=[subquery],
        )
    )


def array_position_bgutil(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    lit = get_func_from_session("lit")

    value_col = value if isinstance(value, Column) else lit(value)

    return Column(
        expression.Coalesce(
            this=expression.Anonymous(
                this="bqutil.fn.find_in_set",
                expressions=[
                    value_col.column_expression,
                    expression.Anonymous(
                        this="ARRAY_TO_STRING",
                        expressions=[
                            Column.ensure_col(col).column_expression,
                            lit(",").column_expression,
                        ],
                    ),
                ],
            ),
            expressions=[lit(0).column_expression],
        )
    )


def array_remove_bgutil(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    lit = get_func_from_session("lit")

    value_col = value if isinstance(value, Column) else lit(value)

    filter_subquery = expression.select(
        "*",
    ).from_(
        expression.Unnest(
            expressions=[Column.ensure_col(col).column_expression],
            alias=expression.TableAlias(
                columns=[expression.to_identifier("x")],
            ),
        )
    )

    agg_subquery = (
        expression.select(
            expression.Anonymous(
                this="ARRAY_AGG",
                expressions=[expression.column("x")],
            ),
        )
        .from_(filter_subquery.subquery("t"))
        .where(
            expression.NEQ(
                this=expression.column("x", "t"),
                expression=value_col.column_expression,
            )
        )
    )

    return Column(agg_subquery.subquery())


def array_distinct_bgutil(col: ColumnOrName) -> Column:
    return Column(
        expression.Anonymous(
            this="bqutil.fn.cw_array_distinct",
            expressions=[Column.ensure_col(col).column_expression],
        )
    )


def array_min_bgutil(col: ColumnOrName) -> Column:
    return Column(
        expression.Anonymous(
            this="bqutil.fn.cw_array_min",
            expressions=[Column.ensure_col(col).column_expression],
        )
    )


def array_max_bgutil(col: ColumnOrName) -> Column:
    return Column(
        expression.Anonymous(
            this="bqutil.fn.cw_array_max",
            expressions=[Column.ensure_col(col).column_expression],
        )
    )


def sort_array_bgutil(col: ColumnOrName, asc: t.Optional[bool] = None) -> Column:
    order = "ASC" if asc or asc is None else "DESC"
    subquery = (
        expression.select("x")
        .from_(
            expression.Unnest(
                expressions=[Column.ensure_col(col).column_expression],
                alias=expression.TableAlias(
                    columns=[expression.to_identifier("x")],
                ),
            )
        )
        .order_by(f"x {order}")
    )

    return Column(expression.Anonymous(this="ARRAY", expressions=[subquery]))


def _is_string_using_typeof_varchar(col: ColumnOrName) -> Column:
    typeof = get_func_from_session("typeof")
    lit = get_func_from_session("lit")
    return lit(typeof(col) == lit("VARCHAR"))


def _is_string_using_typeof_char_varying(col: ColumnOrName) -> Column:
    typeof = get_func_from_session("typeof")
    lit = get_func_from_session("lit")
    return lit(
        (typeof(col) == lit("text"))
        | (typeof(col) == lit("character varying"))
        | (typeof(col) == lit("unknown"))
        | (typeof(col) == lit("text"))
    )


def _is_string_using_typeof_string(col: ColumnOrName) -> Column:
    typeof = get_func_from_session("typeof")
    lit = get_func_from_session("lit")
    return lit(typeof(col) == lit("STRING"))


def _is_string_using_typeof_string_lcase(col: ColumnOrName) -> Column:
    typeof = get_func_from_session("typeof")
    lit = get_func_from_session("lit")
    return lit(typeof(col) == lit("string"))


def _is_integer_using_func(col: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col, "IS_INTEGER")
