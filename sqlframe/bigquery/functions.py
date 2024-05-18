from __future__ import annotations

import inspect
import sys
import typing as t

from sqlglot import exp as sqlglot_expression

import sqlframe.base.functions
from sqlframe.base.util import get_func_from_session
from sqlframe.bigquery.column import Column

if t.TYPE_CHECKING:
    from sqlframe.base._typing import ColumnOrLiteral, ColumnOrName

module = sys.modules["sqlframe.base.functions"]
globals().update(
    {
        name: func
        for name, func in inspect.getmembers(module, inspect.isfunction)
        if hasattr(func, "unsupported_engines")
        and "bigquery" not in func.unsupported_engines
        and "*" not in func.unsupported_engines
    }
)


from sqlframe.base.function_alternatives import (  # noqa
    e_literal as e,
    expm1_from_exp as expm1,
    factorial_from_case_statement as factorial,
    log1p_from_log as log1p,
    rint_from_round as rint,
    collect_set_from_list_distinct as collect_set,
    isnull_using_equal as isnull,
    nanvl_as_case as nanvl,
    percentile_approx_without_accuracy_and_plural as percentile_approx,
    rand_no_seed as rand,
    year_from_extract as year,
    quarter_from_extract as quarter,
    month_from_extract as month,
    dayofweek_from_extract as dayofweek,
    dayofmonth_from_extract_with_day as dayofmonth,
    dayofyear_from_extract as dayofyear,
    hour_from_extract as hour,
    minute_from_extract as minute,
    second_from_extract as second,
    weekofyear_from_extract_as_isoweek as weekofyear,
    make_date_from_date_func as make_date,
    to_date_from_timestamp as to_date,
    last_day_with_cast as last_day,
    sha1_force_sha1_and_to_hex as sha1,
    hash_from_farm_fingerprint as hash,
    base64_from_blob as base64,
    concat_ws_from_array_to_string as concat_ws,
    format_string_with_format as format_string,
    instr_using_strpos as instr,
    overlay_from_substr as overlay,
    split_with_split as split,
    regexp_extract_only_one_group as regexp_extract,
    hex_casted_as_bytes as hex,
    bit_length_from_length as bit_length,
    element_at_using_brackets as element_at,
    array_union_using_array_concat as array_union,
    sequence_from_generate_array as sequence,
)


def typeof(col: ColumnOrName) -> Column:
    return Column(
        sqlglot_expression.Anonymous(
            this="bqutil.fn.typeof", expressions=[Column.ensure_col(col).expression]
        )
    )


def degrees(col: ColumnOrName) -> Column:
    return Column(
        sqlglot_expression.Anonymous(
            this="bqutil.fn.degrees", expressions=[Column.ensure_col(col).expression]
        )
    )


def radians(col: ColumnOrName) -> Column:
    return Column(
        sqlglot_expression.Anonymous(
            this="bqutil.fn.radians", expressions=[Column.ensure_col(col).expression]
        )
    )


def bround(col: ColumnOrName, scale: t.Optional[int] = None) -> Column:
    from sqlframe.base.session import _BaseSession

    lit = get_func_from_session("lit", _BaseSession())

    expressions = [Column.ensure_col(col).cast("bignumeric").expression]
    if scale is not None:
        expressions.append(lit(scale).expression)
    return Column(
        sqlglot_expression.Anonymous(
            this="bqutil.fn.cw_round_half_even",
            expressions=expressions,
        )
    )


def months_between(
    date1: ColumnOrName, date2: ColumnOrName, roundOff: t.Optional[bool] = None
) -> Column:
    roundOff = True if roundOff is None else roundOff
    round = get_func_from_session("round")
    lit = get_func_from_session("lit")

    value = Column(
        sqlglot_expression.Anonymous(
            this="bqutil.fn.cw_months_between",
            expressions=[
                Column.ensure_col(date1).cast("datetime").expression,
                Column.ensure_col(date2).cast("datetime").expression,
            ],
        )
    )
    if roundOff:
        value = round(value, lit(8))
    return value


def next_day(col: ColumnOrName, dayOfWeek: str) -> Column:
    lit = get_func_from_session("lit")

    return Column(
        sqlglot_expression.Anonymous(
            this="bqutil.fn.cw_next_day",
            expressions=[Column.ensure_col(col).cast("date").expression, lit(dayOfWeek).expression],
        )
    )


def from_unixtime(col: ColumnOrName, format: t.Optional[str] = None) -> Column:
    from sqlframe.base.session import _BaseSession

    session: _BaseSession = _BaseSession()
    lit = get_func_from_session("lit")
    to_timestamp = get_func_from_session("to_timestamp")

    expressions = [Column.ensure_col(col).expression]
    if format is not None:
        expressions.append(lit(format).expression)
    return Column(
        sqlglot_expression.Anonymous(
            this="FORMAT_TIMESTAMP",
            expressions=[
                lit(session.DEFAULT_TIME_FORMAT).expression,
                to_timestamp(
                    Column(
                        sqlglot_expression.Anonymous(
                            this="TIMESTAMP_SECONDS", expressions=expressions
                        )
                    ),
                    format,
                ).expression,
            ],
        )
    )


def unix_timestamp(
    timestamp: t.Optional[ColumnOrName] = None, format: t.Optional[str] = None
) -> Column:
    from sqlframe.base.session import _BaseSession

    lit = get_func_from_session("lit")

    if format is None:
        format = _BaseSession().DEFAULT_TIME_FORMAT
    return Column(
        sqlglot_expression.Anonymous(
            this="UNIX_SECONDS",
            expressions=[
                sqlglot_expression.Anonymous(
                    this="PARSE_TIMESTAMP",
                    expressions=[
                        lit(format).expression,
                        Column.ensure_col(timestamp).expression,
                        lit("UTC").expression,
                    ],
                )
            ],
        )
    )


def format_number(col: ColumnOrName, d: int) -> Column:
    round = get_func_from_session("round")
    lit = get_func_from_session("lit")

    return Column(
        sqlglot_expression.Anonymous(
            this="FORMAT",
            expressions=[
                lit(f"%'.{d}f").expression,
                round(Column.ensure_col(col).cast("float"), d).expression,
            ],
        )
    )


def substring_index(str: ColumnOrName, delim: str, count: int) -> Column:
    lit = get_func_from_session("lit")

    return Column(
        sqlglot_expression.Anonymous(
            this="bqutil.fn.cw_substring_index",
            expressions=[
                Column.ensure_col(str).expression,
                lit(delim).expression,
                lit(count).expression,
            ],
        )
    )


def bin(col: ColumnOrName) -> Column:
    return (
        Column(
            sqlglot_expression.Anonymous(
                this="bqutil.fn.to_binary",
                expressions=[Column.ensure_col(col).expression],
            )
        )
        .cast("int")
        .cast("string")
    )


def slice(
    x: ColumnOrName, start: t.Union[ColumnOrName, int], length: t.Union[ColumnOrName, int]
) -> Column:
    lit = get_func_from_session("lit")

    start_col = start if isinstance(start, Column) else lit(start)
    length_col = length if isinstance(length, Column) else lit(length)

    subquery = (
        sqlglot_expression.select(
            sqlglot_expression.column("x"),
        )
        .from_(
            sqlglot_expression.Unnest(
                expressions=[Column.ensure_col(x).expression],
                alias=sqlglot_expression.TableAlias(
                    columns=[sqlglot_expression.to_identifier("x")],
                ),
                offset=sqlglot_expression.to_identifier("offset"),
            )
        )
        .where(
            sqlglot_expression.Between(
                this=sqlglot_expression.column("offset"),
                low=(start_col - lit(1)).expression,
                high=(start_col + length_col).expression,
            )
        )
    )

    return Column(
        sqlglot_expression.Anonymous(
            this="ARRAY",
            expressions=[subquery],
        )
    )


def array_position(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    lit = get_func_from_session("lit")

    value_col = value if isinstance(value, Column) else lit(value)

    return Column(
        sqlglot_expression.Coalesce(
            this=sqlglot_expression.Anonymous(
                this="bqutil.fn.find_in_set",
                expressions=[
                    value_col.expression,
                    sqlglot_expression.Anonymous(
                        this="ARRAY_TO_STRING",
                        expressions=[Column.ensure_col(col).expression, lit(",").expression],
                    ),
                ],
            ),
            expressions=[lit(0).expression],
        )
    )


def array_remove(col: ColumnOrName, value: ColumnOrLiteral) -> Column:
    lit = get_func_from_session("lit")

    value_col = value if isinstance(value, Column) else lit(value)

    filter_subquery = sqlglot_expression.select(
        "*",
    ).from_(
        sqlglot_expression.Unnest(
            expressions=[Column.ensure_col(col).expression],
            alias=sqlglot_expression.TableAlias(
                columns=[sqlglot_expression.to_identifier("x")],
            ),
        )
    )

    agg_subquery = (
        sqlglot_expression.select(
            sqlglot_expression.Anonymous(
                this="ARRAY_AGG",
                expressions=[sqlglot_expression.column("x")],
            ),
        )
        .from_(filter_subquery.subquery("t"))
        .where(
            sqlglot_expression.NEQ(
                this=sqlglot_expression.column("x", "t"),
                expression=value_col.expression,
            )
        )
    )

    return Column(agg_subquery.subquery())


def array_distinct(col: ColumnOrName) -> Column:
    return Column(
        sqlglot_expression.Anonymous(
            this="bqutil.fn.cw_array_distinct",
            expressions=[Column.ensure_col(col).expression],
        )
    )


def array_min(col: ColumnOrName) -> Column:
    return Column(
        sqlglot_expression.Anonymous(
            this="bqutil.fn.cw_array_min",
            expressions=[Column.ensure_col(col).expression],
        )
    )


def array_max(col: ColumnOrName) -> Column:
    return Column(
        sqlglot_expression.Anonymous(
            this="bqutil.fn.cw_array_max",
            expressions=[Column.ensure_col(col).expression],
        )
    )


def sort_array(col: ColumnOrName, asc: t.Optional[bool] = None) -> Column:
    order = "ASC" if asc or asc is None else "DESC"
    subquery = (
        sqlglot_expression.select("x")
        .from_(
            sqlglot_expression.Unnest(
                expressions=[Column.ensure_col(col).expression],
                alias=sqlglot_expression.TableAlias(
                    columns=[sqlglot_expression.to_identifier("x")],
                ),
            )
        )
        .order_by(f"x {order}")
    )

    return Column(sqlglot_expression.Anonymous(this="ARRAY", expressions=[subquery]))


array_sort = sort_array
