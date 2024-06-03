import typing as t

from sqlframe.base.column import Column as Column
from sqlframe.base.function_alternatives import (
    array_union_using_array_concat as array_union,
)
from sqlframe.base.function_alternatives import (
    base64_from_blob as base64,
)
from sqlframe.base.function_alternatives import (
    bit_length_from_length as bit_length,
)
from sqlframe.base.function_alternatives import (
    collect_set_from_list_distinct as collect_set,
)
from sqlframe.base.function_alternatives import (
    concat_ws_from_array_to_string as concat_ws,
)
from sqlframe.base.function_alternatives import (
    dayofmonth_from_extract_with_day as dayofmonth,
)
from sqlframe.base.function_alternatives import (
    dayofweek_from_extract as dayofweek,
)
from sqlframe.base.function_alternatives import (
    dayofyear_from_extract as dayofyear,
)
from sqlframe.base.function_alternatives import (  # noqa
    e_literal as e,
)
from sqlframe.base.function_alternatives import (
    element_at_using_brackets as element_at,
)
from sqlframe.base.function_alternatives import (
    expm1_from_exp as expm1,
)
from sqlframe.base.function_alternatives import (
    factorial_from_case_statement as factorial,
)
from sqlframe.base.function_alternatives import (
    format_string_with_format as format_string,
)
from sqlframe.base.function_alternatives import (
    hash_from_farm_fingerprint as hash,
)
from sqlframe.base.function_alternatives import (
    hex_casted_as_bytes as hex,
)
from sqlframe.base.function_alternatives import (
    hour_from_extract as hour,
)
from sqlframe.base.function_alternatives import (
    instr_using_strpos as instr,
)
from sqlframe.base.function_alternatives import (
    isnull_using_equal as isnull,
)
from sqlframe.base.function_alternatives import (
    last_day_with_cast as last_day,
)
from sqlframe.base.function_alternatives import (
    log1p_from_log as log1p,
)
from sqlframe.base.function_alternatives import (
    make_date_from_date_func as make_date,
)
from sqlframe.base.function_alternatives import (
    minute_from_extract as minute,
)
from sqlframe.base.function_alternatives import (
    month_from_extract as month,
)
from sqlframe.base.function_alternatives import (
    nanvl_as_case as nanvl,
)
from sqlframe.base.function_alternatives import (
    overlay_from_substr as overlay,
)
from sqlframe.base.function_alternatives import (
    percentile_approx_without_accuracy_and_plural as percentile_approx,
)
from sqlframe.base.function_alternatives import (
    quarter_from_extract as quarter,
)
from sqlframe.base.function_alternatives import (
    rand_no_seed as rand,
)
from sqlframe.base.function_alternatives import (
    regexp_extract_only_one_group as regexp_extract,
)
from sqlframe.base.function_alternatives import (
    rint_from_round as rint,
)
from sqlframe.base.function_alternatives import (
    second_from_extract as second,
)
from sqlframe.base.function_alternatives import (
    sequence_from_generate_array as sequence,
)
from sqlframe.base.function_alternatives import (
    sha1_force_sha1_and_to_hex as sha1,
)
from sqlframe.base.function_alternatives import (
    split_with_split as split,
)
from sqlframe.base.function_alternatives import (
    to_date_from_timestamp as to_date,
)
from sqlframe.base.function_alternatives import (
    weekofyear_from_extract_as_isoweek as weekofyear,
)
from sqlframe.base.function_alternatives import (
    year_from_extract as year,
)
from sqlframe.base.functions import abs as abs
from sqlframe.base.functions import acos as acos
from sqlframe.base.functions import acosh as acosh
from sqlframe.base.functions import add_months as add_months
from sqlframe.base.functions import approx_count_distinct as approx_count_distinct
from sqlframe.base.functions import approxCountDistinct as approxCountDistinct
from sqlframe.base.functions import array as array
from sqlframe.base.functions import array_contains as array_contains
from sqlframe.base.functions import array_join as array_join
from sqlframe.base.functions import asc as asc
from sqlframe.base.functions import asc_nulls_first as asc_nulls_first
from sqlframe.base.functions import asc_nulls_last as asc_nulls_last
from sqlframe.base.functions import ascii as ascii
from sqlframe.base.functions import asin as asin
from sqlframe.base.functions import asinh as asinh
from sqlframe.base.functions import atan as atan
from sqlframe.base.functions import atan2 as atan2
from sqlframe.base.functions import atanh as atanh
from sqlframe.base.functions import avg as avg
from sqlframe.base.functions import bitwise_not as bitwise_not
from sqlframe.base.functions import bitwiseNOT as bitwiseNOT
from sqlframe.base.functions import cbrt as cbrt
from sqlframe.base.functions import ceil as ceil
from sqlframe.base.functions import ceiling as ceiling
from sqlframe.base.functions import coalesce as coalesce
from sqlframe.base.functions import col as col
from sqlframe.base.functions import collect_list as collect_list
from sqlframe.base.functions import concat as concat
from sqlframe.base.functions import corr as corr
from sqlframe.base.functions import cos as cos
from sqlframe.base.functions import cosh as cosh
from sqlframe.base.functions import cot as cot
from sqlframe.base.functions import count as count
from sqlframe.base.functions import covar_pop as covar_pop
from sqlframe.base.functions import covar_samp as covar_samp
from sqlframe.base.functions import csc as csc
from sqlframe.base.functions import cume_dist as cume_dist
from sqlframe.base.functions import current_date as current_date
from sqlframe.base.functions import current_timestamp as current_timestamp
from sqlframe.base.functions import date_add as date_add
from sqlframe.base.functions import date_diff as date_diff
from sqlframe.base.functions import date_format as date_format
from sqlframe.base.functions import date_sub as date_sub
from sqlframe.base.functions import date_trunc as date_trunc
from sqlframe.base.functions import dense_rank as dense_rank
from sqlframe.base.functions import desc as desc
from sqlframe.base.functions import desc_nulls_first as desc_nulls_first
from sqlframe.base.functions import desc_nulls_last as desc_nulls_last
from sqlframe.base.functions import exp as exp
from sqlframe.base.functions import explode as explode
from sqlframe.base.functions import explode_outer as explode_outer
from sqlframe.base.functions import expr as expr
from sqlframe.base.functions import floor as floor
from sqlframe.base.functions import get_json_object as get_json_object
from sqlframe.base.functions import greatest as greatest
from sqlframe.base.functions import initcap as initcap
from sqlframe.base.functions import input_file_name as input_file_name
from sqlframe.base.functions import isnan as isnan
from sqlframe.base.functions import lag as lag
from sqlframe.base.functions import lead as lead
from sqlframe.base.functions import least as least
from sqlframe.base.functions import length as length
from sqlframe.base.functions import lit as lit
from sqlframe.base.functions import log as log
from sqlframe.base.functions import log2 as log2
from sqlframe.base.functions import log10 as log10
from sqlframe.base.functions import lower as lower
from sqlframe.base.functions import lpad as lpad
from sqlframe.base.functions import ltrim as ltrim
from sqlframe.base.functions import max as max
from sqlframe.base.functions import max_by as max_by
from sqlframe.base.functions import md5 as md5
from sqlframe.base.functions import mean as mean
from sqlframe.base.functions import min as min
from sqlframe.base.functions import min_by as min_by
from sqlframe.base.functions import nth_value as nth_value
from sqlframe.base.functions import ntile as ntile
from sqlframe.base.functions import nullif as nullif
from sqlframe.base.functions import octet_length as octet_length
from sqlframe.base.functions import percent_rank as percent_rank
from sqlframe.base.functions import posexplode as posexplode
from sqlframe.base.functions import posexplode_outer as posexplode_outer
from sqlframe.base.functions import pow as pow
from sqlframe.base.functions import rank as rank
from sqlframe.base.functions import regexp_replace as regexp_replace
from sqlframe.base.functions import repeat as repeat
from sqlframe.base.functions import reverse as reverse
from sqlframe.base.functions import round as round
from sqlframe.base.functions import row_number as row_number
from sqlframe.base.functions import rpad as rpad
from sqlframe.base.functions import rtrim as rtrim
from sqlframe.base.functions import sec as sec
from sqlframe.base.functions import shiftLeft as shiftLeft
from sqlframe.base.functions import shiftleft as shiftleft
from sqlframe.base.functions import shiftRight as shiftRight
from sqlframe.base.functions import shiftright as shiftright
from sqlframe.base.functions import signum as signum
from sqlframe.base.functions import sin as sin
from sqlframe.base.functions import sinh as sinh
from sqlframe.base.functions import size as size
from sqlframe.base.functions import soundex as soundex
from sqlframe.base.functions import sqrt as sqrt
from sqlframe.base.functions import stddev as stddev
from sqlframe.base.functions import stddev_pop as stddev_pop
from sqlframe.base.functions import stddev_samp as stddev_samp
from sqlframe.base.functions import struct as struct
from sqlframe.base.functions import substring as substring
from sqlframe.base.functions import sum as sum
from sqlframe.base.functions import sum_distinct as sum_distinct
from sqlframe.base.functions import sumDistinct as sumDistinct
from sqlframe.base.functions import tan as tan
from sqlframe.base.functions import tanh as tanh
from sqlframe.base.functions import timestamp_seconds as timestamp_seconds
from sqlframe.base.functions import to_timestamp as to_timestamp
from sqlframe.base.functions import toDegrees as toDegrees
from sqlframe.base.functions import toRadians as toRadians
from sqlframe.base.functions import translate as translate
from sqlframe.base.functions import trim as trim
from sqlframe.base.functions import trunc as trunc
from sqlframe.base.functions import unbase64 as unbase64
from sqlframe.base.functions import unhex as unhex
from sqlframe.base.functions import upper as upper
from sqlframe.base.functions import var_pop as var_pop
from sqlframe.base.functions import var_samp as var_samp
from sqlframe.base.functions import variance as variance
from sqlframe.base.functions import when as when
from sqlframe.base.util import get_func_from_session as get_func_from_session

if t.TYPE_CHECKING:
    from sqlframe.base._typing import ColumnOrLiteral, ColumnOrName

def array_distinct(col: ColumnOrName) -> Column: ...
def array_max(col: ColumnOrName) -> Column: ...
def array_min(col: ColumnOrName) -> Column: ...
def array_position(col: ColumnOrName, value: ColumnOrLiteral) -> Column: ...
def array_remove(col: ColumnOrName, value: ColumnOrLiteral) -> Column: ...
def array_sort(col: ColumnOrName, asc: t.Optional[bool] = ...) -> Column: ...
def bin(col: ColumnOrName) -> Column: ...
def bround(col: ColumnOrName, scale: t.Optional[int] = ...) -> Column: ...
def degrees(col: ColumnOrName) -> Column: ...
def format_number(col: ColumnOrName, d: int) -> Column: ...
def from_unixtime(col: ColumnOrName, format: t.Optional[str] = ...) -> Column: ...
def months_between(
    date1: ColumnOrName, date2: ColumnOrName, roundOff: t.Optional[bool] = ...
) -> Column: ...
def next_day(col: ColumnOrName, dayOfWeek: str) -> Column: ...
def radians(col: ColumnOrName) -> Column: ...
def slice(
    x: ColumnOrName, start: t.Union[ColumnOrName, int], length: t.Union[ColumnOrName, int]
) -> Column: ...
def sort_array(col: ColumnOrName, asc: t.Optional[bool] = ...) -> Column: ...
def substring_index(str: ColumnOrName, delim: str, count: int) -> Column: ...
def typeof(col: ColumnOrName) -> Column: ...
def unix_timestamp(
    timestamp: t.Optional[ColumnOrName] = ..., format: t.Optional[str] = ...
) -> Column: ...
