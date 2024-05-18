import inspect
import sys

import sqlframe.base.functions

module = sys.modules["sqlframe.base.functions"]
globals().update(
    {
        name: func
        for name, func in inspect.getmembers(module, inspect.isfunction)
        if hasattr(func, "unsupported_engines")
        and "postgres" not in func.unsupported_engines
        and "*" not in func.unsupported_engines
    }
)


from sqlframe.base.function_alternatives import (  # noqa
    e_literal as e,
    expm1_from_exp as expm1,
    log1p_from_log as log1p,
    rint_from_round as rint,
    collect_set_from_list_distinct as collect_set,
    isnan_using_equal as isnan,
    isnull_using_equal as isnull,
    nanvl_as_case as nanvl,
    rand_no_seed as rand,
    round_cast_as_numeric as round,
    year_from_extract as year,
    quarter_from_extract as quarter,
    month_from_extract as month,
    dayofweek_from_extract_with_isodow as dayofweek,
    dayofmonth_from_extract_with_day as dayofmonth,
    dayofyear_from_extract_doy as dayofyear,
    hour_from_extract as hour,
    minute_from_extract as minute,
    second_from_extract as second,
    weekofyear_from_extract_as_week as weekofyear,
    make_date_casted_as_integer as make_date,
    date_add_by_multiplication as date_add,
    date_sub_by_multiplication as date_sub,
    date_diff_with_subtraction as date_diff,
    add_months_by_multiplication as add_months,
    months_between_from_age_and_extract as months_between,
    from_unixtime_from_timestamp as from_unixtime,
    unix_timestamp_from_extract as unix_timestamp,
    base64_from_blob as base64,
    bas64_from_encode as base64,
    unbase64_from_decode as unbase64,
    decode_from_convert_from as decode,
    encode_from_convert_to as encode,
    format_number_from_to_char as format_number,
    format_string_with_format as format_string,
    split_from_regex_split_to_array as split,
    array_contains_any as array_contains,
    slice_with_brackets as slice,
    element_at_using_brackets as element_at,
    get_json_object_using_arrow_op as get_json_object,
    array_min_from_subquery as array_min,
    array_max_from_subquery as array_max,
)
