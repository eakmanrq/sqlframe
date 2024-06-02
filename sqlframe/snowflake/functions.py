import inspect
import sys

import sqlframe.base.functions

module = sys.modules["sqlframe.base.functions"]
globals().update(
    {
        name: func
        for name, func in inspect.getmembers(module, inspect.isfunction)
        if hasattr(func, "unsupported_engines")
        and "snowflake" not in func.unsupported_engines
        and "*" not in func.unsupported_engines
    }
)


from sqlframe.base.function_alternatives import (  # noqa
    e_literal as e,
    expm1_from_exp as expm1,
    log1p_from_log as log1p,
    rint_from_round as rint,
    bitwise_not_from_bitnot as bitwise_not,
    skewness_from_skew as skewness,
    isnan_using_equal as isnan,
    isnull_using_equal as isnull,
    nanvl_as_case as nanvl,
    percentile_approx_without_accuracy_and_max_array as percentile_approx,
    bround_using_half_even as bround,
    shiftleft_from_bitshiftleft as shiftleft,
    shiftright_from_bitshiftright as shiftright,
    struct_with_eq as struct,
    make_date_date_from_parts as make_date,
    date_add_no_date_sub as date_add,
    date_sub_by_date_add as date_sub,
    add_months_using_func as add_months,
    months_between_cast_as_date_cast_roundoff as months_between,
    last_day_with_cast as last_day,
    from_unixtime_from_timestamp as from_unixtime,
    unix_timestamp_from_extract as unix_timestamp,
    base64_from_base64_encode as base64,
    unbase64_from_base64_decode_string as unbase64,
    format_number_from_to_char as format_number,
    overlay_from_substr as overlay,
    levenshtein_edit_distance as levenshtein,
    split_with_split as split,
    regexp_extract_coalesce_empty_str as regexp_extract,
    hex_using_encode as hex,
    unhex_hex_decode_str as unhex,
    create_map_with_cast as create_map,
    array_contains_cast_variant as array_contains,
    arrays_overlap_as_plural as arrays_overlap,
    slice_as_array_slice as slice,
    array_join_no_null_replacement as array_join,
    array_position_cast_variant_and_flip as array_position,
    element_at_using_brackets as element_at,
    array_intersect_using_intersection as array_intersect,
    array_union_using_array_concat as array_union,
    sort_array_using_array_sort as sort_array,
    flatten_using_array_flatten as flatten,
    map_concat_using_map_cat as map_concat,
    sequence_from_array_generate_range as sequence,
)
