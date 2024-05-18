from __future__ import annotations

import inspect
import sys

import sqlframe.base.functions  # noqa

module = sys.modules["sqlframe.base.functions"]
globals().update(
    {
        name: func
        for name, func in inspect.getmembers(module, inspect.isfunction)
        if hasattr(func, "unsupported_engines")
        and "duckdb" not in func.unsupported_engines
        and "*" not in func.unsupported_engines
    }
)


from sqlframe.base.function_alternatives import (  # noqa
    e_literal as e,
    expm1_from_exp as expm1,
    log1p_from_log as log1p,
    rint_from_round as rint,
    kurtosis_from_kurtosis_pop as kurtosis,
    collect_set_from_list_distinct as collect_set,
    first_always_ignore_nulls as first,
    factorial_ensure_int as factorial,
    isnull_using_equal as isnull,
    nanvl_as_case as nanvl,
    percentile_approx_without_accuracy as percentile_approx,
    rand_no_seed as rand,
    base64_from_blob as base64,
    decode_from_blob as decode,
    format_string_with_pipes as format_string,
    overlay_from_substr as overlay,
    split_no_limit as split,
    arrays_overlap_using_intersect as arrays_overlap,
    slice_as_list_slice as slice,
    array_join_null_replacement_with_transform as array_join,
    element_at_using_brackets as element_at,
    array_remove_using_filter as array_remove,
    array_union_using_list_concat as array_union,
    array_min_from_sort as array_min,
    array_max_from_sort as array_max,
    sequence_from_generate_series as sequence,
)
