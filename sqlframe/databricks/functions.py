import inspect
import sys

import sqlframe.base.functions  # noqa

module = sys.modules["sqlframe.base.functions"]
globals().update(
    {
        name: func
        for name, func in inspect.getmembers(module, inspect.isfunction)
        if hasattr(func, "unsupported_engines") and "databricks" not in func.unsupported_engines
    }
)


from sqlframe.base.function_alternatives import (  # noqa
    percentile_without_disc as percentile,
    add_months_by_multiplication as add_months,
    arrays_overlap_renamed as arrays_overlap,
    _is_string_using_typeof_string_lcase as _is_string,
    try_element_at_zero_based as try_element_at,
)
