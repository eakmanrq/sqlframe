import inspect
import sys

import sqlframe.base.functions

module = sys.modules["sqlframe.base.functions"]
globals().update(
    {
        name: func
        for name, func in inspect.getmembers(module, inspect.isfunction)
        if hasattr(func, "unsupported_engines")
        and "redshift" not in func.unsupported_engines
        and "*" not in func.unsupported_engines
    }
)


from sqlframe.base.function_alternatives import e_literal as e  # noqa
