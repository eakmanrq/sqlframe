import inspect
import sys

import sqlframe.base.functions  # noqa

module = sys.modules["sqlframe.base.functions"]
globals().update(
    {
        name: func
        for name, func in inspect.getmembers(module, inspect.isfunction)
        if hasattr(func, "unsupported_engines") and "spark" not in func.unsupported_engines
    }
)
