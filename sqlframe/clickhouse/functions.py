import inspect
import sys
from typing import NoReturn

import sqlframe.base.functions  # noqa
from sqlframe.base.exceptions import UnsupportedOperationError as _UnsupportedOperationError

module = sys.modules["sqlframe.base.functions"]
globals().update(
    {
        name: func
        for name, func in inspect.getmembers(module, inspect.isfunction)
        if hasattr(func, "unsupported_engines")
        and "clickhouse" not in func.unsupported_engines
        and "*" not in func.unsupported_engines
    }
)


def __getattr__(name: str) -> NoReturn:
    func = getattr(module, name, None)
    if inspect.isfunction(func) and "clickhouse" in getattr(func, "unsupported_engines", ()):
        raise _UnsupportedOperationError(f"{name} is not supported by ClickHouse")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
