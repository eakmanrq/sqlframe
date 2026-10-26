import inspect
import sys
from typing import NoReturn

import sqlframe.base.functions  # noqa
from sqlframe.base.exceptions import UnsupportedOperationError as _UnsupportedOperationError

# These base implementations generate SQL that ClickHouse cannot execute.
_UNSUPPORTED_FUNCTIONS = frozenset(
    {
        "translate",
        "dayofweek",
        "datediff",
        "to_timestamp",
        "unix_timestamp",
        "from_unixtime",
        "sort_array",
        "hash",
        "format_number",
        "monotonically_increasing_id",
        "struct",
        "to_json",
        "sha2",
    }
)

module = sys.modules["sqlframe.base.functions"]
globals().update(
    {
        name: func
        for name, func in inspect.getmembers(module, inspect.isfunction)
        if hasattr(func, "unsupported_engines")
        and "clickhouse" not in func.unsupported_engines
        and "*" not in func.unsupported_engines
        and name not in _UNSUPPORTED_FUNCTIONS
    }
)


def __getattr__(name: str) -> NoReturn:
    if name in _UNSUPPORTED_FUNCTIONS:
        raise _UnsupportedOperationError(f"{name} is not supported by ClickHouse")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
