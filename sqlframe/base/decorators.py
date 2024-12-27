from __future__ import annotations

import re
import typing as t

from sqlglot import exp
from sqlglot.helper import ensure_list

from sqlframe.base.column import Column

CALLING_CLASS = t.TypeVar("CALLING_CLASS")


def func_metadata(unsupported_engines: t.Optional[t.Union[str, t.List[str]]] = None) -> t.Callable:
    def _metadata(func: t.Callable) -> t.Callable:
        def wrapper(*args, **kwargs):
            funcs_to_not_auto_alias = [
                "posexplode",
                "explode_outer",
                "json_tuple",
                "posexplode_outer",
                "stack",
                "inline",
                "inline_outer",
                "window",
                "session_window",
                "window_time",
            ]

            result = func(*args, **kwargs)
            if (
                isinstance(result, Column)
                and isinstance(result.column_expression, exp.Func)
                and not isinstance(result.expression, exp.Alias)
                and func.__name__ not in funcs_to_not_auto_alias
            ):
                col_name = result.column_expression.find(exp.Identifier)
                if col_name:
                    col_name = col_name.name
                else:
                    col_name = result.column_expression.find(exp.Literal)
                    if col_name:
                        col_name = col_name.this
                alias_name = f"{func.__name__}__{col_name or ''}__"
                # BigQuery has restrictions on alias names so we constrain it to alphanumeric characters and underscores
                return result.alias(re.sub("\W", "_", alias_name))  # type: ignore
            return result

        wrapper.unsupported_engines = (  # type: ignore
            ensure_list(unsupported_engines) if unsupported_engines else []
        )
        return wrapper

    return _metadata
