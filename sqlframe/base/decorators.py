from __future__ import annotations

import typing as t

from sqlglot.helper import ensure_list

CALLING_CLASS = t.TypeVar("CALLING_CLASS")


def func_metadata(unsupported_engines: t.Optional[t.Union[str, t.List[str]]] = None) -> t.Callable:
    def _metadata(func: t.Callable) -> t.Callable:
        func.unsupported_engines = ensure_list(unsupported_engines) if unsupported_engines else []  # type: ignore
        return func

    return _metadata
