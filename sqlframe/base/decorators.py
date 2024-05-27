from __future__ import annotations

import functools
import typing as t

from sqlglot import parse_one
from sqlglot.helper import ensure_list
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

if t.TYPE_CHECKING:
    from sqlframe.base.catalog import _BaseCatalog

CALLING_CLASS = t.TypeVar("CALLING_CLASS")


def normalize(normalize_kwargs: t.Union[str, t.List[str]]) -> t.Callable[[t.Callable], t.Callable]:
    """
    Decorator used to normalize identifiers in the kwargs of a method.
    """

    def decorator(func: t.Callable) -> t.Callable:
        @functools.wraps(func)
        def wrapper(self: CALLING_CLASS, *args, **kwargs) -> CALLING_CLASS:
            from sqlframe.base.session import _BaseSession

            input_dialect = _BaseSession().input_dialect
            kwargs.update(dict(zip(func.__code__.co_varnames[1:], args)))
            for kwarg in ensure_list(normalize_kwargs):
                if kwarg in kwargs:
                    value = kwargs.get(kwarg)
                    if value:
                        expression = (
                            parse_one(value, dialect=input_dialect)
                            if isinstance(value, str)
                            else value
                        )
                        kwargs[kwarg] = normalize_identifiers(expression, input_dialect).sql(
                            dialect=input_dialect
                        )
            return func(self, **kwargs)

        wrapper.__wrapped__ = func  # type: ignore
        return wrapper

    return decorator


def func_metadata(unsupported_engines: t.Optional[t.Union[str, t.List[str]]] = None) -> t.Callable:
    def _metadata(func: t.Callable) -> t.Callable:
        func.unsupported_engines = ensure_list(unsupported_engines) if unsupported_engines else []  # type: ignore
        return func

    return _metadata
