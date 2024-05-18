from __future__ import annotations

import functools
import typing as t

from sqlglot import parse_one
from sqlglot.helper import ensure_list
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

if t.TYPE_CHECKING:
    from sqlframe.base.catalog import _BaseCatalog


def normalize(normalize_kwargs: t.List[str]) -> t.Callable[[t.Callable], t.Callable]:
    """
    Decorator used around DataFrame methods to indicate what type of operation is being performed from the
    ordered Operation enums. This is used to determine which operations should be performed on a CTE vs.
    included with the previous operation.

    Ex: After a user does a join we want to allow them to select which columns for the different
    tables that they want to carry through to the following operation. If we put that join in
    a CTE preemptively then the user would not have a chance to select which column they want
    in cases where there is overlap in names.
    """

    def decorator(func: t.Callable) -> t.Callable:
        @functools.wraps(func)
        def wrapper(self: _BaseCatalog, *args, **kwargs) -> _BaseCatalog:
            kwargs.update(dict(zip(func.__code__.co_varnames[1:], args)))
            for kwarg in normalize_kwargs:
                if kwarg in kwargs:
                    value = kwargs.get(kwarg)
                    if value:
                        expression = parse_one(value, dialect=self.session.input_dialect)
                        kwargs[kwarg] = normalize_identifiers(
                            expression, self.session.input_dialect
                        ).sql(dialect=self.session.input_dialect)
            return func(self, **kwargs)

        wrapper.__wrapped__ = func  # type: ignore
        return wrapper

    return decorator


def func_metadata(unsupported_engines: t.Optional[t.Union[str, t.List[str]]] = None) -> t.Callable:
    def _metadata(func: t.Callable) -> t.Callable:
        func.unsupported_engines = ensure_list(unsupported_engines) if unsupported_engines else []  # type: ignore
        return func

    return _metadata
