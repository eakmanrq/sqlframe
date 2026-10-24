# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import contextlib
import contextvars
import functools
import typing as t
from enum import IntEnum

from typing_extensions import Concatenate, ParamSpec

if t.TYPE_CHECKING:
    from sqlframe.base.dataframe import BaseDataFrame
    from sqlframe.base.group import _BaseGroupedData

    DF = t.TypeVar("DF", bound=BaseDataFrame)
    T = t.TypeVar("T", bound=t.Union[BaseDataFrame, _BaseGroupedData])
else:
    DF = t.TypeVar("DF")
    T = t.TypeVar("T")
P = ParamSpec("P")  # represents arbitrary args + kwargs

# Depth of decorated operations on the current call stack. Decorated methods call
# other decorated methods internally; only the outermost, user-level call validates
# the resulting plan so each operation costs one validation.
_operation_depth: contextvars.ContextVar[int] = contextvars.ContextVar(
    "sqlframe_operation_depth", default=0
)


@contextlib.contextmanager
def nested_operation() -> t.Iterator[None]:
    """Treat decorated operations run inside the block as internal steps of one operation."""
    token = _operation_depth.set(_operation_depth.get() + 1)
    try:
        yield
    finally:
        _operation_depth.reset(token)


def _run_operation(func: t.Callable[[], T], validate: t.Callable[[T], None]) -> T:
    outermost = _operation_depth.get() == 0
    with nested_operation():
        df = func()
    if outermost:
        validate(df)
    return df


class Operation(IntEnum):
    INIT = -1
    NO_OP = 0
    FROM = 1
    WHERE = 2
    GROUP_BY = 3
    HAVING = 4
    SELECT = 5
    ORDER_BY = 6
    LIMIT = 7


# We want to decorate a function (self: DF, *args, **kwargs) -> T
#       where DF is a subclass of BaseDataFrame
#       where T is a subclass of BaseDataFrame or _BaseGroupedData
# And keep its signature, i.e. produce a function of the same shape
# Hence we work with `t.Callable[Concatenate[DF, P], T]`
def operation(
    op: Operation,
) -> t.Callable[
    [t.Callable[Concatenate[DF, P], T]],  # accept such a function
    t.Callable[Concatenate[DF, P], T],  # and return such a function
]:
    """
    Decorator used around DataFrame methods to indicate what type of operation is being performed from the
    ordered Operation enums. This is used to determine which operations should be performed on a CTE vs.
    included with the previous operation.

    Ex: After a user does a join we want to allow them to select which columns for the different
    tables that they want to carry through to the following operation. If we put that join in
    a CTE preemptively then the user would not have a chance to select which column they want
    in cases where there is overlap in names.
    """

    def decorator(
        func: t.Callable[Concatenate[DF, P], T],
    ) -> t.Callable[Concatenate[DF, P], T]:
        @functools.wraps(func)
        def wrapper(self: DF, *args, **kwargs) -> T:
            from sqlframe.base.dataframe import BaseDataFrame

            if self.last_op == Operation.INIT:
                self = self._convert_leaf_to_cte()
                self.last_op = Operation.NO_OP
            last_op = self.last_op
            new_op = op if op != Operation.NO_OP else last_op
            if new_op < last_op or (last_op == new_op == Operation.SELECT):
                self = self._convert_leaf_to_cte()

            def run() -> T:
                df = func(self, *args, **kwargs)
                df.last_op = new_op
                return df

            def validate(df: T) -> None:
                if isinstance(df, BaseDataFrame):
                    df._validate_plan()

            return _run_operation(run, validate)

        wrapper.__wrapped__ = func
        return wrapper

    return decorator


# Here decorate a function (self: _BaseGroupedData[DF], *args, **kwargs) -> DF
# Hence we work with t.Callable[Concatenate[_BaseGroupedData[DF], P], DF]
# We simplify the parameters, as Pyright (used for VSCode autocomplete) doesn't understand this
def group_operation(
    op: Operation,
) -> t.Callable[[t.Callable[P, DF]], t.Callable[P, DF]]:
    """
    Decorator used around DataFrame methods to indicate what type of operation is being performed from the
    ordered Operation enums. This is used to determine which operations should be performed on a CTE vs.
    included with the previous operation.

    Ex: After a user does a join we want to allow them to select which columns for the different
    tables that they want to carry through to the following operation. If we put that join in
    a CTE preemptively then the user would not have a chance to select which column they want
    in cases where there is overlap in names.
    """

    def decorator(
        func: t.Callable[Concatenate[_BaseGroupedData[DF], P], DF],
    ) -> t.Callable[Concatenate[_BaseGroupedData[DF], P], DF]:
        @functools.wraps(func)
        def wrapper(self: _BaseGroupedData[DF], *args, **kwargs) -> DF:
            if self._df.last_op == Operation.INIT:
                self._df = self._df._convert_leaf_to_cte()
                self._df.last_op = Operation.NO_OP
            last_op = self._df.last_op
            new_op = op if op != Operation.NO_OP else last_op
            if new_op < last_op or (last_op == new_op == Operation.SELECT):
                self._df = self._df._convert_leaf_to_cte()

            def run() -> DF:
                df = func(self, *args, **kwargs)
                df.last_op = new_op
                return df

            return _run_operation(run, lambda df: df._validate_plan())

        wrapper.__wrapped__ = func
        return wrapper

    return decorator
