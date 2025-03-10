# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

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
            if self.last_op == Operation.INIT:
                self = self._convert_leaf_to_cte()
                self.last_op = Operation.NO_OP
            last_op = self.last_op
            new_op = op if op != Operation.NO_OP else last_op
            if new_op < last_op or (last_op == new_op == Operation.SELECT):
                self = self._convert_leaf_to_cte()
            df = func(self, *args, **kwargs)
            df.last_op = new_op
            return df

        wrapper.__wrapped__ = func
        return wrapper

    return decorator


# Here decorate a function (self: _BaseGroupedData[DF], *args, **kwargs) -> DF
# Hence we work with t.Callable[Concatenate[_BaseGroupedData[DF], P], DF]
# We simplify the parameters, as Pyright (used for VSCode autocomplete) doesn't unterstand this
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
            df = func(self, *args, **kwargs)
            df.last_op = new_op
            return df

        wrapper.__wrapped__ = func
        return wrapper

    return decorator  # type: ignore
