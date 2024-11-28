import sys
import typing as t
from enum import IntEnum

import sqlglot
from sqlglot import exp
from sqlglot.helper import object_to_dict

from sqlframe.base.dataframe import _BaseDataFrame

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if t.TYPE_CHECKING:
    from sqlframe.base.column import Column
    from sqlframe.base._typing import ColumnOrLiteral
    from sqlframe.base.dataframe import _BaseDataFrame

    DF = t.TypeVar("DF", bound=_BaseDataFrame)
else:
    DF = t.TypeVar("DF")


class Clause(IntEnum):
    INIT = -1
    NO_OP = 0
    UPDATE = 1
    DELETE = 2
    INSERT = 3


class MergeClause:
    def __init__(
        self,
        clause_type: Clause,
        condition: t.Union["Column", str, bool],
        assignments: t.Optional[
            t.Dict[t.Union["Column", str], t.Union["Column", "ColumnOrLiteral", exp.Expression]]
        ] = None,
        by_source: bool = False,
    ):
        self.clause_type = clause_type
        self.condition = condition
        self.assignments = assignments
        self.by_source = by_source


class WhenMatched:
    def __init__(self, condition: t.Union["Column", str, bool]):
        self._condition = condition
        self._clause: t.Union[MergeClause, None] = None

    def update(
        self,
        assignments: t.Dict[t.Union["Column", str], t.Union["Column", "ColumnOrLiteral", exp.Expression]],
    ):
        if self._clause:
            clause = "update" if self._clause.clause_type == Clause.UPDATE else "delete"
            raise ValueError(f"WhenMatched already has an '{clause}' clause")
        self._clause = MergeClause(
            Clause.UPDATE,
            self._condition,
            {k: v for k, v in assignments.items()},
            by_source=False,
        )

    def delete(self):
        if self._clause:
            clause = "update" if self._clause.clause_type == Clause.UPDATE else "delete"
            raise ValueError(f"WhenMatched already has an '{clause}' clause")
        self._clause = MergeClause(Clause.DELETE, self._condition, by_source=False)


class WhenNotMatched:
    def __init__(self, condition: t.Union["Column", str, bool]):
        self._condition = condition
        self._clause: t.Union[MergeClause, None] = None

    def insert(
        self,
        assignments: t.Dict[t.Union["Column", str], t.Union["Column", "ColumnOrLiteral", exp.Expression]],
    ):
        if self._clause:
            raise ValueError(f"WhenNotMatched already has an 'insert' clause")
        self._clause = MergeClause(
            Clause.INSERT,
            self._condition,
            {k: v for k, v in assignments.items()},
            by_source=False,
        )


class WhenNotMatchedBySource:
    def __init__(self, condition: t.Union["Column", str, bool]):
        self._condition = condition
        self._clause: t.Union[MergeClause, None] = None

    def delete(self):
        if self._clause:
            raise ValueError(f"WhenNotMatchedBySource already has an 'update' clause")
        self._clause = MergeClause(Clause.DELETE, self._condition, by_source=True)


class _BaseTable(_BaseDataFrame, t.Generic[DF]):
    _df: t.Type[DF]

    def copy(self, **kwargs):
        return self._df(**object_to_dict(self, **kwargs))

    def __copy__(self):
        return self.copy()

    def update(
        self,
        assignments: t.Dict[t.Union["Column", str], t.Union["Column", "ColumnOrLiteral", exp.Expression]],
        condition: t.Union["Column", str, bool],
    ):
        raise NotImplementedError()

    def merge(
        self,
        source: DF,
        condition: t.Union["Column", str, bool],
        clauses: t.Iterable[t.Union[WhenMatched, WhenNotMatched, WhenNotMatchedBySource]],
    ):
        raise NotImplementedError()

    def delete(self, condition: t.Union["Column", str, bool]):
        raise NotImplementedError()
