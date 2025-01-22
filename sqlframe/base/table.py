import sys
import typing as t
from enum import IntEnum
from uuid import uuid4

from sqlglot import exp
from sqlglot.expressions import _to_s
from sqlglot.helper import object_to_dict

from sqlframe.base.dataframe import DF, SESSION, BaseDataFrame

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if t.TYPE_CHECKING:
    from sqlframe.base._typing import ColumnOrLiteral
    from sqlframe.base.column import Column
    from sqlframe.base.types import Row


class Clause(IntEnum):
    UPDATE = 1
    UPDATE_ALL = 2
    DELETE = 3
    INSERT = 4
    INSERT_ALL = 5


class MergeClause:
    def __init__(
        self,
        clause_type: Clause,
        condition: t.Optional[t.Union[str, t.List[str], "Column", t.List["Column"], bool]] = None,
        assignments: t.Optional[
            t.Dict[t.Union["Column", str], t.Union["Column", "ColumnOrLiteral", exp.Expression]]
        ] = None,
        matched: bool = True,
        by_source: bool = False,
    ):
        self.clause_type = clause_type
        self.condition = condition
        self.assignments = assignments
        self.matched = matched
        self.by_source = by_source


class WhenMatched:
    def __init__(
        self,
        condition: t.Optional[t.Union[str, t.List[str], "Column", t.List["Column"], bool]] = None,
    ):
        self._condition = condition
        self._clause: t.Union[MergeClause, None] = None

    def update(
        self,
        set_: t.Dict[t.Union["Column", str], t.Union["Column", "ColumnOrLiteral", exp.Expression]],
    ) -> Self:
        if self._clause:
            clause = self._clause.clause_type.name.lower()
            raise ValueError(f"WhenMatched already has an '{clause}' clause")
        self._clause = MergeClause(
            Clause.UPDATE,
            self._condition,
            {k: v for k, v in set_.items()},
            matched=True,
            by_source=False,
        )
        return self

    def update_all(self) -> Self:
        if self._clause:
            clause = self._clause.clause_type.name.lower()
            raise ValueError(f"WhenMatched already has an '{clause}' clause")
        self._clause = MergeClause(
            Clause.UPDATE_ALL,
            self._condition,
            {},
            matched=True,
            by_source=False,
        )
        return self

    def delete(self) -> Self:
        if self._clause:
            clause = self._clause.clause_type.name.lower()
            raise ValueError(f"WhenMatched already has an '{clause}' clause")
        self._clause = MergeClause(Clause.DELETE, self._condition, matched=True, by_source=False)
        return self

    @property
    def clause(self):
        return self._clause


class WhenNotMatched:
    def __init__(
        self,
        condition: t.Optional[t.Union[str, t.List[str], "Column", t.List["Column"], bool]] = None,
    ):
        self._condition = condition
        self._clause: t.Union[MergeClause, None] = None

    def insert(
        self,
        values: t.Dict[
            t.Union["Column", str], t.Union["Column", "ColumnOrLiteral", exp.Expression]
        ],
    ) -> Self:
        if self._clause:
            clause = self._clause.clause_type.name.lower()
            raise ValueError(f"WhenNotMatched already has an '{clause}' clause")
        self._clause = MergeClause(
            Clause.INSERT,
            self._condition,
            {k: v for k, v in values.items()},
            matched=False,
            by_source=False,
        )
        return self

    def insert_all(self) -> Self:
        if self._clause:
            clause = self._clause.clause_type.name.lower()
            raise ValueError(f"WhenNotMatched already has an '{clause}' clause")
        self._clause = MergeClause(
            Clause.INSERT_ALL,
            self._condition,
            {},
            matched=False,
            by_source=False,
        )
        return self

    @property
    def clause(self):
        return self._clause


class WhenNotMatchedBySource(object):
    def __init__(
        self,
        condition: t.Optional[t.Union[str, t.List[str], "Column", t.List["Column"], bool]] = None,
    ):
        self._condition = condition
        self._clause: t.Union[MergeClause, None] = None

    def update(
        self,
        set_: t.Dict[t.Union["Column", str], t.Union["Column", "ColumnOrLiteral", exp.Expression]],
    ) -> Self:
        if self._clause:
            clause = self._clause.clause_type.name.lower()
            raise ValueError(f"WhenNotMatchedBySource already has an '{clause}' clause")
        self._clause = MergeClause(
            Clause.UPDATE,
            self._condition,
            {k: v for k, v in set_.items()},
            matched=False,
            by_source=True,
        )
        return self

    def delete(self) -> Self:
        if self._clause:
            clause = self._clause.clause_type.name.lower()
            raise ValueError(f"WhenNotMatchedBySource already has an '{clause}' clause")
        self._clause = MergeClause(Clause.DELETE, self._condition, matched=False, by_source=True)
        return self

    @property
    def clause(self):
        return self._clause


class LazyExpression:
    def __init__(
        self,
        expression: exp.Expression,
        session: SESSION,
    ):
        self._expression = expression
        self._session = session

    def execute(self) -> t.List["Row"]:
        return self._session._collect(self._expression)

    @property
    def expression(self) -> exp.Expression:
        return self._expression

    def __str__(self) -> str:
        return self._expression.sql()

    def __repr__(self) -> str:
        return _to_s(self._expression)


class _BaseTable(BaseDataFrame, t.Generic[DF]):
    _df: t.Type[DF]

    def copy(self, **kwargs):
        kwargs["join_on_uuid"] = str(uuid4())
        return self._df(**object_to_dict(self, **kwargs))

    def __copy__(self):
        return self.copy()

    def table_copy(self):
        return self.__class__(**object_to_dict(self))

    def alias(self, name: str, **kwargs) -> Self:
        df = BaseDataFrame.alias(self, name, **kwargs)
        new_df = self.__class__(**object_to_dict(df))
        return new_df

    def update(
        self,
        set_: t.Dict[t.Union["Column", str], t.Union["Column", "ColumnOrLiteral", exp.Expression]],
        where: t.Optional[t.Union["Column", str, bool]] = None,
    ) -> LazyExpression:
        raise NotImplementedError()

    def merge(
        self,
        source: DF,
        condition: t.Union[str, t.List[str], "Column", t.List["Column"], bool],
        clauses: t.Iterable[t.Union[WhenMatched, WhenNotMatched, WhenNotMatchedBySource]],
    ) -> LazyExpression:
        raise NotImplementedError()

    def delete(
        self,
        where: t.Optional[t.Union["Column", str, bool]] = None,
    ) -> LazyExpression:
        raise NotImplementedError()
