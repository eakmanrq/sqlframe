# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'spark' folder.

from __future__ import annotations

import datetime
import typing as t

from sqlglot import expressions as exp

if t.TYPE_CHECKING:
    from sqlframe.base.column import Column
    from sqlframe.base.types import DataType, StructType

PrimitiveType = t.Union[bool, float, int, str]
ColumnLiterals = t.Union[
    str, float, int, bool, t.List, t.Tuple, datetime.date, datetime.datetime, None
]
ColumnOrName = t.Union[Column, str]
ColumnOrLiteral = t.Union[
    Column, str, float, int, bool, t.List, t.Tuple, datetime.date, datetime.datetime, dict
]
SchemaInput = t.Union[str, t.List[str], t.Tuple[str, ...], StructType, t.Dict[str, t.Optional[str]]]
OutputExpressionContainer = t.Union[exp.Select, exp.Create, exp.Insert]
StorageLevel = str
PathOrPaths = t.Union[str, t.List[str]]
OptionalPrimitiveType = t.Optional[PrimitiveType]
DataTypeOrString = t.Union[DataType, str]


class UserDefinedFunctionLike(t.Protocol):
    func: t.Callable[..., t.Any]
    evalType: int
    deterministic: bool

    @property
    def returnType(self) -> DataType: ...

    def __call__(self, *args: ColumnOrName) -> Column: ...

    def asNondeterministic(self) -> UserDefinedFunctionLike: ...
