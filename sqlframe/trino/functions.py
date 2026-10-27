from __future__ import annotations

import inspect as _inspect
import typing as _t

from sqlglot import exp as _exp

from sqlframe.base import functions as _base
from sqlframe.base.column import Column as _Column
from sqlframe.base.decorators import func_metadata as _meta
from sqlframe.base.exceptions import UnsupportedOperationError as _UnsupportedOperationError
from sqlframe.base.functions import *  # noqa

if _t.TYPE_CHECKING:
    from sqlframe.base._typing import ColumnOrName as _ColumnOrName
    from sqlframe.base.types import ArrayType as _ArrayType
    from sqlframe.base.types import MapType as _MapType
    from sqlframe.base.types import StructType as _StructType


def _lambda_from_func(func: _t.Callable) -> _exp.Lambda:
    # Mark the actual parameter identifiers before invoking the callable, so
    # captured columns and nested lambda scopes cannot be mistaken for them.
    parameters = [
        _Column(_exp.to_identifier(name, quoted=False))
        for name in _inspect.signature(func).parameters
    ]
    marker = "trino_lambda_parameter"
    for parameter in parameters:
        parameter.column_expression.meta[marker] = id(parameter)
    body = func(*parameters).column_expression
    captured_names = {column.name for column in body.find_all(_exp.Column)}
    used_names = captured_names | {parameter.column_expression.name for parameter in parameters}
    names = {}
    variables = []
    for parameter in parameters:
        name = parameter.column_expression.name
        if name in captured_names:
            suffix = 1
            while f"{name}_{suffix}" in used_names:
                suffix += 1
            name = f"{name}_{suffix}"
        used_names.add(name)
        names[id(parameter)] = name
        variables.append(_exp.to_identifier(name, quoted=False))
    for identifier in body.find_all(_exp.Identifier):
        if identifier.meta.get(marker) in names:
            identifier.set("this", names[identifier.meta.pop(marker)])
    return _exp.Lambda(this=body, expressions=variables)


def _without_nulls(col: _Column) -> _Column:
    return _Column(
        _exp.ArrayFilter(
            this=col.column_expression,
            expression=_lambda_from_func(lambda x: x.isNotNull()),
        )
    )


@_meta()
def array_sort(
    col: _ColumnOrName,
    comparator: _t.Optional[_t.Callable[[_Column, _Column], _Column]] = None,
) -> _Column:
    return _Column(
        _exp.ArraySort(
            this=_Column.ensure_col(col).column_expression,
            expression=_lambda_from_func(comparator) if comparator is not None else None,
        )
    )


@_meta()
def sort_array(col: _ColumnOrName, asc: _t.Optional[bool] = None) -> _Column:
    direction = -1 if asc is not False else 1

    def compare(left: _Column, right: _Column) -> _Column:
        return (
            _base.when(left.isNull() & right.isNull(), 0)
            .when(left.isNull(), direction)
            .when(right.isNull(), -direction)
            .when(left < right, direction)
            .when(left > right, -direction)
            .otherwise(0)
        )

    return _Column(array_sort(col, compare).column_expression)


@_meta()
def collect_list(col: _ColumnOrName) -> _Column:
    # Filter the resulting array so the same expression also supports windows.
    aggregate = _base.collect_list(col)
    return _base.coalesce(_without_nulls(aggregate), _base.array([])).alias(
        aggregate.expression.alias
    )


@_meta()
def collect_set(col: _ColumnOrName) -> _Column:
    aggregate = _base.collect_set(col)
    return _base.coalesce(_without_nulls(aggregate), _base.array([])).alias(
        aggregate.expression.alias
    )


@_meta()
def greatest(*cols: _ColumnOrName) -> _Column:
    result = _base.array_max(_without_nulls(_base.array(*cols)))
    return result.alias(_base.greatest(*cols).expression.alias)


@_meta()
def least(*cols: _ColumnOrName) -> _Column:
    result = _base.array_min(_without_nulls(_base.array(*cols)))
    return result.alias(_base.least(*cols).expression.alias)


@_meta()
def regexp_extract(str: _ColumnOrName, pattern: str, idx: _t.Optional[int] = None) -> _Column:
    source = _Column.ensure_col(str)
    result = _base.regexp_extract(source, pattern, 1 if idx is None else idx)
    return _base.when(source.isNull(), None).otherwise(_base.coalesce(result, _base.lit("")))


@_meta()
def split(str: _ColumnOrName, pattern: str, limit: _t.Optional[int] = None) -> _Column:
    if limit is not None and limit > 0:
        raise _UnsupportedOperationError("split with a positive limit is not supported by Trino")
    return _base.split(str, pattern)


@_meta()
def to_timestamp(col: _ColumnOrName, format: _t.Optional[str] = None) -> _Column:
    if format is None:
        raise _UnsupportedOperationError(
            "to_timestamp without a format is not supported by Trino; "
            "use an explicit format or cast to a native Trino timestamp type"
        )
    return _base.to_timestamp(col, format)


@_meta()
def get_json_object(col: _ColumnOrName, path: str) -> _Column:
    raise _UnsupportedOperationError(
        "get_json_object is not supported by Trino: JSON_EXTRACT returns JSON, "
        "not Spark's unquoted string and permissive parsing semantics"
    )


@_meta()
def from_json(
    col: _ColumnOrName,
    schema: _t.Union[_ArrayType, _MapType, _StructType, _Column, str],
    options: _t.Optional[_t.Dict[str, str]] = None,
) -> _Column:
    raise _UnsupportedOperationError("from_json is not supported by Trino")


@_meta()
def to_json(col: _ColumnOrName, options: _t.Optional[_t.Dict[str, str]] = None) -> _Column:
    raise _UnsupportedOperationError(
        "to_json is not supported by Trino: JSON casts do not implement Spark's "
        "null field omission or JSON options"
    )


@_meta()
def json_tuple(col: _ColumnOrName, *fields: str) -> _Column:
    raise _UnsupportedOperationError("json_tuple is not supported by Trino")
