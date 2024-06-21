# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import datetime
import math
import typing as t

import sqlglot
from sqlglot import expressions as exp
from sqlglot.helper import flatten, is_iterable
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sqlframe.base.decorators import normalize
from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.base.types import DataType
from sqlframe.base.util import get_func_from_session, quote_preserving_alias_or_name

if t.TYPE_CHECKING:
    from sqlframe.base._typing import ColumnOrLiteral, ColumnOrName
    from sqlframe.base.window import WindowSpec


class Column:
    def __init__(self, expression: t.Optional[t.Union[ColumnOrLiteral, exp.Expression]]):
        from sqlframe.base.session import _BaseSession

        if isinstance(expression, Column):
            expression = expression.expression  # type: ignore
        elif expression is None or not isinstance(expression, (str, exp.Expression)):
            expression = self._lit(expression).expression  # type: ignore
        elif not isinstance(expression, exp.Column):
            expression = sqlglot.maybe_parse(
                expression, dialect=_BaseSession().input_dialect
            ).transform(_BaseSession().input_dialect.normalize_identifier, copy=False)
        if expression is None:
            raise ValueError(f"Could not parse {expression}")

        self.expression: exp.Expression = expression  # type: ignore

    def __repr__(self):
        return repr(self.expression)

    def __hash__(self):
        return hash(self.expression)

    def __eq__(self, other: ColumnOrLiteral) -> Column:  # type: ignore
        return self.binary_op(exp.EQ, other)

    def __ne__(self, other: ColumnOrLiteral) -> Column:  # type: ignore
        return self.binary_op(exp.NEQ, other)

    def __gt__(self, other: ColumnOrLiteral) -> Column:
        return self.binary_op(exp.GT, other)

    def __ge__(self, other: ColumnOrLiteral) -> Column:
        return self.binary_op(exp.GTE, other)

    def __lt__(self, other: ColumnOrLiteral) -> Column:
        return self.binary_op(exp.LT, other)

    def __le__(self, other: ColumnOrLiteral) -> Column:
        return self.binary_op(exp.LTE, other)

    def __and__(self, other: ColumnOrLiteral) -> Column:
        return self.binary_op(exp.And, other)

    def __or__(self, other: ColumnOrLiteral) -> Column:
        return self.binary_op(exp.Or, other)

    def __mod__(self, other: ColumnOrLiteral) -> Column:
        return self.binary_op(exp.Mod, other, paren=True)

    def __add__(self, other: ColumnOrLiteral) -> Column:
        return self.binary_op(exp.Add, other, paren=True)

    def __sub__(self, other: ColumnOrLiteral) -> Column:
        return self.binary_op(exp.Sub, other, paren=True)

    def __mul__(self, other: ColumnOrLiteral) -> Column:
        return self.binary_op(exp.Mul, other, paren=True)

    def __truediv__(self, other: ColumnOrLiteral) -> Column:
        return self.binary_op(exp.Div, other, paren=True)

    def __div__(self, other: ColumnOrLiteral) -> Column:
        return self.binary_op(exp.Div, other, paren=True)

    def __neg__(self) -> Column:
        return self.unary_op(exp.Neg)

    def __radd__(self, other: ColumnOrLiteral) -> Column:
        return self.inverse_binary_op(exp.Add, other, paren=True)

    def __rsub__(self, other: ColumnOrLiteral) -> Column:
        return self.inverse_binary_op(exp.Sub, other, paren=True)

    def __rmul__(self, other: ColumnOrLiteral) -> Column:
        return self.inverse_binary_op(exp.Mul, other, paren=True)

    def __rdiv__(self, other: ColumnOrLiteral) -> Column:
        return self.inverse_binary_op(exp.Div, other, paren=True)

    def __rtruediv__(self, other: ColumnOrLiteral) -> Column:
        return self.inverse_binary_op(exp.Div, other, paren=True)

    def __rmod__(self, other: ColumnOrLiteral) -> Column:
        return self.inverse_binary_op(exp.Mod, other, paren=True)

    def __pow__(self, power: ColumnOrLiteral, modulo=None):
        return Column(exp.Pow(this=self.expression, expression=Column(power).expression))

    def __rpow__(self, power: ColumnOrLiteral):
        return Column(exp.Pow(this=Column(power).expression, expression=self.expression))

    def __invert__(self):
        return self.unary_op(exp.Not)

    def __rand__(self, other: ColumnOrLiteral) -> Column:
        return self.inverse_binary_op(exp.And, other)

    def __ror__(self, other: ColumnOrLiteral) -> Column:
        return self.inverse_binary_op(exp.Or, other)

    def __call__(self, *args, **kwargs):
        raise UnsupportedOperationError(
            "Tried to call a column which is unexpected. Did you mean to call a method on a DataFrame? If so, make sure the method is typed correctly and is supported. If not, please open an issue requesting support: https://github.com/eakmanrq/sqlframe/issues"
        )

    @classmethod
    def ensure_col(cls, value: t.Optional[t.Union[ColumnOrName, exp.Expression]]) -> Column:
        col = get_func_from_session("col")

        return col(value)

    @classmethod
    def ensure_cols(cls, args: t.List[t.Union[ColumnOrName, exp.Expression]]) -> t.List[Column]:
        return [cls.ensure_col(x) if not isinstance(x, Column) else x for x in args]

    @classmethod
    def _lit(cls, value: ColumnOrLiteral) -> Column:
        from sqlframe.base.session import _BaseSession
        from sqlframe.base.types import Row

        if isinstance(value, Row):
            columns = [
                exp.PropertyEQ(
                    this=exp.to_identifier(k).transform(
                        _BaseSession().input_dialect.normalize_identifier, copy=False
                    ),
                    expression=cls._lit(v).expression,
                )
                for k, v in value.asDict().items()
            ]
            return cls(exp.Struct(expressions=columns))
        if isinstance(value, (list, set)):
            return cls(exp.Array(expressions=[cls._lit(x).expression for x in value]))
        if isinstance(value, tuple):
            return cls(exp.Tuple(expressions=[cls._lit(x).expression for x in value]))
        if isinstance(value, dict):
            return cls(
                exp.VarMap(
                    keys=exp.Array(expressions=[cls._lit(k).expression for k in value.keys()]),
                    values=exp.Array(expressions=[cls._lit(v).expression for v in value.values()]),
                )
            )
        elif value is not None and isinstance(value, float) and math.isnan(value):
            return cls(exp.cast(exp.Literal.string("NaN"), exp.DataType.build("float")))
        elif isinstance(value, datetime.datetime):
            if value.tzinfo is None:
                value = value.isoformat(sep=" ")
                return cls(exp.cast(exp.Literal.string(value), exp.DataType.Type.TIMESTAMP))
            else:
                value = value.astimezone(datetime.timezone.utc).isoformat(sep=" ")
                return cls(exp.cast(exp.Literal.string(value), exp.DataType.Type.TIMESTAMPTZ))
        return cls(exp.convert(value))

    @classmethod
    def invoke_anonymous_function(
        cls, column: t.Optional[ColumnOrName], func_name: str, *args: t.Optional[ColumnOrName]
    ) -> Column:
        columns = [] if column is None else [cls.ensure_col(column)]
        column_args = [cls.ensure_col(arg) for arg in args]
        expressions = [x.expression for x in columns + column_args]
        new_expression = exp.Anonymous(this=func_name.upper(), expressions=expressions)
        return Column(new_expression)

    @classmethod
    def invoke_expression_over_column(
        cls, column: t.Optional[ColumnOrName], callable_expression: t.Callable, **kwargs
    ) -> Column:
        ensured_column = None if column is None else cls.ensure_col(column)
        ensure_expression_values = {
            k: (
                [cls.ensure_col(x).expression for x in v]
                if is_iterable(v)
                else cls.ensure_col(v).expression
            )
            for k, v in kwargs.items()
            if v is not None
        }
        new_expression = (
            callable_expression(**ensure_expression_values)
            if ensured_column is None
            else callable_expression(
                this=ensured_column.column_expression, **ensure_expression_values
            )
        )
        return Column(new_expression)

    def binary_op(
        self, klass: t.Callable, other: ColumnOrLiteral, paren: bool = False, **kwargs
    ) -> Column:
        op = klass(
            this=self.column_expression, expression=Column(other).column_expression, **kwargs
        )
        if paren:
            return Column(exp.Paren(this=op))
        return Column(op)

    def inverse_binary_op(
        self, klass: t.Callable, other: ColumnOrLiteral, paren: bool = False, **kwargs
    ) -> Column:
        op = klass(
            this=Column(other).column_expression, expression=self.column_expression, **kwargs
        )
        if paren:
            return Column(exp.Paren(this=op))
        return Column(op)

    def unary_op(self, klass: t.Callable, **kwargs) -> Column:
        return Column(klass(this=exp.Paren(this=self.column_expression), **kwargs))

    @property
    def is_alias(self):
        return isinstance(self.expression, exp.Alias)

    @property
    def is_column(self):
        return isinstance(self.expression, exp.Column)

    @property
    def column_expression(self) -> t.Union[exp.Column, exp.Literal]:
        return self.expression.unalias()

    @property
    def alias_or_name(self) -> str:
        return quote_preserving_alias_or_name(self.expression)  # type: ignore

    @property
    def column_alias_or_name(self) -> str:
        return quote_preserving_alias_or_name(self.column_expression)  # type: ignore

    @classmethod
    def ensure_literal(cls, value) -> Column:
        from sqlframe.base.functions import lit

        if isinstance(value, cls):
            value = value.expression
        if not isinstance(value, exp.Literal):
            return lit(value)
        return Column(value)

    @property
    def dtype(self) -> t.Optional[DataType]:
        expression = self.expression.unalias()
        if isinstance(expression, exp.Cast):
            return expression.args.get("to")
        return None

    def copy(self) -> Column:
        return Column(self.expression.copy())

    def set_table_name(self, table_name: str, copy=False) -> Column:
        expression = self.expression.copy() if copy else self.expression
        expression.set("table", exp.to_identifier(table_name))
        return Column(expression)

    def sql(self, **kwargs) -> str:
        from sqlframe.base.session import _BaseSession

        return self.expression.sql(**{"dialect": _BaseSession().input_dialect, **kwargs})

    def alias(self, name: str) -> Column:
        from sqlframe.base.session import _BaseSession

        dialect = _BaseSession().input_dialect
        alias: exp.Expression = normalize_identifiers(
            exp.parse_identifier(name, dialect=dialect), dialect=dialect
        )
        new_expression = exp.Alias(
            this=self.column_expression,
            alias=alias.this if isinstance(alias, exp.Column) else alias,
        )
        return Column(new_expression)

    def asc(self) -> Column:
        new_expression = exp.Ordered(this=self.column_expression, desc=False, nulls_first=True)
        return Column(new_expression)

    def desc(self) -> Column:
        new_expression = exp.Ordered(this=self.column_expression, desc=True, nulls_first=False)
        return Column(new_expression)

    asc_nulls_first = asc

    def asc_nulls_last(self) -> Column:
        new_expression = exp.Ordered(this=self.column_expression, desc=False, nulls_first=False)
        return Column(new_expression)

    def desc_nulls_first(self) -> Column:
        new_expression = exp.Ordered(this=self.column_expression, desc=True, nulls_first=True)
        return Column(new_expression)

    desc_nulls_last = desc

    def when(self, condition: Column, value: t.Any) -> Column:
        from sqlframe.base.functions import when

        column_with_if = when(condition, value)
        if not isinstance(self.expression, exp.Case):
            return column_with_if
        new_column = self.copy()
        new_column.expression.args["ifs"].extend(column_with_if.expression.args["ifs"])
        return new_column

    def otherwise(self, value: t.Any) -> Column:
        from sqlframe.base.functions import lit

        true_value = value if isinstance(value, Column) else lit(value)
        new_column = self.copy()
        new_column.expression.set("default", true_value.column_expression)
        return new_column

    def isNull(self) -> Column:
        new_expression = exp.Is(this=self.column_expression, expression=exp.Null())
        return Column(new_expression)

    def isNotNull(self) -> Column:
        new_expression = exp.Not(this=exp.Is(this=self.column_expression, expression=exp.Null()))
        return Column(new_expression)

    def cast(self, dataType: t.Union[str, DataType]) -> Column:
        from sqlframe.base.session import _BaseSession

        if isinstance(dataType, DataType):
            dataType = dataType.simpleString()
        return Column(
            exp.cast(self.column_expression, dataType, dialect=_BaseSession().input_dialect)
        )

    def startswith(self, value: t.Union[str, Column]) -> Column:
        value = self._lit(value) if not isinstance(value, Column) else value
        return self.invoke_expression_over_column(self, exp.StartsWith, expression=value.expression)

    def endswith(self, value: t.Union[str, Column]) -> Column:
        value = self._lit(value) if not isinstance(value, Column) else value
        return self.invoke_anonymous_function(self, "ENDSWITH", value)

    def rlike(self, regexp: str) -> Column:
        return self.invoke_expression_over_column(
            column=self, callable_expression=exp.RegexpLike, expression=self._lit(regexp).expression
        )

    def like(self, other: str):
        return self.invoke_expression_over_column(
            self, exp.Like, expression=self._lit(other).expression
        )

    def ilike(self, other: str):
        return self.invoke_expression_over_column(
            self, exp.ILike, expression=self._lit(other).expression
        )

    def substr(self, startPos: t.Union[int, Column], length: t.Union[int, Column]) -> Column:
        startPos = self._lit(startPos) if not isinstance(startPos, Column) else startPos
        length = self._lit(length) if not isinstance(length, Column) else length
        return Column.invoke_expression_over_column(
            self, exp.Substring, start=startPos.expression, length=length.expression
        )

    def isin(self, *cols: t.Union[ColumnOrLiteral, t.Iterable[ColumnOrLiteral]]):
        columns = flatten(cols) if isinstance(cols[0], (list, set, tuple)) else cols  # type: ignore
        expressions = [self._lit(x).expression for x in columns]
        return Column.invoke_expression_over_column(self, exp.In, expressions=expressions)  # type: ignore

    def between(
        self,
        lowerBound: t.Union[ColumnOrLiteral],
        upperBound: t.Union[ColumnOrLiteral],
    ) -> Column:
        lower_bound_exp = (
            self._lit(lowerBound) if not isinstance(lowerBound, Column) else lowerBound
        )
        upper_bound_exp = (
            self._lit(upperBound) if not isinstance(upperBound, Column) else upperBound
        )
        return Column(
            exp.Between(
                this=self.column_expression,
                low=lower_bound_exp.expression,
                high=upper_bound_exp.expression,
            )
        )

    def over(self, window: WindowSpec) -> Column:
        window_expression = window.expression.copy()
        window_expression.set("this", self.column_expression)
        return Column(window_expression)
