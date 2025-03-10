# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import sys
import typing as t

from sqlglot import expressions as exp
from sqlglot.helper import flatten

from sqlframe.base import functions as F

if t.TYPE_CHECKING:
    from sqlframe.base._typing import ColumnOrName


class Window:
    _JAVA_MIN_LONG = -(1 << 63)  # -9223372036854775808
    _JAVA_MAX_LONG = (1 << 63) - 1  # 9223372036854775807
    _PRECEDING_THRESHOLD = max(-sys.maxsize, _JAVA_MIN_LONG)
    _FOLLOWING_THRESHOLD = min(sys.maxsize, _JAVA_MAX_LONG)

    unboundedPreceding: int = _JAVA_MIN_LONG

    unboundedFollowing: int = _JAVA_MAX_LONG

    currentRow: int = 0

    @classmethod
    def partitionBy(cls, *cols: t.Union[ColumnOrName, t.Collection[ColumnOrName]]) -> WindowSpec:
        return WindowSpec().partitionBy(*cols)

    @classmethod
    def orderBy(cls, *cols: t.Union[ColumnOrName, t.Collection[ColumnOrName]]) -> WindowSpec:
        return WindowSpec().orderBy(*cols)

    @classmethod
    def rowsBetween(cls, start: int, end: int) -> WindowSpec:
        return WindowSpec().rowsBetween(start, end)

    @classmethod
    def rangeBetween(cls, start: int, end: int) -> WindowSpec:
        return WindowSpec().rangeBetween(start, end)


class WindowSpec:
    def __init__(self, expression: exp.Expression = exp.Window()):
        self.expression = expression

    def copy(self):
        return WindowSpec(self.expression.copy())

    def sql(self, **kwargs) -> str:
        from sqlframe.base.session import _BaseSession

        return self.expression.sql(dialect=_BaseSession().input_dialect, **kwargs)

    def partitionBy(self, *cols: t.Union[ColumnOrName, t.Collection[ColumnOrName]]) -> WindowSpec:
        from sqlframe.base.column import Column

        cols = flatten(cols) if isinstance(cols[0], t.Collection) else cols  # type: ignore
        expressions = [Column.ensure_col(x).expression for x in cols]  # type: ignore
        window_spec = self.copy()
        partition_by_expressions = window_spec.expression.args.get("partition_by", [])
        partition_by_expressions.extend(expressions)
        window_spec.expression.set("partition_by", partition_by_expressions)
        return window_spec

    def orderBy(self, *cols: t.Union[ColumnOrName, t.Collection[ColumnOrName]]) -> WindowSpec:
        from sqlframe.base.column import Column

        cols = flatten(cols) if isinstance(cols[0], t.Collection) else cols  # type: ignore
        expressions = [Column.ensure_col(x).expression for x in cols]  # type: ignore
        window_spec = self.copy()
        if window_spec.expression.args.get("order") is None:
            window_spec.expression.set("order", exp.Order(expressions=[]))
        order_by = window_spec.expression.args["order"].expressions
        order_by.extend(expressions)
        window_spec.expression.args["order"].set("expressions", order_by)
        return window_spec

    def _calc_start_end(
        self, start: int, end: int
    ) -> t.Dict[str, t.Optional[t.Union[str, exp.Expression]]]:
        def get_value_and_side(x: int) -> t.Tuple[t.Union[str, exp.Expression], t.Optional[str]]:
            if x == Window.currentRow:
                return "CURRENT ROW", None
            if x < 0:
                side = "PRECEDING"
                value = "UNBOUNDED" if x <= Window.unboundedPreceding else F.lit(abs(x)).expression
                return value, side
            else:
                side = "FOLLOWING"
                value = "UNBOUNDED" if x >= Window.unboundedFollowing else F.lit(x).expression
                return value, side

        start, start_side = get_value_and_side(start)  # type: ignore
        end, end_side = get_value_and_side(end)  # type: ignore
        return {
            "start": start,  # type: ignore
            "start_side": start_side,
            "end": end,  # type: ignore
            "end_side": end_side,
        }

    def rowsBetween(self, start: int, end: int) -> WindowSpec:
        window_spec = self.copy()
        spec = self._calc_start_end(start, end)
        spec["kind"] = "ROWS"
        window_spec.expression.set(
            "spec",
            exp.WindowSpec(
                **{**window_spec.expression.args.get("spec", exp.WindowSpec()).args, **spec}
            ),
        )
        return window_spec

    def rangeBetween(self, start: int, end: int) -> WindowSpec:
        window_spec = self.copy()
        spec = self._calc_start_end(start, end)
        spec["kind"] = "RANGE"
        window_spec.expression.set(
            "spec",
            exp.WindowSpec(
                **{**window_spec.expression.args.get("spec", exp.WindowSpec()).args, **spec}
            ),
        )
        return window_spec
