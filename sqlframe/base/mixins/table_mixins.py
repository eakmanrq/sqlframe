import typing as t

import sqlglot
from sqlglot import exp

from sqlframe.base.column import Column

from sqlframe.base.table import _BaseTable, DF, WhenMatched, WhenNotMatched, WhenNotMatchedBySource


class MergeSupportMixin(_BaseTable, t.Generic[DF]):
    def merge(
        self,
        source: DF,
        condition: t.Union[Column, str, bool],
        clauses: t.Iterable[t.Union[WhenMatched, WhenNotMatched, WhenNotMatchedBySource]],
    ):
        join_expression = self._add_ctes_to_expression(self.expression, source.expression.ctes)
        if isinstance(condition, str):
            condition = self._ensure_and_normalize_cols(
                sqlglot.parse_one(condition, dialect=self.session.input_dialect), join_expression
            )
        else:
            condition = self._ensure_and_normalize_cols(condition, join_expression)

        target_expr = self.expression.args["from"].this
        target_expr.set("alias", exp.TableAlias(this=exp.to_identifier(self.expression.alias_or_name)))

        source_expr = exp.Subquery(this=source.expression, alias=exp.TableAlias(this=exp.to_identifier(source.expression.alias_or_name)))

        merge_expr = exp.Merge(
            this=target_expr,
            using=source_expr,
            on=condition,
        )

        print(repr(merge_expr))
