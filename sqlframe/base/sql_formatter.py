from __future__ import annotations

import string
import typing as t
import uuid

import sqlglot
from sqlglot import Dialect, exp
from sqlglot.tokens import TokenType

from sqlframe.base.column import Column
from sqlframe.base.dataframe import BaseDataFrame

if t.TYPE_CHECKING:
    from sqlframe.base.session import _BaseSession


SQLArgs = t.Union[t.Dict[str, t.Any], t.List[t.Any]]


def _literal_expression(value: t.Any) -> exp.Expr:
    return (value if isinstance(value, Column) else Column._lit(value)).column_expression


class SQLStringFormatter(string.Formatter):
    def __init__(self, session: _BaseSession, dialect: Dialect):
        self.session = session
        self.dialect = dialect
        self.views: t.List[t.Tuple[BaseDataFrame, str]] = []
        self.schema = session.catalog._schema

    def __enter__(self):
        self.session.catalog._schema = self.schema.copy()
        return self

    def __exit__(self, *exc):
        for _, name in self.views:
            self.session.temp_views.pop(name, None)
        self.session.catalog._schema = self.schema

    def get_field(self, field_name, args, kwargs):
        value, first = super().get_field(field_name, args, kwargs)
        if isinstance(value, BaseDataFrame):
            for df, name in self.views:
                if df is value:
                    return name, first
            name = f"_sqlframe_{uuid.uuid4().hex}"
            self.views.append((value, name))
            value.createOrReplaceTempView(name)
            return name, first
        if isinstance(value, Column):
            expression = value.column_expression.copy()
            for column in expression.find_all(exp.Column):
                if column.table in self.session.known_branch_ids:
                    column.set("table", None)
            return expression.sql(dialect=self.dialect), first
        return _literal_expression(value).sql(dialect=self.dialect), first


def bind_parameters(query: t.Union[str, exp.Expr], args: SQLArgs, dialect: Dialect) -> exp.Expr:
    expression = sqlglot.parse_one(query, read=dialect) if isinstance(query, str) else query.copy()
    placeholders = list(expression.find_all(exp.Placeholder))
    named = [node for node in placeholders if node.this is not None]
    positional = [node for node in placeholders if node.this is None]
    if named and positional:
        raise ValueError("Cannot mix named and positional SQL parameters")
    if isinstance(args, dict):
        if positional:
            raise ValueError("Positional SQL parameters require a list of args")
        for node in named:
            if node.name not in args:
                raise ValueError(f"Missing SQL parameter: {node.name}")
            node.replace(_literal_expression(args[node.name]).copy())
        return expression
    if not isinstance(args, list):
        raise TypeError("SQL args must be a dictionary or a list")
    if named:
        raise ValueError("Named SQL parameters require a dictionary of args")
    sql = query if isinstance(query, str) else query.sql(dialect=dialect)
    tokens = [token for token in dialect.tokenize(sql) if token.token_type == TokenType.PLACEHOLDER]
    if len(args) < len(tokens):
        raise ValueError(
            f"Missing positional SQL parameter: expected {len(tokens)}, got {len(args)}"
        )
    # Token offsets preserve SQL text order, including parameters inside CTEs and subqueries.
    for token, value in reversed(list(zip(tokens, args))):
        literal = _literal_expression(value).sql(dialect=dialect)
        sql = sql[: token.start] + literal + sql[token.end + 1 :]
    return sqlglot.parse_one(sql, read=dialect)
