from __future__ import annotations

import typing as t

import pytest
from sqlglot import Dialect, exp, parse_one

if t.TYPE_CHECKING:
    from sqlframe.base.session import _BaseSession

pytest_plugins = ["tests.integration.fixtures"]


@pytest.fixture
def cleanup_session(get_session: t.Callable[[], _BaseSession]) -> t.Iterator[_BaseSession]:
    session = get_session()
    yield session
    session._execute("DROP TABLE IF EXISTS test_table")


def test_session(cleanup_session: _BaseSession):
    session = cleanup_session
    session._execute("DROP TABLE IF EXISTS test_table")
    sql = "CREATE TABLE test_table (cola INT, colb STRING, `col with space` STRING)"
    if session.execution_dialect == Dialect.get_or_raise("databricks"):
        sql += " TBLPROPERTIES('delta.columnMapping.mode' = 'name');"
    session._collect(
        parse_one(
            sql,
            dialect="spark",
        )
    )
    columns = session.catalog.get_columns("test_table")
    if session.execution_dialect == Dialect.get_or_raise("bigquery"):
        cola_type = exp.DataType.build("INT64", dialect=session.execution_dialect)
    elif session.execution_dialect == Dialect.get_or_raise("snowflake"):
        cola_type = exp.DataType.build("DECIMAL", dialect=session.execution_dialect)
    else:
        cola_type = exp.DataType.build("INT", dialect=session.execution_dialect)
    assert columns == {
        "cola": cola_type,
        "colb": exp.DataType.build("STRING", dialect=session.output_dialect),
        "`col with space`": exp.DataType.build("STRING", dialect=session.output_dialect),
    }
