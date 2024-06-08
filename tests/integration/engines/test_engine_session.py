from __future__ import annotations

import typing as t

import pytest
from sqlglot import exp, parse_one

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
    session._execute(parse_one("CREATE TABLE test_table (cola INT, colb STRING)"))
    columns = session.catalog.get_columns("test_table")
    if session.output_dialect == "bigquery":
        cola_type = exp.DataType.build("INT64", dialect=session.output_dialect)
    elif session.output_dialect == "snowflake":
        cola_type = exp.DataType.build("DECIMAL", dialect=session.output_dialect)
    else:
        cola_type = exp.DataType.build("INT", dialect=session.output_dialect)
    if session.output_dialect in ("bigquery", "spark"):
        cola_name = "`cola`"
        colb_name = "`colb`"
    elif session.output_dialect == "snowflake":
        cola_name = '"COLA"'
        colb_name = '"COLB"'
    else:
        cola_name = '"cola"'
        colb_name = '"colb"'
    assert columns == {
        cola_name: cola_type,
        colb_name: exp.DataType.build("VARCHAR", dialect=session.output_dialect)
        if session.output_dialect == "redshift"
        else exp.DataType.build("STRING", dialect=session.output_dialect),
    }
    session._execute("DROP TABLE IF EXISTS test_table")
