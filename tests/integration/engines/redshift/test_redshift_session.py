from __future__ import annotations

import os
import typing as t

import pytest
from sqlglot import exp

from sqlframe.redshift.session import RedshiftSession

if t.TYPE_CHECKING:
    from redshift_connector.core import Connection as RedshiftConnection

pytest_plugins = ["tests.common_fixtures"]
pytestmark = [
    pytest.mark.redshift,
    pytest.mark.xdist_group("redshift_tests"),
]


@pytest.fixture
def cleanup_connector() -> t.Iterator[RedshiftConnection]:
    from redshift_connector import connect

    conn = connect(
        user=os.environ["SQLFRAME_REDSHIFT_USER"],
        password=os.environ["SQLFRAME_REDSHIFT_PASSWORD"],
        database=os.environ["SQLFRAME_REDSHIFT_DATABASE"],
        host=os.environ["SQLFRAME_REDSHIFT_HOST"],
        port=int(os.environ["SQLFRAME_REDSHIFT_PORT"]),
    )
    conn.autocommit = True
    conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS db1")
    conn.cursor().execute("DROP TABLE IF EXISTS db1.test_table")
    conn.cursor().execute("CREATE TABLE IF NOT EXISTS db1.test_table (cola INT, colb VARCHAR(MAX))")

    yield conn
    conn.cursor().execute("DROP TABLE IF EXISTS db1.test_table")


def test_session_from_config(cleanup_connector: RedshiftConnection):
    session = RedshiftSession.builder.config("sqlframe.conn", cleanup_connector).getOrCreate()
    columns = session.catalog.get_columns("dev.db1.test_table")
    assert columns == {
        "cola": exp.DataType.build("INT", dialect=session.output_dialect),
        "colb": exp.DataType.build("STRING", dialect=session.output_dialect),
    }
    assert session.execution_dialect_name == "redshift"
