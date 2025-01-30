from __future__ import annotations

import os
import typing as t

import pytest
from sqlglot import exp

from sqlframe.snowflake.session import SnowflakeSession

if t.TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection

pytest_plugins = ["tests.common_fixtures"]
pytestmark = [
    pytest.mark.snowflake,
    pytest.mark.xdist_group("snowflake_tests"),
]


@pytest.fixture
def cleanup_connector() -> t.Iterator[SnowflakeConnection]:
    from snowflake.connector import connect

    conn = connect(
        account=os.environ["SQLFRAME_SNOWFLAKE_ACCOUNT"],
        user=os.environ["SQLFRAME_SNOWFLAKE_USER"],
        password=os.environ["SQLFRAME_SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SQLFRAME_SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SQLFRAME_SNOWFLAKE_DATABASE"],
        schema=os.environ["SQLFRAME_SNOWFLAKE_SCHEMA"],
    )
    conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS db1")
    conn.cursor().execute("DROP TABLE IF EXISTS db1.test_table")
    conn.cursor().execute("CREATE TABLE IF NOT EXISTS db1.test_table (cola INT, colb STRING)")

    yield conn
    conn.cursor().execute("DROP TABLE IF EXISTS db1.test_table")


def test_session_from_config(cleanup_connector: SnowflakeConnection):
    session = SnowflakeSession.builder.config("sqlframe.conn", cleanup_connector).getOrCreate()
    columns = session.catalog.get_columns("sqlframe.db1.test_table")
    assert columns == {
        "cola": exp.DataType.build("DECIMAL(38, 0)", dialect=session.output_dialect),
        "colb": exp.DataType.build("TEXT", dialect=session.output_dialect),
    }
    assert session.execution_dialect_name == "snowflake"
