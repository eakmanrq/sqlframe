from __future__ import annotations

import os
import typing as t

import pytest
from sqlglot import exp

from sqlframe.databricks.session import DatabricksSession

if t.TYPE_CHECKING:
    from databricks.sql import Connection as DatabricksConnection

pytest_plugins = ["tests.common_fixtures"]
pytestmark = [
    pytest.mark.databricks,
    pytest.mark.xdist_group("databricks_tests"),
]


@pytest.fixture
def cleanup_connector() -> t.Iterator[DatabricksConnection]:
    from databricks.sql import connect

    conn = connect(
        server_hostname=os.environ["SQLFRAME_DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["SQLFRAME_DATABRICKS_HTTP_PATH"],
        access_token=os.environ["SQLFRAME_DATABRICKS_ACCESS_TOKEN"],
        auth_type="access_token",
        catalog=os.environ["SQLFRAME_DATABRICKS_CATALOG"],
        schema=os.environ["SQLFRAME_DATABRICKS_SCHEMA"],
    )
    conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS db1")
    conn.cursor().execute("DROP TABLE IF EXISTS db1.test_table")
    conn.cursor().execute("CREATE TABLE IF NOT EXISTS db1.test_table (cola INT, colb STRING)")

    yield conn
    conn.cursor().execute("DROP TABLE IF EXISTS db1.test_table")


def test_session_from_config(cleanup_connector: DatabricksConnection):
    session = DatabricksSession.builder.config("sqlframe.conn", cleanup_connector).getOrCreate()
    columns = session.catalog.get_columns("sqlframe.db1.test_table")
    assert columns == {
        "cola": exp.DataType.build("INT", dialect=session.output_dialect),
        "colb": exp.DataType.build("STRING", dialect=session.output_dialect),
    }
    assert session.execution_dialect_name == "databricks"
