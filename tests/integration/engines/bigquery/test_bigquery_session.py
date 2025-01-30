import pytest
from sqlglot import exp

from sqlframe.bigquery.session import BigQuerySession

pytestmark = [
    pytest.mark.bigquery,
    pytest.mark.xdist_group("bigquery_tests"),
]


def test_session_from_config():
    from google.cloud.bigquery.dbapi import connect

    conn = connect()
    conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS db1")
    conn.cursor().execute("CREATE TABLE IF NOT EXISTS db1.test_table (cola INT, colb STRING)")
    session = BigQuerySession.builder.config("default_dataset", "sqlframe.db1").getOrCreate()
    columns = session.catalog.get_columns("db1.test_table")
    assert columns == {"cola": exp.DataType.build("BIGINT"), "colb": exp.DataType.build("TEXT")}
    assert session.execution_dialect_name == "bigquery"
