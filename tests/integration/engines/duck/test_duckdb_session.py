import pytest
from duckdb import ConnectionException
from sqlglot import exp

from sqlframe.base.types import Row
from sqlframe.duckdb.session import DuckDBSession

pytest_plugins = ["tests.common_fixtures"]


def test_session_from_config():
    import duckdb

    conn = duckdb.connect()
    conn.execute("CREATE TABLE test_table (cola INT, colb STRING)")
    session = DuckDBSession.builder.config("sqlframe.conn", conn).getOrCreate()
    columns = session.catalog.get_columns("test_table")
    assert columns == {"cola": exp.DataType.build("INT"), "colb": exp.DataType.build("TEXT")}
    assert session.execution_dialect_name == "duckdb"


@pytest.mark.forked
def test_session_stop(duckdb_session: DuckDBSession):
    assert duckdb_session.range(1, 2).collect() == [Row(id=1)]
    duckdb_session.stop()
    with pytest.raises(ConnectionException):
        duckdb_session.range(1, 10).collect()


@pytest.mark.forked
def test_session_new_session(duckdb_session: DuckDBSession):
    # Remove old session
    assert duckdb_session.range(1, 2).collect() == [Row(id=1)]
    duckdb_session.stop()
    new_session = DuckDBSession.builder.getOrCreate()
    assert new_session is not duckdb_session
    assert isinstance(new_session, DuckDBSession)
    assert new_session.range(1, 2).collect() == [Row(id=1)]
