from sqlglot import exp

from sqlframe.duckdb.session import DuckDBSession


def test_session_from_config():
    import duckdb

    conn = duckdb.connect()
    conn.execute("CREATE TABLE test_table (cola INT, colb STRING)")
    session = DuckDBSession.builder.config("sqlframe.conn", conn).getOrCreate()
    columns = session.catalog.get_columns("test_table")
    assert columns == {"cola": exp.DataType.build("INT"), "colb": exp.DataType.build("TEXT")}
    assert session.execution_dialect_name == "duckdb"
