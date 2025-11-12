from sqlglot import exp

from sqlframe.gizmosql.session import GizmoSQLSession


def test_session_from_config(gizmosql_connection):
    conn = gizmosql_connection
    with conn.cursor() as cursor:
        cursor.execute("CREATE OR REPLACE TABLE test_table (cola INT, colb STRING)").fetchall()
    session = GizmoSQLSession.builder.config("sqlframe.conn", conn).getOrCreate()
    columns = session.catalog.get_columns("test_table")
    assert columns == {"cola": exp.DataType.build("INT"), "colb": exp.DataType.build("TEXT")}
    assert session.execution_dialect_name == "duckdb"
