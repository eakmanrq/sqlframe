import os

import pytest
from sqlglot import exp

from sqlframe.base.types import Row
from sqlframe.gizmosql.connect import GizmoSQLConnection, DatabaseOptions
from sqlframe.gizmosql.session import GizmoSQLSession


def test_session_from_config(gizmosql_connection):
    conn = gizmosql_connection
    with conn.cursor() as cursor:
        cursor.execute("CREATE OR REPLACE TABLE test_table (cola INT, colb STRING)").fetchall()
    session = GizmoSQLSession.builder.config("sqlframe.conn", conn).getOrCreate()
    columns = session.catalog.get_columns("test_table")
    assert columns == {"cola": exp.DataType.build("INT"), "colb": exp.DataType.build("TEXT")}
    assert session.execution_dialect_name == "duckdb"


@pytest.mark.forked
def test_session_stop(gizmosql_session: GizmoSQLSession):
    assert gizmosql_session.range(1, 2).collect() == [Row(id=1)]
    gizmosql_session.stop()
    with pytest.raises(RuntimeError, match="Cursor is closed"):
        gizmosql_session.range(1, 10).collect()


@pytest.mark.forked
def test_session_new_session(gizmosql_session: GizmoSQLSession):
    # Remove old session
    assert gizmosql_session.range(1, 2).collect() == [Row(id=1)]
    gizmosql_session.stop()

    # We need to grab a thread-safe connection (we can't use a session fixture)
    new_conn = GizmoSQLConnection(uri="grpc+tls://localhost:31337",
                                  db_kwargs={"username": os.getenv("GIZMOSQL_USERNAME", "gizmosql_username"),
                                             "password": os.getenv("GIZMOSQL_PASSWORD", "gizmosql_password"),
                                             DatabaseOptions.TLS_SKIP_VERIFY.value: "true"
                                             # Not needed if you use a trusted CA-signed TLS cert
                                             },
                                  autocommit=True
                                  )
    new_session = GizmoSQLSession(conn=new_conn)
    assert new_session is not gizmosql_session
    assert isinstance(new_session, GizmoSQLSession)
    assert new_session.range(1, 2).collect() == [Row(id=1)]
