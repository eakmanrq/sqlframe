import os

import pytest

from sqlframe.gizmosql import GizmoSQLSession
from sqlframe.gizmosql.connect import GizmoSQLConnection, DatabaseOptions

# Constants
GIZMOSQL_PORT = 31337
CONTAINER_NAME = "sqlframe-gizmosql-test"


@pytest.fixture(scope="function")
def gizmosql_connection():
    with GizmoSQLConnection(uri="grpc+tls://localhost:31337",
                            db_kwargs={"username": os.getenv("GIZMOSQL_USERNAME", "gizmosql_username"),
                                       "password": os.getenv("GIZMOSQL_PASSWORD", "gizmosql_password"),
                                       DatabaseOptions.TLS_SKIP_VERIFY.value: "true"
                                       # Not needed if you use a trusted CA-signed TLS cert
                                       },
                            autocommit=True
                            ) as conn:
        yield conn


@pytest.fixture(scope="function")
def gizmosql_session(gizmosql_connection) -> GizmoSQLSession:
    conn = gizmosql_connection
    with conn.cursor() as cursor:
        cursor.execute("set TimeZone = 'UTC'").fetchall()
        cursor.execute("SELECT * FROM duckdb_settings() WHERE name = 'TimeZone'")
        assert cursor.fetchone()[1] == "UTC"  # type: ignore

    session = GizmoSQLSession(conn=conn)

    return session


@pytest.fixture(scope="function")
def gizmosql_session_with_tables(gizmosql_session) -> GizmoSQLSession:
    # Register any tables with the Spark catalog
    for table in gizmosql_session.catalog.listTables():
        _ = gizmosql_session.table(table.name)

    return gizmosql_session
