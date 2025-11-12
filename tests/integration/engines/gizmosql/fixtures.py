import os

import pytest

from sqlframe.gizmosql import GizmoSQLSession
from sqlframe.gizmosql.connect import GizmoSQLConnection

# Constants
GIZMOSQL_PORT = 31337
CONTAINER_NAME = "sqlframe-gizmosql-test"


@pytest.fixture(scope="function")
def gizmosql_connection():
    """
    First run this to start a gizmosql server for testing:
    docker run --name sqlframe-gizmosql-test \
               --detach \
               --rm \
               --tty \
               --init \
               --publish 31337:31337 \
               --env TLS_ENABLED="0" \
               --env GIZMOSQL_USERNAME="gizmosql_username" \
               --env GIZMOSQL_PASSWORD="gizmosql_password" \
               --env DATABASE_FILENAME="data/sqlframe.db" \
               --env INIT_SQL_COMMANDS="CALL dsdgen(sf=0.01);" \
               --env PRINT_QUERIES="1" \
               --pull always \
               gizmodata/gizmosql:latest
    """
    with GizmoSQLConnection(
        uri="grpc+tcp://localhost:31337",
        db_kwargs={"username": "gizmosql_username", "password": "gizmosql_password"},
        autocommit=True,
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
