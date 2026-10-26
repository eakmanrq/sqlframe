import pytest
from psycopg2.errors import DivisionByZero, DuplicateTable

from sqlframe.postgres import functions as F
from sqlframe.postgres.session import PostgresSession

pytest_plugins = ["tests.common_fixtures"]
pytestmark = [pytest.mark.postgres, pytest.mark.xdist_group("postgres_tests")]


def test_save_as_table_overwrite_existing_table(postgres_session: PostgresSession):
    session = postgres_session
    session.createDataFrame([(1, "old")], ["id", "label"]).write.saveAsTable("employee")

    session.createDataFrame([(2, "new")], ["id", "note"]).write.mode("overwrite").saveAsTable(
        "employee"
    )

    assert [tuple(row) for row in session.table("employee").collect()] == [(2, "new")]
    assert session.table("employee").columns == ["id", "note"]


def test_save_as_table_overwrite_absent_table(postgres_session: PostgresSession):
    session = postgres_session

    session.createDataFrame([(2, "new")], ["id", "label"]).write.saveAsTable(
        "employee", mode="overwrite"
    )

    assert [tuple(row) for row in session.table("employee").collect()] == [(2, "new")]


def test_save_as_table_overwrite_keeps_other_schema_and_quoted_name(
    postgres_session: PostgresSession,
):
    session = postgres_session
    session.createDataFrame([(1,)], ["id"]).write.saveAsTable("`db1`.`Employee`")
    session.createDataFrame([(2,)], ["id"]).write.saveAsTable("`public`.`Employee`")

    session.createDataFrame([(3,)], ["id"]).write.mode("overwrite").saveAsTable("`db1`.`Employee`")

    assert [tuple(row) for row in session.table("`db1`.`Employee`").collect()] == [(3,)]
    assert [tuple(row) for row in session.table("`public`.`Employee`").collect()] == [(2,)]


def test_save_as_table_failed_overwrite_preserves_existing_table(
    postgres_session: PostgresSession,
):
    session = postgres_session
    session.createDataFrame([(1,)], ["id"]).write.saveAsTable("employee")
    failing = session.createDataFrame([(2,)], ["id"]).select((F.col("id") / F.lit(0)).alias("id"))

    with pytest.raises(DivisionByZero):
        failing.write.mode("overwrite").saveAsTable("employee")

    assert [tuple(row) for row in session.table("employee").collect()] == [(1,)]


def test_save_as_table_overwrite_remains_in_caller_transaction(postgres_session: PostgresSession):
    session = postgres_session
    session.createDataFrame([(1,)], ["id"]).write.saveAsTable("employee")
    session._conn.commit()

    session.createDataFrame([(2,)], ["id"]).write.mode("overwrite").saveAsTable("employee")
    assert [tuple(row) for row in session.table("employee").collect()] == [(2,)]
    session._conn.rollback()

    assert [tuple(row) for row in session.table("employee").collect()] == [(1,)]


def test_save_as_table_overwrite_autocommit_failure_preserves_existing_table(
    function_scoped_postgres,
):
    conn = function_scoped_postgres
    conn.autocommit = True
    session = PostgresSession(conn)
    session.createDataFrame([(1,)], ["id"]).write.saveAsTable("employee")
    failing = session.createDataFrame([(2,)], ["id"]).select((F.col("id") / F.lit(0)).alias("id"))

    with pytest.raises(DivisionByZero):
        failing.write.mode("overwrite").saveAsTable("employee")

    assert [tuple(row) for row in session.table("employee").collect()] == [(1,)]


def test_save_as_table_overwrite_preserves_explicit_autocommit_transaction(
    function_scoped_postgres,
):
    conn = function_scoped_postgres
    conn.autocommit = True
    session = PostgresSession(conn)
    session.createDataFrame([(1,)], ["id"]).write.saveAsTable("employee")
    session._execute("BEGIN")
    session._execute("CREATE TABLE caller_work (id INT)")

    session.createDataFrame([(2,)], ["id"]).write.mode("overwrite").saveAsTable("employee")
    session._execute("ROLLBACK")

    assert [tuple(row) for row in session.table("employee").collect()] == [(1,)]
    session._execute("SELECT to_regclass('caller_work')")
    assert session._cur.fetchone() == (None,)


def test_save_as_table_failed_overwrite_preserves_explicit_autocommit_transaction(
    function_scoped_postgres,
):
    conn = function_scoped_postgres
    conn.autocommit = True
    session = PostgresSession(conn)
    session.createDataFrame([(1,)], ["id"]).write.saveAsTable("employee")
    session._execute("BEGIN")
    session._execute("CREATE TABLE caller_work (id INT)")
    failing = session.createDataFrame([(2,)], ["id"]).select((F.col("id") / F.lit(0)).alias("id"))

    with pytest.raises(DivisionByZero):
        failing.write.mode("overwrite").saveAsTable("employee")

    assert [tuple(row) for row in session.table("employee").collect()] == [(1,)]
    session._execute("SELECT to_regclass('caller_work')")
    assert session._cur.fetchone() == ("caller_work",)
    session._execute("ROLLBACK")


def test_save_as_table_other_modes_keep_their_behavior(postgres_session: PostgresSession):
    session = postgres_session
    session.createDataFrame([(1,)], ["id"]).write.saveAsTable("employee")
    replacement = session.createDataFrame([(2,)], ["id"])

    replacement.write.mode("ignore").saveAsTable("employee")
    assert [tuple(row) for row in session.table("employee").collect()] == [(1,)]
    session._conn.commit()
    with pytest.raises(DuplicateTable):
        replacement.write.mode("error").saveAsTable("employee")
    session._conn.rollback()
    replacement.write.mode("append").saveAsTable("employee")
    assert sorted(tuple(row) for row in session.table("employee").collect()) == [(1,), (2,)]
