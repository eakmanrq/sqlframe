"""Writer-only checks against the memory connector used by make trino-test."""

import os
import uuid

import pytest

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.base.types import Row
from sqlframe.trino import TrinoSession

pytestmark = pytest.mark.trino


@pytest.fixture
def writer_session():
    if not (host := os.getenv("SQLFRAME_TRINO_HOST")):
        pytest.skip("Set SQLFRAME_TRINO_HOST to run against Trino")
    trino = pytest.importorskip("trino")
    conn = trino.dbapi.connect(
        host=host,
        port=int(os.getenv("SQLFRAME_TRINO_PORT", "18080")),
        user="sqlframe",
        catalog="memory",
    )
    schema = f"writer_{uuid.uuid4().hex[:12]}"
    cursor = conn.cursor()
    cursor.execute(f"CREATE SCHEMA memory.{schema}")
    cursor.fetchall()
    cursor.execute(f"USE memory.{schema}")
    cursor.fetchall()
    session = TrinoSession(conn=conn)
    try:
        yield session, f"memory.{schema}.target"
    finally:
        cursor.execute(f"DROP SCHEMA memory.{schema} CASCADE")
        cursor.fetchall()
        session.stop()


def test_create_and_append_mapping(writer_session):
    session, table = writer_session
    session.createDataFrame([(1, "a")], ["id", "label"]).write.saveAsTable(table)
    session.createDataFrame([(2, "b")], ["other_id", "other_label"]).write.insertInto(table)
    session.createDataFrame([("c", 3)], ["label", "id"]).write.saveAsTable(table, mode="append")
    session.createDataFrame([("d", 4)], ["label", "id"]).write.byName.insertInto(table)

    assert session.read.table(table).orderBy("id").collect() == [
        Row(id=1, label="a"),
        Row(id=2, label="b"),
        Row(id=3, label="c"),
        Row(id=4, label="d"),
    ]


@pytest.mark.parametrize("name_style", ["table", "schema.table", "catalog.schema.table"])
def test_append_mode_creates_missing_table_and_resolves_names(writer_session, name_style):
    session, table = writer_session
    parts = table.split(".")
    name = ".".join(parts[-(name_style.count(".") + 1) :])
    session.createDataFrame([(7,)], ["id"]).write.saveAsTable(name, mode="append")
    assert session.read.table(table).collect() == [Row(id=7)]


@pytest.mark.parametrize("mode", [None, "error", "errorifexists"])
def test_existing_table_is_preserved(writer_session, mode):
    session, table = writer_session
    session.createDataFrame([(1,)], ["id"]).write.saveAsTable(table)
    with pytest.raises(FileExistsError, match="already exists"):
        session.createDataFrame([(2,)], ["id"]).write.saveAsTable(table, mode=mode)
    assert session.read.table(table).collect() == [Row(id=1)]


@pytest.mark.parametrize(
    "query,by_name,match",
    [
        ("SELECT 2 AS id", False, "column counts"),
        ("SELECT 'bad' AS id, 2 AS total", False, "type mismatch"),
        ("SELECT BIGINT '2' AS id, 2 AS total", False, "type mismatch"),
        ("SELECT 2 AS wrong, 2 AS total", True, "column names"),
        ("SELECT 2 AS id, NULL AS total", False, "unknown"),
        ("SELECT 2 AS id, 2 AS ID", False, "unique"),
    ],
)
def test_invalid_append_is_rejected_before_insert(
    writer_session, monkeypatch, query, by_name, match
):
    session, table = writer_session
    session._collect(f"CREATE TABLE {table} AS SELECT 1 AS id, 10 AS total")
    statements = []
    execute = session._execute

    def record(statement):
        statements.append(statement)
        return execute(statement)

    monkeypatch.setattr(session, "_execute", record)
    writer = session.sql(query, dialect="trino").write
    with pytest.raises(UnsupportedOperationError, match=match):
        if by_name:
            writer.saveAsTable(table, mode="append")
        else:
            writer.insertInto(table)
    assert not any(sql.startswith(("INSERT", "CREATE", "DROP")) for sql in statements)
    assert session.read.table(table).collect() == [Row(id=1, total=10)]


def test_integer_and_varchar_widening(writer_session):
    session, table = writer_session
    session._collect(f"CREATE TABLE {table} (id bigint, label varchar(10))")
    session.sql("SELECT 1 AS id, 'abc' AS label", dialect="trino").write.insertInto(table)
    assert session.read.table(table).collect() == [Row(id=1, label="abc")]


def test_varchar_narrowing_is_rejected(writer_session):
    session, table = writer_session
    session._collect(f"CREATE TABLE {table} (label varchar(1))")
    with pytest.raises(UnsupportedOperationError, match="type mismatch"):
        session.createDataFrame([("long",)], ["label"]).write.insertInto(table)
    assert session._collect(f"SELECT count(*) FROM {table}")[0][0] == 0


def test_missing_insert_does_not_create_table(writer_session):
    session, table = writer_session
    with pytest.raises(UnsupportedOperationError, match="existing table"):
        session.createDataFrame([(1,)], ["id"]).write.insertInto(table)
    assert session.catalog.listTables() == []


def test_invalid_creation_schema_does_not_create_table(writer_session):
    session, table = writer_session
    with pytest.raises(UnsupportedOperationError, match="unknown"):
        session.sql("SELECT NULL AS value", dialect="trino").write.saveAsTable(table)
    assert session.catalog.listTables() == []


def test_quoted_identifiers_and_column_names(writer_session):
    session, table = writer_session
    table += " with space"
    quoted = ".".join(f"`{part}`" for part in table.split("."))
    df = session.sql('SELECT 1 AS "a.b", \'a\' AS "select"', dialect="trino")
    df.write.saveAsTable(quoted)
    df.write.saveAsTable(quoted, mode="append")
    assert (
        session._collect(f'SELECT count(*) FROM {table.rsplit(".", 1)[0]}."target with space"')[0][
            0
        ]
        == 2
    )


@pytest.mark.parametrize("method", ["insertInto", "saveAsTable"])
@pytest.mark.parametrize("name_parts", [1, 2, 3])
def test_snowflake_input_appends_to_existing_trino_table(writer_session, method, name_parts):
    session, table = writer_session
    session.createDataFrame([(1, "a")], ["id", "label"]).write.saveAsTable(table)
    session = TrinoSession.builder.config("sqlframe.input.dialect", "snowflake").getOrCreate()
    name = ".".join(table.split(".")[-name_parts:])
    writer = session.createDataFrame([(2, "b")], ["id", "label"]).write

    if method == "saveAsTable":
        writer.saveAsTable(name, mode="append")
    else:
        writer.insertInto(name)

    assert session._collect(f"SELECT id, label FROM {table} ORDER BY id") == [
        Row(id=1, label="a"),
        Row(id=2, label="b"),
    ]
