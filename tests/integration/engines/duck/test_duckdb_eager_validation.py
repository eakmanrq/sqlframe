import duckdb
import pytest

from sqlframe.duckdb import DuckDBSession
from sqlframe.duckdb import functions as F


@pytest.fixture
def session():
    with duckdb.connect() as conn:
        yield DuckDBSession(conn)


@pytest.mark.parametrize(
    "operation",
    [
        lambda df: df.select("missing"),
        lambda df: df.filter(F.col("missing") > 0),
        lambda df: df.withColumn("bad", F.col("text") + 1),
        lambda df: df.join(df, on=F.col("missing") == F.col("id")),
        lambda df: df.groupBy("missing").count(),
    ],
    ids=["select", "filter", "type_error", "join", "aggregate"],
)
def test_invalid_operation_fails_when_added(session, operation):
    df = session.createDataFrame([(1, "hello")], ["id", "text"])

    with pytest.raises(duckdb.BinderException):
        operation(df)


def test_validation_can_be_disabled_through_builder():
    with duckdb.connect() as conn:
        session = (
            DuckDBSession.builder.config(map={"sqlframe.eager_validation": False})
            .config("sqlframe.conn", conn)
            .getOrCreate()
        )
        invalid = session.createDataFrame([(1,)], ["id"]).select("missing")

        with pytest.raises(duckdb.BinderException):
            invalid.collect()


def test_validation_does_not_execute_rows(session):
    calls = []

    def track(value):
        calls.append(value)
        return value

    session._conn.create_function("track", track, return_type="INTEGER", side_effects=True)
    df = session.createDataFrame([(1,)], ["id"])

    planned = df.select(F.expr("track(id)").alias("value"))
    assert calls == []
    assert planned.collect()[0].value == 1
    assert calls == [1]


def test_validation_sees_temporary_tables(session):
    session._conn.execute("CREATE TEMP TABLE source (id INTEGER)")

    planned = session.sql("SELECT id FROM source", qualify=False).select("id")

    assert planned.collect() == []


@pytest.mark.skipif(
    not hasattr(duckdb, "ResourceInUseException"),
    reason="this DuckDB release discards an unfinished result on any new statement",
)
def test_validation_preserves_unfinished_connection_result(session):
    session._conn.execute("CREATE TABLE source (id INTEGER)")
    session._conn.execute("SELECT 1 UNION ALL SELECT 2")
    assert session._conn.fetchone() == (1,)

    session.sql("SELECT id FROM source", qualify=False).select("id")

    assert session._conn.fetchone() == (2,)


def test_validation_works_without_resource_in_use_exception(session, monkeypatch):
    # DuckDB 1.2.x, the oldest supported release, has no ResourceInUseException.
    monkeypatch.delattr(duckdb, "ResourceInUseException", raising=False)
    session._conn.execute("CREATE TEMP TABLE source (id INTEGER)")

    planned = session.sql("SELECT id FROM source", qualify=False).select("id")
    with pytest.raises(duckdb.BinderException):
        planned.select("missing")

    assert planned.collect() == []
