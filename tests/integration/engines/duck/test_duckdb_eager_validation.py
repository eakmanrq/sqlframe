import duckdb
import pytest

from sqlframe import activate, deactivate
from sqlframe.duckdb import DuckDBSession
from sqlframe.duckdb import functions as F
from sqlframe.duckdb.dataframe import DuckDBDataFrame


@pytest.fixture
def session():
    with duckdb.connect() as conn:
        yield DuckDBSession(conn, eager_validation=True)


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


def test_validation_is_opt_in_by_default():
    with duckdb.connect() as conn:
        session = DuckDBSession.builder.config("sqlframe.conn", conn).getOrCreate()
        assert session.eager_validation is False
        invalid = session.createDataFrame([(1,)], ["id"]).select("missing")

        with pytest.raises(duckdb.BinderException):
            invalid.collect()


def test_validation_can_be_enabled_through_builder():
    with duckdb.connect() as conn:
        session = (
            DuckDBSession.builder.config(map={"sqlframe.eager_validation": True})
            .config("sqlframe.conn", conn)
            .getOrCreate()
        )
        assert session.eager_validation is True
        df = session.createDataFrame([(1,)], ["id"])

        with pytest.raises(duckdb.BinderException):
            df.select("missing")


def test_validation_can_be_enabled_through_builder_key_value():
    with duckdb.connect() as conn:
        session = (
            DuckDBSession.builder.config("sqlframe.eager_validation", True)
            .config("sqlframe.conn", conn)
            .getOrCreate()
        )
        assert session.eager_validation is True
        df = session.createDataFrame([(1,)], ["id"])

        with pytest.raises(duckdb.BinderException):
            df.select("missing")


def test_validation_and_timezone_config_work_together():
    with duckdb.connect() as conn:
        session = DuckDBSession.builder.config(
            map={
                "sqlframe.eager_validation": True,
                "spark.sql.session.timeZone": "UTC",
                "sqlframe.conn": conn,
            }
        ).getOrCreate()

        assert session.conf.get("spark.sql.session.timeZone") == "UTC"
        with pytest.raises(duckdb.BinderException):
            session.createDataFrame([(1,)], ["id"]).select("missing")


def test_validation_can_be_enabled_through_activate():
    with duckdb.connect() as conn:
        activate("duckdb", conn=conn, config={"sqlframe.eager_validation": True})
        try:
            from pyspark.sql import SparkSession

            session = SparkSession.builder.getOrCreate()
            assert isinstance(session, DuckDBSession)
            assert session.eager_validation is True
            df = session.createDataFrame([(1,)], ["id"])

            with pytest.raises(duckdb.BinderException):
                df.select("missing")
        finally:
            deactivate()


@pytest.mark.parametrize(
    "enable",
    [
        lambda conn, value: DuckDBSession(conn, eager_validation=value),
        lambda conn, value: DuckDBSession.builder.config("sqlframe.eager_validation", value),
        lambda conn, value: DuckDBSession.builder.config(map={"sqlframe.eager_validation": value}),
    ],
    ids=["constructor", "builder_key_value", "builder_map"],
)
@pytest.mark.parametrize("value", ["true", 1], ids=["str", "int"])
def test_validation_rejects_non_bool(enable, value):
    with duckdb.connect() as conn:
        with pytest.raises(ValueError, match="sqlframe.eager_validation must be a boolean"):
            enable(conn, value)


def test_singleton_reinit_keeps_validation_setting():
    with duckdb.connect() as conn:
        enabled = DuckDBSession(conn, eager_validation=True)

        reinit = DuckDBSession()
        assert reinit is enabled
        assert reinit.eager_validation is True
        with pytest.raises(duckdb.BinderException):
            reinit.createDataFrame([(1,)], ["id"]).select("missing")

        disabled = DuckDBSession(eager_validation=False)
        assert disabled is enabled
        assert disabled.eager_validation is False
        invalid = disabled.createDataFrame([(1,)], ["id"]).select("missing")
        with pytest.raises(duckdb.BinderException):
            invalid.collect()


def test_constructor_preserves_positional_schema_argument():
    with duckdb.connect() as conn:
        schema = {"id": "INT"}
        session = DuckDBSession(conn, schema)

        assert session._schema is schema
        assert session.eager_validation is False


@pytest.mark.parametrize(
    "operation",
    [
        lambda df, other: df.withColumn("added", F.col("id") + 1),
        lambda df, other: df.dropna(),
        lambda df, other: df.unionByName(other, allowMissingColumns=True),
        lambda df, other: df.groupBy("id").pivot("key").agg(F.count("value")),
        lambda df, other: df.fillna("z"),
        lambda df, other: df.replace("a", "b"),
    ],
    ids=["withColumn", "dropna", "unionByName", "pivot_agg", "fillna", "replace"],
)
def test_one_validation_per_user_operation(session, monkeypatch, operation):
    validated = []
    original = DuckDBDataFrame._validate_plan

    def counting(self):
        validated.append(self)
        return original(self)

    monkeypatch.setattr(DuckDBDataFrame, "_validate_plan", counting)
    df = session.createDataFrame([(1, "a", None), (2, "b", "x")], ["id", "key", "value"])
    other = df.select("id", "key")
    validated.clear()

    result = operation(df, other)

    assert validated == [result]


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


def test_validation_preserves_unfinished_connection_result(session):
    session._conn.execute("CREATE TABLE source (id INTEGER)")
    session._conn.execute("SELECT 1 UNION ALL SELECT 2")
    assert session._conn.fetchone() == (1,)

    session.sql("SELECT id FROM source", qualify=False).select("id")

    assert session._conn.fetchone() == (2,)


def test_validation_with_unfinished_result_and_temporary_table(session):
    session._conn.execute("CREATE TEMP TABLE source (id INTEGER)")
    session._conn.execute("SELECT 1 UNION ALL SELECT 2")
    assert session._conn.fetchone() == (1,)

    planned = session.sql("SELECT id FROM source", qualify=False).select("id")

    assert session._conn.fetchone() == (2,)
    assert planned.collect() == []


def test_validation_works_without_resource_in_use_exception(session, monkeypatch):
    # DuckDB 1.2.x, the oldest supported release, has no ResourceInUseException.
    monkeypatch.delattr(duckdb, "ResourceInUseException", raising=False)
    session._conn.execute("CREATE TABLE source (id INTEGER)")

    planned = session.sql("SELECT id FROM source", qualify=False).select("id")
    with pytest.raises(duckdb.BinderException):
        planned.select("missing")

    assert planned.collect() == []
