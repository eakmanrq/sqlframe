import pytest
from duckdb import ConnectionException
from sqlglot import exp

from sqlframe.base.types import Row
from sqlframe.duckdb import functions as F
from sqlframe.duckdb.session import DuckDBSession

pytest_plugins = ["tests.common_fixtures"]


@pytest.mark.parametrize("value", [float("inf"), float("-inf")])
def test_float_infinity_literals(value):
    session = DuckDBSession()
    df = session.createDataFrame([(value,), (1.0,)], schema=["a"])
    assert df.select(
        (F.col("a") == value).alias("equal"),
        F.col("a").isin([value, 2.0]).alias("member"),
        F.lit([value, 1.0]).alias("array"),
    ).collect() == [
        Row(equal=True, member=True, array=[value, 1.0]),
        Row(equal=False, member=False, array=[value, 1.0]),
    ]
    assert session.createDataFrame([([value, 1.0],)], schema=["a"]).collect() == [
        Row(a=[value, 1.0])
    ]


def test_session_from_config():
    import duckdb

    conn = duckdb.connect()
    conn.execute("CREATE TABLE test_table (cola INT, colb STRING)")
    session = DuckDBSession.builder.config("sqlframe.conn", conn).getOrCreate()
    columns = session.catalog.get_columns("test_table")
    assert columns == {"cola": exp.DataType.build("INT"), "colb": exp.DataType.build("TEXT")}
    assert session.execution_dialect_name == "duckdb"


def test_conf_get_reads_live_connection_timezone():
    import duckdb

    conn = duckdb.connect()
    conn.execute("SET SESSION TimeZone = 'UTC'")
    session = DuckDBSession(conn)
    df = session.createDataFrame([(1,)], schema=["value"])

    assert df.sparkSession.conf.get("spark.sql.session.timeZone") == "UTC"
    conn.execute("SET SESSION TimeZone = 'Europe/Amsterdam'")
    assert session.conf.get("spark.sql.session.timeZone") == "Europe/Amsterdam"


def test_conf_get_uses_duckdb_connection_default():
    import duckdb

    conn = duckdb.connect()
    session = DuckDBSession(conn)

    result = conn.execute("SELECT current_setting('TimeZone')").fetchone()
    assert result is not None
    assert session.conf.get("spark.sql.session.timeZone") == result[0]


def test_conf_set_changes_duckdb_session_timezone():
    import duckdb

    conn = duckdb.connect()
    session = DuckDBSession(conn)

    session.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")

    assert conn.execute("SELECT current_setting('TimeZone')").fetchone() == ("Asia/Tokyo",)
    assert session.conf.get("spark.sql.session.timeZone") == "Asia/Tokyo"


def test_builder_timezone_config_applies_to_session():
    session = (
        DuckDBSession.Builder()
        .config("spark.sql.session.timeZone", "Europe/Amsterdam")
        .getOrCreate()
    )

    assert session.conf.get("spark.sql.session.timeZone") == "Europe/Amsterdam"
    row = session.sql("SELECT current_setting('TimeZone') AS timezone").first()
    assert row is not None
    assert row.timezone == "Europe/Amsterdam"


def test_builder_timezone_config_map_applies_to_session():
    session = (
        DuckDBSession.Builder()
        .config(map={"spark.sql.session.timeZone": "Asia/Tokyo"})
        .getOrCreate()
    )

    assert session.conf.get("spark.sql.session.timeZone") == "Asia/Tokyo"


def test_conf_rejects_unsupported_spark_options():
    session = DuckDBSession()

    with pytest.raises(NotImplementedError, match="spark.sql.shuffle.partitions"):
        session.conf.get("spark.sql.shuffle.partitions")
    with pytest.raises(NotImplementedError, match="spark.sql.shuffle.partitions"):
        session.conf.set("spark.sql.shuffle.partitions", "2")


def test_session_with_unfinished_connection_result():
    import duckdb

    conn = duckdb.connect()
    conn.execute("SELECT 1 UNION ALL SELECT 2")
    assert conn.fetchone() == (1,)

    session = DuckDBSession(conn)

    assert conn.fetchone() == (2,)
    assert session.sql("SELECT SOUNDEX('Robert') AS result").collect() == [Row(result="R163")]


def test_session_stop(duckdb_session: DuckDBSession):
    assert duckdb_session.range(1, 2).collect() == [Row(id=1)]
    duckdb_session.stop()
    with pytest.raises(ConnectionException):
        duckdb_session.range(1, 10).collect()


def test_session_new_session(duckdb_session: DuckDBSession):
    # Remove old session
    assert duckdb_session.range(1, 2).collect() == [Row(id=1)]
    duckdb_session.stop()
    new_session = DuckDBSession.builder.getOrCreate()
    assert new_session is not duckdb_session
    assert isinstance(new_session, DuckDBSession)
    assert new_session.range(1, 2).collect() == [Row(id=1)]
