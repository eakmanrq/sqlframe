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


def test_cte_name_matching_qualified_source_executes(duckdb_session: DuckDBSession):
    duckdb_session._conn.execute('CREATE SCHEMA "memory"."test"')
    duckdb_session._conn.execute('CREATE TABLE "memory"."test"."orders" AS SELECT 7 AS order_id')
    duckdb_session.catalog.add_table("memory.test.orders", {"order_id": "int"})

    df = duckdb_session.sql(
        "WITH orders AS (SELECT order_id FROM memory.test.orders) SELECT order_id FROM orders"
    )

    assert df.collect() == [Row(order_id=7)]


def test_nested_recursive_cte_executes(duckdb_session: DuckDBSession):
    df = duckdb_session.sql(
        "SELECT q.n FROM (WITH RECURSIVE nums(n) AS ("
        "SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 3"
        ") SELECT n FROM nums) AS q",
        qualify=False,
    )

    assert df.collect() == [Row(n=1), Row(n=2), Row(n=3)]


def test_ctes_with_distinct_declared_columns_execute(duckdb_session: DuckDBSession):
    df = duckdb_session.sql(
        "SELECT q.y FROM (WITH a(x) AS (SELECT 1), b(y) AS (SELECT 1) SELECT b.y FROM b) AS q",
        qualify=False,
    )

    assert df.collect() == [Row(y=1)]


def test_correlated_qualifier_in_nested_cte_executes(duckdb_session: DuckDBSession):
    df = duckdb_session.sql(
        "WITH orders AS (SELECT 1 AS id) "
        "SELECT (WITH x AS (SELECT orders.id AS v) SELECT v FROM x) AS v FROM orders",
        qualify=False,
    )

    assert df.collect() == [Row(v=1)]


def test_session_from_config():
    import duckdb

    conn = duckdb.connect()
    conn.execute("CREATE TABLE test_table (cola INT, colb STRING)")
    session = DuckDBSession.builder.config("sqlframe.conn", conn).getOrCreate()
    columns = session.catalog.get_columns("test_table")
    assert columns == {"cola": exp.DataType.build("INT"), "colb": exp.DataType.build("TEXT")}
    assert session.execution_dialect_name == "duckdb"


def test_sql_rejects_delta_table_registration():
    import duckdb

    conn = duckdb.connect()
    conn.execute("CREATE TABLE existing_table AS SELECT 1 AS id")
    session = DuckDBSession(conn)

    with pytest.raises(NotImplementedError, match="DuckDB.*Delta.*read.format"):
        session.sql("CREATE OR REPLACE TABLE existing_table USING delta LOCATION '/tmp/delta'")

    assert conn.execute("SELECT id FROM existing_table").fetchall() == [(1,)]


def test_sql_rejects_delta_ctas():
    import duckdb

    conn = duckdb.connect()
    session = DuckDBSession(conn)

    with pytest.raises(NotImplementedError, match="DuckDB.*Delta"):
        session.sql("CREATE TABLE delta_table USING delta AS SELECT 1 AS id")

    assert conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'delta_table'"
    ).fetchone() == (0,)


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


def test_conf_get_does_not_leave_unfinished_result():
    session = DuckDBSession()
    df = session.createDataFrame([(1,)], schema=["a"])

    session.conf.get("spark.sql.session.timeZone")

    assert df.toArrow().to_pylist() == [{"a": 1}]


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


def test_dataframe_operation_does_not_overwrite_another_session():
    import duckdb

    from sqlframe.base.session import _BaseSession

    conn_a = duckdb.connect()
    conn_a.execute("CREATE TABLE t AS SELECT 'a' AS src")
    conn_b = duckdb.connect()
    conn_b.execute("CREATE TABLE t AS SELECT 'b' AS src")
    session_a = DuckDBSession(conn_a)
    df_a = session_a.table("t")
    _BaseSession._instance = None
    session_b = DuckDBSession(conn_b)

    df_a.filter(F.col("src") == "a")

    assert session_b.table("t").collect() == [Row(src="b")]
