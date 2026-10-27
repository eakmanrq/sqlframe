"""Query metadata and cache boundaries against the pinned Trino 484 server."""

import pytest

from sqlframe.base import types
from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.trino import TrinoDataFrame, TrinoSession
from sqlframe.trino import functions as F
from tests.integration.trino.test_trino_live import trino_session as trino_session

pytestmark = pytest.mark.trino


@pytest.fixture
def native_session(trino_session):
    session, table = trino_session
    # Preserve native Trino casts while testing its full type system. The default
    # Spark input dialect can translate TIME, timestamp precision, and ROW syntax.
    session = (
        TrinoSession.builder.config("sqlframe.conn", session._conn)
        .config("sqlframe.input.dialect", "trino")
        .getOrCreate()
    )
    return session, table


def test_scalar_schema_for_empty_query(native_session):
    session, _ = native_session
    cases = [
        ("boolean", types.BooleanType()),
        ("tinyint", types.ByteType()),
        ("smallint", types.ShortType()),
        ("integer", types.IntegerType()),
        ("bigint", types.LongType()),
        ("real", types.FloatType()),
        ("double", types.DoubleType()),
        ("decimal(38, 10)", types.DecimalType(38, 10)),
        ("varchar(20)", types.StringType()),
        ("char(5)", types.StringType()),
        ("varbinary", types.BinaryType()),
        ("date", types.DateType()),
        ("timestamp(6)", types.TimestampNTZType()),
        ("timestamp(6) with time zone", types.TimestampType()),
        ("json", types.StringType()),
        ("uuid", types.StringType()),
    ]
    query = session.sql(
        "SELECT "
        + ", ".join(f"CAST(NULL AS {dtype}) AS c{i}" for i, (dtype, _) in enumerate(cases))
        + " WHERE false",
        dialect="trino",
        qualify=False,
    )

    assert query.collect() == []
    assert query.schema == types.StructType(
        [types.StructField(f"c{i}", dtype, True) for i, (_, dtype) in enumerate(cases)]
    )


def test_nested_schema_preserves_row_field_names(native_session, capsys):
    session, orders = native_session
    table = orders.rsplit(".", 1)[0] + ".nested"
    session._cur.execute(
        f"CREATE TABLE {table} AS "
        'SELECT CAST(NULL AS map(varchar, array(row("A.b" decimal(12, 2), '
        '"EventTime" timestamp(6))))) AS payload'
    )
    query = session.sql(f"SELECT payload FROM {table}")

    assert query.schema == types.StructType(
        [
            types.StructField(
                "payload",
                types.MapType(
                    types.StringType(),
                    types.ArrayType(
                        types.StructType(
                            [
                                types.StructField("A.b", types.DecimalType(12, 2)),
                                types.StructField("EventTime", types.TimestampNTZType()),
                            ]
                        )
                    ),
                ),
            )
        ]
    )
    query.printSchema(level=1)
    assert capsys.readouterr().out == (
        "root\n |-- payload: map<string, array<struct<`a.b`: decimal(12, 2), "
        "`eventtime`: timestamp_ntz>>> (nullable = true)\n"
    )


def test_schema_tracks_table_projection_aggregate_cte_and_temp_view(trino_session):
    session, table = trino_session
    frame = session.read.table(table)
    assert isinstance(frame, TrinoDataFrame)
    assert frame.schema.simpleString() == "struct<order_id:int, total:int>"

    aggregated = (
        frame.where("total >= 25")
        .groupBy("order_id")
        .agg(F.sum("total").alias("sum_total"), F.count("total").alias("n"))
    )
    aggregated.createOrReplaceTempView("summed_orders")
    query = session.sql(
        "WITH selected AS (SELECT * FROM summed_orders) SELECT sum_total, n FROM selected"
    )

    assert query.schema.simpleString() == "struct<sum_total:bigint, n:bigint>"
    assert all(field.nullable for field in query.schema)
    assert session.read.table("summed_orders").schema == aggregated.schema


def test_describing_query_does_not_evaluate_it(native_session):
    from trino.exceptions import TrinoUserError

    session, _ = native_session
    query = session.sql("SELECT 1 / 0 AS unevaluated", qualify=False)

    assert query.schema.simpleString() == "struct<unevaluated:int>"
    with pytest.raises(TrinoUserError, match="Division by zero"):
        query.collect()


@pytest.mark.parametrize(
    "expression",
    [
        "CAST(NULL AS time(3))",
        "CAST(NULL AS time(6) with time zone)",
        "CAST(NULL AS timestamp(9))",
        "CAST(NULL AS array(timestamp(12) with time zone))",
        "INTERVAL '1' YEAR",
        "CAST(NULL AS ipaddress)",
        "ROW(1, 'a')",
        "NULL",
    ],
)
def test_unsupported_query_schema_is_explicit(native_session, expression):
    session, _ = native_session
    query = session.sql(f"SELECT {expression} AS payload", dialect="trino", qualify=False)

    with pytest.raises(UnsupportedOperationError, match="Unsupported Trino type .*payload"):
        _ = query.schema


def test_table_discovery_registers_nested_and_temporal_types(trino_session):
    session, orders = trino_session
    table = orders.rsplit(".", 1)[0] + ".typed"
    session._cur.execute(
        f"CREATE TABLE {table} AS SELECT "
        "CAST(ARRAY[1.25] AS array(decimal(12, 2))) AS amounts, "
        'CAST(ROW(1, NULL) AS row("A.b" integer, event_time timestamp(6))) AS record, '
        "X'00FF' AS \"a.b\""
    )

    frame = session.read.table(table)
    assert frame.schema == types.StructType(
        [
            types.StructField("amounts", types.ArrayType(types.DecimalType(12, 2))),
            types.StructField(
                "record",
                types.StructType(
                    [
                        types.StructField("A.b", types.IntegerType()),
                        types.StructField("event_time", types.TimestampNTZType()),
                    ]
                ),
            ),
            types.StructField("a.b", types.BinaryType()),
        ]
    )
    assert frame.select(F.col("`a.b`")).collect()[0][0] == b"\x00\xff"


@pytest.mark.parametrize("operation", ["cache", "persist"])
def test_cache_boundary_leaves_table_query_usable(trino_session, operation):
    session, table = trino_session
    query = session.read.table(table)

    with pytest.raises(UnsupportedOperationError, match=f"Trino.*{operation}"):
        getattr(query, operation)()

    assert query.count() == 3


def test_mixed_native_table_allows_supported_projection(native_session):
    session, orders = native_session
    table = orders.rsplit(".", 1)[0] + ".mixed_types"
    session._cur.execute(
        f"CREATE TABLE {table} AS SELECT 1 AS id, "
        "CAST(NULL AS time(3)) AS clock, CAST(NULL AS timestamp(9)) AS precise, "
        "ROW(1, 'a') AS anonymous, INTERVAL '1' DAY AS duration"
    )
    frame = session.read.table(table)

    assert frame.select("id").collect()[0][0] == 1
    assert frame.select("id").schema.simpleString() == "struct<id:int>"
    for column in ["clock", "precise", "anonymous", "duration"]:
        with pytest.raises(UnsupportedOperationError, match=f"Unsupported Trino type .*{column}"):
            _ = frame.select(column).schema


def test_native_source_casts_survive_public_sql_optimization(native_session):
    session, orders = native_session
    table = orders.rsplit(".", 1)[0] + ".native_casts"
    session._cur.execute(
        f"CREATE TABLE {table} AS SELECT JSON '1' AS j, "
        "UUID '123e4567-e89b-12d3-a456-426614174000' AS u, CAST('abc' AS char(5)) AS c"
    )
    query = session.read.table(table).select(
        *(F.col(column).cast("string").alias(column) for column in ["j", "u", "c"])
    )

    for optimize in [False, True]:
        sql = query.sql(dialect="trino", optimize=optimize)
        assert sql.count("CAST(") == 3
        rows = session._collect(f"DESCRIBE OUTPUT ({sql})")
        assert [row[4] for row in rows] == ["varchar"] * 3
    assert query.collect()[0][0] == "1"
