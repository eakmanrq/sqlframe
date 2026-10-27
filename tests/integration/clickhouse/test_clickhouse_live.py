"""Live connector checks; scripts/test-clickhouse.sh sets SQLFRAME_CLICKHOUSE_HOST/PORT."""

import os
import uuid

import pytest

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.base.session import _BaseSession
from sqlframe.clickhouse import ClickHouseSession
from sqlframe.clickhouse import functions as F

pytestmark = pytest.mark.clickhouse


@pytest.fixture
def clickhouse_session():
    if not (host := os.getenv("SQLFRAME_CLICKHOUSE_HOST")):
        pytest.skip("Set SQLFRAME_CLICKHOUSE_HOST to run against ClickHouse")
    from clickhouse_connect import dbapi

    connection = dbapi.connect(
        host=host,
        port=int(os.getenv("SQLFRAME_CLICKHOUSE_PORT", "18123")),
        username=os.getenv("SQLFRAME_CLICKHOUSE_USER", "default"),
        password=os.getenv("SQLFRAME_CLICKHOUSE_PASSWORD", "sqlframe_test"),
    )
    table = f"sqlframe_events_{uuid.uuid4().hex[:12]}"
    cursor = connection.cursor()
    cursor.execute(f"CREATE TABLE {table} (name String, value Int64, u64 UInt64) ENGINE = Memory")
    cursor.execute(f"INSERT INTO {table} VALUES ('alpha', 2, 18446744073709551615), ('beta', 1, 0)")
    session = ClickHouseSession.builder.config("sqlframe.conn", connection).getOrCreate()
    try:
        yield session, table
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        session.stop()


def test_query_existing_table(clickhouse_session):
    session, table = clickhouse_session
    assert _BaseSession._instance is session

    rows = session.table(table).filter(F.col("value") > 1).select("name", "value").collect()

    assert [row.asDict() for row in rows] == [{"name": "alpha", "value": 2}]


def test_raw_sql_and_catalog_types(clickhouse_session):
    session, table = clickhouse_session
    assert _BaseSession._instance is session

    rows = session.sql(f"SELECT name, u64 FROM {table} WHERE value = 2", qualify=False).collect()
    columns = session.catalog.listColumns(table)
    schema = session.table(table).schema

    assert [row.asDict() for row in rows] == [{"name": "alpha", "u64": 18446744073709551615}]
    assert [(column.name, column.dataType) for column in columns] == [
        ("name", "STRING"),
        ("value", "BIGINT"),
        ("u64", "DECIMAL(20, 0)"),
    ]
    assert schema.simpleString() == "struct<name:string, value:bigint, u64:decimal(20, 0)>"


def test_values_and_aggregation(clickhouse_session):
    session, _ = clickhouse_session
    assert _BaseSession._instance is session

    rows = (
        session.createDataFrame([(1, "a"), (2, "a"), (3, "b")], ["amount", "category"])
        .groupBy("category")
        .agg(F.sum("amount").alias("total"))
        .orderBy("category")
        .collect()
    )

    assert [row.asDict() for row in rows] == [
        {"category": "a", "total": 3},
        {"category": "b", "total": 3},
    ]


def test_create_table_requires_engine(clickhouse_session):
    session, table = clickhouse_session
    target = f"{table}_copy"
    frame = session.createDataFrame([(1, "alpha")], ["id", "label"])

    try:
        with pytest.raises(UnsupportedOperationError, match="clickhouse.engine"):
            frame.write.saveAsTable(target)
        assert session._collect(f"EXISTS TABLE {target}")[0][0] == 0

        frame.write.option("clickhouse.engine", "Memory").saveAsTable(target)
        assert [(c.name, c.dataType) for c in session.catalog.listColumns(target)] == [
            ("id", "BIGINT"),
            ("label", "STRING"),
        ]
        assert [r.asDict() for r in session.table(target).collect()] == [
            {"id": 1, "label": "alpha"}
        ]
    finally:
        session._execute(f"DROP TABLE IF EXISTS {target}")


def test_create_mergetree_with_explicit_order_by(clickhouse_session):
    session, table = clickhouse_session
    target = f"{table}_tree"
    frame = session.table(table).select("value", "name", "u64")
    try:
        frame.write.option("clickhouse.engine", "MergeTree").option(
            "clickhouse.order_by", "value"
        ).saveAsTable(target)
        assert sorted(r["value"] for r in session.table(target).collect()) == [1, 2]
    finally:
        session._execute(f"DROP TABLE IF EXISTS {target}")


def test_append_column_mapping(clickhouse_session):
    session, table = clickhouse_session
    target = f"{table}_mapping"
    session._execute(
        f"CREATE TABLE {target} (left_value Nullable(String), right_value Nullable(String)) "
        "ENGINE = Memory"
    )
    frame = session.createDataFrame([("left", "right")], ["left_value", "right_value"]).select(
        "right_value", "left_value"
    )
    try:
        frame.write.insertInto(target)
        frame.write.saveAsTable(target, mode="append")
        frame.write.mode("append").saveAsTable(target)
        rows = [r.asDict() for r in session.table(target).collect()]
        assert sorted(rows, key=lambda row: (row["left_value"], row["right_value"])) == [
            {"left_value": "left", "right_value": "right"},
            {"left_value": "left", "right_value": "right"},
            {"left_value": "right", "right_value": "left"},
        ]
    finally:
        session._execute(f"DROP TABLE IF EXISTS {target}")


@pytest.mark.parametrize(
    "mode, use_writer_state",
    [
        (None, False),
        ("error", False),
        ("errorifexists", False),
        ("error", True),
        ("errorifexists", True),
    ],
)
def test_existing_table_error_modes_leave_rows_unchanged(
    clickhouse_session, mode, use_writer_state
):
    session, table = clickhouse_session
    frame = session.table(table).filter(F.col("value") > 1)
    before = [r.asDict() for r in session.table(table).orderBy("value").collect()]

    with pytest.raises(FileExistsError, match="ClickHouse table already exists"):
        if use_writer_state:
            frame.write.mode(mode).saveAsTable(table)
        elif mode is None:
            frame.write.saveAsTable(table)
        else:
            frame.write.saveAsTable(table, mode=mode)

    assert [r.asDict() for r in session.table(table).orderBy("value").collect()] == before


def test_unsupported_writes_leave_table_unchanged(clickhouse_session):
    session, table = clickhouse_session
    frame = session.createDataFrame([("gamma", 3, 4)], ["name", "value", "u64"])
    before = [r.asDict() for r in session.table(table).collect()]

    with pytest.raises(UnsupportedOperationError, match="overwrite"):
        frame.write.insertInto(table, overwrite=True)
    with pytest.raises(UnsupportedOperationError, match="overwrite"):
        frame.write.mode("overwrite").saveAsTable(table)
    with pytest.raises(UnsupportedOperationError, match="type"):
        frame.write.saveAsTable(table, mode="append")
    with pytest.raises(UnsupportedOperationError, match="type"):
        frame.write.insertInto(table)
    with pytest.raises(UnsupportedOperationError, match="ignore"):
        frame.write.mode("ignore").saveAsTable(table)
    with pytest.raises(UnsupportedOperationError, match="mode"):
        frame.write.saveAsTable(table, mode="")
    with pytest.raises(UnsupportedOperationError, match="clickhouse.engine"):
        frame.write.saveAsTable(f"{table}_absent", mode="append")

    assert [r.asDict() for r in session.table(table).collect()] == before


def test_unsupported_create_schema_and_engine_do_not_create_table(clickhouse_session):
    session, table = clickhouse_session
    target = f"{table}_unsupported"
    array_frame = session.sql("SELECT [1, 2] AS items", qualify=False)
    scalar_frame = session.createDataFrame([(1,)], ["id"])

    with pytest.raises(UnsupportedOperationError, match="type"):
        array_frame.write.option("clickhouse.engine", "Memory").saveAsTable(target)
    with pytest.raises(UnsupportedOperationError, match="clickhouse.engine"):
        scalar_frame.write.option("clickhouse.engine", "Memory; DROP TABLE events").saveAsTable(
            target
        )
    with pytest.raises(UnsupportedOperationError, match="clickhouse.order_by"):
        scalar_frame.write.option("clickhouse.engine", "MergeTree").saveAsTable(target)
    with pytest.raises(UnsupportedOperationError, match="nullable"):
        scalar_frame.write.option("clickhouse.engine", "MergeTree").option(
            "clickhouse.order_by", "id"
        ).saveAsTable(target)
    with pytest.raises(UnsupportedOperationError, match="clickhouse.order_by"):
        scalar_frame.write.option("clickhouse.engine", "Memory").option(
            "clickhouse.order_by", ""
        ).saveAsTable(target)

    assert session._collect(f"EXISTS TABLE {target}")[0][0] == 0


def test_outer_joins_return_nulls_and_full_join_keys(clickhouse_session):
    session, _ = clickhouse_session
    assert _BaseSession._instance is session
    suffix = uuid.uuid4().hex[:12]
    left_table = f"sqlframe_left_{suffix}"
    right_table = f"sqlframe_right_{suffix}"
    cursor = session._conn.cursor()
    cursor.execute(f"CREATE TABLE {left_table} (id Int64, left_value String) ENGINE = Memory")
    cursor.execute(f"CREATE TABLE {right_table} (id Int64, right_value String) ENGINE = Memory")
    try:
        cursor.execute(f"INSERT INTO {left_table} VALUES (1, 'left'), (2, 'left only')")
        cursor.execute(f"INSERT INTO {right_table} VALUES (1, 'right'), (3, 'right only')")
        left = session.table(left_table)
        right = session.table(right_table)

        left_rows = left.join(right, on="id", how="left").collect()
        right_rows = left.join(right, on="id", how="right").collect()
        full_rows = left.join(right, on="id", how="full").collect()

        assert sorted((row.asDict() for row in left_rows), key=lambda row: row["id"]) == [
            {"id": 1, "left_value": "left", "right_value": "right"},
            {"id": 2, "left_value": "left only", "right_value": None},
        ]
        assert sorted((row.asDict() for row in right_rows), key=lambda row: row["id"]) == [
            {"id": 1, "left_value": "left", "right_value": "right"},
            {"id": 3, "left_value": None, "right_value": "right only"},
        ]
        assert sorted((row.asDict() for row in full_rows), key=lambda row: row["id"]) == [
            {"id": 1, "left_value": "left", "right_value": "right"},
            {"id": 2, "left_value": "left only", "right_value": None},
            {"id": 3, "left_value": None, "right_value": "right only"},
        ]
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS {left_table}")
        cursor.execute(f"DROP TABLE IF EXISTS {right_table}")


@pytest.mark.parametrize("column_name", ["a.b", "a-b", "a b"])
def test_special_column_name_is_read_as_one_identifier(clickhouse_session, column_name):
    session, _ = clickhouse_session
    assert _BaseSession._instance is session
    table = f"sqlframe_special_{uuid.uuid4().hex[:12]}"
    cursor = session._conn.cursor()
    cursor.execute(f"CREATE TABLE {table} (`{column_name}` String, value Int64) ENGINE = Memory")
    try:
        cursor.execute(f"INSERT INTO {table} VALUES ('alpha', 2)")

        columns = session.catalog.listColumns(table)
        frame = session.table(table)
        sql = frame.sql()
        rows = frame.collect()

        assert [column.name for column in columns] == [column_name, "value"]
        assert f"`{column_name}`" in sql
        assert frame.schema.simpleString() == f"struct<{column_name}:string, value:bigint>"
        assert [row.asDict() for row in rows] == [{column_name: "alpha", "value": 2}]
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")


def test_empty_aggregates_follow_spark_null_semantics(clickhouse_session):
    session, table = clickhouse_session
    assert _BaseSession._instance is session

    rows = (
        session.table(table)
        .filter(F.col("value") > 100)
        .groupBy()
        .agg(
            F.sum("value").alias("total"),
            F.avg("value").alias("average"),
            F.min("value").alias("minimum"),
            F.max("value").alias("maximum"),
            F.count("value").alias("number"),
            F.approx_count_distinct("value").alias("approximate_number"),
        )
        .collect()
    )

    assert [row.asDict() for row in rows] == [
        {
            "total": None,
            "average": None,
            "minimum": None,
            "maximum": None,
            "number": 0,
            "approximate_number": 0,
        }
    ]
    approximate_rows = (
        session.table(table)
        .filter(F.col("value") > 100)
        .groupBy()
        .agg(F.approx_count_distinct("value").alias("number"))
        .collect()
    )
    assert [row.asDict() for row in approximate_rows] == [{"number": 0}]


def test_cast_preserves_null_values(clickhouse_session):
    session, _ = clickhouse_session
    assert _BaseSession._instance is session

    rows = session.createDataFrame([(1,), (None,)], ["value"]).collect()

    assert sorted((row.asDict() for row in rows), key=lambda row: row["value"] is None) == [
        {"value": 1},
        {"value": None},
    ]

    table = f"sqlframe_nullable_{uuid.uuid4().hex[:12]}"
    cursor = session._conn.cursor()
    cursor.execute(f"CREATE TABLE {table} (value Nullable(Int64)) ENGINE = Memory")
    try:
        cursor.execute(f"INSERT INTO {table} VALUES (1), (NULL)")
        cast_rows = (
            session.table(table).select(F.col("value").cast("string").alias("text")).collect()
        )
        numeric_rows = (
            session.table(table).select(F.col("value").cast("int").alias("number")).collect()
        )

        assert sorted((row.asDict() for row in cast_rows), key=lambda row: str(row["text"])) == [
            {"text": "1"},
            {"text": None},
        ]
        assert sorted(
            (row.asDict() for row in numeric_rows), key=lambda row: row["number"] is None
        ) == [
            {"number": 1},
            {"number": None},
        ]
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")


def test_uint8_schema_and_array_sort(clickhouse_session):
    session, _ = clickhouse_session
    assert _BaseSession._instance is session
    table = f"sqlframe_uint8_{uuid.uuid4().hex[:12]}"
    cursor = session._conn.cursor()
    cursor.execute(
        f"CREATE TABLE {table} (value UInt8, items Array(UInt8), "
        "optional Nullable(UInt8)) ENGINE = Memory"
    )
    try:
        cursor.execute(f"INSERT INTO {table} VALUES (255, [3, 1, 2], NULL)")
        frame = session.table(table)

        assert frame.schema.simpleString() == (
            "struct<value:smallint, items:array<smallint>, optional:smallint>"
        )
        sorted_frame = frame.select("value", F.array_sort("items").alias("sorted_items"))
        assert "ARRAY_SORT" in sorted_frame.sql()
        assert "arraySort" in sorted_frame.sql(dialect="clickhouse")
        # ClickHouse rendering must not change the DataFrame's Spark expression.
        assert "ARRAY_SORT" in sorted_frame.sql()
        rows = sorted_frame.collect()
        assert [row.asDict() for row in rows] == [{"value": 255, "sorted_items": [1, 2, 3]}]

        nested = frame.select(F.array_sort(F.array_sort("items")).alias("sorted_items"))
        assert "arraySort(arraySort(" in nested.sql(dialect="clickhouse")
        assert [row.asDict() for row in nested.collect()] == [{"sorted_items": [1, 2, 3]}]
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
