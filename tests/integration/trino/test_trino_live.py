"""Live Trino checks; set SQLFRAME_TRINO_HOST after starting compose.yml."""

import os
import uuid

import pytest

from sqlframe import activate_context
from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.base.types import Row
from sqlframe.trino import TrinoSession, Window
from sqlframe.trino import functions as F

pytestmark = pytest.mark.trino


@pytest.fixture
def trino_session():
    if not (host := os.getenv("SQLFRAME_TRINO_HOST")):
        pytest.skip("Set SQLFRAME_TRINO_HOST to run against Trino")
    trino = pytest.importorskip("trino")
    conn = trino.dbapi.connect(
        host=host,
        port=int(os.getenv("SQLFRAME_TRINO_PORT", "18080")),
        user="sqlframe",
        catalog="memory",
    )
    schema = f"sqlframe_{uuid.uuid4().hex[:12]}"
    cursor = conn.cursor()
    cursor.execute(f"CREATE SCHEMA memory.{schema}")
    cursor.execute(
        f"CREATE TABLE memory.{schema}.orders AS "
        "SELECT * FROM (VALUES (1, 10), (2, 25), (3, 40)) AS t(order_id, total)"
    )
    cursor.execute(f"USE memory.{schema}")
    session = TrinoSession(conn=conn)
    yield session, f"memory.{schema}.orders"
    cursor.execute(f"DROP SCHEMA memory.{schema} CASCADE")
    session.stop()


def test_query_existing_table(trino_session):
    session, table = trino_session
    columns = session.catalog.listColumns(table)
    assert [(column.name, column.dataType) for column in columns] == [
        ("order_id", "integer"),
        ("total", "integer"),
    ]

    rows = (
        session.read.table(table)
        .where(F.col("total") > 10)
        .select("order_id", "total")
        .orderBy("order_id")
        .collect()
    )

    assert rows == [Row(order_id=2, total=25), Row(order_id=3, total=40)]


def test_values_and_aggregation(trino_session):
    session, _ = trino_session

    rows = (
        session.createDataFrame([(1, "a"), (2, "a"), (3, "b")], ["amount", "category"])
        .groupBy("category")
        .agg(F.sum("amount").alias("total"))
        .orderBy("category")
        .collect()
    )

    assert rows == [Row(category="a", total=3), Row(category="b", total=3)]


def test_catalog_lists_current_schema_and_tables(trino_session):
    session, table = trino_session
    schema = ".".join(table.split(".")[:2])

    assert session.catalog.currentCatalog() == "memory"
    assert session.catalog.currentDatabase().startswith("sqlframe_")
    tables = session.catalog.listTables()
    assert [(table.name, table.tableType) for table in tables] == [("orders", "TABLE")]
    assert [table.name for table in session.catalog.listTables(dbName=schema, pattern="ord*")] == [
        "orders"
    ]
    assert [table.name for table in session.catalog.listTables(pattern="ORD*")] == ["orders"]
    assert [column.name for column in session.catalog.listColumns("orders")] == [
        "order_id",
        "total",
    ]
    assert [column.name for column in session.catalog.listColumns("orders", dbName=schema)] == [
        "order_id",
        "total",
    ]


def test_catalog_normalizes_unquoted_schema_name(trino_session):
    session, table = trino_session
    schema = table.rsplit(".", 1)[0].upper()

    assert [entry.name for entry in session.catalog.listTables(dbName=schema)] == ["orders"]


def test_catalog_includes_temporary_views(trino_session):
    session, _ = trino_session
    session.createDataFrame([(1, "a")], ["id", "label"]).createOrReplaceTempView("local_view")

    tables = session.catalog.listTables(pattern="local_*")
    assert [(table.name, table.isTemporary) for table in tables] == [("local_view", True)]
    assert [column.name for column in session.catalog.listColumns("local_view")] == [
        "id",
        "label",
    ]


def test_catalog_explicit_database_ignores_same_named_temporary_view(trino_session):
    session, table = trino_session
    schema = table.rsplit(".", 1)[0]
    session.createDataFrame([(99,)], ["local_id"]).createOrReplaceTempView("orders")

    assert [column.name for column in session.catalog.listColumns("orders")] == ["local_id"]
    assert [column.name for column in session.catalog.listColumns("orders", dbName=schema)] == [
        "order_id",
        "total",
    ]


def test_catalog_preserves_dotted_column_name(trino_session):
    session, orders = trino_session
    table = orders.rsplit(".", 1)[0] + ".dotted"
    session._cur.execute(f'CREATE TABLE {table} AS SELECT 1 AS "a.b"')

    assert [column.name for column in session.catalog.listColumns(table)] == ['"a.b"']
    assert session.read.table(table).columns == ["a.b"]


def test_activate_trino_with_live_connection(trino_session):
    session, table = trino_session

    with activate_context("trino", conn=session._conn):
        from pyspark.sql import SparkSession

        activated = SparkSession.builder.getOrCreate()
        assert isinstance(activated, TrinoSession)
        assert activated.read.table(table).select("order_id").orderBy("order_id").collect() == [
            Row(order_id=1),
            Row(order_id=2),
            Row(order_id=3),
        ]


def test_create_and_append_table(trino_session):
    session, orders = trino_session
    table = orders.rsplit(".", 1)[0] + ".written"

    session.createDataFrame([(1, "a"), (2, "b")], ["id", "label"]).write.saveAsTable(table)
    session.createDataFrame([(3, "c")], ["id", "label"]).write.insertInto(table)

    assert session.read.table(table).orderBy("id").collect() == [
        Row(id=1, label="a"),
        Row(id=2, label="b"),
        Row(id=3, label="c"),
    ]


def test_explicit_append_overrides_writer_mode(trino_session):
    session, orders = trino_session
    table = orders.rsplit(".", 1)[0] + ".written"
    session.createDataFrame([(1, "a")], ["id", "label"]).write.saveAsTable(table)

    session.createDataFrame([(2, "b")], ["id", "label"]).write.mode("overwrite").saveAsTable(
        table, mode="append"
    )

    assert session.read.table(table).orderBy("id").collect() == [
        Row(id=1, label="a"),
        Row(id=2, label="b"),
    ]


def test_overwrite_is_rejected_without_changing_table(trino_session):
    session, orders = trino_session
    table = orders.rsplit(".", 1)[0] + ".written"
    session.createDataFrame([(1, "a")], ["id", "label"]).write.saveAsTable(table)
    replacement = session.createDataFrame([(2, "b")], ["id", "label"])

    with pytest.raises(UnsupportedOperationError, match="overwrite"):
        replacement.write.mode("overwrite").saveAsTable(table)
    with pytest.raises(UnsupportedOperationError, match="overwrite"):
        replacement.write.saveAsTable(table, mode="overwrite")
    with pytest.raises(UnsupportedOperationError, match="overwrite"):
        replacement.write.insertInto(table, overwrite=True)

    assert session.read.table(table).collect() == [Row(id=1, label="a")]


def test_sql_query_and_join(trino_session):
    session, table = trino_session
    query = session.sql(f"SELECT order_id, total FROM {table} WHERE total >= 25")
    labels = session.createDataFrame([(2, "medium"), (3, "large")], ["order_id", "label"])

    rows = (
        query.join(labels, on="order_id")
        .select("order_id", "total", "label")
        .orderBy("order_id")
        .collect()
    )

    assert rows == [
        Row(order_id=2, total=25, label="medium"),
        Row(order_id=3, total=40, label="large"),
    ]


def test_window_and_temporary_view(trino_session):
    session, table = trino_session
    ranked = session.read.table(table).select(
        "order_id",
        F.row_number().over(Window.orderBy("total")).alias("rank"),
    )
    ranked.createOrReplaceTempView("ranked_orders")

    rows = session.read.table("ranked_orders").orderBy("order_id").collect()

    assert rows == [
        Row(order_id=1, rank=1),
        Row(order_id=2, rank=2),
        Row(order_id=3, rank=3),
    ]


def test_sql_cte(trino_session):
    session, table = trino_session
    query = session.sql(
        f"WITH selected_orders AS (SELECT order_id FROM {table}) "
        "SELECT order_id FROM selected_orders WHERE order_id = 2"
    )

    assert query.collect() == [Row(order_id=2)]


def test_sql_discovers_aliased_source_table(trino_session):
    session, table = trino_session

    assert session.sql(f"SELECT o.order_id FROM {table} AS o WHERE o.order_id = 2").collect() == [
        Row(order_id=2)
    ]


def test_sql_discovers_mixed_case_cte_reference(trino_session):
    session, _ = trino_session

    assert session.sql("WITH ITEMS AS (SELECT 1 AS id) SELECT id FROM items").collect() == [
        Row(id=1)
    ]


@pytest.mark.parametrize("server_first", [True, False])
def test_sql_joins_server_table_and_temp_view_in_either_registration_order(
    trino_session, server_first
):
    session, table = trino_session

    def register_server():
        session.read.table(table)

    def register_view():
        session.createDataFrame(
            [(2, "medium"), (3, "large")],
            [
                "order_id",
                "label",
            ],
        ).createOrReplaceTempView("labels")

    if server_first:
        register_server()
        register_view()
    else:
        register_view()
        register_server()

    rows = session.sql(
        f"SELECT o.*, l.label FROM {table} AS o "
        "JOIN labels AS l ON o.order_id = l.order_id ORDER BY o.order_id"
    ).collect()

    assert rows == [
        Row(order_id=2, total=25, label="medium"),
        Row(order_id=3, total=40, label="large"),
    ]
