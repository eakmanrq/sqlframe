"""Live Trino checks; set SQLFRAME_TRINO_HOST after starting compose.yml."""

import os
import uuid

import pytest

from sqlframe.base.types import Row
from sqlframe.trino import TrinoSession
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
    session = TrinoSession(conn=conn)
    yield session, f"memory.{schema}.orders"
    cursor.execute(f"DROP SCHEMA memory.{schema} CASCADE")
    session.stop()


def test_query_existing_table(trino_session):
    session, table = trino_session
    session.catalog.add_table(table, {"order_id": "integer", "total": "integer"})

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
