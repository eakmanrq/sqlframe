import math

import pytest

from sqlframe.duckdb import DuckDBSession, Window
from sqlframe.duckdb import functions as F


@pytest.fixture
def session():
    return DuckDBSession()


def test_skewness_over_window(session):
    """skewness().over() should wrap each aggregate in the CASE expression with OVER."""
    expr = F.skewness("cola").over(Window.partitionBy("colb"))
    sql = expr.column_expression.sql(dialect="duckdb")
    assert "OVER" in sql
    assert "SKEWNESS" in sql
    # The CASE expression with conversion formula should be preserved in the window version
    assert "CASE" in sql
    # Each aggregate should be individually wrapped with OVER
    assert "SKEWNESS(cola) OVER (PARTITION BY colb)" in sql
    assert "COUNT(cola) OVER (PARTITION BY colb)" in sql


def test_skewness_aggregate(session):
    """skewness() without .over() should still produce the CASE expression for aggregate use."""
    expr = F.skewness("cola")
    sql = expr.column_expression.sql(dialect="duckdb")
    assert "CASE" in sql
    assert "SKEWNESS" in sql


def test_skewness_window_matches_aggregate(session):
    """Window skewness should produce the same values as aggregate skewness (issue #610)."""
    data = [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 2}, {"a": 1}]
    df = session.createDataFrame(data)

    agg_result = df.select(F.skewness("a")).collect()[0][0]
    window_result = (
        df.withColumn("res", F.skewness("a").over(Window.partitionBy(F.lit(1))))
        .select("res")
        .collect()[0][0]
    )
    assert math.isclose(agg_result, window_result)
