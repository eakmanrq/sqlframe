import pytest

from sqlframe.duckdb import DuckDBSession, Window
from sqlframe.duckdb import functions as F


@pytest.fixture
def session():
    return DuckDBSession()


def test_skewness_over_window(session):
    """skewness().over() should produce valid SQL with OVER clause instead of wrapping the CASE expression."""
    expr = F.skewness("cola").over(Window.partitionBy("colb"))
    sql = expr.column_expression.sql(dialect="duckdb")
    assert "OVER" in sql
    assert "SKEWNESS" in sql
    # Should NOT have CASE in the window version
    assert "CASE" not in sql


def test_skewness_aggregate(session):
    """skewness() without .over() should still produce the CASE expression for aggregate use."""
    expr = F.skewness("cola")
    sql = expr.column_expression.sql(dialect="duckdb")
    assert "CASE" in sql
    assert "SKEWNESS" in sql
