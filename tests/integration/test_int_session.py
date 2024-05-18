import typing as t

import pytest
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import SparkSession as PySparkSession
from pyspark.sql import functions as F

pytest_plugins = ["tests.integration.fixtures"]


def test_sql_simple_select(
    pyspark_session: PySparkSession,
    pyspark_employee: PySparkDataFrame,
    compare_frames: t.Callable,
    get_df: t.Callable,
):
    employee = get_df("employee")
    query = "SELECT fname, lname FROM employee"
    df = pyspark_session.sql(query)
    dfs = employee.session.sql(query)
    compare_frames(df, dfs)


def test_sql_with_join(
    pyspark_session: PySparkSession,
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    compare_frames: t.Callable,
    get_df: t.Callable,
    get_func: t.Callable,
):
    employee = get_df("employee")
    col = get_func("col", employee.session)
    countDistinct = get_func("countDistinct", employee.session)
    store = get_df("store")
    query = """
    SELECT
        e.employee_id
        , s.store_id    
    FROM
        employee e
        INNER JOIN
        store s
        ON
            e.store_id = s.store_id
    """
    df = (
        pyspark_session.sql(query)
        .groupBy(F.col("store_id"))
        .agg(F.countDistinct(F.col("employee_id")))
    )
    dfs = (
        employee.session.sql(query).groupBy(col("store_id")).agg(countDistinct(col("employee_id")))
    )
    compare_frames(df, dfs, compare_schema=False)


def test_nameless_column(
    pyspark_session: PySparkSession,
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    compare_frames: t.Callable,
    get_df: t.Callable,
):
    employee = get_df("employee")
    query = "SELECT MAX(age) FROM employee"
    df = pyspark_session.sql(query)
    dfs = employee.session.sql(query)
    # Spark will alias the column to `max(age)` while sqlglot will alias to `_col_0` so their schemas will differ
    compare_frames(df, dfs, compare_schema=False)
