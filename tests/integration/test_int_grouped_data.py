import typing as t

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

pytest_plugins = ["tests.integration.fixtures"]


def test_group_by(
    pyspark_employee: DataFrame,
    compare_frames: t.Callable,
    get_df: t.Callable,
    get_func: t.Callable,
):
    employee = get_df("employee")
    min = get_func("min", employee.session)
    df_employee = pyspark_employee.groupBy(pyspark_employee.age).agg(
        F.min(pyspark_employee.employee_id)
    )
    dfs_employee = employee.groupBy(employee.age).agg(min(employee.employee_id))
    compare_frames(df_employee, dfs_employee, compare_schema=False)


def test_group_by_where_non_aggregate(
    pyspark_employee: DataFrame,
    compare_frames: t.Callable,
    get_df: t.Callable,
    get_func: t.Callable,
):
    employee = get_df("employee")
    min = get_func("min", employee.session)
    col = get_func("col", employee.session)
    lit = get_func("lit", employee.session)
    df_employee = (
        pyspark_employee.groupBy(pyspark_employee.age)
        .agg(F.min(pyspark_employee.employee_id).alias("min_employee_id"))
        .where(F.col("age") > F.lit(50))
    )
    dfs_employee = (
        employee.groupBy(employee.age)
        .agg(min(employee.employee_id).alias("min_employee_id"))
        .where(col("age") > lit(50))
    )
    compare_frames(df_employee, dfs_employee)


def test_group_by_where_aggregate_like_having(
    pyspark_employee: DataFrame,
    compare_frames: t.Callable,
    get_df: t.Callable,
    get_func: t.Callable,
):
    employee = get_df("employee")
    min = get_func("min", employee.session)
    col = get_func("col", employee.session)
    lit = get_func("lit", employee.session)
    df_employee = (
        pyspark_employee.groupBy(pyspark_employee.age)
        .agg(F.min(pyspark_employee.employee_id).alias("min_employee_id"))
        .where(F.col("min_employee_id") > F.lit(1))
    )
    dfs_employee = (
        employee.groupBy(employee.age)
        .agg(min(employee.employee_id).alias("min_employee_id"))
        .where(col("min_employee_id") > lit(1))
    )
    compare_frames(df_employee, dfs_employee)


def test_cube(
    pyspark_employee: DataFrame,
    compare_frames: t.Callable,
    get_df: t.Callable,
    get_func: t.Callable,
    is_postgres: t.Callable,
    is_bigquery: t.Callable,
):
    employee = get_df("employee")
    max = get_func("max", employee.session)
    spark_agg_columns = [F.max(pyspark_employee.fname).alias("last_fname")]
    sqlf_agg_columns = [max(employee.fname).alias("last_fname")]
    if not is_postgres() and not is_bigquery():
        grouping_id = get_func("grouping_id", employee.session)
        spark_agg_columns.append(F.grouping_id().alias("g_id"))
        sqlf_agg_columns.append(grouping_id().alias("g_id"))
    df_employee = (
        pyspark_employee.cube("age", "store_id").agg(*spark_agg_columns).orderBy("age", "store_id")
    )
    dfs_employee = (
        employee.cube("age", "store_id").agg(*sqlf_agg_columns).orderBy("age", "store_id")
    )
    compare_frames(df_employee, dfs_employee)


def test_count(
    pyspark_employee: DataFrame,
    compare_frames: t.Callable,
    get_df: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.groupBy(pyspark_employee.age).count()
    dfs = employee.groupBy(employee.age).count()
    compare_frames(df, dfs)


def test_mean(
    pyspark_employee: DataFrame,
    compare_frames: t.Callable,
    get_df: t.Callable,
    is_redshift: t.Callable,
):
    if is_redshift():
        pytest.skip("Redshift will return an int for avg since the inputs are ints")
    employee = get_df("employee")
    df = pyspark_employee.groupBy().mean("age", "store_id")
    dfs = employee.groupBy().mean("age", "store_id")
    compare_frames(df, dfs)


def test_avg(
    pyspark_employee: DataFrame,
    compare_frames: t.Callable,
    get_df: t.Callable,
    is_redshift: t.Callable,
):
    if is_redshift():
        pytest.skip("Redshift will return an int for avg since the inputs are ints")
    employee = get_df("employee")
    df = pyspark_employee.groupBy("age").avg("store_id")
    dfs = employee.groupBy("age").avg("store_id")
    compare_frames(df, dfs)


def test_max(
    pyspark_employee: DataFrame,
    compare_frames: t.Callable,
    get_df: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.groupBy("age").max("store_id")
    dfs = employee.groupBy("age").max("store_id")
    compare_frames(df, dfs)


def test_min(
    pyspark_employee: DataFrame,
    compare_frames: t.Callable,
    get_df: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.groupBy("age").min("store_id")
    dfs = employee.groupBy("age").min("store_id")
    compare_frames(df, dfs)


def test_sum(
    pyspark_employee: DataFrame,
    compare_frames: t.Callable,
    get_df: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.groupBy("age").sum("store_id")
    dfs = employee.groupBy("age").sum("store_id")
    compare_frames(df, dfs)
