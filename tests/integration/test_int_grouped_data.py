import typing as t

import pytest
from pyspark.sql import DataFrame, Row
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


def test_pivot_with_values(
    pyspark_employee: DataFrame,
    compare_frames: t.Callable,
    get_session: t.Callable,
    is_postgres: t.Callable,
):
    """Test pivot with explicit values list"""
    sqlf_spark = get_session()
    if is_postgres():
        pytest.skip("Pivot operation is not supported in Postgres")
    spark = pyspark_employee.sparkSession

    # Create test data based on PySpark documentation example
    df1 = spark.createDataFrame(
        [
            Row(course="dotNET", year=2012, earnings=10000),
            Row(course="Java", year=2012, earnings=20000),
            Row(course="dotNET", year=2012, earnings=5000),
            Row(course="dotNET", year=2013, earnings=48000),
            Row(course="Java", year=2013, earnings=30000),
        ]
    )

    # Create the same DataFrame in SQLFrame
    dfs1 = sqlf_spark.createDataFrame(
        [
            {
                "course": "dotNET",
                "year": 2012,
                "earnings": 10000,
            },
            {
                "course": "Java",
                "year": 2012,
                "earnings": 20000,
            },
            {
                "course": "dotNET",
                "year": 2012,
                "earnings": 5000,
            },
            {
                "course": "dotNET",
                "year": 2013,
                "earnings": 48000,
            },
            {
                "course": "Java",
                "year": 2013,
                "earnings": 30000,
            },
            #
            # Row(course="dotNET", year=2012, earnings=10000),
            # Row(course="Java", year=2012, earnings=20000),
            # Row(course="dotNET", year=2012, earnings=5000),
            # Row(course="dotNET", year=2013, earnings=48000),
            # Row(course="Java", year=2013, earnings=30000),
        ]
    )

    # Test pivot with explicit values
    df_pivot = df1.groupBy("year").pivot("course", ["dotNET", "Java"]).sum("earnings")
    dfs_pivot = dfs1.groupBy("year").pivot("course", ["dotNET", "Java"]).sum("earnings")

    compare_frames(df_pivot, dfs_pivot)


def test_pivot_without_values(
    pyspark_employee: DataFrame,
    compare_frames: t.Callable,
    get_session: t.Callable,
    is_postgres: t.Callable,
):
    """Test pivot without values (auto-detect)"""
    sqlf_spark = get_session()
    if is_postgres():
        pytest.skip("Pivot operation is not supported in Postgres")
    spark = pyspark_employee.sparkSession

    # Create test data based on PySpark documentation example
    df1 = spark.createDataFrame(
        [
            Row(course="dotNET", year=2012, earnings=10000),
            Row(course="Java", year=2012, earnings=20000),
            Row(course="dotNET", year=2012, earnings=5000),
            Row(course="dotNET", year=2013, earnings=48000),
            Row(course="Java", year=2013, earnings=30000),
        ]
    )

    # Create the same DataFrame in SQLFrame
    dfs1 = sqlf_spark.createDataFrame(
        [
            {
                "course": "dotNET",
                "year": 2012,
                "earnings": 10000,
            },
            {
                "course": "Java",
                "year": 2012,
                "earnings": 20000,
            },
            {
                "course": "dotNET",
                "year": 2012,
                "earnings": 5000,
            },
            {
                "course": "dotNET",
                "year": 2013,
                "earnings": 48000,
            },
            {
                "course": "Java",
                "year": 2013,
                "earnings": 30000,
            },
        ]
    )

    # Test pivot without values (auto-detect)
    df_pivot = df1.groupBy("year").pivot("course").sum("earnings")
    dfs_pivot = dfs1.groupBy("year").pivot("course").sum("earnings")

    compare_frames(df_pivot, dfs_pivot)


def test_pivot_multiple_aggregations(
    pyspark_employee: DataFrame,
    compare_frames: t.Callable,
    get_session: t.Callable,
    is_postgres: t.Callable,
    is_snowflake: t.Callable,
    get_func: t.Callable,
):
    """Test pivot with multiple aggregation functions"""
    sqlf_spark = get_session()
    if is_postgres():
        pytest.skip("Pivot operation is not supported in Postgres")
    if is_snowflake():
        pytest.skip("Snowflake does not support pivot with multiple aggregations")
    spark = pyspark_employee.sparkSession

    # Create test data
    df1 = spark.createDataFrame(
        [
            Row(course="dotNET", year=2012, earnings=10000),
            Row(course="Java", year=2012, earnings=20000),
            Row(course="dotNET", year=2012, earnings=5000),
            Row(course="dotNET", year=2013, earnings=48000),
            Row(course="Java", year=2013, earnings=30000),
        ]
    )

    # Create the same DataFrame in SQLFrame
    dfs1 = sqlf_spark.createDataFrame(
        [
            {
                "course": "dotNET",
                "year": 2012,
                "earnings": 10000,
            },
            {
                "course": "Java",
                "year": 2012,
                "earnings": 20000,
            },
            {
                "course": "dotNET",
                "year": 2012,
                "earnings": 5000,
            },
            {
                "course": "dotNET",
                "year": 2013,
                "earnings": 48000,
            },
            {
                "course": "Java",
                "year": 2013,
                "earnings": 30000,
            },
        ]
    )

    # Get functions
    sqlf_sum = get_func("sum", sqlf_spark)
    sqlf_avg = get_func("avg", sqlf_spark)

    # Test pivot with agg() and multiple functions
    df_pivot = (
        df1.groupBy("year")
        .pivot("course", ["dotNET", "Java"])
        .agg(F.sum("earnings").alias("total_earnings"), F.avg("earnings").alias("avg_earnings"))
    )
    dfs_pivot = (
        dfs1.groupBy("year")
        .pivot("course", ["dotNET", "Java"])
        .agg(
            sqlf_sum("earnings").alias("total_earnings"), sqlf_avg("earnings").alias("avg_earnings")
        )
    )

    compare_frames(df_pivot, dfs_pivot)


def test_pivot_without_values_and_selects(
    pyspark_employee: DataFrame,
    compare_frames: t.Callable,
    get_session: t.Callable,
    is_postgres: t.Callable,
):
    """Test pivot without values (auto-detect)"""
    sqlf_spark = get_session()
    if is_postgres():
        pytest.skip("Pivot operation is not supported in Postgres")
    spark = pyspark_employee.sparkSession

    # Create test data based on PySpark documentation example
    df1 = spark.createDataFrame(
        [
            Row(course="dotNET", year=2012, earnings=10000),
            Row(course="Java", year=2012, earnings=20000),
            Row(course="dotNET", year=2012, earnings=5000),
            Row(course="dotNET", year=2013, earnings=48000),
            Row(course="Java", year=2013, earnings=30000),
        ]
    )

    # Create the same DataFrame in SQLFrame
    dfs1 = sqlf_spark.createDataFrame(
        [
            {
                "course": "dotNET",
                "year": 2012,
                "earnings": 10000,
            },
            {
                "course": "Java",
                "year": 2012,
                "earnings": 20000,
            },
            {
                "course": "dotNET",
                "year": 2012,
                "earnings": 5000,
            },
            {
                "course": "dotNET",
                "year": 2013,
                "earnings": 48000,
            },
            {
                "course": "Java",
                "year": 2013,
                "earnings": 30000,
            },
        ]
    )

    # Test pivot without values (auto-detect)
    df_pivot = df1.groupBy("year").pivot("course").sum("earnings").select("Java")
    dfs_pivot = dfs1.groupBy("year").pivot("course").sum("earnings").select("Java")

    compare_frames(df_pivot, dfs_pivot)
