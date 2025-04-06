from __future__ import annotations

import typing as t

import pandas as pd
import pytest
from _pytest.fixtures import FixtureRequest
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

from sqlframe.standalone import Window as SWindow
from sqlframe.standalone import functions as SF
from sqlframe.standalone.dataframe import StandaloneDataFrame
from tests.integration.fixtures import StandaloneSession, is_snowflake

if t.TYPE_CHECKING:
    from sqlframe.base.dataframe import BaseDataFrame

    DataFrameMapping = t.Dict[str, BaseDataFrame]

pytest_plugins = ["tests.integration.fixtures"]


def test_empty_df(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    df_empty = pyspark_employee.sparkSession.createDataFrame([], "cola int, colb int")
    dfs_empty = get_df("employee").session.createDataFrame([], "cola int, colb int")
    compare_frames(df_empty, dfs_empty, no_empty=False)


def test_dataframe_from_pandas(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    compare_frames(
        pyspark_employee,
        employee.session.createDataFrame(
            pyspark_employee.toPandas(), schema=pyspark_employee.schema.simpleString()
        ),
    )


def test_simple_select(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df_employee = pyspark_employee.select(F.col("employee_id"))
    dfs_employee = employee.select(SF.col("employee_id"))
    compare_frames(df_employee, dfs_employee)


def test_simple_select_from_table(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee
    dfs = employee.session.read.table("employee")
    compare_frames(df, dfs)


def test_select_star_from_table(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    df = pyspark_employee
    dfs = get_df("employee").session.read.table("employee")
    compare_frames(df, dfs)


def test_simple_select_df_attribute(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df_employee = pyspark_employee.select(pyspark_employee.employee_id)
    dfs_employee = employee.select(employee.employee_id)
    compare_frames(df_employee, dfs_employee)


def test_simple_select_df_dict(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df_employee = pyspark_employee.select(pyspark_employee["employee_id"])
    dfs_employee = employee.select(employee["employee_id"])
    compare_frames(df_employee, dfs_employee)


def test_multiple_selects(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df_employee = pyspark_employee.select(
        pyspark_employee["employee_id"], F.col("fname"), pyspark_employee.lname
    )
    dfs_employee = employee.select(employee["employee_id"], SF.col("fname"), employee.lname)
    compare_frames(df_employee, dfs_employee)


def test_alias_no_op(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df_employee = pyspark_employee.alias("df_employee")
    dfs_employee = employee.alias("dfs_employee")
    compare_frames(df_employee, dfs_employee)


def test_alias_with_select(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df_employee = pyspark_employee.alias("df_employee").select(
        pyspark_employee["employee_id"],
        F.col("df_employee.fname"),
        pyspark_employee.lname,
    )
    dfs_employee = employee.alias("dfs_employee").select(
        employee["employee_id"],
        SF.col("dfs_employee.fname"),
        employee.lname,
    )
    compare_frames(df_employee, dfs_employee)


def test_alias_with_space(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df_employee = (
        pyspark_employee.alias("the employees")
        .select(F.col("the employees.fname").alias("first name"))
        .limit(100)
        .select(F.col("first name").alias("blah blah"))
    )
    dfs_employee = (
        employee.alias("the employees")
        .select(SF.col("the employees.fname").alias("first name"))
        .limit(100)
        .select(SF.col("first name").alias("blah blah"))
    )
    compare_frames(df_employee, dfs_employee)


def test_case_when_otherwise(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select(
        F.when(
            (F.col("age") >= F.lit(40)) & (F.col("age") <= F.lit(60)),
            F.lit("between 40 and 60"),
        )
        .when(F.col("age") < F.lit(40), "less than 40")
        .otherwise("greater than 60")
    )

    dfs = employee.select(
        SF.when(
            (SF.col("age") >= SF.lit(40)) & (SF.col("age") <= SF.lit(60)),
            SF.lit("between 40 and 60"),
        )
        .when(SF.col("age") < SF.lit(40), "less than 40")
        .otherwise("greater than 60")
    )

    compare_frames(df, dfs, compare_schema=False)


def test_case_when_no_otherwise(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select(
        F.when(
            (F.col("age") >= F.lit(40)) & (F.col("age") <= F.lit(60)),
            F.lit("between 40 and 60"),
        ).when(F.col("age") < F.lit(40), "less than 40")
    )

    dfs = employee.select(
        SF.when(
            (SF.col("age") >= SF.lit(40)) & (SF.col("age") <= SF.lit(60)),
            SF.lit("between 40 and 60"),
        ).when(SF.col("age") < SF.lit(40), "less than 40")
    )

    compare_frames(df, dfs, compare_schema=False)


def test_case_when_implicit_lit(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select(
        F.when(
            F.col("fname") == "Jack",
            "name is Jack",
        ).otherwise("Default")
    )

    dfs = employee.select(
        SF.when(
            (SF.col("fname") == "Jack"),
            "name is Jack",
        ).otherwise("Default")
    )

    compare_frames(df, dfs, compare_schema=False)


def test_where_clause_single(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df_employee = pyspark_employee.where(F.col("age") == F.lit(37))
    dfs_employee = employee.where(SF.col("age") == SF.lit(37))
    compare_frames(df_employee, dfs_employee)


def test_where_clause_eq_nullsafe(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df_employee = pyspark_employee.where(F.col("age").eqNullSafe(F.lit(37)))
    dfs_employee = employee.where(SF.col("age") == SF.lit(37))
    compare_frames(df_employee, dfs_employee)


def test_where_clause_multiple_and(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df_employee = pyspark_employee.where(
        (F.col("age") == F.lit(37)) & (F.col("fname") == F.lit("Jack"))
    )
    dfs_employee = employee.where(
        (SF.col("age") == SF.lit(37)) & (SF.col("fname") == SF.lit("Jack"))
    )
    compare_frames(df_employee, dfs_employee)


def test_where_many_and(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df_employee = pyspark_employee.where(
        (F.col("age") == F.lit(37))
        & (F.col("fname") == F.lit("Jack"))
        & (F.col("lname") == F.lit("Shephard"))
        & (F.col("employee_id") == F.lit(1))
    )
    dfs_employee = employee.where(
        (SF.col("age") == SF.lit(37))
        & (SF.col("fname") == SF.lit("Jack"))
        & (SF.col("lname") == SF.lit("Shephard"))
        & (SF.col("employee_id") == SF.lit(1))
    )
    compare_frames(df_employee, dfs_employee)


def test_where_clause_multiple_or(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df_employee = pyspark_employee.where(
        (F.col("age") == F.lit(37)) | (F.col("fname") == F.lit("Kate"))
    )
    dfs_employee = employee.where(
        (SF.col("age") == SF.lit(37)) | (SF.col("fname") == SF.lit("Kate"))
    )
    compare_frames(df_employee, dfs_employee)


def test_where_many_or(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df_employee = pyspark_employee.where(
        (F.col("age") == F.lit(37))
        | (F.col("fname") == F.lit("Kate"))
        | (F.col("lname") == F.lit("Littleton"))
        | (F.col("employee_id") == F.lit(2))
    )
    dfs_employee = employee.where(
        (SF.col("age") == SF.lit(37))
        | (SF.col("fname") == SF.lit("Kate"))
        | (SF.col("lname") == SF.lit("Littleton"))
        | (SF.col("employee_id") == SF.lit(2))
    )
    compare_frames(df_employee, dfs_employee)


def test_where_mixed_and_or(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df_employee = pyspark_employee.where(
        ((F.col("age") == F.lit(65)) & (F.col("fname") == F.lit("John")))
        | ((F.col("lname") == F.lit("Shephard")) & (F.col("age") == F.lit(37)))
    )
    dfs_employee = employee.where(
        ((SF.col("age") == SF.lit(65)) & (SF.col("fname") == SF.lit("John")))
        | ((SF.col("lname") == SF.lit("Shephard")) & (SF.col("age") == SF.lit(37)))
    )
    compare_frames(df_employee, dfs_employee)


def test_where_multiple_chained(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df_employee = pyspark_employee.where(F.col("age") == F.lit(37)).where(
        pyspark_employee.fname == F.lit("Jack")
    )
    dfs_employee = employee.where(SF.col("age") == SF.lit(37)).where(
        employee.fname == SF.lit("Jack")
    )
    compare_frames(df_employee, dfs_employee)


def test_where_sql_expr(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df_employee = pyspark_employee.where("age = 37 AND fname = 'Jack'")
    dfs_employee = employee.where("age = 37 AND fname = 'Jack'")
    compare_frames(df_employee, dfs_employee)


def test_operators(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df_employee = pyspark_employee.where(pyspark_employee["age"] < F.lit(50))
    dfs_employee = employee.where(employee["age"] < SF.lit(50))
    compare_frames(df_employee, dfs_employee)

    df_employee = pyspark_employee.where(pyspark_employee["age"] <= F.lit(37))
    dfs_employee = employee.where(employee["age"] <= SF.lit(37))
    compare_frames(df_employee, dfs_employee)

    df_employee = pyspark_employee.where(pyspark_employee["age"] > F.lit(50))
    dfs_employee = employee.where(employee["age"] > SF.lit(50))
    compare_frames(df_employee, dfs_employee)

    df_employee = pyspark_employee.where(pyspark_employee["age"] >= F.lit(37))
    dfs_employee = employee.where(employee["age"] >= SF.lit(37))
    compare_frames(df_employee, dfs_employee)

    df_employee = pyspark_employee.where(pyspark_employee["age"] != F.lit(50))
    dfs_employee = employee.where(employee["age"] != SF.lit(50))
    compare_frames(df_employee, dfs_employee)

    df_employee = pyspark_employee.where(pyspark_employee["age"] == F.lit(37))
    dfs_employee = employee.where(employee["age"] == SF.lit(37))
    compare_frames(df_employee, dfs_employee)

    df_employee = pyspark_employee.where(pyspark_employee["age"] % F.lit(5) == F.lit(0))
    dfs_employee = employee.where(employee["age"] % SF.lit(5) == SF.lit(0))
    compare_frames(df_employee, dfs_employee)

    df_employee = pyspark_employee.where(pyspark_employee["age"] + F.lit(5) > F.lit(28))
    dfs_employee = employee.where(employee["age"] + SF.lit(5) > SF.lit(28))
    compare_frames(df_employee, dfs_employee)

    df_employee = pyspark_employee.where(pyspark_employee["age"] - F.lit(5) > F.lit(28))
    dfs_employee = employee.where(employee["age"] - SF.lit(5) > SF.lit(28))
    compare_frames(df_employee, dfs_employee)

    df_employee = pyspark_employee.where(
        pyspark_employee["age"] * F.lit(0.5) == pyspark_employee["age"] / F.lit(2)
    )
    dfs_employee = employee.where(employee["age"] * SF.lit(0.5) == employee["age"] / SF.lit(2))
    compare_frames(df_employee, dfs_employee)


def test_join_inner(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    store = get_df("store")
    df_joined = pyspark_employee.join(other=pyspark_store, on=["store_id"], how="inner").select(
        pyspark_employee.employee_id,
        pyspark_employee["fname"],
        F.col("lname"),
        F.col("age"),
        F.coalesce("store_id", "age").alias("store_id_size"),
        pyspark_store.store_name,
        pyspark_store["num_sales"],
    )
    dfs_joined = employee.join(store, on=["store_id"], how="inner").select(
        employee.employee_id,
        employee["fname"],
        SF.col("lname"),
        SF.col("age"),
        SF.coalesce("store_id", "age").alias("store_id_size"),
        store.store_name,
        store["num_sales"],
    )
    compare_frames(df_joined, dfs_joined, sort=True)


@pytest.mark.parametrize(
    "how",
    [
        "inner",
        "cross",
        "outer",
        "full",
        "fullouter",
        "full_outer",
        "left",
        "leftouter",
        "left_outer",
        "right",
        "rightouter",
        "right_outer",
        "semi",
        "leftsemi",
        "left_semi",
        "anti",
        "leftanti",
        "left_anti",
    ],
)
def test_join_various_how(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
    how: str,
):
    employee = get_df("employee")
    store = get_df("store")
    df_joined = pyspark_employee.join(pyspark_store, on=["store_id"], how=how)
    dfs_joined = employee.join(store, on=["store_id"], how=how)
    compare_frames(df_joined, dfs_joined, sort=True)


def test_join_inner_no_select(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    store = get_df("store")
    df_joined = pyspark_employee.select(F.col("store_id"), F.col("fname"), F.col("lname")).join(
        pyspark_store.select(F.col("store_id"), F.col("store_name")),
        on=["store_id"],
        how="inner",
    )
    dfs_joined = employee.select(SF.col("store_id"), SF.col("fname"), SF.col("lname")).join(
        store.select(SF.col("store_id"), SF.col("store_name")),
        on=["store_id"],
        how="inner",
    )
    compare_frames(df_joined, dfs_joined, sort=True)


def test_join_inner_equality_single(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    store = get_df("store")
    df_joined = pyspark_employee.join(
        pyspark_store,
        on=pyspark_employee.store_id == pyspark_store.store_id,
        how="inner",
    ).select(
        pyspark_employee.employee_id,
        pyspark_employee["fname"],
        F.col("lname"),
        F.col("age"),
        pyspark_employee.store_id,
        pyspark_store.store_name,
        pyspark_store["num_sales"],
        F.lit("literal_value"),
    )
    dfs_joined = employee.join(
        store,
        on=employee.store_id == store.store_id,
        how="inner",
    ).select(
        employee.employee_id,
        employee["fname"],
        SF.col("lname"),
        SF.col("age"),
        employee.store_id,
        store.store_name,
        store["num_sales"],
        SF.lit("literal_value"),
    )
    compare_frames(df_joined, dfs_joined, sort=True)


def test_join_inner_equality_multiple(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    store = get_df("store")
    df_joined = pyspark_employee.join(
        pyspark_store,
        on=[
            pyspark_employee.store_id == pyspark_store.store_id,
            pyspark_employee.age == pyspark_store.num_sales,
        ],
        how="inner",
    ).select(
        pyspark_employee.employee_id,
        pyspark_employee["fname"],
        F.col("lname"),
        F.col("age"),
        pyspark_employee.store_id,
        pyspark_store.store_name,
        pyspark_store["num_sales"],
    )
    dfs_joined = employee.join(
        store,
        on=[
            employee.store_id == store.store_id,
            employee.age == store.num_sales,
        ],
        how="inner",
    ).select(
        employee.employee_id,
        employee["fname"],
        SF.col("lname"),
        SF.col("age"),
        employee.store_id,
        store.store_name,
        store["num_sales"],
    )
    compare_frames(df_joined, dfs_joined, sort=True)


def test_join_inner_equality_multiple_bitwise_and(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    store = get_df("store")
    df_joined = pyspark_employee.join(
        pyspark_store,
        on=(pyspark_store.store_id == pyspark_employee.store_id)
        & (pyspark_store.num_sales == pyspark_employee.age),
        how="inner",
    ).select(
        pyspark_employee.employee_id,
        pyspark_employee["fname"],
        F.col("lname"),
        F.col("age"),
        pyspark_employee.store_id,
        pyspark_store.store_name,
        pyspark_store["num_sales"],
    )
    dfs_joined = employee.join(
        store,
        on=(store.store_id == employee.store_id) & (store.num_sales == employee.age),
        how="inner",
    ).select(
        employee.employee_id,
        employee["fname"],
        SF.col("lname"),
        SF.col("age"),
        employee.store_id,
        store.store_name,
        store["num_sales"],
    )
    compare_frames(df_joined, dfs_joined, sort=True)


def test_join_left_outer(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    store = get_df("store")
    df_joined = (
        pyspark_employee.join(pyspark_store, on=["store_id"], how="left_outer")
        .select(
            pyspark_employee.employee_id,
            pyspark_employee["fname"],
            F.col("lname"),
            F.col("age"),
            F.col("store_id"),
            pyspark_store.store_name,
            pyspark_store["num_sales"],
        )
        .orderBy(F.col("employee_id"))
    )
    dfs_joined = (
        employee.join(store, on=["store_id"], how="left_outer")
        .select(
            employee.employee_id,
            employee["fname"],
            SF.col("lname"),
            SF.col("age"),
            SF.col("store_id"),
            store.store_name,
            store["num_sales"],
        )
        .orderBy(SF.col("employee_id"))
    )
    compare_frames(df_joined, dfs_joined)


def test_join_full_outer(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
    is_bigquery: t.Callable,
):
    if is_bigquery():
        pytest.skip(
            "BigQuery doesn't support full outer joins on unnested arrays. You get `Array scan is not allowed with FULL JOIN`"
        )
    employee = get_df("employee")
    store = get_df("store")
    df_joined = pyspark_employee.join(pyspark_store, on=["store_id"], how="full_outer").select(
        pyspark_employee.employee_id,
        pyspark_employee["fname"],
        F.col("lname"),
        F.col("age"),
        F.col("store_id"),
        pyspark_store.store_name,
        pyspark_store["num_sales"],
    )
    dfs_joined = employee.join(store, on=["store_id"], how="full_outer").select(
        employee.employee_id,
        employee["fname"],
        SF.col("lname"),
        SF.col("age"),
        SF.col("store_id"),
        store.store_name,
        store["num_sales"],
    )
    compare_frames(df_joined, dfs_joined, sort=True)


def test_triple_join(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    pyspark_district: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    store = get_df("store")
    district = get_df("district")
    df = (
        pyspark_employee.join(
            pyspark_store, on=pyspark_employee.employee_id == pyspark_store.store_id
        )
        .join(pyspark_district, on=pyspark_store.store_id == pyspark_district.district_id)
        .select(
            pyspark_employee.employee_id,
            pyspark_store.store_id,
            pyspark_district.district_id,
            pyspark_employee.fname,
            pyspark_store.store_name,
            pyspark_district.district_name,
        )
    )
    dfs = (
        employee.join(store, on=employee.employee_id == store.store_id)
        .join(district, on=store.store_id == district.district_id)
        .select(
            employee.employee_id,
            store.store_id,
            district.district_id,
            employee.fname,
            store.store_name,
            district.district_name,
        )
    )
    compare_frames(df, dfs, sort=True)


def test_triple_join_no_select(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    pyspark_district: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
    is_duckdb: t.Callable,
    is_postgres: t.Callable,
    is_redshift: t.Callable,
    is_snowflake: t.Callable,
    is_spark: t.Callable,
):
    if is_duckdb():
        pytest.skip(
            "Duckdb doesn't support duplicate column names and they will just be reduced to a single column"
        )
    if is_postgres():
        pytest.skip(
            "Postgres doesn't support duplicate column names and they will just be reduced to a single column"
        )
    if is_redshift():
        pytest.skip(
            "Redshift doesn't support duplicate column names and they will just be reduced to a single column"
        )
    if is_snowflake():
        pytest.skip(
            "Snowflake doesn't support duplicate column names and they will just be reduced to a single column"
        )
    if is_spark():
        pytest.skip("Spark doesn't support duplicate column names")
    employee = get_df("employee")
    store = get_df("store")
    district = get_df("district")
    df = (
        pyspark_employee.join(
            pyspark_store,
            on=pyspark_employee["employee_id"] == pyspark_store["store_id"],
            how="left",
        )
        .join(
            pyspark_district,
            on=pyspark_store["store_id"] == pyspark_district["district_id"],
            how="left",
        )
        .orderBy(F.col("employee_id"))
    )
    dfs = (
        employee.join(
            store,
            on=employee["employee_id"] == store["store_id"],
            how="left",
        )
        .join(
            district,
            on=store["store_id"] == district["district_id"],
            how="left",
        )
        .orderBy(SF.col("employee_id"))
    )
    compare_frames(df, dfs, sort=True)


def test_triple_joins_filter(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    pyspark_district: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
    is_duckdb: t.Callable,
    is_postgres: t.Callable,
    is_redshift: t.Callable,
    is_snowflake: t.Callable,
    is_spark: t.Callable,
):
    if is_duckdb():
        pytest.skip(
            "Duckdb doesn't support duplicate column names and they will just be reduced to a single column where the last one wins"
        )
    if is_postgres():
        pytest.skip(
            "Postgres doesn't support duplicate column names and they will just be reduced to a single column"
        )
    if is_redshift():
        pytest.skip(
            "Redshift doesn't support duplicate column names and they will just be reduced to a single column"
        )
    if is_snowflake():
        pytest.skip(
            "Snowflake doesn't support duplicate column names and they will just be reduced to a single column"
        )
    if is_spark():
        pytest.skip("Spark doesn't support duplicate column names")
    employee = get_df("employee")
    store = get_df("store")
    district = get_df("district")
    df = (
        pyspark_employee.join(
            pyspark_store,
            on=pyspark_employee["employee_id"] == pyspark_store["store_id"],
            how="left",
        ).join(
            pyspark_district,
            on=pyspark_store["store_id"] == pyspark_district["district_id"],
            how="left",
        )
    ).filter(F.coalesce(pyspark_store["num_sales"], F.lit(0)) > 100)
    dfs = (
        employee.join(
            store,
            on=employee["employee_id"] == store["store_id"],
            how="left",
        ).join(
            district,
            on=store["store_id"] == district["district_id"],
            how="left",
        )
    ).filter(SF.coalesce(store["num_sales"], SF.lit(0)) > 100)
    compare_frames(df, dfs, sort=True)


def test_triple_join_column_name_only(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    pyspark_district: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
    is_duckdb: t.Callable,
    is_postgres: t.Callable,
    is_redshift: t.Callable,
    is_snowflake: t.Callable,
    is_spark: t.Callable,
):
    if is_duckdb():
        pytest.skip(
            "Duckdb doesn't support duplicate column names and they will just be reduced to a single column where the last one wins"
        )
    if is_postgres():
        pytest.skip(
            "Postgres doesn't support duplicate column names and they will just be reduced to a single column"
        )
    if is_redshift():
        pytest.skip(
            "Redshift doesn't support duplicate column names and they will just be reduced to a single column"
        )
    if is_snowflake():
        pytest.skip(
            "Snowflake doesn't support duplicate column names and they will just be reduced to a single column"
        )
    if is_spark():
        pytest.skip("Spark doesn't support duplicate column names")
    employee = get_df("employee")
    store = get_df("store")
    district = get_df("district")
    df = (
        pyspark_employee.join(
            pyspark_store,
            on=pyspark_employee["employee_id"] == pyspark_store["store_id"],
            how="left",
        )
        .join(pyspark_district, on="district_id", how="left")
        .orderBy(F.col("employee_id"))
    )
    dfs = (
        employee.join(
            store,
            on=employee["employee_id"] == store["store_id"],
            how="left",
        )
        .join(district, on="district_id", how="left")
        .orderBy(SF.col("employee_id"))
    )
    compare_frames(df, dfs, sort=True)


def test_join_select_and_select_start(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    store = get_df("store")
    df = pyspark_employee.select(
        F.col("fname"), F.col("lname"), F.col("age"), F.col("store_id")
    ).join(pyspark_store, "store_id", "inner")

    dfs = employee.select(SF.col("fname"), SF.col("lname"), SF.col("age"), SF.col("store_id")).join(
        store, "store_id", "inner"
    )

    compare_frames(df, dfs, sort=True)


def test_join_no_on(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    # No on results in a cross. Testing that "how" is ignored
    pyspark_employee = pyspark_employee.select(F.col("fname"), F.col("lname"))
    pyspark_store = pyspark_store.select(F.col("store_id"), F.col("num_sales"))
    employee = get_df("employee").select(SF.col("fname"), SF.col("lname"))
    store = get_df("store").select(SF.col("store_id"), SF.col("num_sales"))
    df = pyspark_employee.join(pyspark_store, how="inner")
    dfs = employee.join(store, how="inner")

    compare_frames(df, dfs)


def test_cross_join(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    # No on results in a cross. Testing that "how" is ignored
    pyspark_employee = pyspark_employee.select(F.col("fname"), F.col("lname"))
    pyspark_store = pyspark_store.select(F.col("store_id"), F.col("num_sales"))
    employee = get_df("employee").select(SF.col("fname"), SF.col("lname"))
    store = get_df("store").select(SF.col("store_id"), SF.col("num_sales"))
    df = pyspark_employee.crossJoin(pyspark_store)
    dfs = employee.crossJoin(store)

    compare_frames(df, dfs)


def test_branching_root_dataframes(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
    is_duckdb: t.Callable,
    is_postgres: t.Callable,
    is_redshift: t.Callable,
    is_snowflake: t.Callable,
    is_spark: t.Callable,
):
    """
    Test a pattern that has non-intuitive behavior in spark

    Scenario: You do a self-join in a dataframe using an original dataframe and then a modified version
    of it. You then reference the columns by the dataframe name instead of the column function.
    Spark will use the root dataframe's column in the result.
    """
    if is_duckdb():
        pytest.skip(
            "Duckdb doesn't support duplicate column names and they will just be reduced to a single column"
        )
    if is_postgres():
        pytest.skip(
            "Postgres doesn't support duplicate column names and they will just be reduced to a single column"
        )
    if is_redshift():
        pytest.skip(
            "Redshift doesn't support duplicate column names and they will just be reduced to a single column"
        )
    if is_snowflake():
        pytest.skip(
            "Snowflake doesn't support duplicate column names and they will just be reduced to a single column"
        )
    if is_spark():
        pytest.skip(
            "Spark doesn't support duplicate column names and they will just be reduced to a single column."
        )
    employee = get_df("employee")
    df_hydra_employees_only = pyspark_employee.where(F.col("store_id") == F.lit(1))
    df_joined = (
        pyspark_employee.where(F.col("store_id") == F.lit(2))
        .alias("df_arrow_employees_only")
        .join(
            df_hydra_employees_only.alias("df_hydra_employees_only"),
            on=["store_id"],
            how="full_outer",
        )
        .select(
            pyspark_employee.fname,
            F.col("df_arrow_employees_only.fname"),
            df_hydra_employees_only.fname,
            F.col("df_hydra_employees_only.fname"),
        )
    )

    dfs_hydra_employees_only = employee.where(SF.col("store_id") == SF.lit(1))
    dfs_joined = (
        employee.where(SF.col("store_id") == SF.lit(2))
        .alias("dfs_arrow_employees_only")
        .join(
            dfs_hydra_employees_only.alias("dfs_hydra_employees_only"),
            on=["store_id"],
            how="full_outer",
        )
        .select(
            employee.fname,
            SF.col("dfs_arrow_employees_only.fname"),
            dfs_hydra_employees_only.fname,
            SF.col("dfs_hydra_employees_only.fname"),
        )
    )
    compare_frames(df_joined, dfs_joined, sort=True)


def test_basic_union(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    store = get_df("store")
    df_unioned = pyspark_employee.select(F.col("employee_id"), F.col("age")).union(
        pyspark_store.select(F.col("store_id"), F.col("num_sales"))
    )

    dfs_unioned = employee.select(SF.col("employee_id"), SF.col("age")).union(
        store.select(SF.col("store_id"), SF.col("num_sales"))
    )
    compare_frames(df_unioned, dfs_unioned)


def test_union_with_join(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    pyspark_district: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    store = get_df("store")
    district = get_df("district")
    df_joined = pyspark_employee.join(
        pyspark_store,
        on="store_id",
        how="inner",
    )
    df_unioned = df_joined.select(F.col("store_id"), F.col("store_name")).union(
        pyspark_district.select(F.col("district_id"), F.col("district_name"))
    )

    dfs_joined = employee.join(
        store,
        on="store_id",
        how="inner",
    )
    dfs_unioned = dfs_joined.select(SF.col("store_id"), SF.col("store_name")).union(
        district.select(SF.col("district_id"), SF.col("district_name"))
    )

    compare_frames(df_unioned, dfs_unioned)


def test_double_union_all(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    pyspark_district: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    store = get_df("store")
    district = get_df("district")
    df_unioned = (
        pyspark_employee.select(F.col("employee_id"), F.col("fname"))
        .unionAll(pyspark_store.select(F.col("store_id"), F.col("store_name")))
        .unionAll(pyspark_district.select(F.col("district_id"), F.col("district_name")))
    )

    dfs_unioned = (
        employee.select(SF.col("employee_id"), SF.col("fname"))
        .unionAll(store.select(SF.col("store_id"), SF.col("store_name")))
        .unionAll(district.select(SF.col("district_id"), SF.col("district_name")))
    )

    compare_frames(df_unioned, dfs_unioned)


def test_union_by_name(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    store = get_df("store")
    df = pyspark_employee.select(F.col("employee_id"), F.col("fname"), F.col("lname")).unionByName(
        pyspark_store.select(
            F.col("store_name").alias("lname"),
            F.col("store_id").alias("employee_id"),
            F.col("store_name").alias("fname"),
        )
    )

    dfs = employee.select(SF.col("employee_id"), SF.col("fname"), SF.col("lname")).unionByName(
        store.select(
            SF.col("store_name").alias("lname"),
            SF.col("store_id").alias("employee_id"),
            SF.col("store_name").alias("fname"),
        )
    )

    compare_frames(df, dfs)


def test_union_by_name_allow_missing(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
    is_postgres: t.Callable,
):
    if is_postgres():
        pytest.skip(
            "For columns that are missing they are replaced with null and then postgres errors complaining about unioning null with another type"
        )
    employee = get_df("employee")
    store = get_df("store")
    df = pyspark_employee.select(
        F.col("age"), F.col("employee_id"), F.col("fname"), F.col("lname")
    ).unionByName(
        pyspark_store.select(
            F.col("store_name").alias("lname"),
            F.col("store_id").alias("employee_id"),
            F.col("store_name").alias("fname"),
            F.col("num_sales"),
        ),
        allowMissingColumns=True,
    )

    dfs = employee.select(
        SF.col("age"), SF.col("employee_id"), SF.col("fname"), SF.col("lname")
    ).unionByName(
        store.select(
            SF.col("store_name").alias("lname"),
            SF.col("store_id").alias("employee_id"),
            SF.col("store_name").alias("fname"),
            SF.col("num_sales"),
        ),
        allowMissingColumns=True,
    )

    compare_frames(df, dfs)


def test_order_by_default(
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    store = get_df("store")
    df = (
        pyspark_store.groupBy(F.col("district_id"))
        .agg(F.min("num_sales"))
        .orderBy(F.col("district_id"))
    )

    dfs = (
        store.groupBy(SF.col("district_id")).agg(SF.min("num_sales")).orderBy(SF.col("district_id"))
    )

    compare_frames(df, dfs, compare_schema=False)


def test_order_by_array_bool(
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    store = get_df("store")
    df = (
        pyspark_store.groupBy(F.col("district_id"))
        .agg(F.min("num_sales").alias("total_sales"))
        .orderBy(F.col("total_sales"), F.col("district_id"), ascending=[1, 0])
    )

    dfs = (
        store.groupBy(SF.col("district_id"))
        .agg(SF.min("num_sales").alias("total_sales"))
        .orderBy(SF.col("total_sales"), SF.col("district_id"), ascending=[1, 0])
    )

    compare_frames(df, dfs)


def test_order_by_single_bool(
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    store = get_df("store")
    df = (
        pyspark_store.groupBy(F.col("district_id"))
        .agg(F.min("num_sales").alias("total_sales"))
        .orderBy(F.col("total_sales"), F.col("district_id"), ascending=False)
    )

    dfs = (
        store.groupBy(SF.col("district_id"))
        .agg(SF.min("num_sales").alias("total_sales"))
        .orderBy(SF.col("total_sales"), SF.col("district_id"), ascending=False)
    )

    compare_frames(df, dfs)


def test_order_by_column_sort_method(
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    store = get_df("store")
    df = (
        pyspark_store.groupBy(F.col("district_id"))
        .agg(F.min("num_sales").alias("total_sales"))
        .orderBy(F.col("total_sales").asc(), F.col("district_id").desc())
    )

    dfs = (
        store.groupBy(SF.col("district_id"))
        .agg(SF.min("num_sales").alias("total_sales"))
        .orderBy(SF.col("total_sales").asc(), SF.col("district_id").desc())
    )

    compare_frames(df, dfs)


def test_order_by_column_sort_method_nulls_last(
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    store = get_df("store")
    df = (
        pyspark_store.groupBy(F.col("district_id"))
        .agg(F.min("num_sales").alias("total_sales"))
        .orderBy(F.when(F.col("district_id") == F.lit(2), F.col("district_id")).asc_nulls_last())
    )

    dfs = (
        store.groupBy(SF.col("district_id"))
        .agg(SF.min("num_sales").alias("total_sales"))
        .orderBy(
            SF.when(SF.col("district_id") == SF.lit(2), SF.col("district_id")).asc_nulls_last()
        )
    )

    compare_frames(df, dfs)


def test_order_by_column_sort_method_nulls_first(
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    store = get_df("store")
    df = (
        pyspark_store.groupBy(F.col("district_id"))
        .agg(F.min("num_sales").alias("total_sales"))
        .orderBy(F.when(F.col("district_id") == F.lit(1), F.col("district_id")).desc_nulls_first())
    )

    dfs = (
        store.groupBy(SF.col("district_id"))
        .agg(SF.min("num_sales").alias("total_sales"))
        .orderBy(
            SF.when(SF.col("district_id") == SF.lit(1), SF.col("district_id")).desc_nulls_first()
        )
    )

    compare_frames(df, dfs)


def test_intersect(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    store = get_df("store")
    df_employee_duplicate = pyspark_employee.select(F.col("employee_id"), F.col("store_id")).union(
        pyspark_employee.select(F.col("employee_id"), F.col("store_id"))
    )

    df_store_duplicate = pyspark_store.select(F.col("store_id"), F.col("district_id")).union(
        pyspark_store.select(F.col("store_id"), F.col("district_id"))
    )

    df = df_employee_duplicate.intersect(df_store_duplicate)

    dfs_employee_duplicate = employee.select(SF.col("employee_id"), SF.col("store_id")).union(
        employee.select(SF.col("employee_id"), SF.col("store_id"))
    )

    dfs_store_duplicate = store.select(SF.col("store_id"), SF.col("district_id")).union(
        store.select(SF.col("store_id"), SF.col("district_id"))
    )

    dfs = dfs_employee_duplicate.intersect(dfs_store_duplicate)

    compare_frames(df, dfs)


def test_intersect_all(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
    is_bigquery: t.Callable,
    is_redshift: t.Callable,
    is_snowflake: t.Callable,
):
    if is_bigquery():
        pytest.skip("BigQuery doesn't support INTERSECT ALL. Only INTERSECT DISTINCT is supported")
    if is_redshift():
        pytest.skip("Redshift doesn't support INTERSECT ALL. Only INTERSECT DISTINCT is supported")
    if is_snowflake():
        pytest.skip("Snowflake doesn't support INTERSECT ALL. Only INTERSECT DISTINCT is supported")
    employee = get_df("employee")
    store = get_df("store")
    df_employee_duplicate = pyspark_employee.select(F.col("employee_id"), F.col("store_id")).union(
        pyspark_employee.select(F.col("employee_id"), F.col("store_id"))
    )

    df_store_duplicate = pyspark_store.select(F.col("store_id"), F.col("district_id")).union(
        pyspark_store.select(F.col("store_id"), F.col("district_id"))
    )

    df = df_employee_duplicate.intersectAll(df_store_duplicate)

    dfs_employee_duplicate = employee.select(SF.col("employee_id"), SF.col("store_id")).union(
        employee.select(SF.col("employee_id"), SF.col("store_id"))
    )

    dfs_store_duplicate = store.select(SF.col("store_id"), SF.col("district_id")).union(
        store.select(SF.col("store_id"), SF.col("district_id"))
    )

    dfs = dfs_employee_duplicate.intersectAll(dfs_store_duplicate)

    compare_frames(df, dfs)


def test_except_all(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
    is_bigquery: t.Callable,
    is_redshift: t.Callable,
    is_snowflake: t.Callable,
):
    if is_bigquery():
        pytest.skip("BigQuery doesn't support EXCEPT ALL. Only EXCEPT DISTINCT is supported")
    if is_redshift():
        pytest.skip("Redshift doesn't support EXCEPT ALL. Only EXCEPT DISTINCT is supported")
    if is_snowflake():
        pytest.skip("Snowflake doesn't support EXCEPT ALL. Only EXCEPT DISTINCT is supported")
    employee = get_df("employee")
    store = get_df("store")
    df_employee_duplicate = pyspark_employee.select(F.col("employee_id"), F.col("store_id")).union(
        pyspark_employee.select(F.col("employee_id"), F.col("store_id"))
    )

    df_store_duplicate = pyspark_store.select(F.col("store_id"), F.col("district_id")).union(
        pyspark_store.select(F.col("store_id"), F.col("district_id"))
    )

    df = df_employee_duplicate.exceptAll(df_store_duplicate)

    dfs_employee_duplicate = employee.select(SF.col("employee_id"), SF.col("store_id")).union(
        employee.select(SF.col("employee_id"), SF.col("store_id"))
    )

    dfs_store_duplicate = store.select(SF.col("store_id"), SF.col("district_id")).union(
        store.select(SF.col("store_id"), SF.col("district_id"))
    )

    dfs = dfs_employee_duplicate.exceptAll(dfs_store_duplicate)

    compare_frames(df, dfs)


def test_distinct(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select(F.col("age")).distinct()

    dfs = employee.select(SF.col("age")).distinct()

    compare_frames(df, dfs)


def test_union_distinct(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df_unioned = (
        pyspark_employee.select(F.col("employee_id"), F.col("age"))
        .union(pyspark_employee.select(F.col("employee_id"), F.col("age")))
        .distinct()
    )

    dfs_unioned = (
        employee.select(SF.col("employee_id"), SF.col("age"))
        .union(employee.select(SF.col("employee_id"), SF.col("age")))
        .distinct()
    )
    compare_frames(df_unioned, dfs_unioned)


def test_drop_duplicates_no_subset(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select("age").dropDuplicates()
    dfs = employee.select("age").dropDuplicates()
    compare_frames(df, dfs)


def test_drop_duplicates_subset(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
    is_redshift: t.Callable,
):
    if is_redshift():
        # The expected behavior for drop duplicates with subset is not very clear since there is no explicit ordering
        # As a result Redshift drops the first duplicate. Could consider changing this behavior but this function
        # has general determinism issue so just skipping for now.
        pytest.skip("Redshift doesn't match the expected behavior for drop_duplicates with subset")
    employee = get_df("employee")
    df = pyspark_employee.dropDuplicates(["age"])
    dfs = employee.dropDuplicates(["age"])
    compare_frames(df, dfs)


def test_drop_na_default(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select(
        F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age")
    ).dropna()

    dfs = employee.select(
        SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age")
    ).dropna()

    compare_frames(df, dfs)


def test_dropna_how(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select(
        F.lit(None), F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age")
    ).dropna(how="all")

    dfs = employee.select(
        SF.lit(None), SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age")
    ).dropna(how="all")

    compare_frames(df, dfs, compare_schema=False)


def test_dropna_thresh(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select(
        F.lit(None).alias("val"),
        F.lit(1),
        F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age"),
    ).dropna(how="any", thresh=2)

    dfs = employee.select(
        SF.lit(None).alias("val"),
        SF.lit(1),
        SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age"),
    ).dropna(how="any", thresh=2)

    compare_frames(df, dfs, compare_schema=False)


def test_dropna_subset(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select(
        F.lit(None).alias("val"),
        F.lit(1),
        F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age"),
    ).dropna(thresh=1, subset="the_age")

    dfs = employee.select(
        SF.lit(None).alias("val"),
        SF.lit(1),
        SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age"),
    ).dropna(thresh=1, subset="the_age")

    compare_frames(df, dfs, compare_schema=False)


def test_dropna_na_function(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select(
        F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age")
    ).na.drop()

    dfs = employee.select(
        SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age")
    ).na.drop()

    compare_frames(df, dfs)


def test_fillna_default(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select(
        F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age")
    ).fillna(100)

    dfs = employee.select(
        SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age")
    ).fillna(100)

    compare_frames(df, dfs)


def test_fillna_dict_replacement(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select(
        F.col("fname"),
        F.when(F.col("lname").startswith("L"), F.col("lname")).alias("l_lname"),
        F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age"),
    ).fillna({"fname": "Jacob", "l_lname": "NOT_LNAME"})

    dfs = employee.select(
        SF.col("fname"),
        SF.when(SF.col("lname").startswith("L"), SF.col("lname")).alias("l_lname"),
        SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age"),
    ).fillna({"fname": "Jacob", "l_lname": "NOT_LNAME"})

    # For some reason the sqlglot results sets a column as nullable when it doesn't need to
    # This seems to be a nuance in how spark dataframe from sql works so we can ignore
    compare_frames(df, dfs, compare_schema=False)


def test_fillna_na_func(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select(
        F.when(F.col("age") < F.lit(50), F.col("age")).alias("the_age")
    ).na.fill(100)

    dfs = employee.select(
        SF.when(SF.col("age") < SF.lit(50), SF.col("age")).alias("the_age")
    ).na.fill(100)

    compare_frames(df, dfs)


def test_replace_basic(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select(F.col("age"), F.lit(37).alias("test_col")).replace(
        to_replace=37, value=100
    )

    dfs = employee.select(SF.col("age"), SF.lit(37).alias("test_col")).replace(
        to_replace=37, value=100
    )
    compare_frames(df, dfs, compare_schema=False)


def test_replace_basic_subset(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select(F.col("age"), F.lit(37).alias("test_col")).replace(  # type: ignore
        to_replace=37, value=100, subset="age"
    )

    dfs = employee.select(SF.col("age"), SF.lit(37).alias("test_col")).replace(
        to_replace=37, value=100, subset="age"
    )

    compare_frames(df, dfs, compare_schema=False)


def test_replace_mapping(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select(F.col("age"), F.lit(37).alias("test_col")).replace({37: 100})

    dfs = employee.select(SF.col("age"), SF.lit(37).alias("test_col")).replace({37: 100})

    compare_frames(df, dfs, compare_schema=False)


def test_replace_mapping_subset(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select(
        F.col("age"), F.lit(37).alias("test_col"), F.lit(50).alias("test_col_2")
    ).replace({37: 100, 50: 1}, subset=["age", "test_col_2"])

    dfs = employee.select(
        SF.col("age"), SF.lit(37).alias("test_col"), SF.lit(50).alias("test_col_2")
    ).replace({37: 100, 50: 1}, subset=["age", "test_col_2"])

    compare_frames(df, dfs, compare_schema=False)


def test_replace_na_func_basic(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select(F.col("age"), F.lit(37).alias("test_col")).na.replace(  # type: ignore
        to_replace=37, value=100
    )

    dfs = employee.select(SF.col("age"), SF.lit(37).alias("test_col")).na.replace(
        to_replace=37, value=100
    )

    compare_frames(df, dfs, compare_schema=False)


def test_with_column(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.withColumn("test", F.col("age"))

    dfs = employee.withColumn("test", SF.col("age"))

    compare_frames(df, dfs)


def test_with_column_existing_name(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.withColumn("fname", F.lit("blah"))

    dfs = employee.withColumn("fname", SF.lit("blah"))

    compare_frames(df, dfs, compare_schema=False)


def test_with_column_renamed(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.withColumnRenamed("fname", "first_name")

    dfs = employee.withColumnRenamed("fname", "first_name")

    compare_frames(df, dfs)


def test_with_column_renamed_double(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select(F.col("fname").alias("first_name")).withColumnRenamed(
        "first_name", "first_name_again"
    )

    dfs = employee.select(SF.col("fname").alias("first_name")).withColumnRenamed(
        "first_name", "first_name_again"
    )

    compare_frames(df, dfs)


def test_with_columns(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.withColumns(
        {
            "test": F.col("age"),
            "test2": F.col("age"),
        }
    )

    dfs = employee.withColumns(
        {
            "test": SF.col("age"),
            "test2": SF.col("age"),
        }
    )

    compare_frames(df, dfs)


def test_with_columns_reference_another(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
    is_bigquery: t.Callable,
    is_postgres: t.Callable,
    is_snowflake: t.Callable,
):
    # Could consider two options:
    # 1. Use SQLGlot optimizer to properly change the references to be expanded to avoid the issue (a rule already does this)
    # 2. Write specific logic to both these dataframes to detect if there is a self reference and create a new scope
    if is_bigquery():
        pytest.skip(
            "BigQuery doesn't support having selects with columns that reference each other."
        )
    if is_postgres():
        pytest.skip(
            "Postgres doesn't support having selects with columns that reference each other."
        )
    if is_snowflake():
        # Snowflake does allow columns that reference each other but the issue is that if you do this in the final
        # select the columns are replaced with their alias version to show their display name (the case-sensitive
        # name provided by the user) and then, since the column is now aliased and case-sensitive, SF thinks
        # the column doesn't exist since the column of the same case does not exist since it was aliased.
        pytest.skip(
            "Bugged behavior introduced display names means that snowflake can no longer reference itself."
        )
    employee = get_df("employee")
    df = pyspark_employee.withColumns(
        {
            "test": F.col("age"),
            "test2": F.col("test"),
        }
    )

    dfs = employee.withColumns(
        {
            "test": SF.col("age"),
            "test2": SF.col("test"),
        }
    )
    compare_frames(df, dfs)


def test_drop_column_single(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.select(F.col("fname"), F.col("lname"), F.col("age")).drop("age")

    dfs = employee.select(SF.col("fname"), SF.col("lname"), SF.col("age")).drop("age")

    compare_frames(df, dfs)


def test_drop_column_reference_join(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    store = get_df("store")
    df_spark_employee_cols = pyspark_employee.select(
        F.col("fname"), F.col("lname"), F.col("age"), F.col("store_id")
    )
    df_pyspark_store_cols = pyspark_store.select(F.col("store_id"), F.col("store_name"))
    df = df_spark_employee_cols.join(df_pyspark_store_cols, on="store_id", how="inner").drop(
        df_spark_employee_cols.age,
    )

    df_sqlglot_employee_cols = employee.select(
        SF.col("fname"), SF.col("lname"), SF.col("age"), SF.col("store_id")
    )
    df_sqlglot_store_cols = store.select(SF.col("store_id"), SF.col("store_name"))
    dfs = df_sqlglot_employee_cols.join(df_sqlglot_store_cols, on="store_id", how="inner").drop(
        df_sqlglot_employee_cols.age,
    )

    compare_frames(df, dfs, sort=True)


def test_limit(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.limit(1)

    dfs = employee.limit(1)

    compare_frames(df, dfs)


def test_hint_broadcast_alias(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
    get_explain_plan: t.Callable,
    is_duckdb: t.Callable,
):
    employee = get_df("employee")
    store = get_df("store")
    df_joined = pyspark_employee.join(
        pyspark_store.alias("store").hint("broadcast", "store"),
        on=pyspark_employee.store_id == pyspark_store.store_id,
        how="inner",
    ).select(
        pyspark_employee.employee_id,
        pyspark_employee["fname"],
        F.col("lname"),
        F.col("age"),
        pyspark_employee.store_id,
        pyspark_store.store_name,
        pyspark_store["num_sales"],
    )
    dfs_joined = employee.join(
        store.alias("store").hint("broadcast", "store"),
        on=employee.store_id == store.store_id,
        how="inner",
    ).select(
        employee.employee_id,
        employee["fname"],
        SF.col("lname"),
        SF.col("age"),
        employee.store_id,
        store.store_name,
        store["num_sales"],
    )
    df, dfs = compare_frames(df_joined, dfs_joined)
    if isinstance(dfs, PySparkDataFrame):
        assert "ResolvedHint (strategy=broadcast)" in get_explain_plan(df)
        assert "ResolvedHint (strategy=broadcast)" in get_explain_plan(dfs)


def test_hint_broadcast_no_alias(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
    get_explain_plan: t.Callable,
):
    employee = get_df("employee")
    store = get_df("store")
    df_joined = pyspark_employee.join(
        pyspark_store.hint("broadcast"),
        on=pyspark_employee.store_id == pyspark_store.store_id,
        how="inner",
    ).select(
        pyspark_employee.employee_id,
        pyspark_employee["fname"],
        F.col("lname"),
        F.col("age"),
        pyspark_employee.store_id,
        pyspark_store.store_name,
        pyspark_store["num_sales"],
    )
    dfs_joined = employee.join(
        store.hint("broadcast"),
        on=employee.store_id == store.store_id,
        how="inner",
    ).select(
        employee.employee_id,
        employee["fname"],
        SF.col("lname"),
        SF.col("age"),
        employee.store_id,
        store.store_name,
        store["num_sales"],
    )
    df, dfs = compare_frames(df_joined, dfs_joined)
    if isinstance(dfs, PySparkDataFrame):
        assert "ResolvedHint (strategy=broadcast)" in get_explain_plan(df)
        assert "ResolvedHint (strategy=broadcast)" in get_explain_plan(dfs)
        assert "UnresolvedHint BROADCAST, [" in get_explain_plan(dfs)


def test_broadcast_func(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
    get_explain_plan: t.Callable,
):
    employee = get_df("employee")
    store = get_df("store")
    df_joined = pyspark_employee.join(
        F.broadcast(pyspark_store),
        on=pyspark_employee.store_id == pyspark_store.store_id,
        how="inner",
    ).select(
        pyspark_employee.employee_id,
        pyspark_employee["fname"],
        F.col("lname"),
        F.col("age"),
        pyspark_employee.store_id,
        pyspark_store.store_name,
        pyspark_store["num_sales"],
    )
    dfs_joined = employee.join(
        SF.broadcast(store),
        on=employee.store_id == store.store_id,
        how="inner",
    ).select(
        employee.employee_id,
        employee["fname"],
        SF.col("lname"),
        SF.col("age"),
        employee.store_id,
        store.store_name,
        store["num_sales"],
    )
    df, dfs = compare_frames(df_joined, dfs_joined)
    if isinstance(dfs, PySparkDataFrame):
        assert "ResolvedHint (strategy=broadcast)" in get_explain_plan(df)
        assert "ResolvedHint (strategy=broadcast)" in get_explain_plan(dfs)
        assert "'UnresolvedHint BROADCAST, [" in get_explain_plan(dfs)


def test_repartition_by_num(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    """
    The results are different when doing the repartition on a table created using VALUES in SQL.
    So I just use the views instead for these tests
    """
    employee = get_df("employee")
    df = pyspark_employee.repartition(63)

    dfs = employee.session.read.table("employee").repartition(63)
    df, dfs = compare_frames(df, dfs)
    if isinstance(dfs, PySparkDataFrame):
        spark_num_partitions = df.rdd.getNumPartitions()
        sqlf_num_partitions = dfs.rdd.getNumPartitions()
        assert spark_num_partitions == 63
        assert spark_num_partitions == sqlf_num_partitions


def test_repartition_name_only(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
    get_explain_plan: t.Callable,
):
    """
    We use the view here to help ensure the explain plans are similar enough to compare
    """
    employee = get_df("employee")
    df = pyspark_employee.repartition("age")

    dfs = employee.session.read.table("employee").repartition("age")
    df, dfs = compare_frames(df, dfs)
    if isinstance(dfs, PySparkDataFrame):
        assert "RepartitionByExpression [age" in get_explain_plan(df)
        assert "RepartitionByExpression [age" in get_explain_plan(dfs)


def test_repartition_num_and_multiple_names(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
    get_explain_plan: t.Callable,
):
    """
    We use the view here to help ensure the explain plans are similar enough to compare
    """
    employee = get_df("employee")
    df = pyspark_employee.repartition(53, "age", "fname")

    dfs = employee.session.read.table("employee").repartition(53, "age", "fname")
    df, dfs = compare_frames(df, dfs)
    if isinstance(dfs, PySparkDataFrame):
        spark_num_partitions = df.rdd.getNumPartitions()
        sqlglot_num_partitions = dfs.rdd.getNumPartitions()
        assert spark_num_partitions == 53
        assert spark_num_partitions == sqlglot_num_partitions


def test_coalesce(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.coalesce(1)
    dfs = employee.coalesce(1)
    df, dfs = compare_frames(df, dfs)
    if isinstance(dfs, PySparkDataFrame):
        spark_num_partitions = df.rdd.getNumPartitions()
        sqlglot_num_partitions = dfs.rdd.getNumPartitions()
        assert spark_num_partitions == 1
        assert spark_num_partitions == sqlglot_num_partitions


def test_cache_select(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    store = get_df("store")
    df_employee = (
        pyspark_employee.groupBy("store_id")
        .agg(F.countDistinct("employee_id").alias("num_employees"))
        .cache()
    )
    df_joined = df_employee.join(pyspark_store, on="store_id").select(
        pyspark_store.store_id, df_employee.num_employees
    )
    dfs_employee = (
        employee.groupBy("store_id")
        .agg(SF.countDistinct("employee_id").alias("num_employees"))
        .cache()
    )
    dfs_joined = dfs_employee.join(store, on="store_id").select(
        store.store_id, dfs_employee.num_employees
    )
    compare_frames(df_joined, dfs_joined)


def test_persist_select(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    store = get_df("store")
    df_employee = (
        pyspark_employee.groupBy("store_id")
        .agg(F.countDistinct("employee_id").alias("num_employees"))
        .persist()
    )
    df_joined = df_employee.join(pyspark_store, on="store_id").select(
        pyspark_store.store_id, df_employee.num_employees
    )
    dfs_employee = (
        employee.groupBy("store_id")
        .agg(SF.countDistinct("employee_id").alias("num_employees"))
        .persist()
    )
    dfs_joined = dfs_employee.join(store, on="store_id").select(
        store.store_id, dfs_employee.num_employees
    )
    compare_frames(df_joined, dfs_joined)


def test_transform(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    def cast_all_to_int_pyspark(input_df):
        return input_df.select([F.col(col_name).cast("string") for col_name in input_df.columns])

    def cast_all_to_int_sqlframe(input_df):
        return input_df.select([SF.col(col_name).cast("string") for col_name in input_df.columns])

    def sort_columns_asc(input_df):
        return input_df.select(*sorted(input_df.columns))

    employee = get_df("employee")

    df = pyspark_employee.transform(cast_all_to_int_pyspark).transform(sort_columns_asc)
    dfs = employee.transform(cast_all_to_int_sqlframe).transform(sort_columns_asc)
    compare_frames(df, dfs)


def test_unpivot(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.unpivot(
        pyspark_employee.columns,  # type: ignore
        ["fname", "lname"],
        "attribute",
        "value",
    )
    dfs = employee.unpivot(employee.columns, ["fname", "lname"], "attribute", "value")
    compare_frames(df, dfs, sort=True)


def test_unpivot_no_values(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    employee = get_df("employee")
    df = pyspark_employee.unpivot(["employee_id", "fname", "lname"], None, "attribute", "value")
    dfs = employee.unpivot(["employee_id", "fname", "lname"], None, "attribute", "value")
    compare_frames(df, dfs, sort=True)


def test_unpivot_doc_example(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    df = pyspark_employee._session.createDataFrame(
        [(1, 11, 1.1), (2, 12, 1.2)],
        ["id", "int", "double"],
    ).unpivot("id", ["int", "double"], "var", "val")

    dfs = (
        get_df("employee")
        .session.createDataFrame(
            [(1, 11, 1.1), (2, 12, 1.2)],
            ["id", "int", "double"],
        )
        .unpivot("id", ["int", "double"], "var", "val")
    )
    compare_frames(df, dfs, sort=True, compare_schema=False)


# https://github.com/eakmanrq/sqlframe/issues/51
def test_join_full_outer_no_match(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    spark = pyspark_employee._session
    initial = spark.createDataFrame(
        [
            (1, "data"),
            (2, "data"),
        ],
        ["id", "data"],
    )
    for_join = spark.createDataFrame(
        [
            (1, "other_data"),
            (2, "other_data"),
            (3, "other_data"),
        ],
        ["id", "other_data"],
    )
    df = initial.join(for_join, on="id", how="full")

    session = get_df("employee").session
    dfs_initial = session.createDataFrame(
        [
            (1, "data"),
            (2, "data"),
        ],
        ["id", "data"],
    )
    dfs_for_join = session.createDataFrame(
        [
            (1, "other_data"),
            (2, "other_data"),
            (3, "other_data"),
        ],
        ["id", "other_data"],
    )
    dfs = dfs_initial.join(dfs_for_join, on="id", how="full")

    compare_frames(df, dfs)


# https://github.com/eakmanrq/sqlframe/issues/102
def test_join_with_duplicate_column_name(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    spark = pyspark_employee._session
    df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], ["age", "name"])
    df_height = spark.createDataFrame([(170, "Alice"), (180, "Bob")], ["height", "name"])

    df = df.join(df_height, how="left", on="name").withColumn("name", F.upper("name"))

    session = get_df("employee").session
    dfs = session.createDataFrame([(2, "Alice"), (5, "Bob")], ["age", "name"])
    dfs_height = session.createDataFrame([(170, "Alice"), (180, "Bob")], ["height", "name"])

    dfs = dfs.join(dfs_height, how="left", on="name").withColumn("name", SF.upper("name"))

    compare_frames(df, dfs, compare_schema=False)


# https://github.com/eakmanrq/sqlframe/issues/103
def test_chained_join_common_key(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    spark = pyspark_employee._session
    df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], ["age", "name"])
    height = spark.createDataFrame([(170, "Alice"), (180, "Bob")], ["height", "name"])
    location = spark.createDataFrame([("USA", "Alice"), ("Europe", "Bob")], ["location", "name"])

    df = df.join(height, how="left", on="name").join(location, how="left", on="name")

    session = get_df("employee").session
    dfs = session.createDataFrame([(2, "Alice"), (5, "Bob")], ["age", "name"])
    dfs_height = session.createDataFrame([(170, "Alice"), (180, "Bob")], ["height", "name"])
    dfs_location = session.createDataFrame(
        [("USA", "Alice"), ("Europe", "Bob")], ["location", "name"]
    )
    dfs = dfs.join(dfs_height, how="left", on="name").join(dfs_location, how="left", on="name")

    compare_frames(df, dfs, compare_schema=False)


# https://github.com/eakmanrq/sqlframe/issues/185
def test_chaining_joins_with_selects(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    pyspark_district: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
    is_spark: t.Callable,
):
    if is_spark():
        pytest.skip(
            "This test is not supported in Spark. This is related to how duplicate columns are handled in Spark"
        )
    df = (
        pyspark_employee.alias("employee")
        .join(
            pyspark_store.filter(F.col("store_name") != "test").alias("store"),
            on=F.col("employee.employee_id") == F.col("store.store_id"),
        )
        .join(
            pyspark_district.alias("district"),
            on=F.col("store.store_id") == F.col("district.district_id"),
        )
        .join(
            pyspark_district.alias("district2"),
            on=(F.col("store.store_id") + 1) == F.col("district2.district_id"),
            how="left",
        )
        .select("*")
    )

    employee = get_df("employee")
    store = get_df("store")
    district = get_df("district")

    dfs = (
        employee.alias("employee")
        .join(
            store.filter(SF.col("store_name") != "test").alias("store"),
            on=SF.col("employee.employee_id") == SF.col("store.store_id"),
        )
        .join(
            district.alias("district"),
            on=SF.col("store.store_id") == SF.col("district.district_id"),
        )
        .join(
            district.alias("district2"),
            on=(SF.col("store.store_id") + 1) == SF.col("district2.district_id"),
            how="left",
        )
        .select("*")
    )

    compare_frames(df, dfs, compare_schema=False)


# https://github.com/eakmanrq/sqlframe/issues/210
# https://github.com/eakmanrq/sqlframe/issues/212
def test_self_join(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
    is_spark: t.Callable,
):
    if is_spark():
        pytest.skip(
            "This test is not supported in Spark. This is related to how duplicate columns are handled in Spark"
        )
    df_filtered = pyspark_employee.where(F.col("age") > 40)
    df_joined = pyspark_employee.join(
        df_filtered,
        pyspark_employee["employee_id"].eqNullSafe(df_filtered["employee_id"]),
        how="inner",
    )

    employee = get_df("employee")

    dfs_filtered = employee.alias("dfs_filtered").where(SF.col("age") > 40)
    dfs_joined = employee.join(
        dfs_filtered,
        employee["employee_id"].eqNullSafe(dfs_filtered["employee_id"]),
        how="inner",
    )

    compare_frames(df_joined, dfs_joined, compare_schema=False)


# https://github.com/eakmanrq/sqlframe/issues/232
def test_filter_alias(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    df_filtered = pyspark_employee.where((F.col("age") > 40).alias("age_gt_40"))

    employee = get_df("employee")
    dfs_filtered = employee.where((SF.col("age") > 40).alias("age_gt_40"))

    compare_frames(df_filtered, dfs_filtered, compare_schema=False)


# https://github.com/eakmanrq/sqlframe/issues/244
def test_union_common_root(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    df_1 = pyspark_employee.filter(F.col("age") > 40)
    df_2 = df_1.join(
        pyspark_employee.select("employee_id").distinct(),
        on="employee_id",
        how="right",
    )
    df_final = df_1.union(df_2)

    employee = get_df("employee")
    dfs_1 = employee.filter(SF.col("age") > 40)
    dfs_2 = dfs_1.join(
        employee.select("employee_id").distinct(),
        on="employee_id",
        how="right",
    )
    dfs_final = dfs_1.union(dfs_2)

    compare_frames(df_final, dfs_final, compare_schema=False)


# https://github.com/eakmanrq/sqlframe/issues/253
def test_union_common_root_again(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    df_1 = pyspark_employee.filter(F.col("age") > 40)
    df_2 = df_1.join(
        pyspark_employee.select("employee_id").distinct(),
        on="employee_id",
        how="right",
    )
    df_final = df_1.union(df_2).union(pyspark_employee)

    employee = get_df("employee")
    dfs_1 = employee.filter(SF.col("age") > 40)
    dfs_2 = dfs_1.join(
        employee.select("employee_id").distinct(),
        on="employee_id",
        how="right",
    )
    dfs_final = dfs_1.union(dfs_2).union(employee)

    compare_frames(df_final, dfs_final, compare_schema=False)


# https://github.com/eakmanrq/sqlframe/issues/277
def test_filtering_join_key(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    df = pyspark_employee.join(
        pyspark_store,
        on="store_id",
        how="inner",
    ).filter(F.col("store_id") > 1)

    employee = get_df("employee")
    store = get_df("store")
    dfs = employee.join(
        store,
        on="store_id",
        how="inner",
    ).filter(SF.col("store_id") > 1)

    compare_frames(df, dfs, compare_schema=False, sort=True)


# https://github.com/eakmanrq/sqlframe/issues/281
def test_create_column_after_join(
    pyspark_employee: PySparkDataFrame,
    pyspark_store: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    df = pyspark_employee.join(
        pyspark_store,
        on="store_id",
    ).withColumn("new_col", F.lit(1))

    employee = get_df("employee")
    store = get_df("store")
    dfs = employee.join(
        store,
        on="store_id",
    ).withColumn("new_col", SF.lit(1))

    compare_frames(df, dfs, compare_schema=False, sort=True)


# https://github.com/eakmanrq/sqlframe/issues/289
def test_full_outer_nulls_no_match(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    spark = pyspark_employee._session

    df_concept_1 = spark.createDataFrame(
        [
            (1, 100),
            (2, 101),
        ],
        ["A", "col1"],
    )

    df_concept_2 = spark.createDataFrame(
        [
            (3, 102),
            (4, 103),
        ],
        ["B", "col1"],
    )

    df = df_concept_1.join(df_concept_2, on="col1", how="outer")

    session = get_df("employee").session

    dfs_concept_1 = session.createDataFrame(
        [
            (1, 100),
            (2, 101),
        ],
        ["A", "col1"],
    )

    dfs_concept_2 = session.createDataFrame(
        [
            (3, 102),
            (4, 103),
        ],
        ["B", "col1"],
    )

    dfs = dfs_concept_1.join(dfs_concept_2, on="col1", how="outer")

    compare_frames(df, dfs, compare_schema=False)


# https://github.com/eakmanrq/sqlframe/issues/317
def test_rows_between_negative_start(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    session = pyspark_employee.sparkSession
    df = session.createDataFrame(
        pd.DataFrame(
            {"store": [1, 1, 1, 2, 2, 2, 2], "price": [1, 2, 3, 4, 3, 2, 1], "date": list(range(7))}
        )
    )
    window = Window().partitionBy("store").orderBy("date").rowsBetween(-1, 1)
    df = df.withColumn("rolling_price", F.mean("price").over(window))

    employee = get_df("employee")
    sf_session = employee.session
    dfs = sf_session.createDataFrame(
        pd.DataFrame(
            {"store": [1, 1, 1, 2, 2, 2, 2], "price": [1, 2, 3, 4, 3, 2, 1], "date": list(range(7))}
        )
    )
    swindow = SWindow().partitionBy("store").orderBy("date").rowsBetween(-1, 1)
    dfs = dfs.withColumn("rolling_price", SF.mean("price").over(swindow))

    compare_frames(df, dfs, compare_schema=False)


# https://github.com/eakmanrq/sqlframe/issues/317
def test_rows_between_negative_end(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    session = pyspark_employee.sparkSession
    df = session.createDataFrame(
        pd.DataFrame(
            {"store": [1, 1, 1, 2, 2, 2, 2], "price": [1, 2, 3, 4, 3, 2, 1], "date": list(range(7))}
        )
    )
    window = Window().partitionBy("store").orderBy("date").rowsBetween(-2, -1)
    df = df.withColumn("rolling_price", F.mean("price").over(window))

    employee = get_df("employee")
    sf_session = employee.session
    dfs = sf_session.createDataFrame(
        pd.DataFrame(
            {"store": [1, 1, 1, 2, 2, 2, 2], "price": [1, 2, 3, 4, 3, 2, 1], "date": list(range(7))}
        )
    )
    swindow = SWindow().partitionBy("store").orderBy("date").rowsBetween(-2, -1)
    dfs = dfs.withColumn("rolling_price", SF.mean("price").over(swindow))

    compare_frames(df, dfs, compare_schema=False)


# https://github.com/eakmanrq/sqlframe/issues/317
def test_rows_between_positive_both_start_end(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    session = pyspark_employee.sparkSession
    df = session.createDataFrame(
        pd.DataFrame(
            {"store": [1, 1, 1, 2, 2, 2, 2], "price": [1, 2, 3, 4, 3, 2, 1], "date": list(range(7))}
        )
    )
    window = Window().partitionBy("store").orderBy("date").rowsBetween(1, 2)
    df = df.withColumn("rolling_price", F.mean("price").over(window))

    employee = get_df("employee")
    sf_session = employee.session
    dfs = sf_session.createDataFrame(
        pd.DataFrame(
            {"store": [1, 1, 1, 2, 2, 2, 2], "price": [1, 2, 3, 4, 3, 2, 1], "date": list(range(7))}
        )
    )
    swindow = SWindow().partitionBy("store").orderBy("date").rowsBetween(1, 2)
    dfs = dfs.withColumn("rolling_price", SF.mean("price").over(swindow))

    compare_frames(df, dfs, compare_schema=False)


# https://github.com/eakmanrq/sqlframe/issues/356
def test_array_of_grouping_columns(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    df = pyspark_employee.sparkSession.createDataFrame(
        [(2, "Alice"), (2, "Bob"), (2, "Bob"), (5, "Bob")], schema=["age", "name"]
    )
    df = df.groupBy(["name", df.age]).count().sort("name", "age")  # type: ignore

    dfs = get_df("employee").sparkSession.createDataFrame(
        [(2, "Alice"), (2, "Bob"), (2, "Bob"), (5, "Bob")], schema=["age", "name"]
    )
    dfs = dfs.groupBy(["name", dfs.age]).count().sort("name", "age")

    compare_frames(df, dfs, compare_schema=False, sort=True)


# https://github.com/eakmanrq/sqlframe/issues/356
def test_alias_group_by_column(
    pyspark_employee: PySparkDataFrame,
    get_df: t.Callable[[str], BaseDataFrame],
    compare_frames: t.Callable,
):
    df = pyspark_employee.sparkSession.createDataFrame(
        [(2, "Alice"), (2, "Bob"), (2, "Bob"), (5, "Bob")], schema=["age", "name"]
    )
    df = df.groupBy(F.col("name").alias("Firstname")).count()

    dfs = get_df("employee").sparkSession.createDataFrame(
        [(2, "Alice"), (2, "Bob"), (2, "Bob"), (5, "Bob")], schema=["age", "name"]
    )
    dfs = dfs.groupBy(SF.col("name").alias("Firstname")).count()

    compare_frames(df, dfs, compare_schema=False, sort=True)
