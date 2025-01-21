from __future__ import annotations

import datetime
import typing as t

import pytest
from pyspark.sql import SparkSession as PySparkSession

from sqlframe.base.table import WhenMatched, WhenNotMatched, _BaseTable
from sqlframe.base.types import Row
from sqlframe.bigquery.session import BigQuerySession
from sqlframe.databricks import DatabricksSession
from sqlframe.duckdb.session import DuckDBSession
from sqlframe.postgres.session import PostgresSession
from sqlframe.redshift.session import RedshiftSession
from sqlframe.snowflake.session import SnowflakeSession
from sqlframe.spark.session import SparkSession
from sqlframe.standalone.session import StandaloneSession

if t.TYPE_CHECKING:
    from sqlframe.base.dataframe import BaseDataFrame

pytest_plugins = ["tests.integration.fixtures"]


@pytest.fixture
def merge_data() -> t.Tuple[t.List[t.Any], str]:
    return (
        [
            (
                "3368c22d-edd8-4ae9-a0b8-f0956b9ffa88",
                1,
                "Jack",
                "Shephard",
                37,
                1,
                "1999-01-01",
                "2900-01-01",
            ),
            (
                "6db4c8ce-464d-4772-a686-e6bd9e10286f",
                2,
                "John",
                "Locke",
                65,
                1,
                "1999-01-01",
                "2900-01-01",
            ),
            (
                "7984b6b1-cb4c-49a0-bc83-61064f4d77e6",
                3,
                "Kate",
                "Austen",
                37,
                2,
                "1999-01-01",
                "2900-01-01",
            ),
            (
                "8ee4d1bf-c3c7-42a0-8727-233881d6212e",
                4,
                "Claire",
                "Littleton",
                27,
                2,
                "1999-01-01",
                "2900-01-01",
            ),
            (
                "0c67fff1-9471-4360-8624-982b009cf315",
                5,
                "Hugo",
                "Reyes",
                29,
                100,
                "1999-01-01",
                "2900-01-01",
            ),
        ],
        "s_id STRING, employee_id INTEGER, fname STRING, lname STRING, age INTEGER, "
        "store_id INTEGER, start_date DATE, end_date DATE",
    )


@pytest.fixture
def cleanup_employee_df(
    get_engine_df: t.Callable[[str], BaseDataFrame],
) -> t.Iterator[BaseDataFrame]:
    df = get_engine_df("employee")
    df.session._execute("DROP TABLE IF EXISTS update_employee")
    df.session._execute("DROP TABLE IF EXISTS merge_employee")
    df.session._execute("DROP TABLE IF EXISTS delete_employee")
    yield df
    df.session._execute("DROP TABLE IF EXISTS update_employee")
    df.session._execute("DROP TABLE IF EXISTS merge_employee")
    df.session._execute("DROP TABLE IF EXISTS delete_employee")


def test_update_table(cleanup_employee_df: BaseDataFrame, caplog):
    session = cleanup_employee_df.session
    if isinstance(
        session,
        (
            StandaloneSession,
            PySparkSession,
            SparkSession,
        ),
    ):
        pytest.skip("Engine doesn't support update")
    df_employee = cleanup_employee_df
    df_employee.write.saveAsTable("update_employee")
    df = session.read.table("update_employee")
    assert isinstance(df, _BaseTable)
    update_expr = df.update(
        set_={"age": df["age"] + 1},
        where=df["employee_id"] == 1,
    )
    result = update_expr.execute()
    # Postgres, RedshiftSession and BigQuery don't support returning the number of affected rows
    if not isinstance(session, (PostgresSession, BigQuerySession, RedshiftSession)):
        assert result[0][0] == 1

    df2 = session.read.table("update_employee")
    assert (
        df2.where(df2["employee_id"] == 1).select("age").collect()[0]["age"]
        == df_employee.where(df_employee["employee_id"] == 1).select("age").collect()[0]["age"] + 1
    )


def test_delete_table(cleanup_employee_df: BaseDataFrame, caplog):
    session = cleanup_employee_df.session
    if isinstance(
        session,
        (
            StandaloneSession,
            PySparkSession,
            SparkSession,
        ),
    ):
        pytest.skip("Engine doesn't support delete")
    df_employee = cleanup_employee_df
    df_employee.write.saveAsTable("delete_employee")
    df = session.read.table("delete_employee")
    assert isinstance(df, _BaseTable)
    delete_expr = df.delete(where=df["age"] > 28)
    result = delete_expr.execute()
    # Postgres, RedshiftSession and BigQuery don't support returning the number of affected rows
    if not isinstance(session, (PostgresSession, BigQuerySession, RedshiftSession)):
        assert result[0][0] == 4

    df2 = session.read.table("delete_employee")
    assert df2.collect() == [
        Row(employee_id=4, fname="Claire", lname="Littleton", age=27, store_id=2)
    ]


def test_merge_table_simple(cleanup_employee_df: BaseDataFrame, caplog):
    session = cleanup_employee_df.session
    if isinstance(
        session,
        (
            StandaloneSession,
            PySparkSession,
            RedshiftSession,
            SparkSession,
            DuckDBSession,
        ),
    ):
        pytest.skip("Engine doesn't support merge")
    df_employee = cleanup_employee_df
    df_employee.write.saveAsTable("merge_employee")
    df = session.read.table("merge_employee")
    assert isinstance(df, _BaseTable)

    df2 = session.createDataFrame(
        [
            (1, "Jack", "Shephard", 38, 2),
            (6, "Mary", "Sue", 21, 45),
        ],
        ["employee_id", "fname", "lname", "age", "store_id"],
    )

    merge_expr = df.merge(
        df2,
        condition=df.employee_id == df2.employee_id,
        clauses=[
            WhenMatched(condition=df.fname == df2.fname).update(
                set_={
                    "lname": df2.lname,
                    "age": df2.age,
                    "store_id": df2.store_id,
                }
            ),
            WhenNotMatched().insert(
                values={
                    "employee_id": df2.employee_id,
                    "fname": df2.fname,
                    "lname": df2.lname,
                    "age": df2.age,
                    "store_id": df2.store_id,
                }
            ),
        ],
    )
    result = merge_expr.execute()
    # Postgres and BigQuery don't support returning the number of affected rows
    if not isinstance(session, (PostgresSession, BigQuerySession)):
        if isinstance(session, SnowflakeSession):
            assert (result[0][0] + result[0][1]) == 2
        else:
            assert result[0][0] == 2

    df_merged = session.read.table("merge_employee")
    assert sorted(df_merged.collect()) == [
        Row(employee_id=1, fname="Jack", lname="Shephard", age=38, store_id=2),
        Row(employee_id=2, fname="John", lname="Locke", age=65, store_id=1),
        Row(employee_id=3, fname="Kate", lname="Austen", age=37, store_id=2),
        Row(employee_id=4, fname="Claire", lname="Littleton", age=27, store_id=2),
        Row(employee_id=5, fname="Hugo", lname="Reyes", age=29, store_id=100),
        Row(employee_id=6, fname="Mary", lname="Sue", age=21, store_id=45),
    ]


def test_merge_table(cleanup_employee_df: BaseDataFrame, merge_data, get_func, caplog):
    session = cleanup_employee_df.session
    col = get_func("col", session)
    expr = get_func("expr", session)
    lit = get_func("lit", session)
    if isinstance(
        session,
        (
            StandaloneSession,
            PySparkSession,
            RedshiftSession,
            SparkSession,
            DuckDBSession,
        ),
    ):
        pytest.skip("Engine doesn't support merge")

    if isinstance(session, DatabricksSession):
        uuid_func = "uuid"
    elif isinstance(session, PostgresSession):
        uuid_func = "gen_random_uuid"
    elif isinstance(session, SnowflakeSession):
        uuid_func = "uuid_string"
    elif isinstance(session, BigQuerySession):
        uuid_func = "generate_uuid"
    else:
        pytest.skip("Cannot generate uuids in this engine")

    data, schema = merge_data
    df_employee = session.createDataFrame(data, schema)
    df_employee.write.saveAsTable("merge_employee")
    df = session.read.table("merge_employee")
    assert isinstance(df, _BaseTable)

    start_date = "2024-01-01"
    end_date = "2900-01-01"
    df2 = session.createDataFrame(
        [
            (1, "Jack", "Shephard", 38, 2),
            (6, "Mary", "Sue", 21, 45),
        ],
        ["employee_id", "fname", "lname", "age", "store_id"],
    )

    source = df2.select(
        expr(f"{uuid_func}()").alias("s_id"),
        col("employee_id").alias("employee_id"),
        col("fname").alias("fname"),
        col("lname").alias("lname"),
        col("age").alias("age"),
        col("store_id").alias("store_id"),
        lit(end_date).alias("end_date"),
    )

    updates = (
        source.alias("src")
        .join(
            df.alias("tg"),
            on=(col("tg.employee_id") == col("src.employee_id")),
        )
        .where(
            f"tg.end_date = '{end_date}' "
            f"AND ("
            f"src.fname <> tg.fname OR src.lname <> tg.lname OR src.age <> tg.age OR src.store_id <> tg.store_id"
            f")"
        )
    )

    staging = updates.select(
        lit(0).alias("__key__"),
        col("src.*"),
        lit(start_date).cast("date").alias("start_date"),
    ).unionByName(
        source.alias("src").select(
            lit(1).alias("__key__"),
            col("src.*"),
            lit(start_date).cast("date").alias("start_date"),
        )
    )

    merge_expr = df.alias("tg").merge(
        staging.alias("st"),
        condition=(col("st.employee_id") == col("tg.employee_id")) & (col("st.__key__") == lit(1)),
        clauses=[
            WhenMatched(
                condition=(col("tg.end_date") == lit(end_date))
                & (
                    (col("st.fname") != col("tg.fname"))
                    | (col("st.lname") != col("tg.lname"))
                    | (col("st.age") != col("tg.age"))
                    | (col("st.store_id") != col("tg.store_id"))
                )
            ).update(
                set_={
                    "end_date": col("st.start_date"),
                }
            ),
            WhenNotMatched().insert(
                values={
                    "s_id": col("st.s_id"),
                    "employee_id": col("st.employee_id"),
                    "fname": col("st.fname"),
                    "lname": col("st.lname"),
                    "age": col("st.age"),
                    "store_id": col("st.store_id"),
                    "start_date": lit(start_date),
                    "end_date": lit(end_date),
                }
            ),
        ],
    )

    result = merge_expr.execute()
    if not isinstance(session, (PostgresSession, BigQuerySession)):
        if isinstance(session, SnowflakeSession):
            assert (result[0][0] + result[0][1]) == 3
        else:
            assert result[0][0] == 3

    df_merged = session.read.table("merge_employee").select(
        "employee_id", "fname", "lname", "age", "store_id", "start_date", "end_date"
    )

    assert df_merged.count() == 7
    assert sorted(df_merged.collect()) == [
        Row(
            employee_id=1,
            fname="Jack",
            lname="Shephard",
            age=37,
            store_id=1,
            start_date=datetime.date(1999, 1, 1),
            end_date=datetime.date(2024, 1, 1),
        ),
        Row(
            employee_id=1,
            fname="Jack",
            lname="Shephard",
            age=38,
            store_id=2,
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2900, 1, 1),
        ),
        Row(
            employee_id=2,
            fname="John",
            lname="Locke",
            age=65,
            store_id=1,
            start_date=datetime.date(1999, 1, 1),
            end_date=datetime.date(2900, 1, 1),
        ),
        Row(
            employee_id=3,
            fname="Kate",
            lname="Austen",
            age=37,
            store_id=2,
            start_date=datetime.date(1999, 1, 1),
            end_date=datetime.date(2900, 1, 1),
        ),
        Row(
            employee_id=4,
            fname="Claire",
            lname="Littleton",
            age=27,
            store_id=2,
            start_date=datetime.date(1999, 1, 1),
            end_date=datetime.date(2900, 1, 1),
        ),
        Row(
            employee_id=5,
            fname="Hugo",
            lname="Reyes",
            age=29,
            store_id=100,
            start_date=datetime.date(1999, 1, 1),
            end_date=datetime.date(2900, 1, 1),
        ),
        Row(
            employee_id=6,
            fname="Mary",
            lname="Sue",
            age=21,
            store_id=45,
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2900, 1, 1),
        ),
    ]
