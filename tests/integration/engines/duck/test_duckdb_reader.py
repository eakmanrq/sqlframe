import os

import pytest

from sqlframe.base.types import Row
from sqlframe.duckdb import DuckDBSession
from sqlframe.duckdb import functions as F

pytest_plugins = ["tests.common_fixtures"]


def test_employee_extra_line_csv(duckdb_session: DuckDBSession):
    df = duckdb_session.read.load(
        "tests/fixtures/employee_extra_line.csv",
        format="csv",
        schema="employee_id INT, fname STRING, lname STRING, age INT, store_id INT",
        skip=1,
        header=1,
        filename=1,
        null_padding=True,
        ignore_errors=1,
        auto_detect=False,
    )
    assert df.collect() == [
        Row(
            **{
                "employee_id": 1,
                "fname": "Jack",
                "lname": "Shephard",
                "age": 37,
                "store_id": 1,
                "filename": "tests/fixtures/employee_extra_line.csv",
            }
        ),
        Row(
            **{
                "employee_id": 2,
                "fname": "John",
                "lname": "Locke",
                "age": 65,
                "store_id": 1,
                "filename": "tests/fixtures/employee_extra_line.csv",
            }
        ),
        Row(
            **{
                "employee_id": 3,
                "fname": "Kate",
                "lname": "Austen",
                "age": 37,
                "store_id": 2,
                "filename": "tests/fixtures/employee_extra_line.csv",
            }
        ),
        Row(
            **{
                "employee_id": 4,
                "fname": "Claire",
                "lname": "Littleton",
                "age": 27,
                "store_id": 2,
                "filename": "tests/fixtures/employee_extra_line.csv",
            }
        ),
        Row(
            **{
                "employee_id": 5,
                "fname": "Hugo",
                "lname": "Reyes",
                "age": 29,
                "store_id": 100,
                "filename": "tests/fixtures/employee_extra_line.csv",
            }
        ),
    ]


def test_employee_extra_line_csv_multiple(duckdb_session: DuckDBSession):
    df = duckdb_session.read.load(
        ["tests/fixtures/employee_extra_line.csv", "tests/fixtures/employee_extra_line.csv"],
        format="csv",
        schema="employee_id INT, fname STRING, lname STRING, age INT, store_id INT",
        skip=1,
        header=1,
        filename=0,
        null_padding=True,
        ignore_errors=1,
        auto_detect=False,
    )
    assert df.collect() == [
        Row(**{"employee_id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1}),
        Row(**{"employee_id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 1}),
        Row(**{"employee_id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 2}),
        Row(
            **{"employee_id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 2}
        ),
        Row(**{"employee_id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 100}),
        Row(**{"employee_id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1}),
        Row(**{"employee_id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 1}),
        Row(**{"employee_id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 2}),
        Row(
            **{"employee_id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 2}
        ),
        Row(**{"employee_id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 100}),
    ]


def test_employee_delta(duckdb_session: DuckDBSession):
    if os.environ.get("CI"):
        pytest.skip(
            "DuckDB Delta is not working in CI with DuckDB 1.2.1. Error: `duckdb.duckdb.Error: An error occurred while trying to automatically install the required extension 'delta'`"
        )
    df = duckdb_session.read.load(
        "tests/fixtures/employee_delta",
        format="delta",
    )
    assert df.collect() == [
        Row(**{"employee_id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1}),
        Row(**{"employee_id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 1}),
        Row(**{"employee_id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 2}),
        Row(
            **{"employee_id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 2}
        ),
        Row(**{"employee_id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 100}),
    ]


def test_issue_219(duckdb_session: DuckDBSession):
    df1 = duckdb_session.read.csv("tests/fixtures/issue_219.csv")
    df2 = df1.groupBy("kind", "make").agg(F.min("price"))
    # Just making sure this doesn't raise like it did before
    df2.show()

    # 222
    lightbulbs = duckdb_session.read.csv("tests/fixtures/issue_219.csv")
    min_prices = lightbulbs.groupBy("kind", "make").agg(F.min("price"))
    lightbulbs.join(min_prices, "make").show()


# https://github.com/eakmanrq/sqlframe/issues/242
def test_read_parquet_optimize(duckdb_session: DuckDBSession):
    df = duckdb_session.read.parquet(
        "tests/fixtures/employee.parquet"
    )  # Contains a `person_id` and a `person_source_value` column
    df.createOrReplaceTempView("employee")
    duckdb_session.table("employee").sql(optimize=True)  # type: ignore
