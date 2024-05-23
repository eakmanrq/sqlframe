from sqlframe.base.types import Row
from sqlframe.duckdb import DuckDBSession

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
