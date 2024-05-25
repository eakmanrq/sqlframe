import datetime

from sqlframe.base.types import Row
from sqlframe.duckdb import DuckDBDataFrame, DuckDBSession

pytest_plugins = ["tests.integration.fixtures"]


def test_print_schema_basic(postgres_employee: DuckDBDataFrame, capsys):
    postgres_employee.printSchema()
    captured = capsys.readouterr()
    assert (
        captured.out.strip()
        == """
root
 |-- employee_id: int (nullable = true)
 |-- fname: text (nullable = true)
 |-- lname: text (nullable = true)
 |-- age: int (nullable = true)
 |-- store_id: int (nullable = true)""".strip()
    )


def test_print_schema_nested(postgres_session: DuckDBSession, capsys):
    df = postgres_session.createDataFrame(
        [
            (
                1,
                2.0,
                "foo",
                [1, 2, 3],
                datetime.date(2022, 1, 1),
                datetime.datetime(2022, 1, 1, 0, 0, 0),
                datetime.datetime(2022, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
                True,
            )
        ],
        [
            "bigint_col",
            "double_col",
            "string_col",
            "array<bigint>_col",
            "date_col",
            "timestamp_col",
            "timestamptz_col",
            "boolean_col",
        ],
    )
    df.printSchema()
    captured = capsys.readouterr()
    # array does not include type
    assert (
        captured.out.strip()
        == """
root
 |-- bigint_col: bigint (nullable = true)
 |-- double_col: double precision (nullable = true)
 |-- string_col: text (nullable = true)
 |-- array<bigint>_col: array (nullable = true)
 |-- date_col: date (nullable = true)
 |-- timestamp_col: timestamp (nullable = true)
 |-- timestamptz_col: timestamptz (nullable = true)
 |-- boolean_col: boolean (nullable = true)""".strip()
    )
