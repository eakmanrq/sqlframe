import datetime

from sqlframe.base.types import Row
from sqlframe.duckdb import DuckDBDataFrame, DuckDBSession

pytest_plugins = ["tests.integration.fixtures"]


def test_print_schema_basic(duckdb_employee: DuckDBDataFrame, capsys):
    duckdb_employee.printSchema()
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


def test_print_schema_nested(duckdb_session: DuckDBSession, capsys):
    df = duckdb_session.createDataFrame(
        [
            (
                1,
                2.0,
                "foo",
                {"a": 1},
                [Row(a=1, b=2)],
                [1, 2, 3],
                Row(a=1),
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
            "map<string,bigint>_col",
            "array<struct<a:bigint,b:bigint>>",
            "array<bigint>_col",
            "struct<a:bigint>_col",
            "date_col",
            "timestamp_col",
            "timestamptz_col",
            "boolean_col",
        ],
    )
    df.printSchema()
    captured = capsys.readouterr()
    assert (
        captured.out.strip()
        == """
root
 |-- bigint_col: bigint (nullable = true)
 |-- double_col: double (nullable = true)
 |-- string_col: text (nullable = true)
 |-- map<string,bigint>_col: map(text, bigint) (nullable = true)
 |    |-- key: text (nullable = true)
 |    |-- value: bigint (nullable = true)
 |-- array<struct<a:bigint,b:bigint>>: struct(a bigint, b bigint)[] (nullable = true)
 |    |-- element: struct(a bigint, b bigint) (nullable = true)
 |    |    |-- a: bigint (nullable = true)
 |    |    |-- b: bigint (nullable = true)
 |-- array<bigint>_col: bigint[] (nullable = true)
 |    |-- element: bigint (nullable = true)
 |-- struct<a:bigint>_col: struct(a bigint) (nullable = true)
 |    |-- a: bigint (nullable = true)
 |-- date_col: date (nullable = true)
 |-- timestamp_col: timestamp (nullable = true)
 |-- timestamptz_col: timestamptz (nullable = true)
 |-- boolean_col: boolean (nullable = true)""".strip()
    )
