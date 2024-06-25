import datetime

import pytest

from sqlframe.base.types import Row
from sqlframe.bigquery import BigQueryDataFrame, BigQuerySession

pytest_plugins = ["tests.integration.fixtures"]
pytestmark = [
    pytest.mark.bigquery,
    pytest.mark.xdist_group("bigquery_tests"),
]


def test_print_schema_basic(bigquery_employee: BigQueryDataFrame, capsys):
    bigquery_employee.printSchema()
    captured = capsys.readouterr()
    assert (
        captured.out.strip()
        == """
root
 |-- employee_id: int64 (nullable = true)
 |-- fname: string (nullable = true)
 |-- lname: string (nullable = true)
 |-- age: int64 (nullable = true)
 |-- store_id: int64 (nullable = true)""".strip()
    )


def test_print_schema_nested(bigquery_session: BigQuerySession, capsys):
    df = bigquery_session.createDataFrame(
        [
            (
                1,
                2.0,
                "foo",
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
            "array_struct_a_bigint_b_bigint__",
            "array_bigint__col",
            "struct_a_bigint__col",
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
 |-- bigint_col: int64 (nullable = true)
 |-- double_col: float64 (nullable = true)
 |-- string_col: string (nullable = true)
 |-- array_struct_a_bigint_b_bigint__: array<struct<a int64, b int64>> (nullable = false)
 |    |-- element: struct<a int64, b int64> (nullable = true)
 |    |    |-- a: int64 (nullable = true)
 |    |    |-- b: int64 (nullable = true)
 |-- array_bigint__col: array<int64> (nullable = false)
 |    |-- element: int64 (nullable = true)
 |-- struct_a_bigint__col: struct<a int64> (nullable = true)
 |    |-- a: int64 (nullable = true)
 |-- date_col: date (nullable = true)
 |-- timestamp_col: datetime (nullable = true)
 |-- timestamptz_col: timestamp (nullable = true)
 |-- boolean_col: bool (nullable = true)""".strip()
    )
