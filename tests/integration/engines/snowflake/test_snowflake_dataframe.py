import datetime

import pytest

from sqlframe.base.types import Row
from sqlframe.bigquery import BigQueryDataFrame, BigQuerySession

pytest_plugins = ["tests.integration.fixtures"]
pytestmark = [
    pytest.mark.snowflake,
    pytest.mark.xdist_group("snowflake_tests"),
]


def test_print_schema_basic(snowflake_employee: BigQueryDataFrame, capsys):
    snowflake_employee.printSchema()
    captured = capsys.readouterr()
    assert (
        captured.out.strip()
        == """
root
 |-- EMPLOYEE_ID: decimal(38, 0) (nullable = true)
 |-- FNAME: varchar(16777216) (nullable = true)
 |-- LNAME: varchar(16777216) (nullable = true)
 |-- AGE: decimal(38, 0) (nullable = true)
 |-- STORE_ID: decimal(38, 0) (nullable = true)""".strip()
    )


def test_print_schema_nested(snowflake_session: BigQuerySession, capsys):
    df = snowflake_session.createDataFrame(
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
            "map_string_bigint__col",
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
 |-- BIGINT_COL: decimal(38, 0) (nullable = true)
 |-- DOUBLE_COL: float (nullable = true)
 |-- STRING_COL: varchar(16777216) (nullable = true)
 |-- MAP_STRING_BIGINT__COL: map(varchar(16777216), decimal(38, 0)) (nullable = true)
 |    |-- key: varchar(16777216) (nullable = true)
 |    |-- value: decimal(38, 0) (nullable = true)
 |-- ARRAY_STRUCT_A_BIGINT_B_BIGINT__: array(object(a decimal(38, 0), b decimal(38, 0))) (nullable = true)
 |    |-- element: object(a decimal(38, 0), b decimal(38, 0)) (nullable = true)
 |    |    |-- A: decimal(38, 0) (nullable = true)
 |    |    |-- B: decimal(38, 0) (nullable = true)
 |-- ARRAY_BIGINT__COL: array(decimal(38, 0)) (nullable = true)
 |    |-- element: decimal(38, 0) (nullable = true)
 |-- STRUCT_A_BIGINT__COL: object(a decimal(38, 0)) (nullable = true)
 |    |-- A: decimal(38, 0) (nullable = true)
 |-- DATE_COL: date (nullable = true)
 |-- TIMESTAMP_COL: timestampntz(9) (nullable = true)
 |-- TIMESTAMPTZ_COL: timestamptz(9) (nullable = true)
 |-- BOOLEAN_COL: boolean (nullable = true)""".strip()
    )
