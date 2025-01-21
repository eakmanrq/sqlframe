import datetime

import pytest

from sqlframe.base import types
from sqlframe.bigquery import BigQueryDataFrame, BigQuerySession

pytest_plugins = ["tests.integration.fixtures"]
pytestmark = [
    pytest.mark.bigquery,
    pytest.mark.xdist_group("bigquery_tests"),
]


@pytest.fixture()
def bigquery_datatypes(bigquery_session: BigQuerySession) -> BigQueryDataFrame:
    return bigquery_session.createDataFrame(
        [
            (
                1,
                2.0,
                "foo",
                [types.Row(a=1, b=2)],
                [1, 2, 3],
                types.Row(a=1),
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


def test_print_schema_basic(bigquery_employee: BigQueryDataFrame, capsys):
    bigquery_employee.printSchema()
    captured = capsys.readouterr()
    assert (
        captured.out.strip()
        == """
root
 |-- employee_id: bigint (nullable = true)
 |-- fname: string (nullable = true)
 |-- lname: string (nullable = true)
 |-- age: bigint (nullable = true)
 |-- store_id: bigint (nullable = true)""".strip()
    )


def test_print_schema_nested(bigquery_datatypes: BigQueryDataFrame, capsys):
    bigquery_datatypes.printSchema()
    captured = capsys.readouterr()
    assert (
        captured.out.strip()
        == """
root
 |-- bigint_col: bigint (nullable = true)
 |-- double_col: float (nullable = true)
 |-- string_col: string (nullable = true)
 |-- array_struct_a_bigint_b_bigint__: array<struct<a: bigint, b: bigint>> (nullable = false)
 |    |-- element: struct<a: bigint, b: bigint> (nullable = true)
 |    |    |-- a: bigint (nullable = true)
 |    |    |-- b: bigint (nullable = true)
 |-- array_bigint__col: array<bigint> (nullable = false)
 |    |-- element: bigint (nullable = true)
 |-- struct_a_bigint__col: struct<a: bigint> (nullable = true)
 |    |-- a: bigint (nullable = true)
 |-- date_col: date (nullable = true)
 |-- timestamp_col: timestamp (nullable = true)
 |-- timestamptz_col: timestamp (nullable = true)
 |-- boolean_col: boolean (nullable = true)""".strip()
    )


def test_schema(bigquery_employee: BigQueryDataFrame):
    assert bigquery_employee.schema == types.StructType(
        [
            types.StructField(
                "employee_id",
                types.LongType(),
            ),
            types.StructField(
                "fname",
                types.StringType(),
            ),
            types.StructField(
                "lname",
                types.StringType(),
            ),
            types.StructField(
                "age",
                types.LongType(),
            ),
            types.StructField(
                "store_id",
                types.LongType(),
            ),
        ]
    )


def test_schema_nested(bigquery_datatypes: BigQueryDataFrame):
    assert isinstance(bigquery_datatypes.schema, types.StructType)
    struct_fields = list(bigquery_datatypes.schema)
    assert len(struct_fields) == 10
    assert struct_fields[0].name == "bigint_col"
    assert struct_fields[0].dataType == types.LongType()
    assert struct_fields[1].name == "double_col"
    assert struct_fields[1].dataType == types.FloatType()
    assert struct_fields[2].name == "string_col"
    assert struct_fields[2].dataType == types.StringType()
    assert struct_fields[3].name == "array_struct_a_bigint_b_bigint__"
    assert struct_fields[3].dataType == types.ArrayType(
        types.StructType(
            [
                types.StructField(
                    "a",
                    types.LongType(),
                ),
                types.StructField(
                    "b",
                    types.LongType(),
                ),
            ]
        ),
    )
    assert struct_fields[4].name == "array_bigint__col"
    assert struct_fields[4].dataType == types.ArrayType(
        types.LongType(),
    )
    assert struct_fields[5].name == "struct_a_bigint__col"
    assert struct_fields[5].dataType == types.StructType(
        [
            types.StructField(
                "a",
                types.LongType(),
            ),
        ]
    )
    assert struct_fields[6].name == "date_col"
    assert struct_fields[6].dataType == types.DateType()
    assert struct_fields[7].name == "timestamp_col"
    assert struct_fields[7].dataType == types.TimestampType()
    assert struct_fields[8].name == "timestamptz_col"
    assert struct_fields[8].dataType == types.TimestampType()
    assert struct_fields[9].name == "boolean_col"
    assert struct_fields[9].dataType == types.BooleanType()


def test_explain(bigquery_employee: BigQueryDataFrame):
    with pytest.raises(NotImplementedError):
        bigquery_employee.explain()
