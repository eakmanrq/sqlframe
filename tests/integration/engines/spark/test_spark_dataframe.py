import datetime

import pytest

from sqlframe.base import types
from sqlframe.spark import SparkDataFrame, SparkSession

pytest_plugins = ["tests.integration.fixtures"]


@pytest.fixture()
def spark_datatypes(spark_session: SparkSession) -> SparkDataFrame:
    return spark_session.createDataFrame(
        [
            (
                1,
                2.0,
                "foo",
                {"a": 1},
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


def test_print_schema_basic(spark_employee: SparkDataFrame, capsys):
    spark_employee.printSchema()
    captured = capsys.readouterr()
    assert (
        captured.out.strip()
        == """
root
 |-- employee_id: int (nullable = false)
 |-- fname: string (nullable = false)
 |-- lname: string (nullable = false)
 |-- age: int (nullable = false)
 |-- store_id: int (nullable = false)""".strip()
    )


def test_print_schema_nested(spark_datatypes: SparkDataFrame, capsys):
    spark_datatypes.printSchema()
    captured = capsys.readouterr()
    assert (
        captured.out.strip()
        == """
root
 |-- bigint_col: bigint (nullable = false)
 |-- double_col: double (nullable = false)
 |-- string_col: string (nullable = false)
 |-- map<string,bigint>_col: map<string, bigint> (nullable = false)
 |    |-- key: string (nullable = true)
 |    |-- value: bigint (nullable = true)
 |-- array<struct<a:bigint,b:bigint>>: array<struct<a: bigint, b: bigint>> (nullable = false)
 |    |-- element: struct<a: bigint, b: bigint> (nullable = true)
 |    |    |-- a: bigint (nullable = true)
 |    |    |-- b: bigint (nullable = true)
 |-- array<bigint>_col: array<bigint> (nullable = false)
 |    |-- element: bigint (nullable = true)
 |-- struct<a:bigint>_col: struct<a: bigint> (nullable = false)
 |    |-- a: bigint (nullable = true)
 |-- date_col: date (nullable = true)
 |-- timestamp_col: timestamp (nullable = true)
 |-- timestamptz_col: timestamp (nullable = true)
 |-- boolean_col: boolean (nullable = false)""".strip()
    )


def test_schema(spark_employee: SparkDataFrame):
    assert spark_employee.schema == types.StructType(
        [
            types.StructField(
                "employee_id",
                types.IntegerType(),
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
                types.IntegerType(),
            ),
            types.StructField(
                "store_id",
                types.IntegerType(),
            ),
        ]
    )


def test_schema_nested(spark_datatypes: SparkDataFrame):
    assert isinstance(spark_datatypes.schema, types.StructType)
    struct_fields = list(spark_datatypes.schema)
    assert len(struct_fields) == 11
    assert struct_fields[0].name == "bigint_col"
    assert struct_fields[0].dataType == types.LongType()
    assert struct_fields[1].name == "double_col"
    assert struct_fields[1].dataType == types.DoubleType()
    assert struct_fields[2].name == "string_col"
    assert struct_fields[2].dataType == types.StringType()
    assert struct_fields[3].name == "map<string,bigint>_col"
    assert struct_fields[3].dataType == types.MapType(
        types.StringType(),
        types.LongType(),
    )
    assert struct_fields[4].name == "array<struct<a:bigint,b:bigint>>"
    assert struct_fields[4].dataType == types.ArrayType(
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
    assert struct_fields[5].name == "array<bigint>_col"
    assert struct_fields[5].dataType == types.ArrayType(
        types.LongType(),
    )
    assert struct_fields[6].name == "struct<a:bigint>_col"
    assert struct_fields[6].dataType == types.StructType(
        [
            types.StructField(
                "a",
                types.LongType(),
            ),
        ]
    )
    assert struct_fields[7].name == "date_col"
    assert struct_fields[7].dataType == types.DateType()
    assert struct_fields[8].name == "timestamp_col"
    assert struct_fields[8].dataType == types.TimestampType()
    assert struct_fields[9].name == "timestamptz_col"
    assert struct_fields[9].dataType == types.TimestampType()
    assert struct_fields[10].name == "boolean_col"
    assert struct_fields[10].dataType == types.BooleanType()


def test_explain(spark_employee: SparkDataFrame, capsys):
    spark_employee.explain()
    output = capsys.readouterr().out.strip()
    assert "== Physical Plan ==" in output
    assert "LocalTableScan" in output
