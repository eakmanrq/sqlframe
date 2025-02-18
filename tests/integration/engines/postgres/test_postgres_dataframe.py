import datetime

import pytest

from sqlframe.base import types
from sqlframe.postgres import PostgresDataFrame, PostgresSession

pytest_plugins = ["tests.integration.fixtures"]


@pytest.fixture()
def postgres_datatypes(postgres_session: PostgresSession) -> PostgresDataFrame:
    return postgres_session.createDataFrame(
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


def test_print_schema_basic(postgres_employee: PostgresDataFrame, capsys):
    postgres_employee.printSchema()
    captured = capsys.readouterr()
    assert (
        captured.out.strip()
        == """
root
 |-- employee_id: int (nullable = true)
 |-- fname: string (nullable = true)
 |-- lname: string (nullable = true)
 |-- age: int (nullable = true)
 |-- store_id: int (nullable = true)""".strip()
    )


def test_print_schema_nested(postgres_datatypes: PostgresDataFrame, capsys):
    postgres_datatypes.printSchema()
    captured = capsys.readouterr()
    # array does not include type
    assert (
        captured.out.strip()
        == """
root
 |-- bigint_col: bigint (nullable = true)
 |-- double_col: double (nullable = true)
 |-- string_col: string (nullable = true)
 |-- array<bigint>_col: array<bigint> (nullable = true)
 |    |-- element: bigint (nullable = true)
 |-- date_col: date (nullable = true)
 |-- timestamp_col: timestamp (nullable = true)
 |-- timestamptz_col: timestamp (nullable = true)
 |-- boolean_col: boolean (nullable = true)""".strip()
    )


def test_schema(postgres_employee: PostgresDataFrame):
    assert postgres_employee.schema == types.StructType(
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


def test_schema_nested(postgres_datatypes: PostgresDataFrame):
    assert isinstance(postgres_datatypes.schema, types.StructType)
    struct_fields = list(postgres_datatypes.schema)
    assert len(struct_fields) == 8
    assert struct_fields[0].name == "bigint_col"
    assert struct_fields[0].dataType == types.LongType()
    assert struct_fields[1].name == "double_col"
    assert struct_fields[1].dataType == types.DoubleType()
    assert struct_fields[2].name == "string_col"
    assert struct_fields[2].dataType == types.StringType()
    assert struct_fields[3].name == "array<bigint>_col"
    assert struct_fields[3].dataType == types.ArrayType(
        types.LongType(),
    )
    assert struct_fields[4].name == "date_col"
    assert struct_fields[4].dataType == types.DateType()
    assert struct_fields[5].name == "timestamp_col"
    assert struct_fields[5].dataType == types.TimestampType()
    assert struct_fields[6].name == "timestamptz_col"
    assert struct_fields[6].dataType == types.TimestampType()
    assert struct_fields[7].name == "boolean_col"
    assert struct_fields[7].dataType == types.BooleanType()


def test_explain(postgres_employee: PostgresDataFrame, capsys):
    postgres_employee.explain()
    assert (
        capsys.readouterr().out.strip()
        == """Values Scan on "*VALUES*"  (cost=0.00..0.06 rows=5 width=76)""".strip()
    )
