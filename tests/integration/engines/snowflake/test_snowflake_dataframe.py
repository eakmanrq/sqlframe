import datetime

import pytest

from sqlframe.base import types
from sqlframe.snowflake import SnowflakeDataFrame, SnowflakeSession

pytest_plugins = ["tests.integration.fixtures"]
pytestmark = [
    pytest.mark.snowflake,
    pytest.mark.xdist_group("snowflake_tests"),
]


@pytest.fixture()
def snowflake_datatypes(snowflake_session: SnowflakeSession) -> SnowflakeDataFrame:
    return snowflake_session.createDataFrame(
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


def test_print_schema_basic(snowflake_employee: SnowflakeDataFrame, capsys):
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


def test_print_schema_nested(snowflake_datatypes: SnowflakeDataFrame, capsys):
    snowflake_datatypes.printSchema()
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


def test_schema(snowflake_employee: SnowflakeDataFrame):
    assert isinstance(snowflake_employee.schema, types.StructType)
    struct_fields = list(snowflake_employee.schema)
    assert len(struct_fields) == 5
    assert struct_fields[0].name == "EMPLOYEE_ID"
    assert struct_fields[0].dataType == types.DecimalType(38, 0)
    assert struct_fields[1].name == "FNAME"
    assert struct_fields[1].dataType == types.VarcharType(16777216)
    assert struct_fields[2].name == "LNAME"
    assert struct_fields[2].dataType == types.VarcharType(16777216)
    assert struct_fields[3].name == "AGE"
    assert struct_fields[3].dataType == types.DecimalType(38, 0)
    assert struct_fields[4].name == "STORE_ID"
    assert struct_fields[4].dataType == types.DecimalType(38, 0)


def test_schema_nested(snowflake_datatypes: SnowflakeDataFrame):
    assert isinstance(snowflake_datatypes.schema, types.StructType)
    struct_fields = list(snowflake_datatypes.schema)
    assert len(struct_fields) == 11
    assert struct_fields[0].name == "BIGINT_COL"
    assert struct_fields[0].dataType == types.DecimalType(38, 0)
    assert struct_fields[1].name == "DOUBLE_COL"
    assert struct_fields[1].dataType == types.FloatType()
    assert struct_fields[2].name == "STRING_COL"
    assert struct_fields[2].dataType == types.VarcharType(16777216)
    assert struct_fields[3].name == "MAP_STRING_BIGINT__COL"
    assert struct_fields[3].dataType == types.MapType(
        types.VarcharType(16777216),
        types.DecimalType(38, 0),
    )
    assert struct_fields[4].name == "ARRAY_STRUCT_A_BIGINT_B_BIGINT__"
    assert struct_fields[4].dataType == types.ArrayType(
        types.StructType(
            [
                types.StructField(
                    "A",
                    types.DecimalType(38, 0),
                ),
                types.StructField(
                    "B",
                    types.DecimalType(38, 0),
                ),
            ]
        ),
    )
    assert struct_fields[5].name == "ARRAY_BIGINT__COL"
    assert struct_fields[5].dataType == types.ArrayType(
        types.DecimalType(38, 0),
    )
    assert struct_fields[6].name == "STRUCT_A_BIGINT__COL"
    assert struct_fields[6].dataType == types.StructType(
        [
            types.StructField(
                "A",
                types.DecimalType(38, 0),
            ),
        ]
    )
    assert struct_fields[7].name == "DATE_COL"
    assert struct_fields[7].dataType == types.DateType()
    assert struct_fields[8].name == "TIMESTAMP_COL"
    assert struct_fields[8].dataType == types.TimestampType()
    assert struct_fields[9].name == "TIMESTAMPTZ_COL"
    assert struct_fields[9].dataType == types.TimestampType()
    assert struct_fields[10].name == "BOOLEAN_COL"
    assert struct_fields[10].dataType == types.BooleanType()
