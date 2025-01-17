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
 |-- employee_id: decimal(38, 0) (nullable = true)
 |-- fname: varchar(16777216) (nullable = true)
 |-- lname: varchar(16777216) (nullable = true)
 |-- age: decimal(38, 0) (nullable = true)
 |-- store_id: decimal(38, 0) (nullable = true)""".strip()
    )


def test_print_schema_nested(snowflake_datatypes: SnowflakeDataFrame, capsys):
    snowflake_datatypes.printSchema()
    captured = capsys.readouterr()
    assert (
        captured.out.strip()
        == """
root
 |-- bigint_col: decimal(38, 0) (nullable = true)
 |-- double_col: float (nullable = true)
 |-- string_col: varchar(16777216) (nullable = true)
 |-- map_string_bigint__col: map<varchar(16777216), decimal(38, 0)> (nullable = true)
 |    |-- key: varchar(16777216) (nullable = true)
 |    |-- value: decimal(38, 0) (nullable = true)
 |-- array_struct_a_bigint_b_bigint__: array<object<a decimal(38, 0), b decimal(38, 0)>> (nullable = true)
 |    |-- element: object<a decimal(38, 0), b decimal(38, 0)> (nullable = true)
 |    |    |-- a: decimal(38, 0) (nullable = true)
 |    |    |-- b: decimal(38, 0) (nullable = true)
 |-- array_bigint__col: array<decimal(38, 0)> (nullable = true)
 |    |-- element: decimal(38, 0) (nullable = true)
 |-- struct_a_bigint__col: object<a decimal(38, 0)> (nullable = true)
 |    |-- a: decimal(38, 0) (nullable = true)
 |-- date_col: date (nullable = true)
 |-- timestamp_col: timestamp (nullable = true)
 |-- timestamptz_col: timestamp (nullable = true)
 |-- boolean_col: boolean (nullable = true)""".strip()
    )


def test_schema(snowflake_employee: SnowflakeDataFrame):
    assert isinstance(snowflake_employee.schema, types.StructType)
    struct_fields = list(snowflake_employee.schema)
    assert len(struct_fields) == 5
    assert struct_fields[0].name == "employee_id"
    assert struct_fields[0].dataType == types.DecimalType(38, 0)
    assert struct_fields[1].name == "fname"
    assert struct_fields[1].dataType == types.VarcharType(16777216)
    assert struct_fields[2].name == "lname"
    assert struct_fields[2].dataType == types.VarcharType(16777216)
    assert struct_fields[3].name == "age"
    assert struct_fields[3].dataType == types.DecimalType(38, 0)
    assert struct_fields[4].name == "store_id"
    assert struct_fields[4].dataType == types.DecimalType(38, 0)


def test_schema_nested(snowflake_datatypes: SnowflakeDataFrame):
    assert isinstance(snowflake_datatypes.schema, types.StructType)
    struct_fields = list(snowflake_datatypes.schema)
    assert len(struct_fields) == 11
    assert struct_fields[0].name == "bigint_col"
    assert struct_fields[0].dataType == types.DecimalType(38, 0)
    assert struct_fields[1].name == "double_col"
    assert struct_fields[1].dataType == types.FloatType()
    assert struct_fields[2].name == "string_col"
    assert struct_fields[2].dataType == types.VarcharType(16777216)
    assert struct_fields[3].name == "map_string_bigint__col"
    assert struct_fields[3].dataType == types.MapType(
        types.VarcharType(16777216),
        types.DecimalType(38, 0),
    )
    assert struct_fields[4].name == "array_struct_a_bigint_b_bigint__"
    assert struct_fields[4].dataType == types.ArrayType(
        types.StructType(
            [
                types.StructField(
                    "a",
                    types.DecimalType(38, 0),
                ),
                types.StructField(
                    "b",
                    types.DecimalType(38, 0),
                ),
            ]
        ),
    )
    assert struct_fields[5].name == "array_bigint__col"
    assert struct_fields[5].dataType == types.ArrayType(
        types.DecimalType(38, 0),
    )
    assert struct_fields[6].name == "struct_a_bigint__col"
    assert struct_fields[6].dataType == types.StructType(
        [
            types.StructField(
                "a",
                types.DecimalType(38, 0),
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


def test_explain(snowflake_employee: SnowflakeDataFrame, capsys):
    snowflake_employee.explain()
    assert (
        capsys.readouterr().out.strip()
        == """
GlobalStats:
    partitionsTotal=0
    partitionsAssigned=0
    bytesAssigned=0
Operations:
1:0     ->Result  A1.EMPLOYEE_ID, A1.FNAME, A1.LNAME, A1.AGE, A1.STORE_ID  
1:1          ->ValuesClause  (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)
""".strip()
    )
