import datetime

import pytest

from sqlframe.base import types
from sqlframe.databricks import DatabricksDataFrame, DatabricksSession

pytest_plugins = ["tests.integration.fixtures"]
pytestmark = [
    pytest.mark.databricks,
    pytest.mark.xdist_group("databricks_tests"),
]


@pytest.fixture()
def databricks_datatypes(databricks_session: DatabricksSession) -> DatabricksDataFrame:
    return databricks_session.createDataFrame(
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


def test_print_schema_basic(databricks_employee: DatabricksDataFrame, capsys):
    databricks_employee.printSchema()
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


def test_print_schema_nested(databricks_datatypes: DatabricksDataFrame, capsys):
    databricks_datatypes.printSchema()
    captured = capsys.readouterr()
    assert (
        captured.out.strip()
        == """
root
 |-- bigint_col: bigint (nullable = true)
 |-- double_col: double (nullable = true)
 |-- string_col: string (nullable = true)
 |-- map<string,bigint>_col: map<string, bigint> (nullable = true)
 |    |-- key: string (nullable = true)
 |    |-- value: bigint (nullable = true)
 |-- array<struct<a:bigint,b:bigint>>: array<struct<a: bigint, b: bigint>> (nullable = true)
 |    |-- element: struct<a: bigint, b: bigint> (nullable = true)
 |    |    |-- a: bigint (nullable = true)
 |    |    |-- b: bigint (nullable = true)
 |-- array<bigint>_col: array<bigint> (nullable = true)
 |    |-- element: bigint (nullable = true)
 |-- struct<a:bigint>_col: struct<a: bigint> (nullable = true)
 |    |-- a: bigint (nullable = true)
 |-- date_col: date (nullable = true)
 |-- timestamp_col: timestamp (nullable = true)
 |-- timestamptz_col: timestamp (nullable = true)
 |-- boolean_col: boolean (nullable = true)""".strip()
    )


def test_schema(databricks_employee: DatabricksDataFrame):
    assert databricks_employee.schema == types.StructType(
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


def test_schema_nested(databricks_datatypes: DatabricksDataFrame):
    assert isinstance(databricks_datatypes.schema, types.StructType)
    struct_fields = list(databricks_datatypes.schema)
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


def test_explain(databricks_employee: DatabricksDataFrame, capsys):
    databricks_employee.explain()
    output = capsys.readouterr().out.strip()
    assert "== Physical Plan ==" in output
    assert "LocalTableScan" in output
    assert "== Photon Explanation ==" in output


def test_to_arrow(databricks_employee: DatabricksDataFrame):
    arrow_table = databricks_employee.toArrow()
    assert arrow_table.num_rows == 5
    assert arrow_table.num_columns == 5
    assert arrow_table.column_names == [
        "employee_id",
        "fname",
        "lname",
        "age",
        "store_id",
    ]
    assert arrow_table.column(0).to_pylist() == [1, 2, 3, 4, 5]
    assert arrow_table.column(1).to_pylist() == ["Jack", "John", "Kate", "Claire", "Hugo"]
    assert arrow_table.column(2).to_pylist() == [
        "Shephard",
        "Locke",
        "Austen",
        "Littleton",
        "Reyes",
    ]
    assert arrow_table.column(3).to_pylist() == [37, 65, 37, 27, 29]
    assert arrow_table.column(4).to_pylist() == [1, 1, 2, 2, 100]


def test_to_arrow_batch(databricks_employee: DatabricksDataFrame):
    record_batch_reader = databricks_employee.toArrow(batch_size=1)
    first_batch = record_batch_reader.read_next_batch()
    assert first_batch.num_rows == 1
    assert first_batch.num_columns == 5
    assert first_batch.column_names == [
        "employee_id",
        "fname",
        "lname",
        "age",
        "store_id",
    ]
    assert first_batch.column(0).to_pylist() == [1]
    assert first_batch.column(1).to_pylist() == ["Jack"]
    assert first_batch.column(2).to_pylist() == ["Shephard"]
    assert first_batch.column(3).to_pylist() == [37]
    assert first_batch.column(4).to_pylist() == [1]
    second_batch = record_batch_reader.read_next_batch()
    assert second_batch.num_rows == 1
    assert second_batch.num_columns == 5
    assert second_batch.column(0).to_pylist() == [2]
    assert second_batch.column(1).to_pylist() == ["John"]
    assert second_batch.column(2).to_pylist() == ["Locke"]
    assert second_batch.column(3).to_pylist() == [65]
    assert second_batch.column(4).to_pylist() == [1]
    third_batch = record_batch_reader.read_next_batch()
    assert third_batch.num_rows == 1
    assert third_batch.num_columns == 5
    assert third_batch.column(0).to_pylist() == [3]
    assert third_batch.column(1).to_pylist() == ["Kate"]
    assert third_batch.column(2).to_pylist() == ["Austen"]
    assert third_batch.column(3).to_pylist() == [37]
    assert third_batch.column(4).to_pylist() == [2]
    fourth_batch = record_batch_reader.read_next_batch()
    assert fourth_batch.num_rows == 1
    assert fourth_batch.num_columns == 5
    assert fourth_batch.column(0).to_pylist() == [4]
    assert fourth_batch.column(1).to_pylist() == ["Claire"]
    assert fourth_batch.column(2).to_pylist() == ["Littleton"]
    assert fourth_batch.column(3).to_pylist() == [27]
    assert fourth_batch.column(4).to_pylist() == [2]
    fifth_batch = record_batch_reader.read_next_batch()
    assert fifth_batch.num_rows == 1
    assert fifth_batch.num_columns == 5
    assert fifth_batch.column(0).to_pylist() == [5]
    assert fifth_batch.column(1).to_pylist() == ["Hugo"]
    assert fifth_batch.column(2).to_pylist() == ["Reyes"]
    assert fifth_batch.column(3).to_pylist() == [29]
    assert fifth_batch.column(4).to_pylist() == [100]
    with pytest.raises(StopIteration):
        record_batch_reader.read_next_batch()
