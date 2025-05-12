import pytest
from sqlglot import exp

from sqlframe.base import types
from sqlframe.base.util import spark_to_sqlglot
from sqlframe.standalone.session import StandaloneSession


@pytest.fixture(scope="function")
def standalone_session() -> StandaloneSession:
    return StandaloneSession()


def test_add_table_with_spark_types(standalone_session: StandaloneSession):
    """Test that add_table properly converts spark types to sqlglot types."""
    # Create a dictionary with column names as keys and spark types as values
    column_mapping = {
        "col_string": types.StringType(),
        "col_int": types.IntegerType(),
        "col_long": types.LongType(),
        "col_float": types.FloatType(),
        "col_double": types.DoubleType(),
        "col_boolean": types.BooleanType(),
        "col_timestamp": types.TimestampType(),
        "col_date": types.DateType(),
        "col_decimal": types.DecimalType(10, 2),
        "col_binary": types.BinaryType(),
        "col_array": types.ArrayType(types.StringType()),
        "col_map": types.MapType(types.StringType(), types.IntegerType()),
        "col_struct": types.StructType(
            [
                types.StructField("nested_string", types.StringType()),
                types.StructField("nested_int", types.IntegerType()),
            ]
        ),
    }

    # Call add_table with the dictionary
    table_name = "test_table"
    standalone_session.catalog.add_table(table_name, column_mapping)

    # Get the schema from the catalog
    table = exp.to_table(table_name, dialect=standalone_session.input_dialect)
    schema = standalone_session.catalog._schema.find(table)

    # Verify that the schema has been properly updated with the expected sqlglot types
    assert schema is not None

    # Check each column type
    for col_name, spark_type in column_mapping.items():
        expected_sqlglot_type = spark_to_sqlglot(spark_type)
        actual_sqlglot_type = schema[col_name]

        # Compare the SQL representation of the types
        assert actual_sqlglot_type.sql() == expected_sqlglot_type.sql(), (
            f"Column {col_name}: expected {expected_sqlglot_type.sql()}, got {actual_sqlglot_type.sql()}"
        )
