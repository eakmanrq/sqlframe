import pytest

from sqlframe.standalone import types


@pytest.mark.parametrize(
    "dtype, expected",
    [
        (types.StringType(), "string"),
        (types.CharType(100), "char"),
        (types.VarcharType(65), "varchar"),
        (types.BinaryType(), "binary"),
        (types.BooleanType(), "boolean"),
        (types.DateType(), "date"),
        (types.TimestampType(), "timestamp"),
        (types.TimestampNTZType(), "timestamp_ntz"),
        (types.DecimalType(10, 3), "decimal(10, 3)"),
        (types.DoubleType(), "double"),
        (types.FloatType(), "float"),
        (types.ByteType(), "byte"),
        (types.IntegerType(), "integer"),
        (types.LongType(), "long"),
        (types.ShortType(), "short"),
        (types.ArrayType(types.IntegerType()), "array<integer>"),
        (types.MapType(types.IntegerType(), types.StringType()), "map<integer, string>"),
        (
            types.StructType(
                [
                    types.StructField("cola", types.IntegerType()),
                    types.StructField("colb", types.StringType()),
                ]
            ),
            "struct<cola:integer, colb:string>",
        ),
    ],
)
def test_string(dtype, expected):
    assert dtype.simpleString() == expected
