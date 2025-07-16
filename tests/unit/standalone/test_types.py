import pytest

from sqlframe.standalone import types


@pytest.mark.parametrize(
    "dtype, expected",
    [
        (types.StringType(), "string"),
        (types.CharType(100), "char(100)"),
        (types.VarcharType(65), "varchar(65)"),
        (types.BinaryType(), "binary"),
        (types.BooleanType(), "boolean"),
        (types.DateType(), "date"),
        (types.TimestampType(), "timestamp"),
        (types.TimestampNTZType(), "timestamp_ntz"),
        (types.DecimalType(10, 3), "decimal(10, 3)"),
        (types.DoubleType(), "double"),
        (types.FloatType(), "float"),
        (types.ByteType(), "tinyint"),
        (types.IntegerType(), "int"),
        (types.LongType(), "bigint"),
        (types.ShortType(), "smallint"),
        (types.ArrayType(types.IntegerType()), "array<int>"),
        (types.MapType(types.IntegerType(), types.StringType()), "map<int, string>"),
        (
            types.StructType(
                [
                    types.StructField("cola", types.IntegerType()),
                    types.StructField("colb", types.StringType()),
                ]
            ),
            "struct<cola:int, colb:string>",
        ),
    ],
)
def test_string(dtype, expected):
    assert dtype.simpleString() == expected
