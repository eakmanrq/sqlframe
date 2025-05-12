import typing as t

import pytest
from sqlglot import exp, parse_one
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sqlframe.base import types
from sqlframe.base.util import (
    quote_preserving_alias_or_name,
    spark_to_sqlglot,
    sqlglot_to_spark,
)


@pytest.mark.parametrize(
    "expression, expected",
    [
        ("a", "a"),
        ("a AS b", "b"),
        ("`a`", "`a`"),
        ("`a` AS b", "b"),
        ("`a` AS `b`", "`b`"),
        ("`aB`", "`aB`"),
        ("`aB` AS c", "c"),
        ("`aB` AS `c`", "`c`"),
        ("`aB` AS `Cd`", "`Cd`"),
        # We assume inputs have been normalized so `Cd` is returned as is instead of normalized `cd`
        ("`aB` AS Cd", "Cd"),
    ],
)
def test_quote_preserving_alias_or_name(expression: t.Union[exp.Column, exp.Alias], expected: str):
    assert quote_preserving_alias_or_name(parse_one(expression, dialect="bigquery")) == expected  # type: ignore


@pytest.mark.parametrize(
    "dtype, expected",
    [
        ("STRING", types.StringType()),
        ("VARCHAR(100)", types.VarcharType(100)),
        ("CHAR(100)", types.CharType(100)),
        ("DECIMAL(10, 2)", types.DecimalType(10, 2)),
        ("STRING", types.StringType()),
        ("INTEGER", types.IntegerType()),
        ("BIGINT", types.LongType()),
        ("SMALLINT", types.ShortType()),
        ("TINYINT", types.ByteType()),
        ("FLOAT", types.FloatType()),
        ("DOUBLE", types.DoubleType()),
        ("BOOLEAN", types.BooleanType()),
        ("TIMESTAMP", types.TimestampType()),
        ("DATE", types.DateType()),
        ("DECIMAL", types.DecimalType()),
        ("BINARY", types.BinaryType()),
        ("ARRAY<STRING>", types.ArrayType(types.StringType())),
        ("MAP<STRING, INTEGER>", types.MapType(types.StringType(), types.IntegerType())),
        (
            "STRUCT<a STRING, b INTEGER>",
            types.StructType(
                [
                    types.StructField("a", types.StringType()),
                    types.StructField("b", types.IntegerType()),
                ]
            ),
        ),
        (
            "ARRAY<STRUCT<a STRING, b INTEGER>>",
            types.ArrayType(
                types.StructType(
                    [
                        types.StructField("a", types.StringType()),
                        types.StructField("b", types.IntegerType()),
                    ]
                )
            ),
        ),
    ],
)
def test_sqlglot_to_spark(dtype: str, expected: types.DataType):
    assert sqlglot_to_spark(exp.DataType.build(dtype)) == expected


@pytest.mark.parametrize(
    "spark_dtype, expected_str",
    [
        (types.StringType(), "TEXT"),
        (types.VarcharType(100), "VARCHAR(100)"),
        (types.CharType(100), "CHAR(100)"),
        (types.DecimalType(11, 2), "DECIMAL(11, 2)"),
        (types.IntegerType(), "INT"),
        (types.LongType(), "BIGINT"),
        (types.ShortType(), "SMALLINT"),
        (types.ByteType(), "TINYINT"),
        (types.FloatType(), "FLOAT"),
        (types.DoubleType(), "DOUBLE"),
        (types.BooleanType(), "BOOLEAN"),
        (types.TimestampType(), "TIMESTAMP"),
        (types.TimestampNTZType(), "TIMESTAMPNTZ"),
        (types.DateType(), "DATE"),
        (types.DecimalType(), "DECIMAL(10, 0)"),
        (types.BinaryType(), "BINARY"),
        (types.ArrayType(types.StringType()), "ARRAY(TEXT)"),
        (types.MapType(types.StringType(), types.IntegerType()), "MAP(TEXT, INT)"),
        (
            types.StructType(
                [
                    types.StructField("a", types.StringType()),
                    types.StructField("b", types.IntegerType()),
                ]
            ),
            "STRUCT(a TEXT, b INT)",
        ),
        (
            types.ArrayType(
                types.StructType(
                    [
                        types.StructField("a", types.StringType()),
                        types.StructField("b", types.IntegerType()),
                    ]
                )
            ),
            "ARRAY(STRUCT(a TEXT, b INT))",
        ),
    ],
)
def test_spark_to_sqlglot(spark_dtype: types.DataType, expected_str: str):
    sqlglot_dtype = spark_to_sqlglot(spark_dtype)
    assert sqlglot_dtype.sql() == expected_str


@pytest.mark.parametrize(
    "dtype, expected_str",
    [
        ("STRING", "TEXT"),
        ("VARCHAR(100)", "VARCHAR(100)"),
        ("CHAR(100)", "CHAR(100)"),
        ("DECIMAL(10, 2)", "DECIMAL(10, 2)"),
        ("INTEGER", "INT"),
        ("BIGINT", "BIGINT"),
        ("SMALLINT", "SMALLINT"),
        ("TINYINT", "TINYINT"),
        ("FLOAT", "FLOAT"),
        ("DOUBLE", "DOUBLE"),
        ("BOOLEAN", "BOOLEAN"),
        ("TIMESTAMP", "TIMESTAMP"),
        ("DATE", "DATE"),
        ("DECIMAL", "DECIMAL(10, 0)"),
        ("BINARY", "BINARY"),
        ("ARRAY<STRING>", "ARRAY(TEXT)"),
        ("MAP<STRING, INTEGER>", "MAP(TEXT, INT)"),
        (
            "STRUCT<a STRING, b INTEGER>",
            "STRUCT(a TEXT, b INT)",
        ),
        (
            "ARRAY<STRUCT<a STRING, b INTEGER>>",
            "ARRAY(STRUCT(a TEXT, b INT))",
        ),
    ],
)
def test_sqlglot_to_spark_to_sqlglot(dtype: str, expected_str: str):
    """Test round trip conversion from SQLGlot to Spark and back to SQLGlot."""
    spark_dtype = sqlglot_to_spark(exp.DataType.build(dtype))
    sqlglot_dtype = spark_to_sqlglot(spark_dtype)

    assert sqlglot_dtype.sql() == expected_str


@pytest.mark.parametrize(
    "spark_dtype, expected_str",
    [
        (types.StringType(), "TEXT"),
        (types.VarcharType(100), "VARCHAR(100)"),
        (types.CharType(100), "CHAR(100)"),
        (types.DecimalType(11, 2), "DECIMAL(11, 2)"),
        (types.IntegerType(), "INT"),
        (types.LongType(), "BIGINT"),
        (types.ShortType(), "SMALLINT"),
        (types.ByteType(), "TINYINT"),
        (types.FloatType(), "FLOAT"),
        (types.DoubleType(), "DOUBLE"),
        (types.BooleanType(), "BOOLEAN"),
        (types.TimestampType(), "TIMESTAMP"),
        (types.TimestampNTZType(), "TIMESTAMPNTZ"),
        (types.DateType(), "DATE"),
        (types.DecimalType(), "DECIMAL(10, 0)"),
        (types.BinaryType(), "BINARY"),
        (types.ArrayType(types.StringType()), "ARRAY(TEXT)"),
        (types.MapType(types.StringType(), types.IntegerType()), "MAP(TEXT, INT)"),
        (
            types.StructType(
                [
                    types.StructField("a", types.StringType()),
                    types.StructField("b", types.IntegerType()),
                ]
            ),
            "STRUCT(a TEXT, b INT)",
        ),
        (
            types.ArrayType(
                types.StructType(
                    [
                        types.StructField("a", types.StringType()),
                        types.StructField("b", types.IntegerType()),
                    ]
                )
            ),
            "ARRAY(STRUCT(a TEXT, b INT))",
        ),
    ],
)
def test_spark_to_sqlglot_to_spark(spark_dtype: types.DataType, expected_str: str):
    """Test round trip conversion from Spark to SQLGlot and back to Spark."""
    sqlglot_dtype = spark_to_sqlglot(spark_dtype)
    final_spark_dtype = sqlglot_to_spark(sqlglot_dtype)

    assert str(final_spark_dtype) == str(spark_dtype)
