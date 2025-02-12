import typing as t

import pytest
from sqlglot import exp, parse_one
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sqlframe.base import types
from sqlframe.base.util import quote_preserving_alias_or_name, sqlglot_to_spark


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
