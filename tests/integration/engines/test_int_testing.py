from __future__ import annotations

import typing as t

import pytest

from sqlframe.base import types
from sqlframe.base.exceptions import DataFrameDiffError, SchemaDiffError
from sqlframe.testing import assertDataFrameEqual, assertSchemaEqual

if t.TYPE_CHECKING:
    from sqlframe.base.dataframe import _BaseSession


pytest_plugins = ["tests.integration.fixtures"]


def test_df_equal_no_diff(get_session: t.Callable[[], _BaseSession]):
    session = get_session()
    df1 = session.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
    df2 = session.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
    assertDataFrameEqual(df1, df2)
    df1 = session.createDataFrame(data=[("1", 0.1), ("2", 3.23)], schema=["id", "amount"])
    df2 = session.createDataFrame(data=[("1", 0.109), ("2", 3.23)], schema=["id", "amount"])
    assertDataFrameEqual(df1, df2, rtol=1e-1)  # pass, DataFrames are approx equal by rtol
    df1 = session.createDataFrame(data=[(1, 1000), (2, 3000)], schema=["id", "amount"])
    list_of_rows = [types.Row(1, 1000), types.Row(2, 3000)]  # type: ignore
    assertDataFrameEqual(df1, list_of_rows)  # pass, actual and expected data are equal


def test_df_equal_diff(get_session: t.Callable[[], _BaseSession]):
    session = get_session()
    df1 = session.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
    df2 = session.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
    df2 = df2.withColumn("amount", df2["amount"] + 1)
    with pytest.raises(DataFrameDiffError):
        assertDataFrameEqual(df1, df2)
    df1 = session.createDataFrame(
        data=[("1", 1000.00), ("2", 3000.00), ("3", 2000.00)], schema=["id", "amount"]
    )
    df2 = session.createDataFrame(
        data=[("1", 1001.00), ("2", 3000.00), ("3", 2003.00)], schema=["id", "amount"]
    )
    with pytest.raises(DataFrameDiffError):
        assertDataFrameEqual(df1, df2)


def test_schema_equal_no_diff(get_session: t.Callable[[], _BaseSession]):
    session = get_session()
    schema1 = session.createDataFrame(
        data=[("1", 1000), ("2", 3000)], schema=["id", "amount"]
    ).schema
    schema2 = session.createDataFrame(
        data=[("1", 1000), ("2", 3000)], schema=["id", "amount"]
    ).schema
    assertSchemaEqual(schema1, schema2)
    s1 = types.StructType(
        [types.StructField("names", types.ArrayType(types.DoubleType(), True), True)]
    )
    s2 = types.StructType(
        [types.StructField("names", types.ArrayType(types.DoubleType(), True), True)]
    )
    assertSchemaEqual(s1, s2)  # pass, schemas are identical


def test_schema_equal_diff(get_session: t.Callable[[], _BaseSession]):
    session = get_session()
    schema1 = session.createDataFrame(
        data=[("1", 1000), ("2", 3000)], schema=["id", "amount"]
    ).schema
    schema2 = session.createDataFrame(
        data=[("1", 1000), ("2", 3000)], schema=["diff", "amount"]
    ).schema
    with pytest.raises(SchemaDiffError):
        assertSchemaEqual(schema1, schema2)
    df1 = session.createDataFrame(data=[(1, 1000), (2, 3000)], schema=["id", "number"])
    df2 = session.createDataFrame(data=[("1", 1000), ("2", 5000)], schema=["id", "amount"])
    with pytest.raises(SchemaDiffError):
        assertSchemaEqual(df1.schema, df2.schema)
