import typing as t

import pytest
from sqlglot.errors import OptimizeError

from sqlframe.base.exceptions import TableSchemaError
from sqlframe.standalone import functions as F
from sqlframe.standalone.session import StandaloneSession

pytest_plugins = ["tests.common_fixtures", "tests.unit.standalone.fixtures"]


tests = [
    (
        "All lower no intention of CS",
        "test",
        "test",
        {"name": "VARCHAR"},
        "name",
        '''SELECT "TEST"."NAME" AS "name" FROM "TEST" AS "TEST"''',
    ),
    (
        "Table has CS while column does not",
        '"Test"',
        '"Test"',
        {"name": "VARCHAR"},
        "name",
        '''SELECT "TEST"."NAME" AS "name" FROM "Test" AS "TEST"''',
    ),
    (
        "Column has CS while table does not",
        "test",
        "test",
        {'"Name"': "VARCHAR"},
        '"Name"',
        '''SELECT "TEST"."Name" AS "Name" FROM "TEST" AS "TEST"''',
    ),
    (
        "Both Table and column have CS",
        '"Test"',
        '"Test"',
        {'"Name"': "VARCHAR"},
        '"Name"',
        '''SELECT "TEST"."Name" AS "Name" FROM "Test" AS "TEST"''',
    ),
    (
        "Lowercase CS table and column",
        '"test"',
        '"test"',
        {'"name"': "VARCHAR"},
        '"name"',
        '''SELECT "TEST"."name" AS "name" FROM "test" AS "TEST"''',
    ),
    (
        "CS table and column and query table but no CS in query column",
        '"test"',
        '"test"',
        {'"name"': "VARCHAR"},
        "name",
        OptimizeError(),
    ),
    (
        "CS table and column and query column but no CS in query table",
        '"test"',
        "test",
        {'"name"': "VARCHAR"},
        '"name"',
        OptimizeError(),
    ),
]


@pytest.mark.parametrize(
    "table_name, spark_table, schema, spark_column, expected",
    [test[1:] for test in tests],
    ids=[test[0] for test in tests],
)
def test_basic_case_sensitivity(
    sqlf_sf: StandaloneSession,
    compare_sql: t.Callable,
    table_name,
    spark_table,
    schema,
    spark_column,
    expected,
):
    sqlf_sf.catalog.add_table(table_name, schema)
    if isinstance(expected, OptimizeError):
        with pytest.raises((OptimizeError, TableSchemaError)):
            df = sqlf_sf.table(spark_table).select(F.col(spark_column))
            df.sql()
    else:
        df = sqlf_sf.table(spark_table).select(F.col(spark_column))
        compare_sql(df, expected)


@pytest.mark.parametrize(
    "col_func, expected",
    [
        (lambda x: x, '"Name"'),
        (lambda x: x.alias("nAME"), '"Name" AS NAME'),
        (lambda x: x.alias('"nAME"'), '"Name" AS "nAME"'),
    ],
)
def test_alias(
    sqlf_sf: StandaloneSession, compare_sql: t.Callable, col_func: t.Callable, expected: str
):
    # Need to run `F.col` after the Snowflake SQLFrame session has been set since it will be default dialect otherwise
    col = col_func(F.col('"Name"'))
    compare_sql(col, expected, dialect=sqlf_sf.input_dialect)
