import typing as t

import pytest

from sqlframe.standalone import functions as F
from sqlframe.standalone import types
from sqlframe.standalone.session import StandaloneSession

pytest_plugins = ["tests.common_fixtures", "tests.unit.standalone.fixtures"]


def test_cdf_one_row(standalone_session: StandaloneSession, compare_sql: t.Callable):
    df = standalone_session.createDataFrame([[1, 2]], ["cola", "colb"])
    expected = "SELECT CAST(`a1`.`cola` AS BIGINT) AS `cola`, CAST(`a1`.`colb` AS BIGINT) AS `colb` FROM VALUES (1, 2) AS `a1`(`cola`, `colb`)"
    compare_sql(df, expected)


def test_cdf_multiple_rows(standalone_session: StandaloneSession, compare_sql: t.Callable):
    df = standalone_session.createDataFrame([[1, 2], [3, 4], [None, 6]], ["cola", "colb"])
    expected = "SELECT CAST(`a1`.`cola` AS BIGINT) AS `cola`, CAST(`a1`.`colb` AS BIGINT) AS `colb` FROM VALUES (1, 2), (3, 4), (NULL, 6) AS `a1`(`cola`, `colb`)"
    compare_sql(df, expected)


def test_cdf_no_schema(standalone_session: StandaloneSession, compare_sql: t.Callable):
    df = standalone_session.createDataFrame([[1, 2], [3, 4], [None, 6]])
    expected = "SELECT CAST(`a1`.`_1` AS BIGINT) AS `_1`, CAST(`a1`.`_2` AS BIGINT) AS `_2` FROM VALUES (1, 2), (3, 4), (NULL, 6) AS `a1`(`_1`, `_2`)"
    compare_sql(df, expected)


def test_cdf_row_mixed_primitives(standalone_session: StandaloneSession, compare_sql: t.Callable):
    df = standalone_session.createDataFrame([[1, 10.1, "test", False, None]])
    expected = "SELECT CAST(`a1`.`_1` AS BIGINT) AS `_1`, `a1`.`_2` AS `_2`, CAST(`a1`.`_3` AS STRING) AS `_3`, `a1`.`_4` AS `_4`, `a1`.`_5` AS `_5` FROM VALUES (1, 10.1, 'test', FALSE, NULL) AS `a1`(`_1`, `_2`, `_3`, `_4`, `_5`)"
    compare_sql(df, expected)


def test_cdf_dict_rows(standalone_session: StandaloneSession, compare_sql: t.Callable):
    df = standalone_session.createDataFrame(
        [{"cola": 1, "colb": "test"}, {"cola": 2, "colb": "test2"}]
    )
    expected = "SELECT CAST(`a1`.`cola` AS BIGINT) AS `cola`, CAST(`a1`.`colb` AS STRING) AS `colb` FROM VALUES (1, 'test'), (2, 'test2') AS `a1`(`cola`, `colb`)"
    compare_sql(df, expected)


def test_cdf_str_schema(standalone_session: StandaloneSession, compare_sql: t.Callable):
    df = standalone_session.createDataFrame([[1, "test"]], "cola INT, colb STRING")
    expected = "SELECT `a1`.`cola` AS `cola`, CAST(`a1`.`colb` AS STRING) AS `colb` FROM VALUES (1, 'test') AS `a1`(`cola`, `colb`)"
    compare_sql(df, expected)


def test_typed_schema_basic(standalone_session: StandaloneSession, compare_sql: t.Callable):
    schema = types.StructType(
        [
            types.StructField("cola", types.IntegerType()),
            types.StructField("colb", types.StringType()),
        ]
    )
    df = standalone_session.createDataFrame([[1, "test"]], schema)
    expected = "SELECT `a1`.`cola` AS `cola`, CAST(`a1`.`colb` AS STRING) AS `colb` FROM VALUES (1, 'test') AS `a1`(`cola`, `colb`)"
    compare_sql(df, expected)


def test_single_value_rows(standalone_session: StandaloneSession, compare_sql: t.Callable):
    df = standalone_session.createDataFrame([1, 2, 3], "int")
    expected = "SELECT `a1`.`value` AS `value` FROM VALUES (1), (2), (3) AS `a1`(`value`)"
    compare_sql(df, expected)


def test_row_value(standalone_session: StandaloneSession, compare_sql: t.Callable):
    df = standalone_session.createDataFrame([types.Row(cola=1, colb="test")])  # type: ignore
    expected = "SELECT CAST(`a1`.`cola` AS BIGINT) AS `cola`, CAST(`a1`.`colb` AS STRING) AS `colb` FROM VALUES (1, 'test') AS `a1`(`cola`, `colb`)"
    compare_sql(df, expected)


def test_typed_schema_nested_map(standalone_session: StandaloneSession, compare_sql: t.Callable):
    schema = types.StructType(
        [
            types.StructField(
                "cola",
                types.MapType(
                    keyType=types.IntegerType(),
                    valueType=types.StringType(),
                ),
            )
        ]
    )
    df = standalone_session.createDataFrame([[{"sub_cola": 1, "sub_colb": "test"}]], schema)  # type: ignore
    expected = "SELECT `a1`.`cola` AS `cola` FROM VALUES (MAP('sub_cola', 1, 'sub_colb', 'test')) AS `a1`(`cola`)"

    compare_sql(df, expected)


def test_nested_struct(standalone_session: StandaloneSession, compare_sql: t.Callable):
    df = standalone_session.createDataFrame([[types.Row(cola=1, colb="test")]])  # type: ignore
    expected = (
        "SELECT `a1`.`_1` AS `_1` FROM VALUES (STRUCT(1 AS `cola`, 'test' AS `colb`)) AS `a1`(`_1`)"
    )
    compare_sql(df, expected)


def test_sql_select_only(standalone_session: StandaloneSession, compare_sql: t.Callable):
    query = "SELECT cola, colb FROM table"
    standalone_session.catalog.add_table("table", {"cola": "string", "colb": "string"})
    df = standalone_session.sql(query)
    compare_sql(
        df, "SELECT `table`.`cola` AS `cola`, `table`.`colb` AS `colb` FROM `table` AS `table`"
    )


def test_sql_with_aggs(standalone_session: StandaloneSession, compare_sql: t.Callable):
    query = "SELECT cola, colb FROM table"
    standalone_session.catalog.add_table("table", {"cola": "string", "colb": "string"})
    df = standalone_session.sql(query).groupBy(F.col("cola")).agg(F.sum("colb"))
    compare_sql(
        df,
        "WITH `t34970089` AS (SELECT `table`.`cola` AS `cola`, `table`.`colb` AS `colb` FROM `table` AS `table`), `t50917871` AS (SELECT `cola`, `colb` FROM `t34970089`) SELECT `cola`, SUM(`colb`) AS `sum__colb__` FROM `t50917871` GROUP BY `cola`",
        pretty=False,
        optimize=False,
    )


def test_sql_create(standalone_session: StandaloneSession, compare_sql: t.Callable):
    query = "CREATE TABLE new_table AS WITH t1 AS (SELECT cola, colb FROM table) SELECT cola, colb, FROM t1"
    standalone_session.catalog.add_table("table", {"cola": "string", "colb": "string"})
    df = standalone_session.sql(query)
    expected = "CREATE TABLE `new_table` AS SELECT `table`.`cola` AS `cola`, `table`.`colb` AS `colb` FROM `table` AS `table`"
    compare_sql(df, expected)


def test_sql_insert(standalone_session: StandaloneSession, compare_sql: t.Callable):
    query = (
        "WITH t1 AS (SELECT cola, colb FROM table) INSERT INTO new_table SELECT cola, colb FROM t1"
    )
    standalone_session.catalog.add_table("table", {"cola": "string", "colb": "string"})
    df = standalone_session.sql(query)
    expected = "INSERT INTO `new_table` SELECT `table`.`cola` AS `cola`, `table`.`colb` AS `colb` FROM `table` AS `table`"
    compare_sql(df, expected)


def test_session_create_builder_patterns():
    assert StandaloneSession.builder.appName("abc").getOrCreate() == StandaloneSession()


# @pytest.mark.parametrize(
#     "input, expected",
#     [
#         (
#             StandaloneSession._to_row(["a"], [1]),
#             types.Row(a=1),
#         ),
#         (
#             StandaloneSession._to_row(["a", "b"], [1, 2]),
#             types.Row(a=1, b=2),
#         ),
#         (
#             StandaloneSession._to_row(["a", "a"], [1, 2]),
#             types.Row(a=1, a=2),
#         ),
#     ],
# )
# def test_to_row(input, expected):
#     assert input == expected
