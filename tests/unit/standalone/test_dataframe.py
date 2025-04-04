import typing as t

import pytest
from sqlglot import expressions as exp

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.standalone import functions as F
from sqlframe.standalone.dataframe import StandaloneDataFrame

pytest_plugins = ["tests.common_fixtures", "tests.unit.standalone.fixtures"]


def test_hash_select_expression(standalone_employee: StandaloneDataFrame):
    expression = exp.select("cola").from_("table")
    assert standalone_employee._create_hash_from_expression(expression) == "t17051938"


def test_columns(standalone_employee: StandaloneDataFrame):
    assert standalone_employee.columns == ["employee_id", "fname", "lname", "age", "store_id"]


def test_cache(standalone_employee: StandaloneDataFrame, compare_sql: t.Callable):
    df = standalone_employee.select("fname").cache()
    expected_statements = [
        "DROP VIEW IF EXISTS `t31563989`",
        "CACHE LAZY TABLE `t31563989` OPTIONS('storageLevel' = 'MEMORY_AND_DISK') AS SELECT CAST(`a1`.`fname` AS STRING) AS `fname` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)",
        "SELECT `t31563989`.`fname` AS `fname` FROM `t31563989` AS `t31563989`",
    ]
    compare_sql(df, expected_statements)


def test_persist_default(standalone_employee: StandaloneDataFrame, compare_sql: t.Callable):
    df = standalone_employee.select("fname").persist()
    expected_statements = [
        "DROP VIEW IF EXISTS `t31563989`",
        "CACHE LAZY TABLE `t31563989` OPTIONS('storageLevel' = 'MEMORY_AND_DISK_SER') AS SELECT CAST(`a1`.`fname` AS STRING) AS `fname` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)",
        "SELECT `t31563989`.`fname` AS `fname` FROM `t31563989` AS `t31563989`",
    ]
    compare_sql(df, expected_statements)


def test_persist_storagelevel(standalone_employee: StandaloneDataFrame, compare_sql: t.Callable):
    df = standalone_employee.select("fname").persist("DISK_ONLY_2")
    expected_statements = [
        "DROP VIEW IF EXISTS `t31563989`",
        "CACHE LAZY TABLE `t31563989` OPTIONS('storageLevel' = 'DISK_ONLY_2') AS SELECT CAST(`a1`.`fname` AS STRING) AS `fname` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)",
        "SELECT `t31563989`.`fname` AS `fname` FROM `t31563989` AS `t31563989`",
    ]
    compare_sql(df, expected_statements)


def test_with_column_duplicate_alias(standalone_employee: StandaloneDataFrame):
    df = standalone_employee.withColumn("fName", F.col("age").cast("string"))
    assert df.columns == ["employee_id", "fName", "lname", "age", "store_id"]
    # Make sure that the new columns is added with an alias to `fname`
    assert (
        df.sql(pretty=False)
        == "SELECT `a1`.`employee_id` AS `employee_id`, CAST(`a1`.`age` AS STRING) AS `fName`, CAST(`a1`.`lname` AS STRING) AS `lname`, `a1`.`age` AS `age`, `a1`.`store_id` AS `store_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
    )


def test_with_columns(standalone_employee: StandaloneDataFrame):
    df = standalone_employee.withColumns({"new_col1": F.col("age"), "new_col2": F.col("store_id")})
    assert df.columns == [
        "employee_id",
        "fname",
        "lname",
        "age",
        "store_id",
        "new_col1",
        "new_col2",
    ]
    assert (
        df.sql(pretty=False)
        == "SELECT `a1`.`employee_id` AS `employee_id`, CAST(`a1`.`fname` AS STRING) AS `fname`, CAST(`a1`.`lname` AS STRING) AS `lname`, `a1`.`age` AS `age`, `a1`.`store_id` AS `store_id`, `a1`.`age` AS `new_col1`, `a1`.`store_id` AS `new_col2` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
    )


def test_transform(standalone_employee: StandaloneDataFrame):
    def cast_all_to_int(input_df):
        return input_df.select([F.col(col_name).cast("int") for col_name in input_df.columns])

    def sort_columns_asc(input_df):
        return input_df.select(*sorted(input_df.columns))

    df = standalone_employee.transform(cast_all_to_int).transform(sort_columns_asc)
    assert df.columns == ["age", "employee_id", "fname", "lname", "store_id"]
    assert df.sql(pretty=False, optimize=False).endswith(  # type: ignore
        "SELECT CAST(`employee_id` AS INT) AS `employee_id`, CAST(`fname` AS INT) AS `fname`, CAST(`lname` AS INT) AS `lname`, CAST(`age` AS INT) AS `age`, CAST(`store_id` AS INT) AS `store_id` FROM `t51718876`) SELECT `age` AS `age`, `employee_id` AS `employee_id`, `fname` AS `fname`, `lname` AS `lname`, `store_id` AS `store_id` FROM `t16881256`"
    )


# https://github.com/eakmanrq/sqlframe/issues/19
def test_with_column_dual_expression(standalone_employee: StandaloneDataFrame):
    df1 = standalone_employee.withColumn("new_col1", standalone_employee.age)
    df2 = df1.withColumn("new_col2", standalone_employee.store_id)
    assert df2.columns == [
        "employee_id",
        "fname",
        "lname",
        "age",
        "store_id",
        "new_col1",
        "new_col2",
    ]
    assert (
        df2.sql(pretty=False)
        == "SELECT `a1`.`employee_id` AS `employee_id`, CAST(`a1`.`fname` AS STRING) AS `fname`, CAST(`a1`.`lname` AS STRING) AS `lname`, `a1`.`age` AS `age`, `a1`.`store_id` AS `store_id`, `a1`.`age` AS `new_col1`, `a1`.`store_id` AS `new_col2` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
    )


def test_where_expr(standalone_employee: StandaloneDataFrame):
    df = standalone_employee.where("fname = 'Jack' AND age = 37")
    assert df.columns == ["employee_id", "fname", "lname", "age", "store_id"]
    assert (
        df.sql(pretty=False)
        == "SELECT `a1`.`employee_id` AS `employee_id`, CAST(`a1`.`fname` AS STRING) AS `fname`, CAST(`a1`.`lname` AS STRING) AS `lname`, `a1`.`age` AS `age`, `a1`.`store_id` AS `store_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`) WHERE `a1`.`age` = 37 AND CAST(`a1`.`fname` AS STRING) = 'Jack'"
    )


def test_missing_method(standalone_employee: StandaloneDataFrame):
    with pytest.raises(
        UnsupportedOperationError, match="Tried to call a column which is unexpected.*"
    ):
        standalone_employee.missing_method("blah")


def test_expand_star(standalone_employee: StandaloneDataFrame):
    assert (
        standalone_employee.select("*").sql(pretty=False, optimize=False)
        == "WITH `t51718876` AS (SELECT CAST(`employee_id` AS INT) AS `employee_id`, CAST(`fname` AS STRING) AS `fname`, CAST(`lname` AS STRING) AS `lname`, CAST(`age` AS INT) AS `age`, CAST(`store_id` AS INT) AS `store_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)) SELECT `employee_id` AS `employee_id`, `fname` AS `fname`, `lname` AS `lname`, `age` AS `age`, `store_id` AS `store_id` FROM `t51718876`"
    )


def test_expand_star_table_alias(standalone_employee: StandaloneDataFrame):
    assert (
        standalone_employee.alias("blah").select("blah.*").sql(pretty=False, optimize=False)
        == "WITH `t51718876` AS (SELECT CAST(`employee_id` AS INT) AS `employee_id`, CAST(`fname` AS STRING) AS `fname`, CAST(`lname` AS STRING) AS `lname`, CAST(`age` AS INT) AS `age`, CAST(`store_id` AS INT) AS `store_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)), `t37842204` AS (SELECT `employee_id`, `fname`, `lname`, `age`, `store_id` FROM `t51718876`) SELECT `t37842204`.`employee_id` AS `employee_id`, `t37842204`.`fname` AS `fname`, `t37842204`.`lname` AS `lname`, `t37842204`.`age` AS `age`, `t37842204`.`store_id` AS `store_id` FROM `t37842204`"
    )


def test_lineage(standalone_employee: StandaloneDataFrame):
    assert (
        standalone_employee.lineage("age").source.sql()
        == "SELECT a1.age AS age FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS a1(employee_id, fname, lname, age, store_id)"
    )
    assert (
        standalone_employee.session.sql("SELECT * FROM employee").lineage("age").source.sql()
        == "SELECT employee.age AS age FROM employee AS employee"
    )


# Issue: https://github.com/eakmanrq/sqlframe/issues/286
def test_unquoted_identifiers(standalone_employee: StandaloneDataFrame):
    assert (
        standalone_employee.sql(dialect="snowflake", pretty=False, quote_identifiers=False)
        == """SELECT A1.EMPLOYEE_ID AS "employee_id", CAST(A1.FNAME AS TEXT) AS "fname", CAST(A1.LNAME AS TEXT) AS "lname", A1.AGE AS "age", A1.STORE_ID AS "store_id" FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS A1(EMPLOYEE_ID, FNAME, LNAME, AGE, STORE_ID)"""
    )


# https://github.com/eakmanrq/sqlframe/issues/356
def test_aliased_dictionary_agg(standalone_employee: StandaloneDataFrame):
    assert (
        standalone_employee.groupBy("fname").agg({"age": "avg", "lname": "count"}).sql(pretty=False)
        == "SELECT CAST(`a1`.`fname` AS STRING) AS `fname`, AVG(`a1`.`age`) AS `avg(age)`, COUNT(CAST(`a1`.`lname` AS STRING)) AS `count(lname)` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`) GROUP BY CAST(`a1`.`fname` AS STRING)"
    )
