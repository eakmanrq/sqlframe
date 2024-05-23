import typing as t

from sqlglot import expressions as exp

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
        "DROP VIEW IF EXISTS t31563989",
        "CACHE LAZY TABLE t31563989 OPTIONS('storageLevel' = 'MEMORY_AND_DISK') AS SELECT CAST(`a1`.`fname` AS STRING) AS `fname` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)",
        "SELECT `t31563989`.`fname` AS `fname` FROM `t31563989` AS `t31563989`",
    ]
    compare_sql(df, expected_statements)


def test_persist_default(standalone_employee: StandaloneDataFrame, compare_sql: t.Callable):
    df = standalone_employee.select("fname").persist()
    expected_statements = [
        "DROP VIEW IF EXISTS t31563989",
        "CACHE LAZY TABLE t31563989 OPTIONS('storageLevel' = 'MEMORY_AND_DISK_SER') AS SELECT CAST(`a1`.`fname` AS STRING) AS `fname` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)",
        "SELECT `t31563989`.`fname` AS `fname` FROM `t31563989` AS `t31563989`",
    ]
    compare_sql(df, expected_statements)


def test_persist_storagelevel(standalone_employee: StandaloneDataFrame, compare_sql: t.Callable):
    df = standalone_employee.select("fname").persist("DISK_ONLY_2")
    expected_statements = [
        "DROP VIEW IF EXISTS t31563989",
        "CACHE LAZY TABLE t31563989 OPTIONS('storageLevel' = 'DISK_ONLY_2') AS SELECT CAST(`a1`.`fname` AS STRING) AS `fname` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)",
        "SELECT `t31563989`.`fname` AS `fname` FROM `t31563989` AS `t31563989`",
    ]
    compare_sql(df, expected_statements)


def test_with_column_duplicate_alias(standalone_employee: StandaloneDataFrame):
    df = standalone_employee.withColumn("fname", F.col("age").cast("string"))
    assert df.columns == ["employee_id", "fname", "lname", "age", "store_id"]
    # Make sure that the new columns is added with an alias to `fname`
    assert (
        df.sql(pretty=False)
        == "SELECT `a1`.`employee_id` AS `employee_id`, CAST(`a1`.`age` AS STRING) AS `fname`, CAST(`a1`.`lname` AS STRING) AS `lname`, `a1`.`age` AS `age`, `a1`.`store_id` AS `store_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
    )


def test_where_expr(standalone_employee: StandaloneDataFrame):
    df = standalone_employee.where("fname = 'Jack' AND age = 37")
    assert df.columns == ["employee_id", "fname", "lname", "age", "store_id"]
    assert (
        df.sql(pretty=False)
        == "SELECT `a1`.`employee_id` AS `employee_id`, CAST(`a1`.`fname` AS STRING) AS `fname`, CAST(`a1`.`lname` AS STRING) AS `lname`, `a1`.`age` AS `age`, `a1`.`store_id` AS `store_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`) WHERE `a1`.`age` = 37 AND CAST(`a1`.`fname` AS STRING) = 'Jack'"
    )
