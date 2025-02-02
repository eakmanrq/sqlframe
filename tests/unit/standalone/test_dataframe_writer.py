import typing as t

import pytest
import sqlglot

from sqlframe.standalone.dataframe import StandaloneDataFrame
from sqlframe.standalone.session import StandaloneSession

pytest_plugins = ["tests.common_fixtures", "tests.unit.standalone.fixtures"]


def test_insertInto_full_path(standalone_employee: StandaloneDataFrame, compare_sql: t.Callable):
    df = standalone_employee.write.insertInto("catalog.db.table_name")
    expected = "INSERT INTO `catalog`.`db`.`table_name` SELECT `a1`.`employee_id` AS `employee_id`, CAST(`a1`.`fname` AS STRING) AS `fname`, CAST(`a1`.`lname` AS STRING) AS `lname`, `a1`.`age` AS `age`, `a1`.`store_id` AS `store_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
    compare_sql(df, expected)


def test_insertInto_db_table(standalone_employee: StandaloneDataFrame, compare_sql: t.Callable):
    df = standalone_employee.write.insertInto("db.table_name")
    expected = "INSERT INTO `db`.`table_name` SELECT `a1`.`employee_id` AS `employee_id`, CAST(`a1`.`fname` AS STRING) AS `fname`, CAST(`a1`.`lname` AS STRING) AS `lname`, `a1`.`age` AS `age`, `a1`.`store_id` AS `store_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
    compare_sql(df, expected)


def test_insertInto_table(standalone_employee: StandaloneDataFrame, compare_sql: t.Callable):
    df = standalone_employee.write.insertInto("table_name")
    expected = "INSERT INTO `table_name` SELECT `a1`.`employee_id` AS `employee_id`, CAST(`a1`.`fname` AS STRING) AS `fname`, CAST(`a1`.`lname` AS STRING) AS `lname`, `a1`.`age` AS `age`, `a1`.`store_id` AS `store_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
    compare_sql(df, expected)


def test_insertInto_overwrite(standalone_employee: StandaloneDataFrame, compare_sql: t.Callable):
    df = standalone_employee.write.insertInto("table_name", overwrite=True)
    expected = "INSERT OVERWRITE TABLE `table_name` SELECT `a1`.`employee_id` AS `employee_id`, CAST(`a1`.`fname` AS STRING) AS `fname`, CAST(`a1`.`lname` AS STRING) AS `lname`, `a1`.`age` AS `age`, `a1`.`store_id` AS `store_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
    compare_sql(df, expected)


def test_insertInto_byName(standalone_employee: StandaloneDataFrame, compare_sql: t.Callable):
    standalone_employee.session.catalog.add_table("table_name", {"employee_id": "INT"})
    df = standalone_employee.write.byName.insertInto("table_name")
    expected = "INSERT INTO `table_name` SELECT `a1`.`employee_id` AS `employee_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
    compare_sql(df, expected)


def test_insertInto_cache(standalone_employee: StandaloneDataFrame, compare_sql: t.Callable):
    df = standalone_employee.cache().write.insertInto("table_name")
    expected_statements = [
        "DROP VIEW IF EXISTS `t12441709`",
        "CACHE LAZY TABLE `t12441709` OPTIONS('storageLevel' = 'MEMORY_AND_DISK') AS SELECT `a1`.`employee_id` AS `employee_id`, CAST(`a1`.`fname` AS STRING) AS `fname`, CAST(`a1`.`lname` AS STRING) AS `lname`, `a1`.`age` AS `age`, `a1`.`store_id` AS `store_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)",
        "INSERT INTO `table_name` SELECT `t12441709`.`employee_id` AS `employee_id`, `t12441709`.`fname` AS `fname`, `t12441709`.`lname` AS `lname`, `t12441709`.`age` AS `age`, `t12441709`.`store_id` AS `store_id` FROM `t12441709` AS `t12441709`",
    ]
    compare_sql(df, expected_statements)


def test_saveAsTable_format(standalone_employee: StandaloneDataFrame):
    with pytest.raises(NotImplementedError):
        standalone_employee.write.saveAsTable("table_name", format="parquet").sql(pretty=False)[0]


def test_saveAsTable_append(standalone_employee: StandaloneDataFrame, compare_sql: t.Callable):
    df = standalone_employee.write.saveAsTable("table_name", mode="append")
    expected = "INSERT INTO `table_name` SELECT `a1`.`employee_id` AS `employee_id`, CAST(`a1`.`fname` AS STRING) AS `fname`, CAST(`a1`.`lname` AS STRING) AS `lname`, `a1`.`age` AS `age`, `a1`.`store_id` AS `store_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
    compare_sql(df, expected)


def test_saveAsTable_overwrite(standalone_employee: StandaloneDataFrame, compare_sql: t.Callable):
    df = standalone_employee.write.saveAsTable("table_name", mode="overwrite")
    expected = "CREATE OR REPLACE TABLE `table_name` AS SELECT `a1`.`employee_id` AS `employee_id`, CAST(`a1`.`fname` AS STRING) AS `fname`, CAST(`a1`.`lname` AS STRING) AS `lname`, `a1`.`age` AS `age`, `a1`.`store_id` AS `store_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
    compare_sql(df, expected)


def test_saveAsTable_error(standalone_employee: StandaloneDataFrame, compare_sql: t.Callable):
    df = standalone_employee.write.saveAsTable("table_name", mode="error")
    expected = "CREATE TABLE `table_name` AS SELECT `a1`.`employee_id` AS `employee_id`, CAST(`a1`.`fname` AS STRING) AS `fname`, CAST(`a1`.`lname` AS STRING) AS `lname`, `a1`.`age` AS `age`, `a1`.`store_id` AS `store_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
    compare_sql(df, expected)


def test_saveAsTable_ignore(standalone_employee: StandaloneDataFrame, compare_sql: t.Callable):
    df = standalone_employee.write.saveAsTable("table_name", mode="ignore")
    expected = "CREATE TABLE IF NOT EXISTS `table_name` AS SELECT `a1`.`employee_id` AS `employee_id`, CAST(`a1`.`fname` AS STRING) AS `fname`, CAST(`a1`.`lname` AS STRING) AS `lname`, `a1`.`age` AS `age`, `a1`.`store_id` AS `store_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
    compare_sql(df, expected)


def test_mode_standalone(standalone_employee: StandaloneDataFrame, compare_sql: t.Callable):
    df = standalone_employee.write.mode("ignore").saveAsTable("table_name")
    expected = "CREATE TABLE IF NOT EXISTS `table_name` AS SELECT `a1`.`employee_id` AS `employee_id`, CAST(`a1`.`fname` AS STRING) AS `fname`, CAST(`a1`.`lname` AS STRING) AS `lname`, `a1`.`age` AS `age`, `a1`.`store_id` AS `store_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
    compare_sql(df, expected)


def test_mode_override(standalone_employee: StandaloneDataFrame, compare_sql: t.Callable):
    df = standalone_employee.write.mode("ignore").saveAsTable("table_name", mode="overwrite")
    expected = "CREATE OR REPLACE TABLE `table_name` AS SELECT `a1`.`employee_id` AS `employee_id`, CAST(`a1`.`fname` AS STRING) AS `fname`, CAST(`a1`.`lname` AS STRING) AS `lname`, `a1`.`age` AS `age`, `a1`.`store_id` AS `store_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
    compare_sql(df, expected)


def test_saveAsTable_cache(standalone_employee: StandaloneDataFrame, compare_sql: t.Callable):
    df = standalone_employee.cache().write.saveAsTable("table_name")
    expected_statements = [
        "DROP VIEW IF EXISTS `t12441709`",
        "CACHE LAZY TABLE `t12441709` OPTIONS('storageLevel' = 'MEMORY_AND_DISK') AS SELECT `a1`.`employee_id` AS `employee_id`, CAST(`a1`.`fname` AS STRING) AS `fname`, CAST(`a1`.`lname` AS STRING) AS `lname`, `a1`.`age` AS `age`, `a1`.`store_id` AS `store_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)",
        "CREATE TABLE `table_name` AS SELECT `t12441709`.`employee_id` AS `employee_id`, `t12441709`.`fname` AS `fname`, `t12441709`.`lname` AS `lname`, `t12441709`.`age` AS `age`, `t12441709`.`store_id` AS `store_id` FROM `t12441709` AS `t12441709`",
    ]
    compare_sql(df, expected_statements)


def test_quotes(standalone_session: StandaloneSession, compare_sql: t.Callable):
    standalone_session.catalog.add_table("`Test`", {"`ID`": "STRING"})
    df = standalone_session.table("`Test`")
    compare_sql(df.select(df["`ID`"]), ["SELECT `test`.`id` AS `ID` FROM `test` AS `test`"])
