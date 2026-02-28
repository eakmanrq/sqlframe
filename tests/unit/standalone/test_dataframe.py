import typing as t

import pytest
from sqlglot import expressions as exp

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.standalone import functions as F
from sqlframe.standalone.dataframe import StandaloneDataFrame
from sqlframe.standalone.session import StandaloneSession

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


def test_with_columns_renamed(standalone_employee: StandaloneDataFrame):
    df = standalone_employee.withColumnsRenamed(
        {"fname": "first_name", "lname": "last_name", "nonexistent_col": "new_name"}
    )
    assert df.columns == [
        "employee_id",
        "first_name",
        "last_name",
        "age",
        "store_id",
    ]
    assert (
        df.sql(pretty=False)
        == "SELECT `a1`.`employee_id` AS `employee_id`, CAST(`a1`.`fname` AS STRING) AS `first_name`, CAST(`a1`.`lname` AS STRING) AS `last_name`, `a1`.`age` AS `age`, `a1`.`store_id` AS `store_id` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`)"
    )


def test_with_columns_renamed_nonexistent(standalone_employee: StandaloneDataFrame):
    # Test that non-existent columns are silently ignored
    df = standalone_employee.withColumnsRenamed({"nonexistent_col": "new_name"})
    # Verify that the DataFrame columns remain unchanged
    assert df.columns == ["employee_id", "fname", "lname", "age", "store_id"]


def test_transform(standalone_employee: StandaloneDataFrame):
    def cast_all_to_int(input_df):
        return input_df.select([F.col(col_name).cast("int") for col_name in input_df.columns])

    def sort_columns_asc(input_df):
        return input_df.select(*sorted(input_df.columns))

    df = standalone_employee.transform(cast_all_to_int).transform(sort_columns_asc)
    assert df.columns == ["age", "employee_id", "fname", "lname", "store_id"]
    assert df.sql(pretty=False, optimize=False).endswith(
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
        == """SELECT "a1"."age" AS "age" FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS "a1"("employee_id", "fname", "lname", "age", "store_id")"""
    )
    assert (
        standalone_employee.session.sql("SELECT * FROM employee").lineage("age").source.sql()
        == 'SELECT "employee"."age" AS "age" FROM "employee" AS "employee"'
    )


# Issue: https://github.com/eakmanrq/sqlframe/issues/286
def test_unquoted_identifiers(standalone_employee: StandaloneDataFrame):
    assert (
        standalone_employee.sql(dialect="snowflake", pretty=False, quote_identifiers=False)
        == """SELECT A1.EMPLOYEE_ID AS "employee_id", CAST(A1.FNAME AS VARCHAR) AS "fname", CAST(A1.LNAME AS VARCHAR) AS "lname", A1.AGE AS "age", A1.STORE_ID AS "store_id" FROM (VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100)) AS A1(EMPLOYEE_ID, FNAME, LNAME, AGE, STORE_ID)"""
    )


# https://github.com/eakmanrq/sqlframe/issues/356
def test_aliased_dictionary_agg(standalone_employee: StandaloneDataFrame):
    assert (
        standalone_employee.groupBy("fname").agg({"age": "avg", "lname": "count"}).sql(pretty=False)
        == "SELECT CAST(`a1`.`fname` AS STRING) AS `fname`, AVG(`a1`.`age`) AS `avg(age)`, COUNT(CAST(`a1`.`lname` AS STRING)) AS `count(lname)` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`) GROUP BY CAST(`a1`.`fname` AS STRING)"
    )


def test_aliased_group_by(standalone_employee: StandaloneDataFrame):
    assert (
        standalone_employee.alias("employee")
        .select(F.col("employee.fname"))
        .groupBy(F.col("employee.fname"))
        .count()
        .sql(pretty=False)
        == "SELECT CAST(`a1`.`fname` AS STRING) AS `fname`, COUNT(*) AS `count` FROM VALUES (1, 'Jack', 'Shephard', 37, 1), (2, 'John', 'Locke', 65, 1), (3, 'Kate', 'Austen', 37, 2), (4, 'Claire', 'Littleton', 27, 2), (5, 'Hugo', 'Reyes', 29, 100) AS `a1`(`employee_id`, `fname`, `lname`, `age`, `store_id`) GROUP BY CAST(`a1`.`fname` AS STRING)"
    )


# https://github.com/eakmanrq/sqlframe/issues/549
def test_chained_with_column_renamed_after_join(standalone_session: StandaloneSession):
    """Test that chaining multiple withColumnRenamed after a join doesn't produce duplicate columns."""
    from sqlframe.standalone import types as T

    schema_a = T.StructType(
        [
            T.StructField("foo", T.IntegerType(), False),
            T.StructField("bar", T.StringType(), False),
            T.StructField("baz", T.IntegerType(), False),
        ]
    )
    schema_b = T.StructType(
        [
            T.StructField("foo", T.IntegerType(), False),
            T.StructField("baz", T.IntegerType(), False),
            T.StructField("qux", T.StringType(), False),
        ]
    )
    df_a = standalone_session.createDataFrame([(1, "a", 10)], schema=schema_a)
    df_b = standalone_session.createDataFrame([(1, 20, "b")], schema=schema_b)

    joined = df_a.join(df_b, df_a["foo"] == df_b["foo"])
    renamed = joined.withColumnRenamed("bar", "bar2").withColumnRenamed("qux", "qux2")

    sql = renamed.sql(pretty=False)
    # The SQL should have correct table-qualified references for both tables
    # Before the fix, the CTE conversion caused all ambiguous columns to resolve to `a1`
    assert "`a2`.`foo`" in sql, f"Expected a2.foo in SQL but got: {sql}"
    assert "`a2`.`baz`" in sql, f"Expected a2.baz in SQL but got: {sql}"
    columns = renamed.columns
    assert "bar2" in columns
    assert "qux2" in columns
    assert "bar" not in columns
    assert "qux" not in columns


# https://github.com/eakmanrq/sqlframe/issues/184
def test_join_select_join_alias_resolution(standalone_session: StandaloneSession):
    """Test that alias references resolve correctly after join().select().join() chain."""
    from sqlframe.standalone import types as T

    # Both employee and store share "store_id" column to reproduce the ambiguity issue
    employee_schema = T.StructType(
        [
            T.StructField("employee_id", T.IntegerType(), False),
            T.StructField("fname", T.StringType(), False),
            T.StructField("store_id", T.IntegerType(), False),
        ]
    )
    store_schema = T.StructType(
        [
            T.StructField("store_id", T.IntegerType(), False),
            T.StructField("store_name", T.StringType(), False),
        ]
    )
    district_schema = T.StructType(
        [
            T.StructField("district_id", T.IntegerType(), False),
            T.StructField("district_name", T.StringType(), False),
        ]
    )

    employee = standalone_session.createDataFrame([(10, "Jack", 10)], schema=employee_schema)
    store = standalone_session.createDataFrame([(10, "Main St")], schema=store_schema)
    district = standalone_session.createDataFrame([(10, "Downtown")], schema=district_schema)

    result = (
        employee.alias("employee")
        .join(
            store.filter(F.col("store_name") != "test").alias("store"),
            on=F.col("employee.employee_id") == F.col("store.store_id"),
        )
        .select(
            F.col("employee.employee_id"),
            F.col("employee.fname"),
            F.col("employee.store_id"),
            F.col("store.store_id"),
            F.col("store.store_name"),
        )
        .join(
            district.alias("district"),
            on=F.col("store.store_id") == F.col("district.district_id"),
        )
    )

    sql = result.sql(pretty=False, optimize=False)
    # After the first join is wrapped into a CTE, "store.store_id" in the second join's
    # ON clause should resolve to the wrapper CTE, not the inner store CTE.
    # Parse the outer query's FROM/JOIN tables and verify the ON clause only references those.
    import sqlglot

    parsed = sqlglot.parse_one(sql, dialect="spark")
    # Get tables in the outer FROM clause
    from_tables = set()
    if parsed.args.get("from_"):
        from_tables.add(parsed.args["from_"].this.alias_or_name)
    if parsed.args.get("joins"):
        for join in parsed.args["joins"]:
            from_tables.add(join.this.alias_or_name)
    # Get tables referenced in the ON clause
    on_clause = parsed.args["joins"][-1].args["on"]
    on_tables = {col.table for col in on_clause.find_all(exp.Column) if col.table}
    # All tables in ON clause must be in FROM clause
    assert on_tables.issubset(from_tables), (
        f"ON clause references {on_tables - from_tables} which are not in FROM {from_tables}"
    )
