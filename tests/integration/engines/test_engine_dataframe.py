from __future__ import annotations

import typing as t

from sqlframe.base.session import _BaseSession
from sqlframe.base.types import DoubleType, LongType, Row, StructField, StructType
from sqlframe.snowflake import SnowflakeSession
from sqlframe.spark import SparkSession

if t.TYPE_CHECKING:
    from sqlframe.base.dataframe import BaseDataFrame

pytest_plugins = ["tests.integration.fixtures"]


def test_collect(get_engine_df: t.Callable[[str], BaseDataFrame], get_func):
    employee = get_engine_df("employee")
    col = get_func("col", employee.session)
    results = employee.select(col("fname"), col("lname")).collect()
    assert results == [
        Row(**{"fname": "Jack", "lname": "Shephard"}),
        Row(**{"fname": "John", "lname": "Locke"}),
        Row(**{"fname": "Kate", "lname": "Austen"}),
        Row(**{"fname": "Claire", "lname": "Littleton"}),
        Row(**{"fname": "Hugo", "lname": "Reyes"}),
    ]


def test_show(
    get_engine_df: t.Callable[[str], BaseDataFrame],
    get_func,
    capsys,
    caplog,
):
    employee = get_engine_df("employee")
    lit = get_func("lit", employee.session)
    col = get_func("col", employee.session)
    employee = (
        employee.select("EmPloyee_Id", "fname", "lnamE", "AGE", "stoRe_iD", lit(1).alias("One"))
        .withColumnRenamed("sToRe_id", "SToRE_Id")
        .withColumns(
            {
                "lNamE": col("lname"),
                "tWo": lit(2),
            }
        )
    )
    employee.show()
    captured = capsys.readouterr()
    assert (
        captured.out
        == """+-------------+--------+-----------+-----+----------+-----+-----+
| EmPloyee_Id | fname  |   lNamE   | AGE | SToRE_Id | One | tWo |
+-------------+--------+-----------+-----+----------+-----+-----+
|      1      |  Jack  |  Shephard |  37 |    1     |  1  |  2  |
|      2      |  John  |   Locke   |  65 |    1     |  1  |  2  |
|      3      |  Kate  |   Austen  |  37 |    2     |  1  |  2  |
|      4      | Claire | Littleton |  27 |    2     |  1  |  2  |
|      5      |  Hugo  |   Reyes   |  29 |   100    |  1  |  2  |
+-------------+--------+-----------+-----+----------+-----+-----+\n"""
    )
    assert "Truncate is ignored so full results will be displayed" not in caplog.text
    employee.show(truncate=True)
    captured = capsys.readouterr()
    assert "Truncate is ignored so full results will be displayed" in caplog.text


def test_show_limit(
    get_engine_df: t.Callable[[str], BaseDataFrame], capsys, is_snowflake: t.Callable
):
    employee = get_engine_df("employee")
    employee.show(1)
    captured = capsys.readouterr()
    if isinstance(employee.session, SnowflakeSession):
        assert (
            captured.out
            == """+-------------+-------+----------+-----+----------+
| EMPLOYEE_ID | FNAME |  LNAME   | AGE | STORE_ID |
+-------------+-------+----------+-----+----------+
|      1      |  Jack | Shephard |  37 |    1     |
+-------------+-------+----------+-----+----------+\n"""
        )
    else:
        assert (
            captured.out
            == """+-------------+-------+----------+-----+----------+
| employee_id | fname |  lname   | age | store_id |
+-------------+-------+----------+-----+----------+
|      1      |  Jack | Shephard |  37 |    1     |
+-------------+-------+----------+-----+----------+\n"""
        )


# https://github.com/eakmanrq/sqlframe/issues/294
def test_show_from_create_version_1(get_session: t.Callable[[], _BaseSession], capsys):
    session = get_session()
    df = session.createDataFrame([(1, 4), (2, 5), (3, 6)], schema=["foo", "BAR"])
    df.show()
    captured = capsys.readouterr()
    assert (
        captured.out.strip()
        == """
+-----+-----+
| foo | BAR |
+-----+-----+
|  1  |  4  |
|  2  |  5  |
|  3  |  6  |
+-----+-----+
""".strip()
    )
    assert df.columns == ["foo", "BAR"]


# https://github.com/eakmanrq/sqlframe/issues/294
def test_show_from_create_version_2(get_session: t.Callable[[], _BaseSession], capsys):
    session = get_session()
    df = session.createDataFrame(
        [
            {"a": 1, "BAR": 1},
            {"a": 1, "BAR": 2},
        ]
    )
    df.show()
    captured = capsys.readouterr()
    assert (
        captured.out.strip()
        == """
+---+-----+
| a | BAR |
+---+-----+
| 1 |  1  |
| 1 |  2  |
+---+-----+
""".strip()
    )
    assert df.columns == ["a", "BAR"]


# https://github.com/eakmanrq/sqlframe/issues/291
def test_show_from_create_with_space(get_session: t.Callable[[], _BaseSession], capsys):
    session = get_session()
    df = session.createDataFrame(
        [
            {"zor ro": 1},
        ]
    )
    df.show()
    captured = capsys.readouterr()
    assert (
        captured.out.strip()
        == """
+--------+
| zor ro |
+--------+
|   1    |
+--------+
""".strip()
    )


# https://github.com/eakmanrq/sqlframe/issues/291
def test_show_from_create_with_space_with_schema(get_session: t.Callable[[], _BaseSession], capsys):
    session = get_session()
    data = {"an tan": [1, 3, 2], "b": [4, 4, 6], "z": [7.0, 8.0, 9.0]}

    df = session.createDataFrame([*zip(*data.values())], schema=[*data.keys()])
    df.show()
    captured = capsys.readouterr()
    assert (
        captured.out.strip()
        == """
+--------+---+-----+
| an tan | b |  z  |
+--------+---+-----+
|   1    | 4 | 7.0 |
|   3    | 4 | 8.0 |
|   2    | 6 | 9.0 |
+--------+---+-----+
    """.strip()
    )
    assert df.columns == ["an tan", "b", "z"]
    assert df.collect() == [
        Row(**{"an tan": 1, "b": 4, "z": 7.0}),
        Row(**{"an tan": 3, "b": 4, "z": 8.0}),
        Row(**{"an tan": 2, "b": 6, "z": 9.0}),
    ]
    assert df.schema.fields[0].name == "an tan"
    df.printSchema()
    captured = capsys.readouterr()
    assert "|-- an tan:" in captured.out.strip()
