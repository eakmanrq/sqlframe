from __future__ import annotations

import typing as t

from sqlframe.base.types import Row
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
