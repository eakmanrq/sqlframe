from __future__ import annotations

import typing as t

from sqlframe.base.types import Row

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
    employee = employee.select("*", lit(1).alias("one"))
    employee.show()
    captured = capsys.readouterr()
    assert (
        captured.out
        == """+-------------+--------+-----------+-----+----------+-----+
| employee_id | fname  |   lname   | age | store_id | one |
+-------------+--------+-----------+-----+----------+-----+
|      1      |  Jack  |  Shephard |  37 |    1     |  1  |
|      2      |  John  |   Locke   |  65 |    1     |  1  |
|      3      |  Kate  |   Austen  |  37 |    2     |  1  |
|      4      | Claire | Littleton |  27 |    2     |  1  |
|      5      |  Hugo  |   Reyes   |  29 |   100    |  1  |
+-------------+--------+-----------+-----+----------+-----+\n"""
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
    assert (
        captured.out
        == """+-------------+-------+----------+-----+----------+
| employee_id | fname |  lname   | age | store_id |
+-------------+-------+----------+-----+----------+
|      1      |  Jack | Shephard |  37 |    1     |
+-------------+-------+----------+-----+----------+\n"""
    )
