import datetime
from decimal import Decimal

import pytest

from sqlframe.base.types import Row
from sqlframe.duckdb import DuckDBSession
from sqlframe.duckdb import functions as F


@pytest.mark.parametrize("style", ["named", "positional", "keyword"])
def test_sql_parameter_styles_execute(style):
    session = DuckDBSession()
    people = session.createDataFrame(
        [("Alice", 34, "NY"), ("Bob", 45, "CA"), ("Cara", 29, "NY")],
        ["name", "age", "state"],
    )
    if style == "keyword":
        result = session.sql(
            "SELECT name FROM {people} WHERE age > {age} AND state = {state}",
            people=people,
            age=30,
            state="NY",
        )
    else:
        people.createOrReplaceTempView("people")
        if style == "named":
            result = session.sql(
                "SELECT name FROM people WHERE age > :min_age AND state = :state",
                args={"min_age": 30, "state": "NY"},
            )
        else:
            result = session.sql(
                "SELECT name FROM people WHERE age > ? AND state = ?",
                args=[30, "NY"],
            )
    assert result.collect() == [Row(name="Alice")]


@pytest.mark.parametrize(
    "value",
    [
        "O'Reilly ? :value {value} \\ ' OR TRUE --",
        7,
        2.5,
        True,
        None,
        datetime.date(2024, 1, 2),
        datetime.datetime(2024, 1, 2, 3, 4, 5),
        Decimal("12.34"),
        [1, 2],
    ],
)
@pytest.mark.parametrize("style", ["named", "positional", "keyword"])
def test_sql_parameter_literal_types_execute(value, style):
    session = DuckDBSession()
    if style == "named":
        result = session.sql("SELECT :value AS value", args={"value": value})
    elif style == "positional":
        result = session.sql("SELECT ? AS value", args=[value])
    else:
        result = session.sql("SELECT {value} AS value", value=value)
    assert result.collect() == [Row(value=value)]


def test_sql_positional_parameter_order_executes():
    session = DuckDBSession()
    result = session.sql(
        "WITH x AS (SELECT ? AS a) SELECT ? AS b, (SELECT ? AS c) AS c FROM x WHERE a = ?",
        args=[1, 2, 3, 1],
    )
    assert result.collect() == [Row(b=2, c=3)]


def test_sql_keyword_dataframes_execute_after_cleanup():
    session = DuckDBSession()
    left = session.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    right = session.createDataFrame([(1, 34), (2, 45)], ["id", "age"])
    result = session.sql(
        "SELECT l.name, r.age FROM {left} l JOIN {right} r ON l.id = r.id WHERE r.age > :age",
        args={"age": 40},
        left=left,
        right=right,
    )
    assert not session.temp_views
    assert result.collect() == [Row(name="Bob", age=45)]


def test_sql_keyword_dataframe_columns_execute():
    session = DuckDBSession()
    people = session.createDataFrame([(34,), (29,)], ["age"])
    result = session.sql(
        "SELECT {people[age]} FROM {people} WHERE {people.age} > {min_age}",
        people=people,
        min_age=30,
    )
    assert result.collect() == [Row(age=34)]


def test_sql_column_literal_parameter_executes():
    session = DuckDBSession()
    result = session.sql("SELECT :value AS value", args={"value": F.lit([1, 2])})
    assert result.collect() == [Row(value=[1, 2])]


@pytest.mark.parametrize("value", [-2, -2.5, Decimal("-2.50")])
@pytest.mark.parametrize("prefix", ["1-", "-"])
@pytest.mark.parametrize("style", ["named", "positional", "keyword", "keyword_column"])
def test_sql_negative_parameter_arithmetic_executes(value, prefix, style):
    session = DuckDBSession()
    if style == "named":
        result = session.sql(f"SELECT {prefix}:value AS value", args={"value": value})
    elif style == "positional":
        result = session.sql(f"SELECT {prefix}? AS value", args=[value])
    else:
        result = session.sql(
            f"SELECT {prefix}{{value}} AS value",
            value=F.lit(value) if style == "keyword_column" else value,
        )
    expected = 1 - value if prefix == "1-" else -value
    assert result.collect() == [Row(value=expected)]


@pytest.mark.parametrize("aliases", [False, True])
def test_sql_keyword_joined_dataframe_columns_execute(aliases):
    session = DuckDBSession()
    left = session.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    right = session.createDataFrame([(2, 45), (3, 29)], ["id", "age"])
    sources = "FROM {left} l JOIN {right} r" if aliases else "FROM {left} JOIN {right}"
    result = session.sql(
        "SELECT {left.id}, {left.name}, {right.age} " + sources + " ON {left.id} = {right.id}",
        left=left,
        right=right,
    )
    assert not session.temp_views
    assert result.collect() == [Row(id=2, name="Bob", age=45)]
