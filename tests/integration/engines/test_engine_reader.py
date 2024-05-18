from __future__ import annotations

import typing as t

from sqlframe.base.types import Row

if t.TYPE_CHECKING:
    from sqlframe.base.session import _BaseSession

pytest_plugins = ["tests.integration.fixtures"]


def test_load_no_format(get_session: t.Callable[[], _BaseSession]):
    session = get_session()
    df = session.read.load("tests/fixtures/employee.json")
    assert df.collect() == [
        Row(**{"employee_id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1}),
        Row(**{"employee_id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 1}),
        Row(**{"employee_id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 2}),
        Row(
            **{"employee_id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 2}
        ),
        Row(**{"employee_id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 100}),
    ]


def test_load_no_format_schema(get_session: t.Callable[[], _BaseSession]):
    session = get_session()
    df = session.read.load(
        "tests/fixtures/employee.json",
        schema="employee_id STRING, fname STRING, lname STRING, age INT, store_id INT",
    )
    assert df.collect() == [
        Row(**{"employee_id": "1", "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1}),
        Row(**{"employee_id": "2", "fname": "John", "lname": "Locke", "age": 65, "store_id": 1}),
        Row(**{"employee_id": "3", "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 2}),
        Row(
            **{
                "employee_id": "4",
                "fname": "Claire",
                "lname": "Littleton",
                "age": 27,
                "store_id": 2,
            }
        ),
        Row(**{"employee_id": "5", "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 100}),
    ]


def test_load_json(get_session: t.Callable[[], _BaseSession]):
    session = get_session()
    df = session.read.load("tests/fixtures/employee.json", format="json")
    assert df.collect() == [
        Row(**{"employee_id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1}),
        Row(**{"employee_id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 1}),
        Row(**{"employee_id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 2}),
        Row(
            **{"employee_id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 2}
        ),
        Row(**{"employee_id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 100}),
    ]


def test_json(get_session: t.Callable[[], _BaseSession]):
    session = get_session()
    df = session.read.json("tests/fixtures/employee.json")
    assert df.collect() == [
        Row(**{"employee_id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1}),
        Row(**{"employee_id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 1}),
        Row(**{"employee_id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 2}),
        Row(
            **{"employee_id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 2}
        ),
        Row(**{"employee_id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 100}),
    ]


def test_load_parquet(get_session: t.Callable[[], _BaseSession]):
    session = get_session()
    df = session.read.load("tests/fixtures/employee.parquet", format="parquet")
    assert df.collect() == [
        Row(**{"employee_id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1}),
        Row(**{"employee_id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 1}),
        Row(**{"employee_id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 2}),
        Row(
            **{"employee_id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 2}
        ),
        Row(**{"employee_id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 100}),
    ]


def test_parquet(get_session: t.Callable[[], _BaseSession]):
    session = get_session()
    df = session.read.parquet("tests/fixtures/employee.parquet")
    assert df.collect() == [
        Row(**{"employee_id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1}),
        Row(**{"employee_id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 1}),
        Row(**{"employee_id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 2}),
        Row(
            **{"employee_id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 2}
        ),
        Row(**{"employee_id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 100}),
    ]


def test_load_csv(get_session: t.Callable[[], _BaseSession]):
    session = get_session()
    df = session.read.load("tests/fixtures/employee.csv", format="csv")
    assert df.collect() == [
        Row(**{"employee_id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1}),
        Row(**{"employee_id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 1}),
        Row(**{"employee_id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 2}),
        Row(
            **{"employee_id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 2}
        ),
        Row(**{"employee_id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 100}),
    ]


def test_csv(get_session: t.Callable[[], _BaseSession]):
    session = get_session()
    df = session.read.csv("tests/fixtures/employee.csv")
    assert df.collect() == [
        Row(**{"employee_id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1}),
        Row(**{"employee_id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 1}),
        Row(**{"employee_id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 2}),
        Row(
            **{"employee_id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 2}
        ),
        Row(**{"employee_id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 100}),
    ]
