from __future__ import annotations

import typing as t

from sqlframe.base.types import Row

if t.TYPE_CHECKING:
    from sqlframe.base.session import _BaseSession

pytest_plugins = ["tests.integration.fixtures"]


def test_load_no_format(get_session: t.Callable[[], _BaseSession], fixture_root_path: str):
    session = get_session()
    df = session.read.load(f"{fixture_root_path}/tests/fixtures/employee.json")
    expected = [
        Row(**{"employee_id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1}),
        Row(**{"employee_id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 1}),
        Row(**{"employee_id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 2}),
        Row(
            **{"employee_id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 2}
        ),
        Row(**{"employee_id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 100}),
    ]
    assert sorted([sorted(row.asDict().items()) for row in df.collect()]) == sorted(
        [sorted(row.asDict().items()) for row in expected]
    )


def test_load_no_format_schema(get_session: t.Callable[[], _BaseSession], fixture_root_path: str):
    session = get_session()
    df = session.read.load(
        f"{fixture_root_path}/tests/fixtures/employee.json",
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


def test_load_json(get_session: t.Callable[[], _BaseSession], fixture_root_path: str):
    session = get_session()
    df = session.read.load(f"{fixture_root_path}/tests/fixtures/employee.json", format="json")
    expected = [
        Row(**{"employee_id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1}),
        Row(**{"employee_id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 1}),
        Row(**{"employee_id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 2}),
        Row(
            **{"employee_id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 2}
        ),
        Row(**{"employee_id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 100}),
    ]
    assert sorted([sorted(row.asDict().items()) for row in df.collect()]) == sorted(
        [sorted(row.asDict().items()) for row in expected]
    )


def test_json(get_session: t.Callable[[], _BaseSession], fixture_root_path: str):
    session = get_session()
    df = session.read.json(f"{fixture_root_path}/tests/fixtures/employee.json")
    expected = [
        Row(**{"employee_id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1}),
        Row(**{"employee_id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 1}),
        Row(**{"employee_id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 2}),
        Row(
            **{"employee_id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 2}
        ),
        Row(**{"employee_id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 100}),
    ]
    assert sorted([sorted(row.asDict().items()) for row in df.collect()]) == sorted(
        [sorted(row.asDict().items()) for row in expected]
    )


def test_load_parquet(get_session: t.Callable[[], _BaseSession], fixture_root_path: str):
    session = get_session()
    df = session.read.load(f"{fixture_root_path}/tests/fixtures/employee.parquet", format="parquet")
    assert df.collect() == [
        Row(**{"employee_id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1}),
        Row(**{"employee_id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 1}),
        Row(**{"employee_id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 2}),
        Row(
            **{"employee_id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 2}
        ),
        Row(**{"employee_id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 100}),
    ]


def test_parquet(get_session: t.Callable[[], _BaseSession], fixture_root_path: str):
    session = get_session()
    df = session.read.parquet(f"{fixture_root_path}/tests/fixtures/employee.parquet")
    assert df.collect() == [
        Row(**{"employee_id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1}),
        Row(**{"employee_id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 1}),
        Row(**{"employee_id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 2}),
        Row(
            **{"employee_id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 2}
        ),
        Row(**{"employee_id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 100}),
    ]


def test_load_csv(get_session: t.Callable[[], _BaseSession], fixture_root_path: str):
    session = get_session()
    df = session.read.load(
        f"{fixture_root_path}/tests/fixtures/employee.csv",
        format="csv",
        header=True,
        inferSchema=True,
    )
    expected = [
        Row(**{"employee_id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1}),
        Row(**{"employee_id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 1}),
        Row(**{"employee_id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 2}),
        Row(
            **{"employee_id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 2}
        ),
        Row(**{"employee_id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 100}),
    ]
    assert sorted([sorted(row.asDict().items()) for row in df.collect()]) == sorted(
        [sorted(row.asDict().items()) for row in expected]
    )


def test_csv(get_session: t.Callable[[], _BaseSession], fixture_root_path: str):
    session = get_session()
    df = session.read.csv(
        f"{fixture_root_path}/tests/fixtures/employee.csv", header=True, inferSchema=True
    )
    expected = [
        Row(**{"employee_id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1}),
        Row(**{"employee_id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 1}),
        Row(**{"employee_id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 2}),
        Row(
            **{"employee_id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 2}
        ),
        Row(**{"employee_id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 100}),
    ]
    assert sorted([sorted(row.asDict().items()) for row in df.collect()]) == sorted(
        [sorted(row.asDict().items()) for row in expected]
    )
