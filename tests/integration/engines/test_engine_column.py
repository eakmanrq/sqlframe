from __future__ import annotations

import typing as t

import pytest

from sqlframe.base import types
from sqlframe.base.types import Row
from sqlframe.bigquery import BigQuerySession
from sqlframe.postgres import PostgresSession

if t.TYPE_CHECKING:
    from sqlframe.base.session import _BaseSession

pytest_plugins = ["tests.integration.fixtures"]


def test_columnt_eq_null_safe(get_session: t.Callable[[], _BaseSession], get_func):
    session = get_session()
    df1 = session.createDataFrame([Row(id=1, value="foo"), Row(id=2, value=None)])
    lit = get_func("lit", session)
    values = df1.select(
        df1["value"] == "foo",
        df1["value"].eqNullSafe("foo"),
        df1["value"].eqNullSafe(None),
        df1["id"].eqNullSafe(1),
        df1["id"] == lit(None),
        df1["id"].eqNullSafe(None),
    ).collect()

    assert values[0] == (True, True, False, True, None, False)
    assert values[1] == (None, False, True, False, None, False)


def test_column_get_item_array(get_session: t.Callable[[], _BaseSession], get_func):
    session = get_session()
    lit = get_func("lit", session)
    assert session.range(1).select(lit(["a", "b", "c"]).getItem(0).alias("value")).first()[0] == "a"


def test_column_get_item_map(get_session: t.Callable[[], _BaseSession], get_func):
    session = get_session()
    lit = get_func("lit", session)
    if not isinstance(session, (PostgresSession, BigQuerySession)):
        assert session.range(1).select(lit({"key": "value"}).getItem("key")).first()[0] == "value"
