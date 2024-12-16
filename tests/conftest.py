from __future__ import annotations

import sys
import time

import pytest


def pytest_collection_modifyitems(items, *args, **kwargs):
    for item in items:
        if not [x for x in item.iter_markers() if x.name != "parametrize"]:
            item.add_marker("fast")


@pytest.fixture(scope="session", autouse=True)
def set_tz():
    import os

    os.environ["TZ"] = "UTC"
    time.tzset()
    yield
    del os.environ["TZ"]


@pytest.fixture(scope="function", autouse=True)
def rescope_sparksession_singleton():
    from sqlframe.base.session import _BaseSession

    _BaseSession._instance = None
    yield
