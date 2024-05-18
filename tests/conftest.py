from __future__ import annotations

import time

import pytest


@pytest.fixture(scope="session", autouse=True)
def set_tz():
    import os

    os.environ["TZ"] = "US/Pacific"
    time.tzset()
    yield
    del os.environ["TZ"]


@pytest.fixture(scope="function", autouse=True)
def rescope_sparksession_singleton():
    from sqlframe.base.session import _BaseSession

    _BaseSession._instance = None
    yield
