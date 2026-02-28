from __future__ import annotations

import sys
import time

import pytest


def pytest_collection_modifyitems(config, items, *args, **kwargs):
    # Skip @pytest.mark.forked tests when running under pytest-xdist workers.
    # fork() in xdist worker subprocesses causes deadlocks with multi-threaded
    # C extensions (DuckDB, macOS Obj-C runtime).
    worker_count = config.getoption("-n", default="0")
    if str(worker_count) not in ("0", "no"):
        skip_forked = pytest.mark.skip(reason="forked tests are incompatible with pytest-xdist")
        for item in items:
            if item.get_closest_marker("forked"):
                item.add_marker(skip_forked)

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
    for cls in _BaseSession.__subclasses__():
        if hasattr(cls, "Builder"):
            cls.builder = cls.Builder()
    yield
