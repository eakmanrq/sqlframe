from __future__ import annotations

import pytest


@pytest.fixture(scope="function", autouse=True)
def rescope_sparksession_singleton():
    from sqlframe.base.session import _BaseSession

    _BaseSession._instance = None
    yield
