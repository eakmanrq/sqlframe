from unittest.mock import MagicMock

import findspark
import pytest

from sqlframe import activate, activate_context, deactivate
from sqlframe import testing as SQLFrameTesting


@pytest.mark.forked
def test_activate_testing():
    activate()
    findspark.init()
    from pyspark import testing

    assert testing == SQLFrameTesting
    assert testing.assertDataFrameEqual == SQLFrameTesting.assertDataFrameEqual
    assert testing.assertSchemaEqual == SQLFrameTesting.assertSchemaEqual
    from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

    assert assertDataFrameEqual == SQLFrameTesting.assertDataFrameEqual
    assert assertSchemaEqual == SQLFrameTesting.assertSchemaEqual
    import pyspark.testing as testing

    assert testing == SQLFrameTesting
    assert testing.assertDataFrameEqual == SQLFrameTesting.assertDataFrameEqual


@pytest.mark.forked
def test_activate_no_engine():
    activate()
    findspark.init()
    # A way that people check if pyspark is available
    from pyspark import context

    assert isinstance(context, MagicMock)

    deactivate()

    from pyspark import context

    assert not isinstance(context, MagicMock)
    assert context is not MagicMock
    assert context is not None


@pytest.mark.forked
def test_activate_no_engine_context_manager():
    with activate_context():
        findspark.init()
        # A way that people check if pyspark is available
        from pyspark import context

        assert isinstance(context, MagicMock)
    from pyspark import context

    assert not isinstance(context, MagicMock)
    assert context is not MagicMock
    assert context is not None


@pytest.mark.forked
def test_activate_testing_context_manager():
    with activate_context():
        findspark.init()
        from pyspark import testing

        assert testing == SQLFrameTesting
        assert testing.assertDataFrameEqual == SQLFrameTesting.assertDataFrameEqual
        assert testing.assertSchemaEqual == SQLFrameTesting.assertSchemaEqual
        from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

        assert assertDataFrameEqual == SQLFrameTesting.assertDataFrameEqual
        assert assertSchemaEqual == SQLFrameTesting.assertSchemaEqual
        import pyspark.testing as testing

        assert testing == SQLFrameTesting
        assert testing.assertDataFrameEqual == SQLFrameTesting.assertDataFrameEqual
