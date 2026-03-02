from unittest.mock import MagicMock

import findspark

from sqlframe import activate, activate_context, deactivate
from sqlframe import testing as SQLFrameTesting


def test_activate_testing():
    activate()
    try:
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
    finally:
        deactivate()


def test_activate_no_engine():
    activate()
    try:
        findspark.init()
        # A way that people check if pyspark is available
        from pyspark import context

        assert isinstance(context, MagicMock)
    finally:
        deactivate()

    from pyspark import context

    assert not isinstance(context, MagicMock)
    assert context is not MagicMock
    assert context is not None


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
