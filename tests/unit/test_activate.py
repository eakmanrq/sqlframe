import subprocess
import sys
from textwrap import dedent
from unittest.mock import MagicMock

import findspark
import pytest

from sqlframe import activate, activate_context, deactivate
from sqlframe import testing as SQLFrameTesting


@pytest.mark.parametrize("engine", ["spark", "duckdb"])
def test_activate_context_restores_imports_after_body_exception(engine):
    script = dedent(
        f"""
        import builtins
        import importlib
        import sys

        from sqlframe import ACTIVATE_CONFIG, activate_context

        original_import = builtins.__import__
        original_import_module = importlib.import_module
        real_sql = importlib.import_module("pyspark.sql")
        real_pyspark = sys.modules["pyspark"]
        error = RuntimeError("context body failed")

        try:
            with activate_context({engine!r}, config={{"test.option": "active"}}):
                from pyspark.sql import SparkSession
                expected = importlib.import_module("sqlframe." + {engine!r}).SparkSession
                assert SparkSession is expected
                assert ACTIVATE_CONFIG["test.option"] == "active"
                raise error
        except RuntimeError as caught:
            assert caught is error
        else:
            raise AssertionError("context swallowed the body exception")

        assert builtins.__import__ is original_import, "builtins import hook leaked"
        assert importlib.import_module is original_import_module, "importlib hook leaked"
        assert sys.modules["pyspark"] is real_pyspark, "pyspark module swap leaked"
        assert importlib.import_module("pyspark.sql") is real_sql
        from pyspark.sql import SparkSession
        assert SparkSession is real_sql.SparkSession
        assert not ACTIVATE_CONFIG
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stderr


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
