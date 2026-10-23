import os
import subprocess
import sys
from textwrap import dedent

import pytest

from sqlframe import activate, deactivate
from sqlframe.base.util import classproperty
from sqlframe.spark import Column as SparkColumn
from sqlframe.spark import Row as SparkRow
from sqlframe.spark import (
    SparkCatalog,
    SparkDataFrame,
    SparkDataFrameNaFunctions,
    SparkDataFrameReader,
    SparkDataFrameStatFunctions,
    SparkDataFrameWriter,
    SparkGroupedData,
    SparkSession,
    SparkUDFRegistration,
)
from sqlframe.spark import Window as SparkWindow
from sqlframe.spark import WindowSpec as SparkWindowSpec
from sqlframe.spark import functions as SparkF
from sqlframe.spark import types as SparkTypes


def test_replace_pyspark_spark(check_pyspark_imports):
    check_pyspark_imports(
        "spark",
        sqlf_session=SparkSession,
        sqlf_catalog=SparkCatalog,
        sqlf_column=SparkColumn,
        sqlf_dataframe=SparkDataFrame,
        sqlf_grouped_data=SparkGroupedData,
        sqlf_window=SparkWindow,
        sqlf_window_spec=SparkWindowSpec,
        sqlf_functions=SparkF,
        sqlf_types=SparkTypes,
        sqlf_udf_registration=SparkUDFRegistration,
        sqlf_dataframe_reader=SparkDataFrameReader,
        sqlf_dataframe_writer=SparkDataFrameWriter,
        sqlf_dataframe_na_functions=SparkDataFrameNaFunctions,
        sqlf_dataframe_stat_functions=SparkDataFrameStatFunctions,
        sqlf_row=SparkRow,
    )


@pytest.mark.parametrize("check_after_deactivate", [False, True])
def test_repeated_spark_activation_preserves_imports(check_after_deactivate):
    script = dedent(
        f"""
        import builtins
        import importlib

        from sqlframe import ACTIVATE_CONFIG, activate, deactivate
        from sqlframe.spark import SparkSession as SQLFrameSession

        original_import = builtins.__import__
        original_import_module = importlib.import_module
        conn = object()

        for _ in range(2):
            activate("spark", config={{"test.option": "first"}})
            activate("spark", conn=conn, config={{"test.option": "second"}})
            assert ACTIVATE_CONFIG["sqlframe.conn"] is conn
            assert ACTIVATE_CONFIG["test.option"] == "second"

            if {check_after_deactivate!r}:
                deactivate()

            import math
            assert math.sqrt(4) == 2
            assert importlib.import_module("math") is math

            if not {check_after_deactivate!r}:
                from pyspark.sql import SparkSession
                assert SparkSession is SQLFrameSession
                assert importlib.import_module("pyspark.sql").SparkSession is SQLFrameSession
                deactivate()

            assert builtins.__import__ is original_import
            assert importlib.import_module is original_import_module
            assert not ACTIVATE_CONFIG
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("engines", [("spark", "duckdb"), ("duckdb", "spark")])
def test_switch_engines_restores_imports(engines):
    script = dedent(
        f"""
        import builtins
        import importlib
        import sys

        from sqlframe import ACTIVATE_CONFIG, activate, deactivate

        original_import = builtins.__import__
        original_import_module = importlib.import_module
        real_sql = importlib.import_module("pyspark.sql")
        real_pyspark = sys.modules["pyspark"]

        for engine in {engines!r}:
            activate(engine, config={{"test.engine": engine}})
            from pyspark.sql import SparkSession
            expected = importlib.import_module("sqlframe." + engine).SparkSession
            assert SparkSession is expected, (engine, SparkSession, expected)
            assert importlib.import_module("pyspark.sql").SparkSession is expected
            assert ACTIVATE_CONFIG["test.engine"] == engine

        deactivate()
        assert builtins.__import__ is original_import
        assert importlib.import_module is original_import_module
        assert sys.modules["pyspark"] is real_pyspark
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


@pytest.mark.parametrize("hook", ["import_module", "__import__"])
def test_captured_spark_import_hook_survives_deactivation(hook):
    script = dedent(
        f"""
        import builtins
        import importlib

        from sqlframe import activate, deactivate
        from sqlframe.spark import SparkSession as SQLFrameSession

        original_import = builtins.__import__
        original_import_module = importlib.import_module
        activate("spark")
        from {('importlib' if hook == 'import_module' else 'builtins')} import {hook} as captured

        def load(name):
            if {hook!r} == "__import__":
                return captured(name, fromlist=("SparkSession",))
            return captured(name)

        assert load("pyspark.sql").SparkSession is SQLFrameSession
        for _ in range(2):
            deactivate()
            assert builtins.__import__ is original_import
            assert importlib.import_module is original_import_module
            assert load("math").sqrt(4) == 2
            assert load("pyspark.sql") is original_import_module("pyspark.sql")
            assert load("pyspark.sql").SparkSession is not SQLFrameSession
            activate("spark")
            assert load("pyspark.sql").SparkSession is SQLFrameSession
        deactivate()
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.local
def test_activate_with_existing_pyspark_session_reuses_connection(monkeypatch):
    from pyspark.sql import SparkSession as PySparkSession

    spark = (
        PySparkSession.builder.master("local[1]")
        .appName("sqlframe-activate-existing-session")
        .getOrCreate()
    )
    # The shared test fixture replaces classproperty with a cached Builder.
    monkeypatch.setattr(SparkSession, "builder", classproperty(lambda cls: cls.Builder()))

    try:
        activate(engine="spark", conn=spark)
        from pyspark.sql import SparkSession as ActivatedSession

        sqlframe_session = ActivatedSession.builder.getOrCreate()
        assert sqlframe_session.spark_session is spark
        assert sqlframe_session.sql("SELECT 1 AS value").collect()[0].value == 1
    finally:
        deactivate()
        try:
            assert PySparkSession.builder.getOrCreate() is spark
            assert spark.sql("SELECT 1 AS value").collect()[0].value == 1
        finally:
            spark.stop()


@pytest.mark.local
def test_activate_spark_without_existing_session_runs_sql():
    script = dedent(
        """
        import importlib

        from sqlframe import activate, deactivate

        activate(engine="spark")
        spark = None
        try:
            from pyspark.sql import SparkSession, functions as F
            from pyspark.sql.session import SparkSession as SessionFromSubmodule
            import pyspark.sql.functions as FunctionsFromSubmodule

            assert SparkSession is SessionFromSubmodule
            assert F is FunctionsFromSubmodule
            assert importlib.import_module("pyspark.sql").SparkSession is SparkSession
            assert importlib.import_module("pyspark.sql.functions") is F
            session = SparkSession.builder.master("local[1]").appName("sqlframe-activation").getOrCreate()
            spark = session.spark_session
            assert session.sql("SELECT 1 AS value").collect()[0].value == 1
            assert [row.id for row in session.range(3).select(F.col("id")).collect()] == [0, 1, 2]
        finally:
            deactivate()
            if spark is not None:
                spark.stop()

        from pyspark.sql import SparkSession as RestoredSparkSession

        assert RestoredSparkSession is not SparkSession
        assert importlib.import_module("pyspark.sql").SparkSession is RestoredSparkSession
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env={**os.environ, "SPARK_LOCAL_IP": "127.0.0.1"},
        timeout=90,
    )
    assert result.returncode == 0, result.stderr
