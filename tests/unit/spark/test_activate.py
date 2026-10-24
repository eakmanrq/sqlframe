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


@pytest.mark.parametrize("hook", ["__import__", "import_module"])
@pytest.mark.parametrize("check_after_deactivate", [False, True])
def test_spark_activation_preserves_third_party_import_hook(hook, check_after_deactivate):
    script = dedent(
        f"""
        import builtins
        import importlib

        from sqlframe import activate, deactivate
        from sqlframe.spark import SparkSession as SQLFrameSession

        owner = builtins if {hook!r} == "__import__" else importlib
        original = getattr(owner, {hook!r})
        calls = []

        def wrapper(*args, **kwargs):
            calls.append(args[0])
            return original(*args, **kwargs)

        setattr(owner, {hook!r}, wrapper)
        for _ in range(2):
            activate("spark")
            activate("spark")
            captured = getattr(owner, {hook!r})
            from pyspark.sql import SparkSession
            assert SparkSession is SQLFrameSession
            assert importlib.import_module("pyspark.sql").SparkSession is SQLFrameSession

            if {check_after_deactivate!r}:
                deactivate()
                deactivate()
                assert getattr(owner, {hook!r}) is wrapper

            calls.clear()
            assert captured("math").sqrt(4) == 2
            assert "math" in calls, "third-party import hook was bypassed"
            calls.clear()
            assert getattr(owner, {hook!r})("math").sqrt(4) == 2
            assert "math" in calls
            deactivate()
            assert getattr(owner, {hook!r}) is wrapper
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


@pytest.mark.parametrize("engine", ["spark", "duckdb"])
def test_deactivate_twice_preserves_real_pyspark_modules(engine):
    script = dedent(
        f"""
        import builtins
        import importlib
        import sys

        from pyspark.sql import SparkSession as RealSession
        from sqlframe import activate, deactivate

        real_pyspark = sys.modules["pyspark"]
        real_sql = sys.modules["pyspark.sql"]
        original_import = builtins.__import__
        original_import_module = importlib.import_module

        activate({engine!r})
        deactivate()
        deactivate()

        assert sys.modules["pyspark"] is real_pyspark
        assert sys.modules["pyspark.sql"] is real_sql
        assert builtins.__import__ is original_import
        assert importlib.import_module is original_import_module
        from pyspark.sql import SparkSession
        assert SparkSession is RealSession
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
        from {("importlib" if hook == "import_module" else "builtins")} import {hook} as captured

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


def test_unrouted_dotted_import_preserves_spark_application_alias():
    script = dedent(
        """
        import importlib
        import sys

        from sqlframe import activate, deactivate
        from sqlframe.spark import SparkSession as SQLFrameSession
        from sqlframe.spark import functions as SQLFrameFunctions

        real_pyspark = importlib.import_module("pyspark")
        real_sql = importlib.import_module("pyspark.sql")
        activate("spark")
        try:
            import pyspark.sql
            routed_pyspark = pyspark
            import pyspark.sql.utils
            assert pyspark is routed_pyspark, "unrouted dotted import rebound pyspark"
            assert pyspark.sql.SparkSession is SQLFrameSession
            assert pyspark.sql.functions is SQLFrameFunctions
            assert "pyspark.sql.utils" in sys.modules
            from pyspark.sql.utils import is_remote
            assert is_remote is sys.modules["pyspark.sql.utils"].is_remote

            for caller in ("pyspark.sql.internal", "sqlframe.spark.internal"):
                namespace = {"__name__": caller}
                exec("import pyspark.sql.utils", namespace)
                assert namespace["pyspark"] is real_pyspark
                assert namespace["pyspark"].sql is real_sql

            try:
                import pyspark.sql.nonexistent_sqlframe_test_module
            except ModuleNotFoundError:
                pass
            else:
                raise AssertionError("unrouted import must still load the real module")
        finally:
            deactivate()
        import pyspark.sql.utils
        assert pyspark is real_pyspark
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("module", ["ml.feature", "sql.utils"])
def test_unrouted_import_as_alias_returns_real_module(module):
    script = dedent(
        f"""
        import sys
        from types import ModuleType

        from sqlframe import activate, deactivate
        from sqlframe.spark import SparkSession as SQLFrameSession
        from sqlframe.spark import functions as SQLFrameFunctions

        activate("spark")
        try:
            import pyspark.sql
            routed_pyspark = pyspark
            import pyspark.{module} as imported
            assert isinstance(imported, ModuleType), type(imported)
            assert imported is sys.modules["pyspark.{module}"]
            if {module!r} == "ml.feature":
                from pyspark.ml.feature import VectorAssembler
                assert imported.VectorAssembler is VectorAssembler
                import pyspark.ml
                assert pyspark.ml is sys.modules["pyspark.ml"]
            else:
                from pyspark.sql.utils import is_remote
                assert imported.is_remote is is_remote
            import pyspark.{module}
            assert pyspark is routed_pyspark
            assert pyspark.sql.SparkSession is SQLFrameSession
            assert pyspark.sql.functions is SQLFrameFunctions
            import pyspark.sql.functions as functions
            assert functions is SQLFrameFunctions
        finally:
            deactivate()
        import pyspark.{module} as restored
        assert restored is imported
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
        assert isinstance(sqlframe_session, SparkSession)
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


@pytest.mark.parametrize("form", ["alias", "plain", "from"])
def test_testing_utils_import_preserves_sqlframe_helper(form):
    script = dedent(
        f"""
        import importlib
        import sqlframe.testing as testing
        import sqlframe.testing.utils as expected
        from sqlframe import activate, deactivate

        for _ in range(2):
            activate("spark")
            try:
                if {form!r} == "alias":
                    import pyspark.testing.utils as imported
                    helper = imported.assertDataFrameEqual
                    assert imported is expected
                elif {form!r} == "plain":
                    import pyspark.testing.utils
                    helper = pyspark.testing.utils.assertDataFrameEqual
                    assert pyspark.testing.utils is expected
                else:
                    from pyspark.testing.utils import assertDataFrameEqual as helper
                assert helper is expected.assertDataFrameEqual
                assert importlib.import_module("pyspark.testing.utils") is expected
                assert testing.utils is expected
            finally:
                deactivate()
            import sqlframe.testing.utils as restored
            assert restored is expected
            assert testing.utils is expected
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("existing", [False, True])
def test_unrouted_import_preserves_parent_attributes(existing):
    script = dedent(
        f"""
        import sqlframe.spark as parent
        from sqlframe import activate, deactivate

        assert "utils" not in vars(parent)
        sentinel = object()
        if {existing!r}:
            parent.utils = sentinel
        for _ in range(2):
            activate("spark")
            try:
                import pyspark.sql.utils
                if {existing!r}:
                    assert parent.utils is sentinel
            finally:
                deactivate()
            if {existing!r}:
                assert parent.utils is sentinel
            else:
                assert "utils" not in vars(parent)
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stderr
