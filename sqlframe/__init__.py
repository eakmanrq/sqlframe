from __future__ import annotations

import importlib
import sys
import typing as t
from contextlib import contextmanager
from unittest.mock import MagicMock

from sqlglot.dialects import spark

spark.Spark.Generator.readparquet_sql = lambda self, x: x.sql(dialect="duckdb")  # type: ignore

if t.TYPE_CHECKING:
    from sqlframe.base.session import CONN

ENGINE_TO_PREFIX = {
    "bigquery": "BigQuery",
    "duckdb": "DuckDB",
    "postgres": "Postgres",
    "redshift": "Redshift",
    "snowflake": "Snowflake",
    "spark": "Spark",
    "databricks": "Databricks",
    "standalone": "Standalone",
}

NAME_TO_FILE_OVERRIDE = {
    "DataFrameNaFunctions": "dataframe",
    "DataFrameStatFunctions": "dataframe",
    "DataFrameReader": "readwriter",
    "DataFrameWriter": "readwriter",
    "GroupedData": "group",
    "SparkSession": "session",
    "WindowSpec": "window",
    "UDFRegistration": "udf",
}

ACTIVATE_CONFIG = {}
_SAVED_PYSPARK_MODULES: t.Dict[str, t.Any] = {}


def activate(
    engine: t.Optional[str] = None,
    conn: t.Optional[t.Any] = None,
    config: t.Optional[t.Dict[str, t.Any]] = None,
) -> None:
    import sqlframe
    from sqlframe import testing

    # Save original pyspark modules so deactivate() can restore them
    # without re-importing (which would lose class-level state like
    # SparkContext._active_spark_context).
    _SAVED_PYSPARK_MODULES.clear()
    for k, v in sys.modules.items():
        if k.startswith("pyspark"):
            _SAVED_PYSPARK_MODULES[k] = v

    pyspark_mock = MagicMock()
    pyspark_mock.__file__ = "pyspark"
    sys.modules["pyspark"] = pyspark_mock
    pyspark_mock.testing = testing
    sys.modules["pyspark.testing"] = testing
    if conn:
        ACTIVATE_CONFIG["sqlframe.conn"] = conn
    for key, value in (config or {}).items():
        ACTIVATE_CONFIG[key] = value
    if not engine:
        return
    engine = engine.lower()
    if engine not in ENGINE_TO_PREFIX:
        raise ValueError(
            f"Unsupported engine {engine}. Supported engines are {', '.join(ENGINE_TO_PREFIX)}"
        )
    prefix = ENGINE_TO_PREFIX[engine]
    engine_module = importlib.import_module(f"sqlframe.{engine}")

    sys.modules["pyspark.sql"] = engine_module
    pyspark_mock.sql = engine_module
    types = engine_module.__dict__.copy()
    resolved_files = set()
    for name, obj in types.items():
        if name.startswith(prefix) or name in [
            "Column",
            "Window",
            "WindowSpec",
            "functions",
            "types",
        ]:
            name_without_prefix = name.replace(prefix, "")
            if name_without_prefix == "Session":
                name_without_prefix = "SparkSession"
            setattr(engine_module, name_without_prefix, obj)
            file = NAME_TO_FILE_OVERRIDE.get(name_without_prefix, name_without_prefix).lower()
            engine_file = importlib.import_module(f"sqlframe.{engine}.{file}")
            if engine_file not in resolved_files:
                sys.modules[f"pyspark.sql.{file}"] = engine_file
                resolved_files.add(engine_file)
            setattr(engine_file, name_without_prefix, obj)


def deactivate() -> None:
    pyspark_imports = [k for k in sys.modules if k.startswith("pyspark")]

    for k in pyspark_imports:
        del sys.modules[k]

    if _SAVED_PYSPARK_MODULES:
        # Restore the original module objects to preserve class-level state
        for k, v in _SAVED_PYSPARK_MODULES.items():
            sys.modules[k] = v
        _SAVED_PYSPARK_MODULES.clear()
    else:
        # No saved modules — pyspark wasn't imported before activate().
        # Try importing fresh to see if pyspark is installed.
        for k in pyspark_imports:
            try:
                sys.modules[k] = importlib.import_module(k)
            except (ImportError, AttributeError):
                # AttributeError: pyspark.pandas (via pyspark.testing) triggers
                # "np.NaN was removed in NumPy 2.0" on import
                pass
    ACTIVATE_CONFIG.clear()


@contextmanager
def activate_context(
    engine: t.Optional[str] = None,
    conn: t.Optional[CONN] = None,
    config: t.Optional[t.Dict[str, t.Any]] = None,
):
    activate(engine, conn, config)
    yield
    deactivate()
