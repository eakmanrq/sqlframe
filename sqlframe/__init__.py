from __future__ import annotations

import builtins
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
    "clickhouse": "ClickHouse",
    "duckdb": "DuckDB",
    "postgres": "Postgres",
    "redshift": "Redshift",
    "snowflake": "Snowflake",
    "spark": "Spark",
    "databricks": "Databricks",
    "standalone": "Standalone",
    "trino": "Trino",
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
_PYSPARK_MODULE_SWAP_ACTIVE = False
_SPARK_IMPORT_MODULES: t.Dict[str, t.Any] = {}
_SPARK_ATTACHED_CHILDREN: t.List[t.Tuple[t.Any, str, t.Any]] = []
# Imported modules can retain our hooks after deactivation, so their delegates
# must outlive any individual activation.
_SPARK_ORIGINAL_IMPORT = builtins.__import__
_SPARK_ORIGINAL_IMPORT_MODULE = importlib.import_module
_SPARK_IMPORT_ACTIVE = False


def _spark_import(name, globals=None, locals=None, fromlist=(), level=0):
    original_import = _SPARK_ORIGINAL_IMPORT
    if _SPARK_IMPORT_ACTIVE and level == 0:
        replacement = _SPARK_IMPORT_MODULES.get(name)
        caller = (globals or {}).get("__name__", "")
        if caller.split(".", 1)[0] not in {"pyspark", "sqlframe"}:
            if replacement is not None:
                return replacement if fromlist else _SPARK_IMPORT_MODULES["pyspark"]
            if not fromlist and name.startswith("pyspark."):
                # Load unrouted modules normally, but keep application imports
                # from rebinding their top-level alias to the real package.
                original_import(name, globals, locals, fromlist, level)
                parent = _SPARK_IMPORT_MODULES["pyspark"]
                parts = name.split(".")
                for index in range(1, len(parts)):
                    module_name = ".".join(parts[: index + 1])
                    child = _SPARK_IMPORT_MODULES.get(module_name)
                    if child is None:
                        child = sys.modules[module_name]
                        # Aliased dotted imports traverse the returned package.
                        # Expose real children without replacing routed modules.
                        if parts[index] not in vars(parent):
                            setattr(parent, parts[index], child)
                            _SPARK_ATTACHED_CHILDREN.append((parent, parts[index], child))
                    parent = child
                return _SPARK_IMPORT_MODULES["pyspark"]
    return original_import(name, globals, locals, fromlist, level)


def _spark_import_module(name, package=None):
    original_import_module = _SPARK_ORIGINAL_IMPORT_MODULE
    replacement = _SPARK_IMPORT_MODULES.get(name) if _SPARK_IMPORT_ACTIVE else None
    if replacement is not None:
        caller = sys._getframe(1).f_globals.get("__name__", "")
        if caller.split(".", 1)[0] not in {"pyspark", "sqlframe"}:
            return replacement
    return original_import_module(name, package)


def activate(
    engine: t.Optional[str] = None,
    conn: t.Optional[t.Any] = None,
    config: t.Optional[t.Dict[str, t.Any]] = None,
) -> None:
    global _SPARK_IMPORT_ACTIVE, _PYSPARK_MODULE_SWAP_ACTIVE
    global _SPARK_ORIGINAL_IMPORT, _SPARK_ORIGINAL_IMPORT_MODULE

    if _SPARK_IMPORT_ACTIVE or _PYSPARK_MODULE_SWAP_ACTIVE:
        deactivate()

    import sqlframe
    from sqlframe import testing

    if conn:
        ACTIVATE_CONFIG["sqlframe.conn"] = conn
    for key, value in (config or {}).items():
        ACTIVATE_CONFIG[key] = value

    if not engine or engine.lower() != "spark":
        # Other engines replace pyspark modules outright. Spark must keep its
        # real modules available to the live PySpark connection.
        _SAVED_PYSPARK_MODULES.clear()
        for k, v in sys.modules.items():
            if k.startswith("pyspark"):
                _SAVED_PYSPARK_MODULES[k] = v

        _PYSPARK_MODULE_SWAP_ACTIVE = True
        pyspark_mock = MagicMock()
        pyspark_mock.__file__ = "pyspark"
        sys.modules["pyspark"] = pyspark_mock
        pyspark_mock.testing = testing
        sys.modules["pyspark.testing"] = testing
    if not engine:
        return
    engine = engine.lower()
    if engine not in ENGINE_TO_PREFIX:
        raise ValueError(
            f"Unsupported engine {engine}. Supported engines are {', '.join(ENGINE_TO_PREFIX)}"
        )
    prefix = ENGINE_TO_PREFIX[engine]
    engine_module = importlib.import_module(f"sqlframe.{engine}")
    if engine == "spark":
        setattr(engine_module, "functions", importlib.import_module("sqlframe.spark.functions"))
        pyspark_mock = MagicMock()
        pyspark_mock.__file__ = "pyspark"
        pyspark_mock.testing = testing

    if engine != "spark":
        sys.modules["pyspark.sql"] = engine_module
    pyspark_mock.sql = engine_module
    spark_modules = {
        "pyspark": pyspark_mock,
        "pyspark.testing": testing,
        "pyspark.testing.utils": testing.utils,
        "pyspark.sql": engine_module,
    }
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
                if engine == "spark":
                    spark_modules[f"pyspark.sql.{file}"] = engine_file
                else:
                    sys.modules[f"pyspark.sql.{file}"] = engine_file
                resolved_files.add(engine_file)
            setattr(engine_file, name_without_prefix, obj)

    if engine == "spark":
        # Preserve hooks installed since SQLFrame was imported, without ever
        # making our own hooks delegate to themselves.
        if builtins.__import__ is not _spark_import:
            _SPARK_ORIGINAL_IMPORT = builtins.__import__
        if importlib.import_module is not _spark_import_module:
            _SPARK_ORIGINAL_IMPORT_MODULE = importlib.import_module
        _SPARK_IMPORT_MODULES.clear()
        _SPARK_IMPORT_MODULES.update(spark_modules)
        _SPARK_IMPORT_ACTIVE = True
        setattr(builtins, "__import__", _spark_import)
        setattr(importlib, "import_module", _spark_import_module)


def deactivate() -> None:
    global _SPARK_IMPORT_ACTIVE, _PYSPARK_MODULE_SWAP_ACTIVE

    if _SPARK_IMPORT_ACTIVE:
        _SPARK_IMPORT_ACTIVE = False
        setattr(builtins, "__import__", _SPARK_ORIGINAL_IMPORT)
        setattr(importlib, "import_module", _SPARK_ORIGINAL_IMPORT_MODULE)
        for parent, name, child in reversed(_SPARK_ATTACHED_CHILDREN):
            if vars(parent).get(name) is child:
                delattr(parent, name)
        _SPARK_ATTACHED_CHILDREN.clear()
        _SPARK_IMPORT_MODULES.clear()

    if not _PYSPARK_MODULE_SWAP_ACTIVE:
        ACTIVATE_CONFIG.clear()
        return

    _PYSPARK_MODULE_SWAP_ACTIVE = False

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
    try:
        yield
    finally:
        deactivate()
