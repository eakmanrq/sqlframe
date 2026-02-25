# CLAUDE.md

## Development Commands

```bash
make install-dev       # Install all deps (uv sync --all-extras)
make style             # Run pre-commit (ruff formatting/linting) — ALWAYS run before committing
make fast-test         # Unit tests only (pytest -n auto tests/unit)
make local-test        # Unit + local integration tests (pytest -n auto -m "fast or local" --reruns 5)
make slow-test         # All tests (pytest -n auto tests)
make duckdb-test       # Single engine tests (also: bigquery-test, postgres-test, spark-test, etc.)
make stubs             # Regenerate .pyi stub files for engine functions modules
```

Type checking: `ty check`

### Running Single Tests

```bash
pytest tests/unit/standalone/test_dataframe.py              # Single file
pytest tests/unit/standalone/test_dataframe.py::test_columns # Single test
pytest -k "test_name_pattern" tests/                         # By pattern
```

## Architecture

SQLFrame implements the PySpark DataFrame API, translating operations into SQL executed against database engines (BigQuery, Databricks, DuckDB, Postgres, Redshift, Snowflake, Spark, Standalone). Core dependency is `sqlglot` for SQL parsing/generation.

### Directory Structure

- `sqlframe/base/` — Abstract base classes, shared logic, functions, mixins
- `sqlframe/<engine>/` — Dialect-specific implementations (session, dataframe, catalog, etc.)
- `sqlframe/testing/` — assertDataFrameEqual, assertSchemaEqual
- `tests/unit/` — SQL generation tests (no live DB)
- `tests/integration/` — Cross-engine and engine-specific tests against live DBs

### Generic Base Classes

Base classes use Python generics so dialect implementations get full type safety:

```python
class _BaseSession(t.Generic[CATALOG, READER, WRITER, DF, TABLE, CONN, UDF_REGISTRATION]): ...
class BaseDataFrame(t.Generic[SESSION, WRITER, NA, STAT, GROUP_DATA]): ...
```

Each engine subclasses these with concrete type parameters and assigns class references (`_catalog`, `_reader`, `_df`, etc.) used for instantiation.

### Dialect Implementations

Each engine directory (`sqlframe/duckdb/`, etc.) contains thin subclasses of the base classes. The session's `Builder` sets `DEFAULT_EXECUTION_DIALECT`. Three dialects exist per session: `input_dialect` (parsing), `output_dialect` (display), `execution_dialect` (engine).

### Mixin Pattern

Capabilities are composed via mixins in `sqlframe/base/mixins/`:

- **Catalog mixins** (`catalog_mixins.py`): `GetCurrentCatalogFromFunctionMixin`, `ListTablesFromInfoSchemaMixin`, etc. — each engine's catalog assembles the mixins it supports
- **DataFrame mixins** (`dataframe_mixins.py`): `NoCachePersistSupportMixin`, `TypedColumnsFromTempViewMixin`
- **Reader/Writer mixins** (`readwriter_mixins.py`): `PandasLoaderMixin`, `PandasWriterMixin`
- **Table mixins** (`table_mixins.py`): `UpdateSupportMixin`, `DeleteSupportMixin`, `MergeSupportMixin`

MRO order: mixins first, then base class last.

### Functions System

All PySpark functions live in `sqlframe/base/functions.py` with `@func_metadata` decorator. Each engine's `functions.py` filters to only supported functions by checking `unsupported_engines`. Use `unsupported_engines=["engine_name"]` to exclude from specific engines, `"*"` to exclude from all.

### Operation Tracking & CTE Building

DataFrame methods are decorated with `@operation(Operation.X)` to track operation order. When incompatible operations are chained, the current expression is wrapped into a CTE before continuing. SQL is built as sqlglot AST and only converted to string at execution or `df.sql()` time.

### Session Singleton

`_BaseSession` uses a singleton pattern (`_instance` class var). Tests use the `rescope_sparksession_singleton` fixture to reset between tests.

### activate/deactivate

`sqlframe.activate("duckdb")` patches `sys.modules` so PySpark imports resolve to SQLFrame classes, enabling drop-in replacement of existing PySpark code.

## Test Markers

`fast` (auto-assigned to unmarked tests), `duckdb`, `bigquery`, `postgres`, `snowflake`, `databricks`, `spark`, `local`
