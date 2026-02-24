---
name: sqlglot-upgrade
description: This skill should be used when handling a sqlglot version bump, updating sqlglot compatibility, working on a renovate/sqlglot branch, fixing sqlglot-related test failures, or migrating functions from invoke_anonymous_function to sqlglot expression classes.
version: 1.0.0
---

# SQLGlot Upgrade Skill

Guides the process of updating SQLFrame to be compatible with a new version of sqlglot.

## Overview

SQLFrame wraps Spark's DataFrame API and uses sqlglot to transpile SQL across dialects. When sqlglot adds new expression classes for SQL functions, SQLFrame can drop hand-rolled dialect workarounds and anonymous function calls in favor of proper typed expressions that sqlglot transpiles natively.

## Upgrade Process

### 1. Update the Dependency

Update the sqlglot version constraint in `setup.py` and install:
```bash
uv pip install -e ".[dev]"
```

### 2. Check for API Removals

sqlglot occasionally removes helpers from `sqlglot.helper`. Common removals:
- `classproperty` was removed in v29 — replace with plain class attributes (`builder = Builder()`)
- Grep for sqlglot helper imports across the codebase: `from sqlglot.helper import ...`

### 3. Discover New Expression Classes

Check what `Func` subclasses were added in the new sqlglot version:
```bash
# Compare expression classes between versions
python -c "import sqlglot.expressions as e; print([x for x in dir(e) if not x.startswith('_')])"
```

Also check the sqlglot CHANGELOG or git log for the release.

### 4. Run Tests to Find Failures

```bash
uv run pytest tests/unit/standalone/test_functions.py -x
uv run pytest tests/ -x --ignore=tests/integration
```

### 5. Fix Each Failure

#### Pattern A: Switch from `invoke_anonymous_function` to `invoke_expression_over_column`

When sqlglot adds a proper expression class for a function, replace the raw function name call with the typed expression. This allows sqlglot to handle dialect-specific transpilation automatically.

**Before:**
```python
@meta(unsupported_engines=["bigquery", "duckdb", "postgres"])
def array_except(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_anonymous_function(col1, "ARRAY_EXCEPT", Column.ensure_col(col2))
```

**After:**
```python
@meta(unsupported_engines="postgres")
def array_except(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return Column.invoke_expression_over_column(
        col1, expression.ArrayExcept, expression=Column.ensure_col(col2)
    )
```

Key rules:
- The first arg to `invoke_expression_over_column` maps to `this` on the expression
- Additional args use the keyword names from the expression class definition in `sqlglot/expressions.py`
- A second positional column argument typically uses `expression=col`
- Remove engines from `unsupported_engines` that sqlglot now handles natively — check sqlglot's dialect files to confirm

#### Pattern B: Remove Dialect-Specific Workarounds

When sqlglot gains native support for a dialect, remove the manual branching:

**Before:**
```python
@meta()
def array_min(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import array_min_bgutil, array_min_from_sort, array_min_from_subquery
    session = _get_session()
    if session._is_bigquery:
        return array_min_bgutil(col)
    if session._is_duckdb:
        return array_min_from_sort(col)
    if session._is_postgres:
        return array_min_from_subquery(col)
    return Column.invoke_anonymous_function(col, "ARRAY_MIN")
```

**After:**
```python
@meta()
def array_min(col: ColumnOrName) -> Column:
    from sqlframe.base.function_alternatives import array_min_from_subquery
    session = _get_session()
    if session._is_postgres:
        return array_min_from_subquery(col)
    return Column.invoke_expression_over_column(col, expression.ArrayMin)
```

Check that the removed alternatives are no longer used anywhere before removing them from `function_alternatives.py`.

#### Pattern C: Expressions with Named Keyword Args

For expressions with multiple named fields (not just `this` and `expression`), build the kwargs dict:

**Example (`make_timestamp` → `TimestampFromParts`):**
```python
kwargs: t.Dict[str, exp.Expression] = {
    "year": Column.ensure_col(years).column_expression,
    "month": Column.ensure_col(months).column_expression,
    "day": Column.ensure_col(days).column_expression,
    "hour": Column.ensure_col(hours).column_expression,
    "min": Column.ensure_col(mins).column_expression,
    "sec": Column.ensure_col(secs).column_expression,
}
if timezone is not None:
    kwargs["zone"] = Column.ensure_col(timezone).column_expression
return Column(expression.TimestampFromParts(**kwargs))
```

Look up the field names in `sqlglot/expressions.py` for the target expression class.

#### Pattern D: Functions That Were Previously Unsupported

If a function was marked `@meta(unsupported_engines="*")` (unsupported everywhere), and sqlglot now has a proper expression for it, change it to `@meta()` and implement using the expression class.

### 6. Update Tests

Test expectations in `tests/unit/standalone/test_functions.py` use the raw SQL output. When switching from `invoke_anonymous_function` to a typed expression, the output format may change:

- `BIT_GET(cola, colb)` → `GETBIT(cola, colb)` (canonical sqlglot form)
- `CURDATE()` → `CURRENT_DATE` (no parens for zero-arg date functions)

Run the specific test after each change to verify:
```bash
uv run pytest tests/unit/standalone/test_functions.py::test_bit_get -xvs
```

### 7. Verify Integration Tests Still Pass

```bash
uv run pytest tests/integration/test_functions.py -x
```

### 8. Update Dialect Documentation

Update `docs/<dialect>.md` for each dialect that gained support for a function. The docs use alphabetically-sorted bullet lists — omitted functions are unsupported, so only add entries for functions that are now supported.

**Which files to update:** `bigquery.md`, `duckdb.md`, `postgres.md`, `snowflake.md` (Spark and Standalone use a blanket statement; Databricks and Redshift have no function lists).

**How to determine which dialects to add a function to:** Check the function's `@meta(unsupported_engines=...)` decorator. If a dialect is not in that list, it's supported and should appear in the docs.

**Format** — plain bullet with a link to the PySpark API doc:
```markdown
* [make_timestamp](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.make_timestamp.html)
```

Add dialect-specific caveats as indented sub-bullets when behavior differs from Spark:
```markdown
* [element_at](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.element_at.html)
    * Only works on strings (does not work on arrays)
```

**PySpark API URL pattern:**
`https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.<function_name>.html`

## Key Files

| File | Purpose |
|------|---------|
| `sqlframe/base/functions.py` | Main function implementations |
| `sqlframe/base/function_alternatives.py` | Dialect-specific workarounds |
| `tests/unit/standalone/test_functions.py` | Unit tests (check SQL output) |
| `setup.py` | sqlglot version constraint |
| `sqlframe/spark/session.py` | SparkSession (uses sqlglot helpers) |

## Checking sqlglot Expression Classes

To find the correct expression class and its field names:
```bash
python -c "import sqlglot.expressions as e; help(e.ArrayExcept)"
# or
python -c "from sqlglot import exp; import inspect; print(inspect.getsource(exp.TimestampFromParts))"
```

The `arg_types` dict on each expression class lists all valid keyword arguments.
