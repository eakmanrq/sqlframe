<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/eakmanrq/sqlframe/main/docs/images/wordmark-dark.svg">
    <img src="https://raw.githubusercontent.com/eakmanrq/sqlframe/main/docs/images/wordmark.svg" alt="SQLFrame" width="270" height="56">
  </picture>

  <h3>Your PySpark code. Your database.</h3>
  <p>Run the PySpark DataFrame API directly on your database.<br>No Spark cluster required.</p>

  <p>
    <a href="https://sqlframe.readthedocs.io/en/stable/quickstart/">Quickstart</a> ·
    <a href="https://sqlframe.readthedocs.io/en/stable/">Documentation</a> ·
    <a href="https://github.com/eakmanrq/sqlframe/issues">Report an issue</a>
  </p>
</div>

## Why SQLFrame?

- **Familiar API.** Write transformations with PySpark DataFrame operations, joins, and window functions.
- **Direct execution.** Run your transformations on your database engine.
- **Visible SQL.** Inspect, debug, and share the queries behind your DataFrame code.

## Try it locally

Install SQLFrame with DuckDB. You need Python 3.10 or later; no cloud account or Spark installation is required.

```bash
pip install "sqlframe[duckdb]"
```

```python
from sqlframe.duckdb import DuckDBSession

session = DuckDBSession()
df = session.createDataFrame(
    [("US", 120), ("UK", 80), ("US", 30)],
    ["country", "revenue"],
)
df.groupBy("country").sum("revenue").orderBy("country").show()
```

```text
+---------+--------------+
| country | sum(revenue) |
+---------+--------------+
|    UK   |      80      |
|    US   |     150      |
+---------+--------------+
```

To inspect a transformation's SQL, call `df.sql(optimize=True)`. Follow the [quickstart](https://sqlframe.readthedocs.io/en/stable/quickstart/) for a complete walkthrough.

## Choose your engine

| Engine | Install |
| :----- | :------ |
| [DuckDB](https://sqlframe.readthedocs.io/en/stable/duckdb/) | `pip install "sqlframe[duckdb]"` |
| [BigQuery](https://sqlframe.readthedocs.io/en/stable/bigquery/) | `pip install "sqlframe[bigquery]"` |
| [Snowflake](https://sqlframe.readthedocs.io/en/stable/snowflake/) | `pip install "sqlframe[snowflake]"` |
| [Postgres](https://sqlframe.readthedocs.io/en/stable/postgres/) | `pip install "sqlframe[postgres]"` |
| [Databricks](https://sqlframe.readthedocs.io/en/stable/databricks/) | `pip install "sqlframe[databricks]"` |
| [Spark](https://sqlframe.readthedocs.io/en/stable/spark/) | `pip install "sqlframe[spark]"` |
| [Standalone](https://sqlframe.readthedocs.io/en/stable/standalone/) | `pip install sqlframe` |

Standalone sessions generate SQL without a database connection. For local Parquet support, install `sqlframe[standalone]`.

Community integrations: [GizmoSQL](https://github.com/gizmodata/sqlframe-gizmosql) · [Redshift](https://sqlframe.readthedocs.io/en/stable/redshift/) (`pip install "sqlframe[redshift]"`).

SQLFrame is also available on conda-forge: `conda install -c conda-forge sqlframe`.

## Already using PySpark?

Activate SQLFrame before importing PySpark to run supported transformations on another engine.

```python
from sqlframe import activate

activate("duckdb")

from pyspark.sql import SparkSession

session = SparkSession.builder.getOrCreate()
```

You can also import SQLFrame directly, as shown in the quickstart. See the [activation guide](https://sqlframe.readthedocs.io/en/stable/configuration/#activating-sqlframe) and your engine's [API compatibility reference](https://sqlframe.readthedocs.io/en/stable/reference/) for details.

## Keep exploring

- [Examples](https://sqlframe.readthedocs.io/en/stable/examples/): longer pipelines with aggregations and window functions.
- [Generated SQL](https://sqlframe.readthedocs.io/en/stable/configuration/#generated-sql): formatting, optimization, dialect overrides, and OpenAI enrichment.
- [Input and output dialects](https://sqlframe.readthedocs.io/en/stable/configuration/#input-and-output-dialect): configure SQLFrame for your engine's conventions.
- [Standalone SQL generation](https://sqlframe.readthedocs.io/en/stable/standalone/): generate queries without connecting to an engine.

SQLFrame is open source under the [MIT license](LICENSE).
