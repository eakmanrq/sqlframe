# Your first SQLFrame transformation

Run the PySpark DataFrame API locally with DuckDB. You only need Python 3.10 or later; no cloud account or Spark installation is required.

## 1. Install SQLFrame

```bash
pip install "sqlframe[duckdb]"
```

## 2. Create a session

```python
from sqlframe.duckdb import DuckDBSession

session = DuckDBSession()
```

The default session uses an in-memory DuckDB database. To work with an existing connection or a database file, see the [DuckDB guide](duckdb.md#creating-a-session).

## 3. Run a transformation

Create a small DataFrame, group by country, and calculate total revenue.

```python
df = session.createDataFrame(
    [("US", 120), ("UK", 80), ("US", 30)],
    ["country", "revenue"],
)

totals = df.groupBy("country").sum("revenue").orderBy("country")
totals.show()
```

```text title="Output"
+---------+--------------+
| country | sum(revenue) |
+---------+--------------+
|    UK   |      80      |
|    US   |     150      |
+---------+--------------+
```

SQLFrame translates the transformations into SQL and runs them on DuckDB.

## 4. Inspect the SQL

```python
print(totals.sql(optimize=True))
```

Use the SQL to understand, debug, or share your transformation. Learn more about [generated SQL options](configuration.md#generated-sql), including formatting, optimization, and dialects.

## Already using PySpark?

Activate SQLFrame before importing PySpark to run supported transformations on DuckDB while keeping your existing imports.

```python
from sqlframe import activate

activate("duckdb")

from pyspark.sql import SparkSession

session = SparkSession.builder.getOrCreate()
```

See the [activation guide](configuration.md#activating-sqlframe) for connection options and scoped activation, and the [API reference](reference/duckdb.md) for supported methods.

## Where to go next

<div class="sf-paths" markdown="1">
<div class="sf-path" markdown="1">

## [Choose your engine →](engines.md)

Connect to your database or warehouse.
</div>
<div class="sf-path" markdown="1">

## [Generate SQL without a connection →](standalone.md)

Use a standalone session to create queries.
</div>
</div>
