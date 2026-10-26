# Trino (first slice)

`TrinoSession` uses SQLFrame's standalone DataFrame planner to generate Trino SQL.
With a supplied [Trino Python DB-API connection](https://github.com/trinodb/trino-python-client),
`collect()` also runs the generated query. SQL generation works without a connection.

## Install and connect

```bash
pip install "sqlframe[trino]"
```

```python
import trino

from sqlframe.trino import TrinoSession
from sqlframe.trino import functions as F

conn = trino.dbapi.connect(
    host="trino.example.com",
    port=443,
    user="analyst",
    catalog="hive",
    schema="sales",
    http_scheme="https",
)
session = TrinoSession(conn=conn)

# Connected sessions discover table columns from Trino when read.table() runs.
df = session.read.table("hive.sales.orders").where(F.col("total") > 10)
print(df.sql())  # Inspect the Trino SQL before running it.
rows = df.collect()
```

Trino's memory connector is covered by live tests for creating a table from a
DataFrame and appending with `insertInto()`. `saveAsTable(mode="append")` uses the
same insert path. Overwrite mode is rejected before sending SQL because Trino
connectors vary in whether they can replace tables.

For code that imports PySpark, call `activate("trino", conn=conn)` before the
PySpark imports:

```python
from sqlframe import activate

activate("trino", conn=conn)
from pyspark.sql import SparkSession

session = SparkSession.builder.getOrCreate()
```

For SQL generation alone, construct `TrinoSession()` without a connection and
register each table schema with `session.catalog.add_table()` before reading it.
Calling `collect()` without a connection raises a clear error.

## Current limits

- Connected sessions discover columns using Trino's `SHOW COLUMNS`. Offline sessions require explicit schemas. Nullability is currently treated as nullable in catalog metadata.
- This is a bundled engine rather than a third-party plugin package. The current activation registry is fixed to bundled engines.
- Unit tests cover SQL generation and DB-API query collection with a fake connection. Live Docker tests cover table reads, schema discovery, catalog listing, a `VALUES` aggregation, table creation, and append. Overwrite, complex types, engine-specific functions, and broad PySpark API parity remain unsupported or unverified.

The [Standalone API reference](reference/standalone.md) describes the planner
that this session reuses; its method list is not a Trino compatibility guarantee.

## Local Trino tests

The repository includes a pinned Trino 484 Docker Compose service with an in-memory
catalog. Run the live integration tests with Docker Desktop or another Docker daemon:

```bash
make trino-test
```

The command starts Trino on `127.0.0.1:18080`, waits for it to become healthy,
runs `tests/integration/trino`, and removes the container afterward. The same
command runs in CI. For an existing server, set `SQLFRAME_TRINO_HOST` and
`SQLFRAME_TRINO_PORT` and run the test file directly.
