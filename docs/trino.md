# Trino

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

Table writes require a connection. `saveAsTable(table)` creates a missing table
using `CREATE TABLE AS SELECT`. With no mode, `error`, or `errorifexists`, an
existing table raises `FileExistsError`. `saveAsTable(table, mode="append")`
creates a missing table or appends to an existing table by column name; source
columns may be reordered. `insertInto(table)` requires an existing table and
maps columns by position. Use `df.write.byName.insertInto(table)` for name mapping.

```python
df.write.saveAsTable("memory.default.copied_orders")
df.select("total", "order_id").write.saveAsTable(
    "memory.default.copied_orders", mode="append"
)
```

Writers accept a table, schema.table, or catalog.schema.table identifier in the
session's input dialect (Spark by default, with backticks for quoted names).
Unqualified parts use the connection's current catalog and schema. Identifiers
are normalized and quoted before use; SQL fragments and views are rejected.
Writer options and formats are unsupported. `overwrite`, `ignore`, and other
modes are rejected, including `.mode("overwrite").insertInto(...)`. An explicit
`saveAsTable(mode=...)` overrides `.mode(...)`.

Before any `CREATE` or `INSERT`, the writer uses Trino metadata to check table
existence and source columns. Append also checks target columns: column counts
must match, names must match for name mapping, and names must be unique ignoring
case. Native types must match exactly, except for widening between integer types
and widening `varchar(n)` to a longer or unbounded `varchar`. An unbounded
`varchar` cannot append to `varchar(n)`, even when its current values would fit.
In-memory DataFrames commonly generate unbounded `varchar`; select from a server
table with compatible native types when needed. Other conversions, including
decimal or timestamp precision changes, require explicit source casts.
Unknown types (such as untyped nulls) are rejected; cast them first. Metadata or
query analysis failures stop the write. This path requires Trino's
[`DESCRIBE OUTPUT (query)`](https://trino.io/docs/current/sql/describe-output.html)
support, tested with Trino 484.

Live tests cover creation, positional and name-based append, safe widening, and
rejections against the [memory connector](https://trino.io/docs/current/connector/memory.html).
This is not a guarantee for every connector: write operations, native type
mapping, constraints, permissions, and table properties remain connector-specific.
Connectors requiring explicit creation properties need tables provisioned outside
this writer. In particular, Trino's
[`CREATE TABLE AS`](https://trino.io/docs/current/sql/create-table-as.html)
replacement support varies across connectors, so this writer does not replace
tables. Append uses plain `INSERT`, so connector session or catalog settings such
as Hive's
[`insert_existing_partitions_behavior=OVERWRITE`](https://trino.io/docs/current/connector/hive.html#hive-general-configuration-properties)
can still overwrite existing partitions; the writer does not inspect these settings.
Preflight does not establish a transaction, validate row values or
nullability constraints, or prevent concurrent schema changes. Runtime failures
can still occur; inspect connector-specific table and data state before retrying
a failed write, since retries may duplicate rows. After a failed write to a memory
table, drop and recreate the table manually, as the
[memory connector limitations](https://trino.io/docs/current/connector/memory.html#limitations)
require because the table is left in an undefined state. Memory tables are volatile.

Connected sessions can also use `session.sql()` to query existing tables without
registering their schemas first. Live tests cover SQL queries with CTEs, joins,
window functions, and temporary views, alongside the DataFrame read path.

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
- Unit tests cover SQL generation and DB-API query collection with a fake connection. Live Docker tests cover the query, catalog, and write paths described above. Complex types, engine-specific functions, and broad PySpark API parity remain unverified.
- SQLFrame's CTE name rewriting currently fails when a CTE has the same name as a qualified source table in its body. Use a distinct CTE name for such queries.

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
