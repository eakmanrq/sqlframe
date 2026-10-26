# Trino (first slice)

`TrinoSession` uses SQLFrame's standalone DataFrame planner to generate Trino SQL.
With a supplied [Trino Python DB-API connection](https://github.com/trinodb/trino-python-client),
`collect()` also runs the generated query. SQL generation works without a connection.

## Install and connect

```bash
pip install sqlframe trino
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

# Supply the schema before using read.table(). Trino catalog discovery is not
# implemented in this first slice.
session.catalog.add_table(
    "hive.sales.orders",
    column_mapping={"order_id": "bigint", "total": "double"},
)
df = session.read.table("hive.sales.orders").where(F.col("total") > 10)
print(df.sql())  # Inspect the Trino SQL before running it.
rows = df.collect()
```

For SQL generation alone, construct `TrinoSession()` without a connection and
use `df.sql()`. Calling `collect()` without a connection raises a clear error.

## Current limits

- Register each table schema with `session.catalog.add_table()` before reading it. Automatic Trino catalog lookup is not implemented.
- Use direct `sqlframe.trino` imports. `activate("trino")` is not registered, and this is not a third-party plugin package. The current activation registry is fixed to bundled engines.
- This slice tests SQL generation and DB-API query collection with a fake connection. It has not been tested against a running Trino server. Writes, schema discovery, complex types, engine-specific functions, and broad PySpark API parity remain unverified.

The [Standalone API reference](reference/standalone.md) describes the planner
that this session reuses; its method list is not a Trino compatibility guarantee.
