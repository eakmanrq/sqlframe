# Get started with ClickHouse

SQLFrame can run PySpark style DataFrame queries against ClickHouse through
[ClickHouse Connect](https://clickhouse.com/docs/integrations/language-clients/python/driver-api).
SQLFrame generates SQL with SQLGlot's ClickHouse dialect and uses the driver's
DB API cursor to fetch rows. Install the connector extra:

```bash
pip install "sqlframe[clickhouse]"
```

Create a DB API connection to your ClickHouse server and pass it to the session:

```python
from clickhouse_connect import dbapi
from sqlframe.clickhouse import ClickHouseSession
from sqlframe.clickhouse import functions as F

conn = dbapi.connect(host="localhost", port=8123, username="default", password="")
spark = ClickHouseSession.builder.config("sqlframe.conn", conn).getOrCreate()

rows = (
    spark.table("events")
    .filter(F.col("value") > 1)
    .select("name", "value")
    .collect()
)
spark.stop()
```

`spark.table()` describes the ClickHouse table to obtain its columns and types.
`spark.createDataFrame()` also supports small in-memory data through generated
`SELECT` statements. To run raw SQL over a table that has not been described,
use `spark.sql("SELECT ...", qualify=False)`.

To redirect existing PySpark imports, call `activate("clickhouse", conn=conn)`
before importing `pyspark.sql`, and call `deactivate()` when finished.

## Current limits

The following functions are unsupported because their generated SQL is not
compatible with ClickHouse: `translate`, `dayofweek`, `datediff`, `to_timestamp`,
`unix_timestamp`, `from_unixtime`, `sort_array`, `hash`, `format_number`,
`monotonically_increasing_id`, `struct`, `to_json`, and `sha2`. Accessing these
functions through `sqlframe.clickhouse.functions` raises
`UnsupportedOperationError` before a query is executed. Other functions remain
subject to the SQLGlot dialect and server version limitations described below.

`spark.table()` and `spark.read.table()` require table and database names whose
case survives Spark identifier normalization (normally lowercase). Names such
as `Events`, including backtick-quoted names, raise `UnsupportedOperationError`
before any query is executed. Use lowercase names, or preserve the original
case through raw SQL with SQLGlot's case-sensitive marker:

```python
spark.sql(
    'SELECT * FROM `Events` /* sqlglot.meta case_sensitive */',
    qualify=False,
).collect()
```

`qualify=False` alone does not disable Spark identifier normalization; the
marker preserves the table's case.
For these raw queries, call `.collect()` directly. Rendering them with
`DataFrame.sql()` can raise a SQLGlot optimization error.

`spark.table()` requires column names whose case survives Spark identifier
normalization (normally lowercase). ClickHouse names such as `userId` or `USERID`
raise `UnsupportedOperationError` during schema discovery, before a SELECT is
executed. Quoting a column reference does not bypass this limitation. Use
lowercase column names, or query the original names through raw SQL. Mark each
mixed-case column with SQLGlot's case-sensitive marker and alias it to a
lowercase name:

```python
spark.sql(
    "SELECT `userId` /* sqlglot.meta case_sensitive */ AS user_id, value FROM events",
    qualify=False,
).collect()
```

The alias is required: without it, the outer projection that SQLFrame generates
normalizes the column to `userid`, which ClickHouse does not find. Quoting the
column alone does not work either, because the Spark input dialect reads
`"userId"` as a string literal.

Within one session, use one style for all table references: either qualified
names such as `default.events` or unqualified names such as `events`. Mixing
the two styles, even for different tables, is unsupported.

This integration supports queries and DataFrame transformations. It does not
implement `DataFrame.write.saveAsTable()`, `insertInto()`, or write statements
passed to `spark.sql()` because ClickHouse table creation needs an engine and
write modes do not map directly to Spark.
Use ClickHouse Connect for table creation and inserts. File readers and writers,
catalog listing APIs, UDF registration, and table mutation or merge APIs are
also unsupported. `cache()` and `persist()` are unsupported for this engine.
`array_sort()` supports its default ascending order; a custom comparator is
unsupported.
For left, right, and full outer joins, SQLFrame adds `join_use_nulls=1` to the
ClickHouse query so unmatched columns contain nulls as they do in Spark.
SQLFrame also adds `aggregate_functions_null_for_empty=1` so aggregates over an
empty input return null where Spark does; `count` and `approx_count_distinct`
still return zero.
ClickHouse `LowCardinality` columns use their underlying value type in the
Spark schema. Unsigned integers up to `UInt64` are widened to a Spark type
that holds their full range; `Int128`, `Int256`, `UInt128`, and `UInt256` are
unsupported.
Query behavior depends on the ClickHouse SQL version and SQLGlot's dialect
coverage; test advanced transformations against your server before relying on
them in production.

## Local ClickHouse tests

The repository includes a pinned ClickHouse 25.8.33.6 Docker Compose service.
Run the live connector tests with Docker Desktop or another Docker daemon:

```bash
make clickhouse-test
```

The command starts ClickHouse in a run-unique Compose project on a free
`127.0.0.1` port, waits for it to become healthy, runs
`tests/integration/clickhouse` against that port, and removes the container
afterward, so concurrent runs from separate checkouts do not collide.
The same command runs in CI. For an existing server, set
`SQLFRAME_CLICKHOUSE_HOST`, `SQLFRAME_CLICKHOUSE_PORT`,
`SQLFRAME_CLICKHOUSE_USER`, and `SQLFRAME_CLICKHOUSE_PASSWORD` and run the test
file directly.
