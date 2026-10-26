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

`spark.table()` requires column names whose case survives Spark identifier
normalization (normally lowercase). ClickHouse names such as `userId` or `USERID`
raise `UnsupportedOperationError` during schema discovery, before a SELECT is
executed. Quoting a column reference does not bypass this limitation. Use
lowercase column names, or query the original names through raw SQL such as
`spark.sql('SELECT "userId" FROM events', qualify=False).collect()`.

This integration supports queries and DataFrame transformations. It does not
implement `DataFrame.write.saveAsTable()` or `insertInto()` because ClickHouse
table creation needs an engine and write modes do not map directly to Spark.
Use ClickHouse Connect for table creation and inserts. File readers and writers,
catalog listing APIs, UDF registration, and table mutation or merge APIs are
also unsupported. `cache()` and `persist()` are unsupported for this engine.
ClickHouse `LowCardinality` columns use their underlying value type in the
Spark schema. Unsigned integers up to `UInt64` are widened to a Spark type
that holds their full range; `UInt128` and `UInt256` are unsupported.
Query behavior depends on the ClickHouse SQL version and SQLGlot's dialect
coverage; test advanced transformations against your server before relying on
them in production.
