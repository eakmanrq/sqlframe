# Choose your engine

One familiar DataFrame API, connected to the database you already use. Choose an engine for installation and connection instructions.

<div class="sf-engine-grid" markdown="1">
<div class="sf-engine-card" markdown="1">

## [DuckDB →](duckdb.md)

Start locally with an in-memory database or a DuckDB file.

`pip install "sqlframe[duckdb]"`
</div>
<div class="sf-engine-card" markdown="1">

## [BigQuery →](bigquery.md)

Run transformations on your Google Cloud data warehouse.

`pip install "sqlframe[bigquery]"`
</div>
<div class="sf-engine-card" markdown="1">

## [Snowflake →](snowflake.md)

Connect to Snowflake with the Snowflake Python connector.

`pip install "sqlframe[snowflake]"`
</div>
<div class="sf-engine-card" markdown="1">

## [Postgres →](postgres.md)

Run DataFrame pipelines directly on your Postgres database.

`pip install "sqlframe[postgres]"`
</div>
<div class="sf-engine-card" markdown="1">

## [Databricks →](databricks.md)

Connect to a Databricks SQL warehouse.

`pip install "sqlframe[databricks]"`
</div>
<div class="sf-engine-card" markdown="1">

## [Spark →](spark.md)

Use SQLFrame with your Spark environment.

`pip install "sqlframe[spark]"`
</div>
<div class="sf-engine-card" markdown="1">

## [Standalone →](standalone.md)

Generate SQL without connecting to a database.

`pip install sqlframe`
</div>
<div class="sf-engine-card" markdown="1">

## [Redshift →](redshift.md)

Community-maintained integration for Amazon Redshift.

`pip install "sqlframe[redshift]"`
</div>
<div class="sf-engine-card" markdown="1">

## [Trino (first slice) →](trino.md)

Generate Trino SQL and collect queries through a supplied DB-API connection, with explicit table schemas.

`pip install "sqlframe[trino]"`
</div>
</div>

## Community integrations

[GizmoSQL](https://github.com/gizmodata/sqlframe-gizmosql) provides a community-maintained SQLFrame integration. Follow its repository for installation and support.

## API compatibility

Each engine supports a different set of PySpark methods. Check the [API compatibility reference](reference/index.md) for the engine you plan to use.
