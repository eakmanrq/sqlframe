---
template: home.html
title: Your PySpark code. Your database.
hide:
  - navigation
  - toc
---

<section class="sf-hero" id="overview" markdown="1">
<div markdown="1">
<span class="sf-eyebrow">The universal DataFrame API</span>

# Your PySpark code.<br><span>Your database.</span>

<p class="sf-lead">Run familiar DataFrame transformations directly on your database. No Spark cluster required.</p>

<div class="sf-actions" markdown="1">
[Get started →](quickstart.md){ .md-button .md-button--primary }
[Choose your engine](engines.md){ .md-button }
</div>
</div>

<div class="sf-code-panel" markdown="1">
<div class="sf-code-title">Your DataFrame · Python</div>

```python
from sqlframe.duckdb import functions as F

totals = (
    session.table("orders")
    .groupBy("country")
    .agg(F.sum("revenue").alias("total_revenue"))
)
```

<div class="sf-code-title">↓ SQL · totals.sql(optimize=True)</div>

```sql
SELECT
  `orders`.`country` AS `country`,
  SUM(`orders`.`revenue`) AS `total_revenue`
FROM `orders` AS `orders`
GROUP BY
  `orders`.`country`
```

</div>
</section>

<div class="sf-engine-strip" markdown="1">
<span class="sf-eyebrow">Runs on</span>
[DuckDB](duckdb.md)
[BigQuery](bigquery.md)
[Snowflake](snowflake.md)
[Postgres](postgres.md)
[Databricks](databricks.md)
[Spark](spark.md)
</div>

<section class="sf-benefits" markdown="1">
<div markdown="1">
<span class="sf-eyebrow">01 / Familiar</span>

## Keep your DataFrame API

Use familiar PySpark transformations, joins, and window functions.
</div>
<div markdown="1">
<span class="sf-eyebrow">02 / Direct</span>

## Use your database

Execute transformations where your data already lives.
</div>
<div markdown="1">
<span class="sf-eyebrow">03 / Transparent</span>

## See the SQL

Inspect and share the SQL behind your DataFrame code.
</div>
</section>

<section class="sf-quickstart" id="installation" markdown="1">
<div class="sf-section-heading" markdown="1">

## Start small. Run locally.

[DuckDB quickstart →](quickstart.md)
</div>

```bash
pip install "sqlframe[duckdb]"
```

<div class="sf-example-grid" markdown="1">
<div class="sf-code-panel" markdown="1">
<div class="sf-code-title">Python · No cloud account needed</div>

```python
from sqlframe.duckdb import DuckDBSession

session = DuckDBSession()
df = session.createDataFrame(
    [("US", 120), ("UK", 80), ("US", 30)],
    ["country", "revenue"],
)
df.groupBy("country").sum("revenue").orderBy("country").show()
```

</div>
<div class="sf-result" markdown="1">
<div class="sf-code-title">Result</div>

| country | sum(revenue) |
| :------ | -----------: |
| UK      |           80 |
| US      |          150 |

Three rows of data. One familiar API. Executed by DuckDB.
</div>
</div>
</section>

<section class="sf-paths" markdown="1">
<div class="sf-path" id="activating-sqlframe" markdown="1">

## [Bring an existing PySpark pipeline →](configuration.md#activating-sqlframe)

Keep your imports using SQLFrame activation.
</div>
<div class="sf-path" markdown="1">

## [Just need the SQL? →](standalone.md)

Generate queries with a standalone session.
</div>
<div class="sf-path" id="configuration" markdown="1">

## [Make SQLFrame your own →](configuration.md)

Configure dialects, generated SQL, and session behavior.
</div>
<div class="sf-path" id="example-usage" markdown="1">

## [Explore more examples →](examples.md)

Build transformations with joins and window functions.
</div>
</section>
