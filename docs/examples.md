# More examples

See how SQLFrame handles longer transformations with familiar PySpark operations.
Start with the [local quickstart](quickstart.md) if you are new to SQLFrame.

## Year-over-year analysis with BigQuery

This example uses the public BigQuery natality dataset. Follow the
[BigQuery setup guide](bigquery.md) to configure your connection first.


```python
from sqlframe import activate

# Activate SQLFrame to run directly on BigQuery
activate(engine="bigquery")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

session = SparkSession.builder.getOrCreate()
table_path = '"bigquery-public-data".samples.natality'
# Top 5 years with the greatest year-over-year % change in new families with single child
df = (
  session.table(table_path)
  .where(F.col("ever_born") == 1)
  .groupBy("year")
  .agg(F.count("*").alias("num_single_child_families"))
  .withColumn(
    "last_year_num_single_child_families",
    F.lag(F.col("num_single_child_families"), 1).over(Window.orderBy("year"))
  )
  .withColumn(
    "percent_change",
    (F.col("num_single_child_families") - F.col("last_year_num_single_child_families"))
    / F.col("last_year_num_single_child_families")
  )
  .orderBy(F.abs(F.col("percent_change")).desc())
  .select(
    F.col("year").alias("year"),
    F.format_number("num_single_child_families", 0).alias("new families single child"),
    F.format_number(F.col("percent_change") * 100, 2).alias("percent change"),
  )
  .limit(5)
)
```
```sql
-- df.sql(optimize=True)
WITH `t94228` AS (
  SELECT
    `natality`.`year` AS `year`,
    COUNT(*) AS `num_single_child_families`
  FROM `bigquery-public-data`.`samples`.`natality` AS `natality`
  WHERE
    `natality`.`ever_born` = 1
  GROUP BY
    `natality`.`year`
), `t39093` AS (
  SELECT
    `t94228`.`year` AS `year`,
    `t94228`.`num_single_child_families` AS `num_single_child_families`,
    LAG(`t94228`.`num_single_child_families`, 1) OVER (ORDER BY `t94228`.`year`) AS `last_year_num_single_child_families`
  FROM `t94228` AS `t94228`
)
SELECT
  `t39093`.`year` AS `year`,
  FORMAT('%\'.0f', ROUND(CAST(`t39093`.`num_single_child_families` AS FLOAT64), 0)) AS `new families single child`,
  FORMAT('%\'.2f', ROUND(CAST((((`t39093`.`num_single_child_families` - `t39093`.`last_year_num_single_child_families`) / `t39093`.`last_year_num_single_child_families`) * 100) AS FLOAT64), 2)) AS `percent change`
FROM `t39093` AS `t39093`
ORDER BY
  ABS(`percent_change`) DESC
LIMIT 5
```
```python
>>> df.show()
+------+---------------------------+----------------+
| year | new families single child | percent change |
+------+---------------------------+----------------+
| 1989 |         1,650,246         |     25.02      |
| 1974 |          783,448          |     14.49      |
| 1977 |         1,057,379         |     11.38      |
| 1985 |         1,308,476         |     11.15      |
| 1975 |          868,985          |     10.92      |
+------+---------------------------+----------------+
```
