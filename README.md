<div align="center">
  <img src="https://sqlframe.readthedocs.io/en/stable/docs/images/sqlframe_logo.png" alt="SQLFrame Logo" width="400"/>
</div>

SQLFrame implements the PySpark DataFrame API in order to enable running transformation pipelines directly on database engines - no Spark clusters or dependencies required.

SQLFrame currently supports the following engines (many more in development):

* [BigQuery](https://sqlframe.readthedocs.io/en/stable/bigquery/)
* [DuckDB](https://sqlframe.readthedocs.io/en/stable/duckdb)
* [Postgres](https://sqlframe.readthedocs.io/en/stable/postgres)
* [Snowflake](https://sqlframe.readthedocs.io/en/stable/snowflake)
* [Spark](https://sqlframe.readthedocs.io/en/stable/spark)

There are also two engines in development. These engines lack test coverage and robust documentation, but are available for early testing:

* [Redshift](https://sqlframe.readthedocs.io/en/stable/redshift)
* [Databricks](https://sqlframe.readthedocs.io/en/stable/databricks)

SQLFrame also has a "Standalone" session that be used to generate SQL without any connection to a database engine.

* [Standalone](https://sqlframe.readthedocs.io/en/stable/standalone)

SQLFrame is great for:

* Users who want a DataFrame API that leverages the full power of their engine to do the processing
* Users who want to run PySpark code quickly locally without the overhead of starting a Spark session
* Users who want a SQL representation of their DataFrame code for debugging or sharing with others
* Users who want to run PySpark DataFrame code without the complexity of using Spark for processing

## Installation

```bash
# BigQuery
pip install "sqlframe[bigquery]"
# DuckDB
pip install "sqlframe[duckdb]"
# Postgres
pip install "sqlframe[postgres]"
# Snowflake
pip install "sqlframe[snowflake]"
# Spark
pip install "sqlframe[spark]"
# Redshift (in development)
pip install "sqlframe[redshift]"
# Databricks (in development)
pip install "sqlframe[databricks]"
# Standalone
pip install sqlframe
```

See specific engine documentation for additional setup instructions.

## Configuration

SQLFrame generates consistently accurate yet complex SQL for engine execution. 
However, when using df.sql(optimize=True), it produces more human-readable SQL. 
For details on how to configure this output and leverage OpenAI to enhance the SQL, see [Generated SQL Configuration](https://sqlframe.readthedocs.io/en/stable/configuration/#generated-sql).

SQLFrame by default uses the Spark dialect for input and output.
This can be changed to make SQLFrame feel more like a native DataFrame API for the engine you are using.
See [Input and Output Dialect Configuration](https://sqlframe.readthedocs.io/en/stable/configuration/#input-and-output-dialect).

## Activating SQLFrame

SQLFrame can either replace pyspark imports or be used alongside them.
To replace pyspark imports, use the [activate function](https://sqlframe.readthedocs.io/en/stable/configuration/#activating-sqlframe) to set the engine to use.

```python
from sqlframe import activate

# Activate SQLFrame to run directly on DuckDB
activate(engine="duckdb")

from pyspark.sql import SparkSession
session = SparkSession.builder.getOrCreate()
```

SQLFrame can also be directly imported which both maintains pyspark imports but also allows for a more engine-native DataFrame API:

```python
from sqlframe.duckdb import DuckDBSession

session = DuckDBSession.builder.getOrCreate()
```

## Example Usage

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
```python
>>> df.sql(optimize=True)
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
