# General Configuration

## Input and Output Dialect

By default, SQLFrame processes all string inputs using the Spark dialect (e.g., date format strings, SQL) and generates outputs in the Spark dialect (e.g., column names, data types). 
This configuration is ideal if you aim to use the PySpark DataFrame API as if running on Spark while actually executing on another engine.

This configuration can be changed to make SQLFrame feel more like a native DataFrame API for the engine you are using.

Example: Using BigQuery to Change Default Behavior

```python
from sqlframe.bigquery import BigQuerySession

session = BigQuerySession.builder.config(
    map={
        "sqlframe.input.dialect": "bigquery",
        "sqlframe.output.dialect": "bigquery",
    }
).getOrCreate()
```

In this configuration, you can use BigQuery syntax for elements such as date format strings and will receive BigQuery column names and data types in the output.

SQLFrame supports multiple dialects, all of which can be specific as the `input_dialect` and `output_dialect`.

## Activating SQLFrame

SQLFrame can be activated in order to replace `pyspark` imports with `sqlframe` imports for the given engine. 
This allows you to use SQLFrame as a drop-in replacement for PySpark by just adding two lines of code.

### Activate with Engine

If you just provide an engine to `activate` then it will create a connection for that engine with default settings (if the engine supports it).

```python

from sqlframe import activate
activate("duckdb")

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# "spark" is not a SQLFrame DuckDBSession and will run directly on DuckDB
```

### Activate with Connection

If you provide a connection to `activate` then it will use that connection for the engine.

```python
import duckdb
from sqlframe import activate
connection = duckdb.connect("file.duckdb")
activate("duckdb", conn=connection)

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# "spark" is a SQLFrame DuckDBSession and will run directly on DuckDB using `file.duckdb` for persistence
```

### Activate with Configuration

If you provide a configuration to `activate` then it will use that configuration to create a connection for the engine.

```python
from sqlframe import activate
activate("duckdb", config={"sqlframe.input.dialect": "duckdb"})

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# "spark" is a SQLFrame DuckDBSession and will run directly on DuckDB with input dialect set to DuckDB
```

### Deactivating

If you want to deactivate SQLFrame and revert back to PySpark, you can use the `deactivate` function.

```python
from sqlframe import deactivate

deactivate()
```

### Context-Based Activation

For more fine-grained control, SQLFrame provides a context manager that allows you to activate SQLFrame only within a specific block of code. 
This is particularly useful in testing environments or when you need to avoid global activation that might affect other parts of your application.

The `activate_context` function provides a context manager that activates SQLFrame only within the `with` block:

```python
import duckdb
from sqlframe import activate_context

# SQLFrame is not active here
connection = duckdb.connect()

with activate_context("duckdb", conn=connection):
    # SQLFrame is active only within this block
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([{"a": 1}])
    result = df.collect()

# SQLFrame is automatically deactivated here
```

## Generated SQL

### Pretty

If the SQL should be returned in a "pretty" format meaning it has newlines and indentation. Defaults to `True`.

```python
from sqlframe.standalone import StandaloneSession

session = StandaloneSession()

df = session.createDataFrame([{'a': 1, 'b': 2}])
```
```python
>>> print(df.sql())
SELECT
  CAST(`a1`.`a` AS BIGINT) AS `a`,
  CAST(`a1`.`b` AS BIGINT) AS `b`
FROM VALUES
  (1, 2) AS `a1`(`a`, `b`)
```
```python
>>> print(df.sql(pretty=False))
SELECT CAST(`a3`.`a` AS BIGINT) AS `a`, CAST(`a3`.`b` AS BIGINT) AS `b` FROM VALUES (1, 2) AS `a3`(`a`, `b`)
```

### Optimized

Optimized SQL is SQL that has been processed by SQLGlot's optimizer. 
For complex queries this will significantly reduce the number of CTEs produced and remove extra unused columns. 
Defaults to `False`.

```python
from sqlframe.bigquery import BigQuerySession
from sqlframe.bigquery import functions as F
from sqlframe.bigquery import Window

session = BigQuerySession()
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
>>> print(df.sql(optimize=True))
WITH `t94228042` AS (
  SELECT
    `natality`.`year` AS `year`,
    COUNT(*) AS `num_single_child_families`
  FROM `bigquery-public-data`.`samples`.`natality` AS `natality`
  WHERE
    `natality`.`ever_born` = 1
  GROUP BY
    `natality`.`year`
), `t30206548` AS (
  SELECT
    `t94228042`.`year` AS `year`,
    `t94228042`.`num_single_child_families` AS `num_single_child_families`,
    LAG(`t94228042`.`num_single_child_families`, 1) OVER (ORDER BY `t94228042`.`year`) AS `last_year_num_single_child_families`
  FROM `t94228042` AS `t94228042`
)
SELECT
  `t30206548`.`year` AS `year`,
  FORMAT('%\'.0f', ROUND(CAST(`t30206548`.`num_single_child_families` AS FLOAT64), 0)) AS `new families single child`,
  FORMAT(
    '%\'.2f',
    ROUND(
      CAST((
        (
          (
            `t30206548`.`num_single_child_families` - `t30206548`.`last_year_num_single_child_families`
          ) / `t30206548`.`last_year_num_single_child_families`
        ) * 100
      ) AS FLOAT64),
      2
    )
  ) AS `percent change`
FROM `t30206548` AS `t30206548`
ORDER BY
  ABS(`percent_change`) DESC
LIMIT 5
```
```python
>>> print(df.sql(optimize=False))
WITH t14183493 AS (
  SELECT
    `source_year`,
    `year`,
    `month`,
    `day`,
    `wday`,
    `state`,
    `is_male`,
    `child_race`,
    `weight_pounds`,
    `plurality`,
    `apgar_1min`,
    `apgar_5min`,
    `mother_residence_state`,
    `mother_race`,
    `mother_age`,
    `gestation_weeks`,
    `lmp`,
    `mother_married`,
    `mother_birth_state`,
    `cigarette_use`,
    `cigarettes_per_day`,
    `alcohol_use`,
    `drinks_per_week`,
    `weight_gain_pounds`,
    `born_alive_alive`,
    `born_alive_dead`,
    `born_dead`,
    `ever_born`,
    `father_race`,
    `father_age`,
    `record_weight`
  FROM bigquery-public-data.samples.natality
), t17633417 AS (
  SELECT
    year,
    COUNT(*) AS num_single_child_families
  FROM t14183493
  WHERE
    ever_born = 1
  GROUP BY
    year
), t32066970 AS (
  SELECT
    year,
    num_single_child_families,
    LAG(num_single_child_families, 1) OVER (ORDER BY year) AS last_year_num_single_child_families
  FROM t17633417
), t21362690 AS (
  SELECT
    year,
    num_single_child_families,
    last_year_num_single_child_families,
    (
      (
        num_single_child_families - last_year_num_single_child_families
      ) / last_year_num_single_child_families
    ) AS percent_change
  FROM t32066970
  ORDER BY
    ABS(percent_change) DESC
)
SELECT
  year AS year,
  FORMAT('%\'.0f', ROUND(CAST(num_single_child_families AS FLOAT64), 0)) AS `new families single child`,
  FORMAT('%\'.2f', ROUND(CAST((
    percent_change * 100
  ) AS FLOAT64), 2)) AS `percent change`
FROM t21362690
LIMIT 5
```

### Override Dialect

The dialect of the generated SQL will be based on the session's output dialect. 
However, you can override the dialect by passing a string to the `dialect` parameter. 
This is useful when you want to generate SQL for a different database.

```python
# create session and `df` like normal
df.sql(dialect="bigquery")
```

### OpenAI Enrichment

OpenAI's models can be used to enrich the generated SQL to make it more human-like.
You can have it just provide more readable CTE names or you can have it try to make the whole SQL statement more readable.

#### Example

```python
# create session and `df` like normal
# The model to use defaults to `gpt-4o` but can be changed by passing a string to the `openai_model` parameter.
>>> df.sql(openai_config={"mode": "cte_only", "model": "gpt-3.5-turbo"})
WITH `single_child_families_by_year` AS (
  SELECT
    `natality`.`year` AS `year`,
    COUNT(*) AS `num_single_child_families`
  FROM `bigquery-public-data`.`samples`.`natality` AS `natality`
  WHERE
    `natality`.`ever_born` = 1
  GROUP BY
    `natality`.`year`
), `families_with_percent_change` AS (
  SELECT
    `single_child_families_by_year`.`year` AS `year`,
    `single_child_families_by_year`.`num_single_child_families` AS `num_single_child_families`,
    LAG(`single_child_families_by_year`.`num_single_child_families`, 1) OVER (ORDER BY `single_child_families_by_year`.`year`) AS `last_year_num_single_child_families`
  FROM `single_child_families_by_year` AS `single_child_families_by_year`
)
SELECT
  `families_with_percent_change`.`year` AS `year`,
  FORMAT('%\'.0f', ROUND(CAST(`families_with_percent_change`.`num_single_child_families` AS FLOAT64), 0)) AS `new families single child`,
  FORMAT(
    '%\'.2f',
    ROUND(
      CAST((
        (
          (
            `families_with_percent_change`.`num_single_child_families` - `families_with_percent_change`.`last_year_num_single_child_families`
          ) / `families_with_percent_change`.`last_year_num_single_child_families`
        ) * 100
      ) AS FLOAT64),
      2
    )
  ) AS `percent change`
FROM `families_with_percent_change` AS `families_with_percent_change`
ORDER BY
  ABS(`percent_change`) DESC
LIMIT 5
```

#### Parameters

| Parameter         | Description                                                           | Default    |
|-------------------|-----------------------------------------------------------------------|------------|
| `mode`            | The mode to use. Can be `cte_only` or `full`.                         | `cte_only` |
| `model`           | The OpenAI model to use. Note: The default may change in new releases | `gpt-4o`   |
| `prompt_override` | A string to use to override the default prompt.                       | None       |
