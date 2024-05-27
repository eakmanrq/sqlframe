# General Configuration

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

Optimized SQL is SQL that has been processed by SQLGlot's optimizer. For complex queries this will significantly reduce the number of CTEs produced and remove extra unused columns. Defaults to `True`.

```python
from sqlframe.bigquery import BigQuerySession
from sqlframe.bigquery import functions as F
from sqlframe.bigquery import Window

session = BigQuerySession()
table_path = "bigquery-public-data.samples.natality"
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

The dialect of the generated SQL will be based on the session's dialect. However, you can override the dialect by passing a string to the `dialect` parameter. This is useful when you want to generate SQL for a different database.

```python
# create session and `df` like normal
df.sql(dialect="bigquery")
```

### OpenAI Enriched

OpenAI's models can be used to enrich the generated SQL to make it more human-like. 
This is useful when you want to generate SQL that is more readable for humans.
You must have `OPENAI_API_KEY` set in your environment variables to use this feature.

```python
# create session and `df` like normal
# The model to use defaults to `gpt-4o` but can be changed by passing a string to the `openai_model` parameter.
>>> df.sql(optimize=False, use_openai=True)
WITH natality_data AS (
  SELECT
    year,
    ever_born
  FROM `bigquery-public-data`.`samples`.`natality`
), single_child_families AS (
  SELECT
    year,
    COUNT(*) AS num_single_child_families
  FROM natality_data
  WHERE ever_born = 1
  GROUP BY year
), lagged_families AS (
  SELECT
    year,
    num_single_child_families,
    LAG(num_single_child_families, 1) OVER (ORDER BY year) AS last_year_num_single_child_families
  FROM single_child_families
), percent_change_families AS (
  SELECT
    year,
    num_single_child_families,
    ((num_single_child_families - last_year_num_single_child_families) / last_year_num_single_child_families) AS percent_change
  FROM lagged_families
  ORDER BY ABS(percent_change) DESC
)
SELECT
  year,
  FORMAT('%\'.0f', ROUND(CAST(num_single_child_families AS FLOAT64), 0)) AS `new families single child`,
  FORMAT('%\'.2f', ROUND(CAST((percent_change * 100) AS FLOAT64), 2)) AS `percent change`
FROM percent_change_families
LIMIT 5
```
