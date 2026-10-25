# Spark

Use SQLFrame with an existing Spark workflow and inspect the SQL behind your transformations.

## Installation

```bash
pip install "sqlframe[spark]"
```

## Enabling SQLFrame

SQLFrame can be enabled by directly importing the `sqlframe.spark` package.
If converting a PySpark pipeline, all `pyspark.sql` should be replaced with `sqlframe.spark`.
In addition, many classes will have a `Spark` prefix. 
For example, `SparkDataFrame` instead of `DataFrame`.

```python
# PySpark import
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.dataframe import DataFrame
# SQLFrame import
from sqlframe.spark import SparkSession
from sqlframe.spark import functions as F
from sqlframe.spark import SparkDataFrame
```

### Activate

To keep using `pyspark.sql` imports, call `activate` before importing the SQLFrame classes you want to use:

```python
from sqlframe import activate, deactivate

activate("spark")
from pyspark.sql import SparkSession

session = SparkSession.builder.getOrCreate()
deactivate()
```

You can also pass an existing PySpark session with `activate("spark", conn=existing_session)`.
The activated SQLFrame session uses that same connection. `deactivate()` restores normal PySpark imports.

## Creating a Session

SQLFrame's SparkSession is created the same way you would normally create a SparkSession.
The configuration you apply to the builder will be applied the the SparkSession that SQLFrame will create.

```python
from sqlframe.spark import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Now you can use SQLFrame
```

## Example Usage

```python
from sqlframe.spark import SparkSession
from sqlframe.spark import functions as F


spark = SparkSession.builder.appName("MyApp").getOrCreate()

df = (
    spark.table("insights.covid19_metrics_by_country")
    .where(F.col("date").between("2021-01-01", "2021-12-31"))
    .groupby("continent", "country")
    .agg(
        F.sum("total_case").alias("total_cases"),
        F.sum("total_deaths").alias("total_deaths"),
        (F.sum("total_deaths").cast("float") / F.nullif(F.sum("total_case"), F.lit(0))).alias("death_rate"),
    )
    .orderBy(F.col("death_rate").desc_nulls_last())
)
# or do `print(df.sql())` to print the SQL 
df.show(5)
"""
+---------------+---------+-------------+--------------+---------------+
|   continent   | country | total_cases | total_deaths |   death_rate  |
+---------------+---------+-------------+--------------+---------------+
|      Asia     |  Yemen  |   2372121   |    475483    |  0.2004463516 |
| South America |   Peru  |  675530177  |   62217492   | 0.09210172116 |
| North America |  Mexico |  1039445357 |   89674958   | 0.08627193089 |
|     Africa    |  Sudan  |   13109385  |    929258    | 0.07088494235 |
|      Asia     |  Syria  |   9963515   |    654570    | 0.06569669439 |
+---------------+---------+-------------+--------------+---------------+
"""
```

## Supported PySpark API Methods

Find the full method lists, support notes, and SQLFrame-specific extensions in the
[Spark API reference](reference/spark.md).

- <span id="catalog-class"></span>[Catalog Class](reference/spark.md#catalog-class)
- <span id="column-class"></span>[Column Class](reference/spark.md#column-class)
- <span id="dataframe-class"></span>[DataFrame Class](reference/spark.md#dataframe-class)
- <span id="functions"></span>[Functions](reference/spark.md#functions)
- <span id="groupeddata-class"></span>[GroupedData Class](reference/spark.md#groupeddata-class)
- <span id="dataframereader-class"></span>[DataFrameReader Class](reference/spark.md#dataframereader-class)
- <span id="dataframewriter-class"></span>[DataFrameWriter Class](reference/spark.md#dataframewriter-class)
- <span id="sparksession-class"></span>[SparkSession Class](reference/spark.md#sparksession-class)
- <span id="datatypes"></span>[DataTypes](reference/spark.md#datatypes)
- <span id="window-class"></span>[Window Class](reference/spark.md#window-class)
- <span id="windowspec-class"></span>[WindowSpec Class](reference/spark.md#windowspec-class)
