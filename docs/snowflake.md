# Snowflake

Use the PySpark DataFrame API to transform data directly in Snowflake.

## Installation

```bash
pip install "sqlframe[snowflake]"
```

## Enabling SQLFrame

SQLFrame can be used in two ways:

* Directly importing the `sqlframe.snowflake` package 
* Using the [activate](./configuration.md#activating-sqlframe) function to allow for continuing to use `pyspark.sql` but have it use SQLFrame behind the scenes.

### Import

If converting a PySpark pipeline, all `pyspark.sql` should be replaced with `sqlframe.snowflake`.
In addition, many classes will have a `Snowflake` prefix. 
For example, `SnowflakeDataFrame` instead of `DataFrame`.


```python
# PySpark import
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.dataframe import DataFrame
# SQLFrame import
from sqlframe.snowflake import SnowflakeSession
from sqlframe.snowflake import functions as F
from sqlframe.snowflake import SnowflakeDataFrame
```

### Activate

If you would like to continue using `pyspark.sql` but have it use SQLFrame behind the scenes, you can use the [activate](./configuration.md#activating-sqlframe) function.

```python
import os

from snowflake.connector import connect
from sqlframe import activate
conn = connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"],
)
activate("snowflake", conn=conn)

from pyspark.sql import SparkSession
```

`SparkSession` will now be a SQLFrame `SnowflakeSession` object and everything will be run on Snowflake directly.

See [activate configuration](./configuration.md#activating-sqlframe) for information on how to pass in a connection and config options.


## Creating a Session

SQLFrame uses the [Snowflake Python Connector](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector) to connect to Snowflake. 
A SnowflakeQuerySession, which implements the PySpark Session API, can be created by passing in a `snowflake.connector.connection.SnowflakeConnection` object.

=== "Import"

    ```python
    import os
    
    from snowflake.connector import connect
    from sqlframe.snowflake import SnowflakeSession
    from sqlframe.snowflake import functions as F
    
    connection = connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )
    session = SnowflakeSession(conn=connection)
    ```

=== Activate

    ```python
    import os
    
    from snowflake.connector import connect
    from sqlframe import activate
    conn = connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )
    activate("snowflake", conn=conn)
    
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    ```

## Using Snowflake Unique Functions

Snowflake may have a function that isn't represented within the PySpark API. 
If that is the case, you can call it directly using PySpark [call_function](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.call_function.html) function.

```python
import os

from snowflake.connector import connect
from sqlframe.snowflake import SnowflakeSession
from sqlframe.snowflake import functions as F

connection = connect(
    account=os.environ["SQLFRAME_SNOWFLAKE_ACCOUNT"],
    user=os.environ["SQLFRAME_SNOWFLAKE_USER"],
    password=os.environ["SQLFRAME_SNOWFLAKE_PASSWORD"],
    warehouse=os.environ["SQLFRAME_SNOWFLAKE_WAREHOUSE"],
    # Dataset: https://app.snowflake.com/marketplace/listing/GZ1M6ZVQIAF/insights-global-covid-statistics
    database="GLOBAL_COVID_STATISTICS",
    schema=os.environ["SQLFRAME_SNOWFLAKE_SCHEMA"],
)
session = SnowflakeSession(conn=connection)
(
    session.range(1)
    .select(F.call_function("SNOWFLAKE.CORTEX.COMPLETE", F.lit("snowflake-arctic"), F.lit("What are large language models?")).alias("prompt_example"))
    .show()
)
```

## Example Usage

```python
import os

from snowflake.connector import connect
from sqlframe.snowflake import SnowflakeSession
from sqlframe.snowflake import functions as F

connection = connect(
    account=os.environ["SQLFRAME_SNOWFLAKE_ACCOUNT"],
    user=os.environ["SQLFRAME_SNOWFLAKE_USER"],
    password=os.environ["SQLFRAME_SNOWFLAKE_PASSWORD"],
    warehouse=os.environ["SQLFRAME_SNOWFLAKE_WAREHOUSE"],
    # Dataset: https://app.snowflake.com/marketplace/listing/GZ1M6ZVQIAF/insights-global-covid-statistics
    database="GLOBAL_COVID_STATISTICS",
    schema=os.environ["SQLFRAME_SNOWFLAKE_SCHEMA"],
)

session = SnowflakeSession(conn=connection)
df = (
    session.table("INSIGHTS.COVID19_METRICS_BY_COUNTRY")
    .where(F.col("date").between("2021-01-01", "2021-12-31"))
    .groupby("continent", "country")
    .agg(
        F.sum("total_case").alias("total_cases"),
        F.sum("total_deaths").alias("total_deaths"),
        (F.sum("total_deaths").cast("float") / F.nullif(F.sum("total_case"), F.lit(0))).alias("death_rate"),
    )
    .orderBy(F.col("death_rate").desc_nulls_last())
)
df.show(5)
"""
+---------------+---------+-------------+--------------+---------------+
|   CONTINENT   | COUNTRY | TOTAL_CASES | TOTAL_DEATHS |   DEATH_RATE  |
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
[Snowflake API reference](reference/snowflake.md).

- <span id="catalog-class"></span>[Catalog Class](reference/snowflake.md#catalog-class)
- <span id="column-class"></span>[Column Class](reference/snowflake.md#column-class)
- <span id="dataframe-class"></span>[DataFrame Class](reference/snowflake.md#dataframe-class)
- <span id="functions"></span>[Functions](reference/snowflake.md#functions)
- <span id="groupeddata-class"></span>[GroupedData Class](reference/snowflake.md#groupeddata-class)
- <span id="dataframereader-class"></span>[DataFrameReader Class](reference/snowflake.md#dataframereader-class)
- <span id="dataframewriter-class"></span>[DataFrameWriter Class](reference/snowflake.md#dataframewriter-class)
- <span id="sparksession-class"></span>[SparkSession Class](reference/snowflake.md#sparksession-class)
- <span id="datatypes"></span>[DataTypes](reference/snowflake.md#datatypes)
- <span id="window-class"></span>[Window Class](reference/snowflake.md#window-class)
- <span id="windowspec-class"></span>[WindowSpec Class](reference/snowflake.md#windowspec-class)

## Extra Functionality not Present in PySpark

SQLFrame supports the following extra functionality not in PySpark

### Table Class

SQLFrame provides a `Table` class that supports extra DML operations like `update`, `delete` and `merge`. This class is returned when using the `table` function from the `DataFrameReader` class.

```python
import os

from snowflake.connector import connect
from sqlframe.snowflake import SnowflakeSession
from sqlframe.base.table import WhenMatched, WhenNotMatched

connection = connect(
    account=os.environ["SQLFRAME_SNOWFLAKE_ACCOUNT"],
    user=os.environ["SQLFRAME_SNOWFLAKE_USER"],
    password=os.environ["SQLFRAME_SNOWFLAKE_PASSWORD"],
    warehouse=os.environ["SQLFRAME_SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SQLFRAME_SNOWFLAKE_DATABASE"],
    schema=os.environ["SQLFRAME_SNOWFLAKE_SCHEMA"],
)
session = SnowflakeSession(conn=connection)

df_employee = session.createDataFrame(
    [
        {"id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1},
        {"id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 2},
        {"id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 3},
        {"id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 1},
        {"id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 3},
    ]
)

df_employee.write.mode("overwrite").saveAsTable("employee")

table_employee = session.table("employee")  # This object is of Type SnowflakeTable
```

#### Update Statement
The `update` method of the `Table` class is equivalent to the `UPDATE table_name` statement used in standard `sql`.

```python
# Generates a `LazyExpression` object which can be executed using the `execute` method
update_expr = table_employee.update(
    set_={"age": table_employee["age"] + 1},
    where=table_employee["id"] == 1,
)

# Executes the update statement
update_expr.execute()

# Show the result
table_employee.show()
```

Output:
```
+----+--------+-----------+-----+----------+
| id | fname  |   lname   | age | store_id | 
+----+--------+-----------+-----+----------+
| 1  |  Jack  |  Shephard |  38 |    1     |
| 2  |  John  |   Locke   |  65 |    2     |
| 3  |  Kate  |   Austen  |  37 |    3     |
| 4  | Claire | Littleton |  27 |    1     |
| 5  |  Hugo  |   Reyes   |  29 |    3     |
+----+--------+-----------+-----+----------+
```
#### Delete Statement
The `delete` method of the `Table` class is equivalent to the `DELETE FROM table_name` statement used in standard `sql`.

```python
# Generates a `LazyExpression` object which can be executed using the `execute` method
delete_expr = table_employee.delete(
    where=table_employee["id"] == 1,
)

# Executes the delete statement
delete_expr.execute()

# Show the result
table_employee.show()
```

Output:
```
+----+--------+-----------+-----+----------+
| id | fname  |   lname   | age | store_id | 
+----+--------+-----------+-----+----------+
| 2  |  John  |   Locke   |  65 |    2     |
| 3  |  Kate  |   Austen  |  37 |    3     |
| 4  | Claire | Littleton |  27 |    1     |
| 5  |  Hugo  |   Reyes   |  29 |    3     |
+----+--------+-----------+-----+----------+
```
#### Merge Statement

The `merge` method of the `Table` class is equivalent to the `MERGE INTO table_name` statement used in some `sql` engines.

```python
df_new_employee = session.createDataFrame(
    [
        {"id": 1, "fname": "Jack", "lname": "Shephard", "age": 38, "store_id": 1, "delete": False},
        {"id": 2, "fname": "Cate", "lname": "Austen", "age": 39, "store_id": 5, "delete": False},
        {"id": 5, "fname": "Ugo", "lname": "Reyes", "age": 29, "store_id": 3, "delete": True},
        {"id": 6, "fname": "Sun-Hwa", "lname": "Kwon", "age": 27, "store_id": 5, "delete": False},
    ]
)

# Generates a `LazyExpression` object which can be executed using the `execute` method
merge_expr = table_employee.merge(
    df_new_employee,
    condition=table_employee["id"] == df_new_employee["id"],
    clauses=[
        WhenMatched(condition=table_employee["fname"] == df_new_employee["fname"]).update(
            set_={
                "age": df_new_employee["age"],
            }
        ),
        WhenMatched(condition=df_new_employee["delete"]).delete(),
        WhenNotMatched().insert(
            values={
                "id": df_new_employee["id"],
                "fname": df_new_employee["fname"],
                "lname": df_new_employee["lname"],
                "age": df_new_employee["age"],
                "store_id": df_new_employee["store_id"],
            }
        ),
    ],
)

# Executes the merge statement
merge_expr.execute()

# Show the result
table_employee.show()
```

Output:
```
+----+---------+-----------+-----+----------+
| id | fname   |   lname   | age | store_id | 
+----+---------+-----------+-----+----------+
| 1  |  Jack   |  Shephard |  38 |    1     |
| 2  |  John   |   Locke   |  65 |    2     |
| 3  |  Kate   |   Austen  |  37 |    3     |
| 4  | Claire  | Littleton |  27 |    1     |
| 6  | Sun-Hwa |   Kwon    |  27 |    5     |
+----+---------+-----------+-----+----------+
```
