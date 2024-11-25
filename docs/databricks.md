from test import auth_type

# Databricks (In Development)

## Installation

```bash
pip install "sqlframe[databricks]"
```

## Enabling SQLFrame

SQLFrame can be used in two ways:

* Directly importing the `sqlframe.databricks` package 
* Using the [activate](./configuration.md#activating-sqlframe) function to allow for continuing to use `pyspark.sql` but have it use SQLFrame behind the scenes.

### Import

If converting a PySpark pipeline, all `pyspark.sql` should be replaced with `sqlframe.databricks`.
In addition, many classes will have a `Databricks` prefix. 
For example, `DatabricksDataFrame` instead of `DataFrame`.


```python
# PySpark import
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.dataframe import DataFrame
# SQLFrame import
from sqlframe.databricks import DatabricksSession
from sqlframe.databricks import functions as F
from sqlframe.databricks import DatabricksDataFrame
```

### Activate

If you would like to continue using `pyspark.sql` but have it use SQLFrame behind the scenes, you can use the [activate](./configuration.md#activating-sqlframe) function.

```python
import os

from databricks.sql import connect
from sqlframe import activate
conn = connect(
    server_hostname="dbc-xxxxxxxx-xxxx.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/xxxxxxxxxxxxxxxx",
    access_token=os.environ["ACCESS_TOKEN"],  # Replace this with how you get your databricks access token
    auth_type="access_token",
    catalog="catalog",
    schema="schema",
)
activate("databricks", conn=conn)

from pyspark.sql import SparkSession
```

`SparkSession` will now be a SQLFrame `DatabricksSession` object and everything will be run on Databricks directly.

See [activate configuration](./configuration.md#activating-sqlframe) for information on how to pass in a connection and config options.

## Creating a Session

SQLFrame uses [Databricks SQL Connector for Python](https://github.com/databricks/databricks-sql-python) to connect to Databricks. 
A DatabricksSession, which implements the PySpark Session API, is created by passing in a `databricks.sql.client.Connection` object.

=== "Import"

    ```python
    import os
   
    from databricks.sql import connect
    from sqlframe.databricks import DatabricksSession
    
    conn = connect(
        server_hostname="dbc-xxxxxxxx-xxxx.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/xxxxxxxxxxxxxxxx",
        access_token=os.environ["ACCESS_TOKEN"],  # Replace this with how you get your databricks access token
        auth_type="access_token",
        catalog="catalog",
        schema="schema",
    )
    session = DatabricksSession(conn=conn)
    ```

=== "Activate"

    ```python
    import os

    from databricks.sql import connect
    from sqlframe import activate

    conn = connect(
        server_hostname="dbc-xxxxxxxx-xxxx.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/xxxxxxxxxxxxxxxx",
        access_token=os.environ["ACCESS_TOKEN"],  # Replace this with how you get your databricks access token
        auth_type="access_token",
        catalog="catalog",
        schema="schema",
    )
    activate("databricks", conn=conn)

    from pyspark.sql import SparkSession
    session = SparkSession.builder.getOrCreate()
    ```

## Example Usage

```python
import os

from databricks.sql import connect
from sqlframe import activate

conn = connect(
    server_hostname="dbc-xxxxxxxx-xxxx.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/xxxxxxxxxxxxxxxx",
    access_token=os.environ["ACCESS_TOKEN"],  # Replace this with how you get your databricks access token
    auth_type="access_token",
    catalog="catalog",
    schema="schema",
)
activate("databricks", conn=conn)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

session = SparkSession.builder.getOrCreate()
table_path = '"catalog.db.table"'
# Get columns in the table
print(session.catalog.listColumns(table_path))
# Get the top 5 years with the greatest year-over-year % change in new families with a single child
(
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
    .show()
)
"""
+------+---------------------------+----------------+
| year | new families single child | percent change |
+------+---------------------------+----------------+
| 1989 |         1,650,246         |     25.02      |
| 1974 |          783,448          |     14.49      |
| 1977 |         1,057,379         |     11.38      |
| 1985 |         1,308,476         |     11.15      |
| 1975 |          868,985          |     10.92      |
+------+---------------------------+----------------+
"""
```
