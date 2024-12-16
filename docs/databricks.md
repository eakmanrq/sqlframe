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
table_path = "samples.nyctaxi.trips"
# Get columns in the table
print(session.catalog.listColumns(table_path))
# Get the number of rides per hour
(
    session.table(table_path)
    .where(F.col("tpep_pickup_datetime").between("2016-01-01", "2016-01-16"))
    .withColumn("dropoff_hour", F.hour(F.col("tpep_dropoff_datetime")))
    .groupBy("dropoff_hour").count()
    .select(
        F.format_string('%02d:00', F.col("dropoff_hour")).alias("dropoff Hour"),
        F.col("count").alias("number of rides")
    ).orderBy("dropoff Hour")
    .limit(5)
    .show()
)
"""
+----------------+-------------------+
| `dropoff hour` | `number of rides` |
+----------------+-------------------+
|     00:00      |        205        |
|     01:00      |        159        |
|     02:00      |        117        |
|     03:00      |         88        |
|     04:00      |         73        |
+----------------+-------------------+
"""
```
