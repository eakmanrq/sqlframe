from sqlframe.base.table import WhenNotMatchedBySourcefrom sqlframe.base.table import WhenMatched

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

## Extra Functionality not Present in PySpark

SQLFrame supports the following extra functionality not in PySpark

### Table Class

SQLFrame provides a `Table` class that supports extra DML operations like `update`, `delete` and `merge`. This class is returned when using the `table` function from the `DataFrameReader` class.

```python
import os
   
from databricks.sql import connect
from sqlframe.databricks import DatabricksSession
from sqlframe.base.table import WhenMatched, WhenNotMatched, WhenNotMatchedBySource

conn = connect(
    server_hostname="dbc-xxxxxxxx-xxxx.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/xxxxxxxxxxxxxxxx",
    access_token=os.environ["ACCESS_TOKEN"],  # Replace this with how you get your databricks access token
    auth_type="access_token",
    catalog="catalog",
    schema="schema",
)
session = DatabricksSession(conn=conn)

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

table_employee = session.table("employee")  # This object is of Type DatabricksTable
```

#### Update Statement
The `update` method of the `Table` class is equivalent to the `UPDATE table_name` statement used in standard `sql`.

```python
# Generates a `LazyExpression` object which can be executed using the `execute` method
update_expr = table_employee.update(
    set_={"age": table_employee["age"] + 1},
    where=table_employee["id"] == 1,
)

# Excecutes the update statement
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

# Excecutes the delete statement
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

# Excecutes the merge statement
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


Some engines like `Databricks` support an extra clause inside the `merge` statement which is `WHEN NOT MATCHED BY SOURCE THEN DELETE`.

```python
df_new_employee = session.createDataFrame(
    [
        {"id": 1, "fname": "Jack", "lname": "Shephard", "age": 38, "store_id": 1},
        {"id": 2, "fname": "Cate", "lname": "Austen", "age": 39, "store_id": 5},
        {"id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 3},
        {"id": 6, "fname": "Sun-Hwa", "lname": "Kwon", "age": 27, "store_id": 5},
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
        WhenNotMatched().insert(
            values={
                "id": df_new_employee["id"],
                "fname": df_new_employee["fname"],
                "lname": df_new_employee["lname"],
                "age": df_new_employee["age"],
                "store_id": df_new_employee["store_id"],
            }
        ),
        WhenNotMatchedBySource().delete(),
    ],
)

# Excecutes the merge statement
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
| 5  |  Hugo   |   Reyes   |  29 |    3     |
| 6  | Sun-Hwa |   Kwon    |  27 |    5     |
+----+---------+-----------+-----+----------+
```
