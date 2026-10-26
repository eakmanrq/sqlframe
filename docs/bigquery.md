# BigQuery

Run DataFrame transformations directly on BigQuery using the BigQuery client connection.

## Installation

```bash
pip install "sqlframe[bigquery]"
```

## Enabling SQLFrame

SQLFrame can be used in two ways:

* Directly importing the `sqlframe.bigquery` package 
* Using the [activate](./configuration.md#activating-sqlframe) function to allow for continuing to use `pyspark.sql` but have it use SQLFrame behind the scenes.

### Import

If converting a PySpark pipeline, all `pyspark.sql` should be replaced with `sqlframe.bigquery`.
In addition, many classes will have a `BigQuery` prefix. 
For example, `BigQueryDataFrame` instead of `DataFrame`.


```python
# PySpark import
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.dataframe import DataFrame
# SQLFrame import
from sqlframe.bigquery import BigQuerySession
from sqlframe.bigquery import functions as F
from sqlframe.bigquery import BigQueryDataFrame
```

### Activate

If you would like to continue using `pyspark.sql` but have it use SQLFrame behind the scenes, you can use the [activate](./configuration.md#activating-sqlframe) function.

```python
from sqlframe import activate
activate("bigquery", config={"default_dataset": "sqlframe.db1"})

from pyspark.sql import SparkSession
```

`SparkSession` will now be a SQLFrame `BigQuerySession` object and everything will be run on BigQuery directly.

See [activate configuration](./configuration.md#activating-sqlframe) for information on how to pass in a connection and config options.

## Creating a Session

SQLFrame uses the [BigQuery DBAPI Connection](https://cloud.google.com/python/docs/reference/bigquery/latest/dbapi#class-googlecloudbigquerydbapiconnectionclientnone-bqstorageclientnone) to connect to BigQuery. 
A BigQuerySession, which implements the PySpark Session API, can be created by passing in a `google.cloud.bigquery.dbapi.Connection` object or by allowing SQLFrame to create a connection for you.
By default, SQLFrame will create a connection by inferring it from the environment (for example using gcloud auth).
Regardless of approach, it is recommended to configure `default_dataset` in the `BigQuerySession` constructor in order to make it easier to use the catalog methods (see example below).

=== "Import + Without Providing Connection"

    ```python
    from sqlframe.bigquery import BigQuerySession

    session = BigQuerySession(default_dataset="sqlframe.db1")
    ```

=== "Import + With Providing Connection"

    ```python
    import google.auth
    from google.api_core import client_info
    from google.oauth2 import service_account
    from google.cloud.bigquery.dbapi import connect
    from sqlframe.bigquery import BigQuerySession
    
    creds = service_account.Credentials.from_service_account_file("path/to/credentials.json")
    
    client = google.cloud.bigquery.Client(
        project="my-project",
        credentials=creds,
        location="us-central1",
        client_info=client_info.ClientInfo(user_agent="sqlframe"),
    )
    
    conn = connect(client=client)
    session = BigQuerySession(conn=conn, default_dataset="sqlframe.db1")
    ```

=== "Activate + Without Providing Connection"

    ```python
    from sqlframe import activate
    activate("bigquery", config={"default_dataset": "sqlframe.db1"})
     
    from pyspark.sql import SparkSession
    session = SparkSession.builder.getOrCreate()
    ```

=== "Activate + With Providing Connection"

    ```python
    import google.auth
    from google.api_core import client_info
    from google.oauth2 import service_account
    from google.cloud.bigquery.dbapi import connect
    from sqlframe import activate
    creds = service_account.Credentials.from_service_account_file("path/to/credentials.json")
    
    client = google.cloud.bigquery.Client(
        project="my-project",
        credentials=creds,
        location="us-central1",
        client_info=client_info.ClientInfo(user_agent="sqlframe"),
    )
    
    conn = connect(client=client)
    activate("bigquery", conn=conn, config={"default_dataset": "sqlframe.db1"})

    from pyspark.sql import SparkSession
    session = SparkSession.builder.getOrCreate()
    ```

## Using BigQuery Unique Functions

BigQuery may have a function that isn't represented within the PySpark API. 
If that is the case, you can call it directly using PySpark [call_function](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.call_function.html) function.

```python
from sqlframe.bigquery import BigQuerySession
from sqlframe.bigquery import functions as F

session = BigQuerySession(default_dataset="sqlframe.db1")
(
    session.table('"bigquery-public-data".samples.natality')
    .select(F.call_function("FARM_FINGERPRINT", F.col("source")).alias("source_hash"))
    .show()
)
```

## Example Usage

```python
from sqlframe.bigquery import BigQuerySession
from sqlframe.bigquery import functions as F
from sqlframe.bigquery import Window

session = BigQuerySession(default_dataset="sqlframe.db1")
table_path = '"bigquery-public-data".samples.natality'
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

## Supported PySpark API Methods

Find the full method lists, support notes, and SQLFrame-specific extensions in the
[BigQuery API reference](reference/bigquery.md).

- <span id="catalog-class"></span>[Catalog Class](reference/bigquery.md#catalog-class)
- <span id="column-class"></span>[Column Class](reference/bigquery.md#column-class)
- <span id="dataframe-class"></span>[DataFrame Class](reference/bigquery.md#dataframe-class)
- <span id="functions"></span>[Functions](reference/bigquery.md#functions)
- <span id="groupeddata-class"></span>[GroupedData Class](reference/bigquery.md#groupeddata-class)
- <span id="dataframereader-class"></span>[DataFrameReader Class](reference/bigquery.md#dataframereader-class)
- <span id="dataframewriter-class"></span>[DataFrameWriter Class](reference/bigquery.md#dataframewriter-class)
- <span id="sparksession-class"></span>[SparkSession Class](reference/bigquery.md#sparksession-class)
- <span id="datatypes"></span>[DataTypes](reference/bigquery.md#datatypes)
- <span id="window-class"></span>[Window Class](reference/bigquery.md#window-class)
- <span id="windowspec-class"></span>[WindowSpec Class](reference/bigquery.md#windowspec-class)

## Extra Functionality not Present in PySpark

SQLFrame supports the following extra functionality not in PySpark

### Table Class

SQLFrame provides a `Table` class that supports extra DML operations like `update`, `delete` and `merge`. This class is returned when using the `table` function from the `DataFrameReader` class.

```python
import google.auth
from google.api_core import client_info
from google.oauth2 import service_account
from google.cloud.bigquery.dbapi import connect
from sqlframe.bigquery import BigQuerySession
from sqlframe.base.table import WhenMatched, WhenNotMatched, WhenNotMatchedBySource

creds = service_account.Credentials.from_service_account_file("path/to/credentials.json")

client = google.cloud.bigquery.Client(
    project="my-project",
    credentials=creds,
    location="us-central1",
    client_info=client_info.ClientInfo(user_agent="sqlframe"),
)

conn = connect(client=client)
session = BigQuerySession(conn=conn, default_dataset="sqlframe.db1")

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

table_employee = session.table("employee")  # This object is of Type BigqueryTable
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


Some engines like `BigQuery` support an extra clause inside the `merge` statement which is `WHEN NOT MATCHED BY SOURCE THEN DELETE`.

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
| 5  |  Hugo   |   Reyes   |  29 |    3     |
| 6  | Sun-Hwa |   Kwon    |  27 |    5     |
+----+---------+-----------+-----+----------+
```
