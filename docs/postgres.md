# Postgres

Run familiar DataFrame transformations directly on your Postgres database.

## Installation

```bash
pip install "sqlframe[postgres]"
```

## Enabling SQLFrame

SQLFrame can be used in two ways:

* Directly importing the `sqlframe.postgres` package 
* Using the [activate](./configuration.md#activating-sqlframe) function to allow for continuing to use `pyspark.sql` but have it use SQLFrame behind the scenes.

### Import

If converting a PySpark pipeline, all `pyspark.sql` should be replaced with `sqlframe.postgres`.
In addition, many classes will have a `Postgres` prefix. 
For example, `PostgresDataFrame` instead of `DataFrame`.


```python
# PySpark import
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.dataframe import DataFrame
# SQLFrame import
from sqlframe.postgres import PostgresSession
from sqlframe.postgres import functions as F
from sqlframe.postgres import PostgresDataFrame
```

### Activate

If you would like to continue using `pyspark.sql` but have it use SQLFrame behind the scenes, you can use the [activate](./configuration.md#activating-sqlframe) function.

```python
from psycopg2 import connect
from sqlframe import activate
conn = connect(
    dbname="postgres",
    user="postgres",
    password="password",
    host="localhost",
    port="5432",
)
activate("postgres", conn=conn)

from pyspark.sql import SparkSession
```

`SparkSession` will now be a SQLFrame `PostgresSession` object and everything will be run on Postgres directly.

See [activate configuration](./configuration.md#activating-sqlframe) for information on how to pass in a connection and config options.

## Creating a Session

SQLFrame uses the `psycopg2` package to connect to Postgres. 
A PostgresSession, which implements the PySpark Session API, is created by passing in a `psycopg2.Connection` object.

=== "Import"

    ```python
    from psycopg2 import connect
    from sqlframe.postgres import PostgresSession
    
    conn = connect(
        dbname="postgres",
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
    )
    session = PostgresSession(conn=conn)
    ```

=== "Activate"

    ```python
    from sqlframe import activate

    conn = connect(
        dbname="postgres",
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
    )
    activate("postgres", conn=conn)

    from pyspark.sql import SparkSession
    session = SparkSession.builder.getOrCreate()
    ```


## Using Postgres Unique Functions

Postgres may have a function that isn't represented within the PySpark API. 
If that is the case, you can call it directly using PySpark [call_function](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.call_function.html) function.

```python
from psycopg2 import connect
from sqlframe.postgres import PostgresSession
from sqlframe.postgres import functions as F

conn = connect(
    dbname="postgres",
    user="postgres",
    password="password",
    host="localhost",
    port="5432",
)
session = PostgresSession(conn=conn)
(
    session.table("example.table")
    .select(F.call_function("PG_DATABASE_SIZE", F.lit("some_database")).alias("database_size"))
    .show()
)
```

## Example Usage

```python
from psycopg2 import connect
from sqlframe.postgres import functions as F
from sqlframe.postgres import PostgresSession

conn = connect(
    dbname="postgres",
    user="postgres",
    password="password",
    host="localhost",
    port="5432",
)
session = PostgresSession(conn=conn)

df_employee = session.createDataFrame(
    [
        {"id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1},
        {"id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 2},
        {"id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 3},
        {"id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 1},
        {"id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 3},
    ]
)
df_store = session.createDataFrame(
    [
        {"store_id": 1, "store_name": "The Hatch"},
        {"store_id": 2, "store_name": "The Pearl"},
        {"store_id": 3, "store_name": "The Swan"},
    ]
)

(
    df_employee
    .join(df_store, on="store_id")
    .groupBy("store_name")
    .agg(F.count("*").alias("total_employees"))
    .show()
)
```

## Supported PySpark API Methods

Find the full method lists, support notes, and SQLFrame-specific extensions in the
[Postgres API reference](reference/postgres.md).

- <span id="catalog-class"></span>[Catalog Class](reference/postgres.md#catalog-class)
- <span id="column-class"></span>[Column Class](reference/postgres.md#column-class)
- <span id="dataframe-class"></span>[DataFrame Class](reference/postgres.md#dataframe-class)
- <span id="functions"></span>[Functions](reference/postgres.md#functions)
- <span id="groupeddata-class"></span>[GroupedData Class](reference/postgres.md#groupeddata-class)
- <span id="dataframereader-class"></span>[DataFrameReader Class](reference/postgres.md#dataframereader-class)
- <span id="dataframewriter-class"></span>[DataFrameWriter Class](reference/postgres.md#dataframewriter-class)
- <span id="sparksession-class"></span>[SparkSession Class](reference/postgres.md#sparksession-class)
- <span id="datatypes"></span>[DataTypes](reference/postgres.md#datatypes)
- <span id="window-class"></span>[Window Class](reference/postgres.md#window-class)
- <span id="windowspec-class"></span>[WindowSpec Class](reference/postgres.md#windowspec-class)

## Extra Functionality not Present in PySpark

SQLFrame supports the following extra functionality not in PySpark

### Table Class

SQLFrame provides a `Table` class that supports extra DML operations like `update`, `delete` and `merge`. This class is returned when using the `table` function from the `DataFrameReader` class.

```python
from psycopg2 import connect
from sqlframe.postgres import PostgresSession
from sqlframe.base.table import WhenMatched, WhenNotMatched, WhenNotMatchedBySource

conn = connect(
    dbname="postgres",
    user="postgres",
    password="password",
    host="localhost",
    port="5432",
)
session = PostgresSession(conn=conn)

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

table_employee = session.table("employee")  # This object is of Type PostgresTable
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


Some engines like `Postgres` support an extra clause inside the `merge` statement which is `WHEN NOT MATCHED BY SOURCE THEN DELETE`.

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
