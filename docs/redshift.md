# Redshift (In Development)

## Installation

```bash
pip install "sqlframe[redshift]"
```

## Enabling SQLFrame

SQLFrame can be used in two ways:

* Directly importing the `sqlframe.redshift` package 
* Using the [activate](./configuration.md#activating-sqlframe) function to allow for continuing to use `pyspark.sql` but have it use SQLFrame behind the scenes.

### Import

If converting a PySpark pipeline, all `pyspark.sql` should be replaced with `sqlframe.redshift`.
In addition, many classes will have a `Redshift` prefix. 
For example, `RedshiftDataFrame` instead of `DataFrame`.


```python
# PySpark import
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.dataframe import DataFrame
# SQLFrame import
from sqlframe.redshift import RedshiftSession
from sqlframe.redshift import functions as F
from sqlframe.redshift import RedshiftDataFrame
```

### Activate

If you would like to continue using `pyspark.sql` but have it use SQLFrame behind the scenes, you can use the [activate](./configuration.md#activating-sqlframe) function.

```python
import os

from redshift_connector import connect
from sqlframe import activate
conn = connect(
    user="user",
    password=os.environ["PASSWORD"],  # Replace this with how you get your password
    database="database",
    host="xxxxx.xxxxxx.region.redshift-serverless.amazonaws.com",
    port=5439,
)
activate("redshift", conn=conn)

from pyspark.sql import SparkSession
```

`SparkSession` will now be a SQLFrame `RedshiftSession` object and everything will be run on Redshift directly.

See [activate configuration](./configuration.md#activating-sqlframe) for information on how to pass in a connection and config options.

## Creating a Session

SQLFrame uses [Redshift DBAPI Python Connector](https://github.com/aws/amazon-redshift-python-driver) to connect to Redshift. 
A RedshiftSession, which implements the PySpark Session API, is created by passing in a `redshift_connector.Connection` object.

=== "Import"

    ```python
    import os
   
    from redshift_connector import connect
    from sqlframe.redshift import RedshiftSession
    
    conn = connect(
        user="user",
        password=os.environ["PASSWORD"],  # Replace this with how you get your password
        database="database",
        host="xxxxx.xxxxxx.region.redshift-serverless.amazonaws.com",
        port=5439,
    )
    session = RedshiftSession(conn=conn)
    ```

=== "Activate"

    ```python
    import os

    from redshift_connector import connect
    from sqlframe import activate

    conn = connect(
        user="user",
        password=os.environ["PASSWORD"],  # Replace this with how you get your password
        database="database",
        host="xxxxx.xxxxxx.region.redshift-serverless.amazonaws.com",
        port=5439,
    )
    activate("redshift", conn=conn)

    from pyspark.sql import SparkSession
    session = SparkSession.builder.getOrCreate()
    ```

## Example Usage

```python
import os

from redshift_connector import connect
from sqlframe import activate

conn = connect(
    user="user",
    password=os.environ["PASSWORD"],  # Replace this with how you get your password
    database="database",
    host="xxxxx.xxxxxx.region.redshift-serverless.amazonaws.com",
    port=5439,
)
activate("redshift", conn=conn)

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

## Extra Functionality not Present in PySpark

SQLFrame supports the following extra functionality not in PySpark

### Table Class

SQLFrame provides a `Table` class that supports extra DML operations like `update` and `delete`. This class is returned when using the `table` function from the `DataFrameReader` class.

```python
import os
   
from redshift_connector import connect
from sqlframe.redshift import RedshiftSession
    
conn = connect(
    user="user",
    password=os.environ["PASSWORD"],  # Replace this with how you get your password
    database="database",
    host="xxxxx.xxxxxx.region.redshift-serverless.amazonaws.com",
    port=5439,
)
session = RedshiftSession(conn=conn)

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

table_employee = session.table("employee")  # This object is of Type RedshiftTable
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
