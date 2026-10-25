# Get started with DuckDB {#duckdb}

Run PySpark-style transformations locally with an in-memory database or your existing DuckDB connection. No credentials needed.

## 1. Install SQLFrame {#installation}

```bash
pip install "sqlframe[duckdb]"
```

## 2. Create a session {#creating-a-session}

Start in memory, connect to a database file, or keep your existing PySpark imports.

=== "In memory"

    ```python
    from sqlframe.duckdb import DuckDBSession

    session = DuckDBSession()
    ```

=== "Database file"

    ```python
    import duckdb
    from sqlframe.duckdb import DuckDBSession

    connection = duckdb.connect("analytics.duckdb")
    session = DuckDBSession(conn=connection)
    ```

=== "Existing connection"

    ```python
    import duckdb
    from sqlframe.duckdb import DuckDBSession

    connection = duckdb.connect(database=":memory:")
    session = DuckDBSession(conn=connection)
    ```

=== "PySpark imports"

    ```python
    from sqlframe import activate

    activate("duckdb")

    from pyspark.sql import SparkSession

    session = SparkSession.builder.getOrCreate()
    ```

!!! tip "Start with an in-memory database"
    The default session uses an in-memory DuckDB database. Pass a connection to keep data in a DuckDB file. See [activation configuration](configuration.md#activating-sqlframe) to supply a connection when keeping PySpark imports.

## 3. Run your first transformation {#first-transformation}

Create a small DataFrame, group by country, and calculate total revenue.

```python
df = session.createDataFrame(
    [("US", 120), ("UK", 80), ("US", 30)],
    ["country", "revenue"],
)
df.groupBy("country").sum("revenue").orderBy("country").show()
```

```text title="Output"
+---------+--------------+
| country | sum(revenue) |
+---------+--------------+
|    UK   |      80      |
|    US   |     150      |
+---------+--------------+
```

## Enabling SQLFrame

SQLFrame supports direct imports and activation of existing `pyspark.sql` imports.

### Direct imports {#import}

When converting a PySpark pipeline, replace `pyspark.sql` imports with `sqlframe.duckdb`. Some classes use a `DuckDB` prefix:

```python
from sqlframe.duckdb import DuckDBSession, DuckDBDataFrame
from sqlframe.duckdb import functions as F
```

### Keep your PySpark imports {#activate}

Use the **PySpark imports** tab above to activate SQLFrame before importing PySpark. `SparkSession` then refers to SQLFrame's `DuckDBSession`, and supported transformations run directly on DuckDB.

See the [activation guide](configuration.md#activating-sqlframe) for connection options, configuration, and scoped activation.

### Session time zone

`DuckDBSession.conf` supports `spark.sql.session.timeZone` for reading and setting the
DuckDB connection's time zone. The initial value comes from DuckDB, including any
setting on a supplied connection.

```python
session = (
    DuckDBSession.builder
    .config("spark.sql.session.timeZone", "Europe/Amsterdam")
    .getOrCreate()
)

time_zone = session.conf.get("spark.sql.session.timeZone")
session.conf.set("spark.sql.session.timeZone", "UTC")
```

Other Spark session configuration keys are not mapped to DuckDB by `conf`.

## Using DuckDB Unique Functions

DuckDB may have a function that isn't represented within the PySpark API. 
If that is the case, you can call it directly using PySpark [call_function](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.call_function.html) function.

```python
from sqlframe.duckdb import DuckDBSession
from sqlframe.duckdb import functions as F

session = DuckDBSession()
(
    session.table("example.table")
    .select(F.call_function("CURRENT_SETTING", F.lit("access_mode")).alias("access_mode_value"))
    .show()
)
```

## Example Usage

```python
from sqlframe.duckdb import DuckDBSession
from sqlframe.duckdb import functions as F

session = DuckDBSession()

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
[DuckDB API reference](reference/duckdb.md).

- <span id="catalog-class"></span>[Catalog Class](reference/duckdb.md#catalog-class)
- <span id="column-class"></span>[Column Class](reference/duckdb.md#column-class)
- <span id="dataframe-class"></span>[DataFrame Class](reference/duckdb.md#dataframe-class)
- <span id="functions"></span>[Functions](reference/duckdb.md#functions)
- <span id="groupeddata-class"></span>[GroupedData Class](reference/duckdb.md#groupeddata-class)
- <span id="dataframereader-class"></span>[DataFrameReader Class](reference/duckdb.md#dataframereader-class)
- <span id="dataframewriter-class"></span>[DataFrameWriter Class](reference/duckdb.md#dataframewriter-class)
- <span id="sparksession-class"></span>[SparkSession Class](reference/duckdb.md#sparksession-class)
- <span id="datatypes"></span>[DataTypes](reference/duckdb.md#datatypes)
- <span id="window-class"></span>[Window Class](reference/duckdb.md#window-class)
- <span id="windowspec-class"></span>[WindowSpec Class](reference/duckdb.md#windowspec-class)

## Extra Functionality not Present in PySpark

SQLFrame supports the following extra functionality not in PySpark

### Table Class

SQLFrame provides a `Table` class that supports extra DML operations like `update` and `delete`. This class is returned when using the `table` function from the `DataFrameReader` class.

```python
import duckdb
from sqlframe.duckdb import DuckDBSession
    
conn = duckdb.connect(database=":memory:")
session = DuckDBSession(conn=conn)

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

table_employee = session.table("employee")  # This object is of Type DuckDBTable
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
