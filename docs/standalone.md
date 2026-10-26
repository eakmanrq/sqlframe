# Standalone

Generate SQL without connecting to a database. Provide table schemas explicitly so SQLFrame can resolve columns and types. To execute a query, use an engine-specific session.

## Installation

```bash
pip install sqlframe
```

To read local Parquet files, install the optional Parquet dependency:

```bash
pip install "sqlframe[standalone]"
```

`session.read.parquet(path)` and `session.read.load(path)` load the
file into memory and include its rows in the generated SQL. Standalone mode does not
execute the SQL; use an engine-specific session for large files or query execution.

## Enabling SQLFrame

SQLFrame can be used in two ways:

* Directly importing the `sqlframe.standalone` package 
* Using the [activate](./configuration.md#activating-sqlframe) function to allow for continuing to use `pyspark.sql` but have it use SQLFrame behind the scenes.

### Import

If converting a PySpark pipeline, all `pyspark.sql` should be replaced with `sqlframe.standalone`.
In addition, many classes will have a `Standalone` prefix. 
For example, `StandaloneDataFrame` instead of `DataFrame`.


```python
# PySpark import
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.dataframe import DataFrame
# SQLFrame import
from sqlframe.standalone import StandaloneSession
from sqlframe.standalone import functions as F
from sqlframe.standalone import StandaloneDataFrame
```

### Activate

If you would like to continue using `pyspark.sql` but have it use SQLFrame behind the scenes, you can use the [activate](./configuration.md#activating-sqlframe) function.

```python
from sqlframe import activate
activate("standalone")

from pyspark.sql import SparkSession
```

## Creating a Session

Standalone supports defining both an `input_dialect` and an `output_dialect` which can be different from each other.
`input_dialect` is the dialect used when using the DataFrame API and `output_dialect` is the dialect used when generating the SQL query.
For example if you want Snowflake behavior of converting lowercase unquoted columns to uppercase, then you would set `input_dialect` to `snowflake`. 
If you plan on running the query against BigQuery, then you would set `output_dialect` to `bigquery`.
Default is `spark` for both input and output dialects.

=== "Import"

    ```python
    from sqlframe.standalone import StandaloneSession
    
    session = StandaloneSession.builder.config(map={"sqlframe.input.dialect": 'duckdb', "sqlframe.output.dialect": 'bigquery'}).getOrCreate()
    ```

=== "Activate"

    ```python
    from sqlframe import activate
    activate("standalone", config={"sqlframe.input.dialect": 'duckdb', "sqlframe.output.dialect": 'duckdb'})
    
    from pyspark.sql import SparkSession
    session = SparkSession.builder.getOrCreate()
    ```

## Accessing Tables

PySpark DataFrame API, and currently SQLFrame, requires that a table can be access to get it's schema information.
Other engine-specific SQLFrame Sessions, like DuckDBSession or BigQuerySession, do this at runtime automatically.
However, in Standalone mode, you need to provide the schema information explicitly since it does not have this connection information.

```python
from sqlframe.standalone import StandaloneSession

session = StandaloneSession()

session.catalog.add_table("test_table", column_mapping={"a": "int", "b": "int", "c": "int"})
df = session.read.table("test_table")
df.sql()
```

Output:
```sql
"""
SELECT
  `test_table`.`a` AS `a`,
  `test_table`.`b` AS `b`,
  `test_table`.`c` AS `c`
FROM `test_table` AS `test_table`
"""
```

## Supported PySpark API Methods

Find the full method lists, support notes, and SQLFrame-specific extensions in the
[Standalone API reference](reference/standalone.md).

- <span id="column-class"></span>[Column Class](reference/standalone.md#column-class)
- <span id="dataframe-class"></span>[DataFrame Class](reference/standalone.md#dataframe-class)
- <span id="functions"></span>[Functions](reference/standalone.md#functions)
- <span id="groupeddata-class"></span>[GroupedData Class](reference/standalone.md#groupeddata-class)
- <span id="sparksession-class"></span>[SparkSession Class](reference/standalone.md#sparksession-class)
- <span id="datatypes"></span>[DataTypes](reference/standalone.md#datatypes)
- <span id="window-class"></span>[Window Class](reference/standalone.md#window-class)
- <span id="windowspec-class"></span>[WindowSpec Class](reference/standalone.md#windowspec-class)
