# Postgres

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

See something that you would like to see supported? [Open an issue](https://github.com/eakmanrq/sqlframe/issues)!

### Catalog Class

* add_table
    * SQLFrame Specific: Adds a table to known schemas that SQLFrame tracks
* [currentCatalog](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.currentCatalog.html)
* [currentDatabase](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.currentDatabase.html)
* [databaseExists](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.databaseExists.html)
* [functionExists](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.functionExists.html)
* [getDatabase](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.getDatabase.html)
* [getFunction](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.getFunction.html)
* [getTable](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.getTable.html)
* get_columns
    * SQLFrame Specific: Similar to `listColumns` but returns SQLGlot expressions instead
* get_columns_from_schema
    * SQLFrame Specific: Gets the columns from the known schemas to SQLFrame
* [listCatalogs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.listCatalogs.html)
* [listColumns](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.listColumns.html)
* [listDatabases](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.listDatabases.html)
* [listFunctions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.listFunctions.html)
* [listTables](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.listTables.html)
* [setCurrentCatalog](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.setCurrentCatalog.html)
* [setCurrentDatabase](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.setCurrentDatabase.html)
* [tableExists](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.tableExists.html)

### Column Class

* [alias](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.alias.html)
* [alias](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.alias.html)
* [asc](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.asc.html)
* [asc_nulls_first](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.asc_nulls_first.html)
* [asc_nulls_last](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.asc_nulls_last.html)
* [between](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.between.html)
* [cast](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.cast.html)
* [desc](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.desc.html)
* [desc_nulls_first](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.desc_nulls_first.html)
* [desc_nulls_last](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.desc_nulls_last.html)
* [endswith](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.endswith.html)
* [ilike](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.ilike.html)
* [isNotNull](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.isNotNull.html)
* [isNull](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.isNull.html)
* [isin](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.isin.html)
* [like](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.like.html)
* [otherwise](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.otherwise.html)
* [over](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.over.html)
* [rlike](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.rlike.html)
* sql 
    * SQLFrame Specific: Get the SQL representation of a given column
* [startswith](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.startswith.html)
* [substr](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.substr.html)
* [when](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.when.html)

### DataFrame Class

* [agg](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.agg.html)
* [alias](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.alias.html)
* [approxQuantile](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.approxQuantile.html)
* [cache](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.cache.html)
* [coalesce](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.coalesce.html)
* [collect](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.collect.html)
* [columns](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.columns.html)
* [copy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.copy.html)
* [corr](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.corr.html)
* [count](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.count.html)
* [cov](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.cov.html)
* [createOrReplaceTempView](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.createOrReplaceTempView.html)
* [crossJoin](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.crossJoin.html)
* [cube](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.cube.html)
* [distinct](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.distinct.html)
* [drop](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.drop.html)
* [dropDuplicates](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicates.html)
* [drop_duplicates](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.drop_duplicates.html)
* [dropna](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropna.html)
* [exceptAll](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.exceptAll.html)
* [explain](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html)
* [fillna](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.fillna.html)
* [filter](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html)
* [first](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.first.html)
* [groupBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)
* [groupby](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupby.html)
* [head](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.head.html)
* [intersect](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.intersect.html)
* [intersectAll](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.intersectAll.html)
* [join](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html)
* [limit](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.limit.html)
* lineage
   * Get lineage for a specific column. [Returns a SQLGlot Node](https://sqlglot.com/sqlglot/lineage.html#Node). Can be used to get lineage SQL or HTML representation.
* [na](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.na.html)
* [orderBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.orderBy.html)
* [persist](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.persist.html)
* [printSchema](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.printSchema.html)
* [replace](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.replace.html)
* [schema](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.schema.html)
* [select](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.select.html)
* [show](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.show.html)
    * Vertical Argument is not Supported
* [sort](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sort.html)
* sql
    * SQLFrame Specific: Get the SQL representation of a given DataFrame
* [stat](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.stat.html)
* [toDF](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toDF.html)
* [toPandas](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toPandas.html)
* [union](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.union.html)
* [unionAll](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unionAll.html)
* [unionByName](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unionByName.html)
* [unpivot](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unpivot.html)
* [where](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.where.html)
* [withColumn](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn.html)
* [withColumnRenamed](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumnRenamed.html)
* [write](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.write.html)

### Functions

* [abs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.abs.html)
* [acos](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.acos.html)
* [acosh](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.acosh.html)
* [add_months](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.add_months.html)
* [any_value](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.any_value.html)
  * Returns the max value and ignore nulls is not supported
* [array](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array.html)
* [array_contains](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_contains.html)
* [array_join](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_join.html)
* [array_max](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_max.html)
* [array_min](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_min.html)
* [array_position](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_position.html)
* [array_remove](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_remove.html)
* [array_size](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_size.html)
* [arrays_overlap](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.arrays_overlap.html)
* [asc](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.asc.html)
* [asc_nulls_first](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.asc_nulls_first.html)
* [asc_nulls_last](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.asc_nulls_last.html)
* [ascii](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ascii.html)
* [asin](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.asin.html)
* [asinh](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.asinh.html)
* [atan](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.atan.html)
* [atan2](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.atan2.html)
* [atanh](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.atanh.html)
* [avg](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.avg.html)
* [base64](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.base64.html)
* [bit_length](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bit_length.html)
* [bitwiseNOT](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bitwiseNOT.html)
* [bitwise_not](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bitwise_not.html)
* [bool_and](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bool_and.html)
* [bool_or](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bool_or.html)
* [btrim](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.btrim.html)
* [call_function](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.call_function.html)
* [cbrt](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.cbrt.html)
* [ceil](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ceil.html)
* [ceiling](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ceiling.html)
* [char](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.char.html)
* [char_length](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.char_length.html)
* [character_length](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.character_length.html)
* [coalesce](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.coalesce.html)
* [col](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.col.html)
* [collect_list](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.collect_list.html)
* [collect_set](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.collect_set.html)
* [concat](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.concat.html)
    * Only works on strings (does not work on arrays)
* [concat_ws](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.concat_ws.html)
* [corr](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.corr.html)
* [cos](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.cos.html)
* [cosh](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.cosh.html)
* [cot](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.cot.html)
* [count](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.count.html)
* [countDistinct](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.countDistinct.html)
* [count_distinct](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.count_distinct.html)
* [count_if](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.count_if.html)
* [covar_pop](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.covar_pop.html)
* [covar_samp](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.covar_samp.html)
* [cume_dist](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.cume_dist.html)
* [current_date](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.current_date.html)
* [current_timestamp](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.current_timestamp.html)
* [current_user](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.current_user.html)
* [date_add](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_add.html)
* [dateadd](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dateadd.html)
* [date_diff](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_diff.html)
* [datediff](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.datediff.html)
* [date_format](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_format.html)
* [date_sub](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_sub.html)
* [date_trunc](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_trunc.html)
    * Rounded whole number is returned
* [dayofmonth](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dayofmonth.html)
* [dayofweek](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dayofweek.html)
* [dayofyear](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dayofyear.html)
* [decode](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.decode.html)
* [degrees](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.degrees.html)
* [dense_rank](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dense_rank.html)
* [desc](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.desc.html)
* [desc_nulls_first](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.desc_nulls_first.html)
* [desc_nulls_last](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.desc_nulls_last.html)
* [e](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.e.html)
* [element_at](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.element_at.html)
    * Only works on strings (does not work on arrays)
* [encode](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.encode.html)
* [endswith](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.endswith.html)
* [exp](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.exp.html)
* [explode](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode.html)
    * Doesn't support exploding maps
* [expm1](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.expm1.html)
* [expr](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.expr.html)
* [extract](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.extract.html)
* [factorial](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.factorial.html)
* [floor](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.floor.html)
* [format_number](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.format_number.html)
* [format_string](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.format_string.html)
* [from_unixtime](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_unixtime.html)
* [get_json_object](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.get_json_object.html)
* [greatest](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.greatest.html)
* [hour](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.hour.html)
* [initcap](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.initcap.html)
* [input_file_name](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.input_file_name.html)
* [instr](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.instr.html)
* [isnan](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.isnan.html)
* [isnull](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.isnull.html)
* [lag](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lag.html)
* [last_day](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.last_day.html)
* [lcase](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lcase.html)
* [lead](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lead.html)
* [least](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.least.html)
* [left](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.left.html)
* [length](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.length.html)
* [levenshtein](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.levenshtein.html)
* [lit](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lit.html)
* [ln](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ln.html)
* [locate](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.locate.html)
* [log](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.log.html)
* [log10](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.log10.html)
* [log1p](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.log1p.html)
* [log2](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.log2.html)
* [lower](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lower.html)
* [lpad](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lpad.html)
* [ltrim](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ltrim.html)
* [make_date](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.make_date.html)
* [max](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.max.html)
* [md5](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.md5.html)
* [mean](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.mean.html)
* [min](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.min.html)
* [minute](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.minute.html)
* [month](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.month.html)
* [months_between](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.months_between.html)
* [nanvl](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.nanvl.html)
* [nth_value](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.nth_value.html)
* [ntile](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ntile.html)
* [octet_length](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.octet_length.html)
* [overlay](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.overlay.html)
* [percent_rank](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percent_rank.html)
* [percentile](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percentile.html)
* [position](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.position.html)
* [pow](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pow.html)
* [quarter](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.quarter.html)
* [radians](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.radians.html)
* [rand](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rand.html)
* [rank](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rank.html)
* [regexp](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp.html)
* [regexp_like](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_like.html)
* [regexp_replace](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_replace.html)
* [repeat](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.repeat.html)
* [replace](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.replace.html)
* [reverse](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.reverse.html)
    * Only works on strings (does not work on arrays)
* [right](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.right.html)
* [rint](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rint.html)
* [rlike](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rlike.html)
* [round](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.round.html)
* [row_number](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.row_number.html)
* [rpad](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rpad.html)
* [rtrim](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rtrim.html)
* [second](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.second.html)
* [shiftLeft](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.shiftLeft.html)
* [shiftRight](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.shiftRight.html)
* [shiftleft](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.shiftleft.html)
* [shiftright](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.shiftright.html)
* [sign](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sign.html)
* [signum](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.signum.html)
* [sin](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sin.html)
* [sinh](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sinh.html)
* [size](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.size.html)
* [slice](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.slice.html)
* [soundex](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.soundex.html)
* [split](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.split.html)
* [sqrt](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sqrt.html)
* [stddev](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.stddev.html)
* [stddev_pop](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.stddev_pop.html)
* [stddev_samp](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.stddev_samp.html)
* [substring](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.substring.html)
* [sum](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sum.html)
* [sumDistinct](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sumDistinct.html)
* [sum_distinct](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sum_distinct.html)
* [tan](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.tan.html)
* [tanh](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.tanh.html)
* [timestamp_seconds](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.timestamp_seconds.html)
* [toDegrees](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.toDegrees.html)
* [toRadians](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.toRadians.html)
* [to_date](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_date.html)
* [to_number](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_number.html)
* [to_timestamp](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp.html)
* [to_timestamp_ntz](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp_ntz.html)
* [translate](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.translate.html)
* [trim](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.trim.html)
* [trunc](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.trunc.html)
* [try_element_at](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_element_at.html)
  * Negative index returns null and cannot lookup elements in maps
* [try_to_timestamp](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_to_timestamp.html)
* [typeof](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.typeof.html)
* [ucase](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.ucase.html)
* [unbase64](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.unbase64.html)
* [unix_micros](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.unix_micros.html)
* [unix_millis](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.unix_millis.html)
* [unix_seconds](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.unix_seconds.html)
* [unix_timestamp](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.unix_timestamp.html)
* [upper](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.upper.html)
* [var_pop](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.var_pop.html)
* [var_samp](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.var_samp.html)
* [variance](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.variance.html)
* [weekofyear](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.weekofyear.html)
* [when](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.when.html)
* [year](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.year.html)* 

### GroupedData Class

* [agg](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.agg.html)
* [avg](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.avg.html)
* [count](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.count.html)
* [max](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.max.html)
* [mean](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.mean.html)
* [min](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.min.html)
* [pivot](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.pivot.html)
* [sum](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.sum.html)

### DataFrameReader Class

* [csv](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.csv.html)
* [json](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.json.html)
* [load](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.load.html)
* [parquet](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.parquet.html)
* [table](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.table.html)

### DataFrameWriter Class

* [csv](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.csv.html)
* [insertInto](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.insertInto.html)
* [json](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.json.html)
* [mode](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.mode.html)
* [parquet](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.parquet.html)
* [saveAsTable](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.saveAsTable.html)
* sql
     * SQLFrame Specific: Get the SQL representation of the DataFrame

### SparkSession Class

* [builder](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.builder.html)
* [catalog](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.catalog.html)
* [createDataFrame](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.createDataFrame.html)
* [range](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.range.html)
* [read](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.read.html)
* [sql](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.sql.html)
* [table](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.table.html)

### DataTypes

* [ArrayType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.ArrayType.html)
* [BinaryType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.BinaryType.html)
* [BooleanType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.BooleanType.html)
* [ByteType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.ByteType.html)
* [CharType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.CharType.html)
* [DataType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.DataType.html)
* [DateType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.DateType.html)
* [DecimalType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.DecimalType.html)
* [DoubleType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.DoubleType.html)
* [FloatType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.FloatType.html)
* [IntegerType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.IntegerType.html)
* [LongType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.LongType.html)
* [Row](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/row.html)
* [ShortType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.ShortType.html)
* [StringType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.StringType.html)
* [StructField](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.StructField.html)
* [StructType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.StructType.html)
* [TimestampNTZType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.TimestampNTZType.html)
* [TimestampType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.TimestampType.html)
* [VarcharType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.VarcharType.html)

### Window Class

* [currentRow](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.currentRow.html)
* [orderBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.orderBy.html)
* [partitionBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.partitionBy.html)
* [rangeBetween](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.rangeBetween.html)
* [rowsBetween](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.rowsBetween.html)
* [unboundedFollowing](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.unboundedFollowing.html)
* [unboundedPreceding](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.unboundedPreceding.html)

### WindowSpec Class

* [orderBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.WindowSpec.orderBy.html)
* [partitionBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.WindowSpec.partitionBy.html)
* [rangeBetween](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.WindowSpec.rangeBetween.html)
* [rowsBetween](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.WindowSpec.rowsBetween.html)
* sql
    * SQLFrame Specific: Get the SQL representation of the WindowSpec

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
