from __future__ import annotations

import math
import os
import re
import typing as t
import warnings
from decimal import Decimal

import pytest
from _pytest.fixtures import FixtureRequest
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import SparkSession as PySparkSession
from pyspark.sql import types as PySparkTypes

from sqlframe.base.types import Row
from sqlframe.base.util import (
    dialect_to_string,
)
from sqlframe.base.util import (
    get_func_from_session as get_func_from_session_without_fallback,
)
from sqlframe.bigquery import types as BigQueryTypes
from sqlframe.bigquery.dataframe import BigQueryDataFrame
from sqlframe.bigquery.session import BigQuerySession
from sqlframe.databricks import types as DatabricksTypes
from sqlframe.databricks.dataframe import DatabricksDataFrame
from sqlframe.databricks.session import DatabricksSession
from sqlframe.duckdb import types as DuckDBTypes
from sqlframe.duckdb.dataframe import DuckDBDataFrame
from sqlframe.duckdb.session import DuckDBSession
from sqlframe.postgres import types as PostgresTypes
from sqlframe.postgres.dataframe import PostgresDataFrame
from sqlframe.postgres.session import PostgresSession
from sqlframe.redshift import types as RedshiftTypes
from sqlframe.redshift.dataframe import RedshiftDataFrame
from sqlframe.redshift.session import RedshiftSession
from sqlframe.snowflake import types as SnowflakeTypes
from sqlframe.snowflake.dataframe import SnowflakeDataFrame
from sqlframe.snowflake.session import SnowflakeSession
from sqlframe.spark import types as SparkTypes
from sqlframe.spark.dataframe import SparkDataFrame
from sqlframe.spark.session import SparkSession
from sqlframe.standalone import types as StandaloneTypes
from sqlframe.standalone.dataframe import StandaloneDataFrame
from sqlframe.standalone.session import StandaloneSession

if t.TYPE_CHECKING:
    from sqlframe.base.dataframe import BaseDataFrame
    from sqlframe.base.session import _BaseSession
    from tests.types import DistrictData, EmployeeData, StoreData

pytest_plugins = ["tests.common_fixtures"]


ENGINE_PARAMETERS = [
    pytest.param(
        "pyspark",
        marks=[
            pytest.mark.pyspark_session,
            pytest.mark.local,
        ],
    ),
    pytest.param(
        "standalone",
        marks=[
            pytest.mark.standalone,
            pytest.mark.local,
        ],
    ),
    pytest.param(
        "duckdb",
        marks=[
            pytest.mark.duckdb,
            pytest.mark.local,
        ],
    ),
    pytest.param(
        "postgres",
        marks=[
            pytest.mark.postgres,
            pytest.mark.local,
        ],
    ),
    pytest.param(
        "bigquery",
        marks=[
            pytest.mark.bigquery,
            pytest.mark.remote,
            # Set xdist group in order to serialize tests
            pytest.mark.xdist_group("bigquery_tests"),
        ],
    ),
    pytest.param(
        "snowflake",
        marks=[
            pytest.mark.snowflake,
            pytest.mark.remote,
            # Set xdist group in order to serialize tests
            pytest.mark.xdist_group("snowflake_tests"),
        ],
    ),
    pytest.param(
        "databricks",
        marks=[
            pytest.mark.databricks,
            pytest.mark.remote,
            # Set xdist group in order to serialize tests
            pytest.mark.xdist_group("databricks_tests"),
        ],
    ),
    pytest.param(
        "spark",
        marks=[
            pytest.mark.spark_session,
            pytest.mark.local,
        ],
    ),
    # pytest.param(
    #     "redshift",
    #     marks=[
    #         pytest.mark.redshift,
    #         pytest.mark.remote,
    #         # Set xdist group in order to serialize tests
    #         pytest.mark.xdist_group("redshift_tests"),
    #     ],
    # ),
]

ENGINE_PARAMETERS_NO_PYSPARK_STANDALONE = [
    x for x in ENGINE_PARAMETERS if "pyspark" not in x.values and "standalone" not in x.values
]

ENGINE_PARAMETERS_NO_STANDALONE = [x for x in ENGINE_PARAMETERS if "standalone" not in x.values]

ENGINE_PARAMETERS_NO_PYSPARK = [x for x in ENGINE_PARAMETERS if "pyspark" not in x.values]


@pytest.fixture(scope="module", autouse=True)
def filter_warnings():
    warnings.filterwarnings("ignore", category=ResourceWarning)


@pytest.fixture(scope="module")
def _employee_data() -> EmployeeData:
    return [
        (1, "Jack", "Shephard", 37, 1),
        (2, "John", "Locke", 65, 1),
        (3, "Kate", "Austen", 37, 2),
        (4, "Claire", "Littleton", 27, 2),
        (5, "Hugo", "Reyes", 29, 100),
    ]


@pytest.fixture(scope="module")
def _store_data() -> StoreData:
    return [
        (1, "Hydra", 1, 37),
        (2, "Arrow", 2, 2000),
    ]


@pytest.fixture(scope="module")
def _district_data() -> t.List[t.Tuple[int, str]]:
    return [
        (1, "Los Angeles"),
        (2, "Santa Monica"),
    ]


@pytest.fixture(scope="module")
def pyspark_employee(
    pyspark_session: PySparkSession, _employee_data: EmployeeData
) -> PySparkDataFrame:
    pyspark_employee_schema = PySparkTypes.StructType(
        [
            PySparkTypes.StructField("employee_id", PySparkTypes.IntegerType(), False),
            PySparkTypes.StructField("fname", PySparkTypes.StringType(), False),
            PySparkTypes.StructField("lname", PySparkTypes.StringType(), False),
            PySparkTypes.StructField("age", PySparkTypes.IntegerType(), False),
            PySparkTypes.StructField("store_id", PySparkTypes.IntegerType(), False),
        ]
    )
    df = pyspark_session.createDataFrame(data=_employee_data, schema=pyspark_employee_schema)
    df.createOrReplaceTempView("employee")
    return df


@pytest.fixture(scope="module")
def pyspark_store(pyspark_session: PySparkSession, _store_data: StoreData) -> PySparkDataFrame:
    spark_store_schema = PySparkTypes.StructType(
        [
            PySparkTypes.StructField("store_id", PySparkTypes.IntegerType(), False),
            PySparkTypes.StructField("store_name", PySparkTypes.StringType(), False),
            PySparkTypes.StructField("district_id", PySparkTypes.IntegerType(), False),
            PySparkTypes.StructField("num_sales", PySparkTypes.IntegerType(), False),
        ]
    )
    df = pyspark_session.createDataFrame(data=_store_data, schema=spark_store_schema)
    df.createOrReplaceTempView("store")
    return df


@pytest.fixture(scope="module")
def pyspark_district(
    pyspark_session: PySparkSession, _district_data: DistrictData
) -> PySparkDataFrame:
    spark_district_schema = PySparkTypes.StructType(
        [
            PySparkTypes.StructField("district_id", PySparkTypes.IntegerType(), False),
            PySparkTypes.StructField("district_name", PySparkTypes.StringType(), False),
        ]
    )
    df = pyspark_session.createDataFrame(data=_district_data, schema=spark_district_schema)
    df.createOrReplaceTempView("district")
    return df


@pytest.fixture(scope="function")
def standalone_store(
    standalone_session: StandaloneSession, _store_data: StoreData
) -> StandaloneDataFrame:
    sqlf_store_schema = StandaloneTypes.StructType(
        [
            StandaloneTypes.StructField("store_id", StandaloneTypes.IntegerType(), False),
            StandaloneTypes.StructField("store_name", StandaloneTypes.StringType(), False),
            StandaloneTypes.StructField("district_id", StandaloneTypes.IntegerType(), False),
            StandaloneTypes.StructField("num_sales", StandaloneTypes.IntegerType(), False),
        ]
    )
    df = standalone_session.createDataFrame(data=_store_data, schema=sqlf_store_schema)
    standalone_session.catalog.add_table("store", sqlf_store_schema)  # type: ignore
    return df


@pytest.fixture(scope="function")
def standalone_district(
    standalone_session: StandaloneSession, _district_data: DistrictData
) -> StandaloneDataFrame:
    sqlf_district_schema = StandaloneTypes.StructType(
        [
            StandaloneTypes.StructField("district_id", StandaloneTypes.IntegerType(), False),
            StandaloneTypes.StructField("district_name", StandaloneTypes.StringType(), False),
        ]
    )
    df = standalone_session.createDataFrame(data=_district_data, schema=sqlf_district_schema)
    standalone_session.catalog.add_table("district", sqlf_district_schema)  # type: ignore
    return df


@pytest.fixture(scope="function")
def duckdb_employee(duckdb_session: DuckDBSession, _employee_data: EmployeeData) -> DuckDBDataFrame:
    duckdb_employee_schema = DuckDBTypes.StructType(
        [
            DuckDBTypes.StructField("employee_id", DuckDBTypes.IntegerType(), False),
            DuckDBTypes.StructField("fname", DuckDBTypes.StringType(), False),
            DuckDBTypes.StructField("lname", DuckDBTypes.StringType(), False),
            DuckDBTypes.StructField("age", DuckDBTypes.IntegerType(), False),
            DuckDBTypes.StructField("store_id", DuckDBTypes.IntegerType(), False),
        ]
    )
    df = duckdb_session.createDataFrame(data=_employee_data, schema=duckdb_employee_schema)
    df.createOrReplaceTempView("employee")
    return df


@pytest.fixture(scope="function")
def duckdb_store(duckdb_session: DuckDBSession, _store_data: StoreData) -> DuckDBDataFrame:
    duckdb_store_schema = DuckDBTypes.StructType(
        [
            DuckDBTypes.StructField("store_id", DuckDBTypes.IntegerType(), False),
            DuckDBTypes.StructField("store_name", DuckDBTypes.StringType(), False),
            DuckDBTypes.StructField("district_id", DuckDBTypes.IntegerType(), False),
            DuckDBTypes.StructField("num_sales", DuckDBTypes.IntegerType(), False),
        ]
    )
    df = duckdb_session.createDataFrame(data=_store_data, schema=duckdb_store_schema)
    df.createOrReplaceTempView("store")
    return df


@pytest.fixture(scope="function")
def duckdb_district(duckdb_session: DuckDBSession, _district_data: DistrictData) -> DuckDBDataFrame:
    duckdb_district_schema = DuckDBTypes.StructType(
        [
            DuckDBTypes.StructField("district_id", DuckDBTypes.IntegerType(), False),
            DuckDBTypes.StructField("district_name", DuckDBTypes.StringType(), False),
        ]
    )
    df = duckdb_session.createDataFrame(data=_district_data, schema=duckdb_district_schema)
    df.createOrReplaceTempView("district")
    return df


@pytest.fixture
def postgres_employee(
    postgres_session: PostgresSession, _employee_data: EmployeeData
) -> PostgresDataFrame:
    postgres_employee_schema = PostgresTypes.StructType(
        [
            PostgresTypes.StructField("employee_id", PostgresTypes.IntegerType(), False),
            PostgresTypes.StructField("fname", PostgresTypes.StringType(), False),
            PostgresTypes.StructField("lname", PostgresTypes.StringType(), False),
            PostgresTypes.StructField("age", PostgresTypes.IntegerType(), False),
            PostgresTypes.StructField("store_id", PostgresTypes.IntegerType(), False),
        ]
    )
    df = postgres_session.createDataFrame(data=_employee_data, schema=postgres_employee_schema)
    df.createOrReplaceTempView("employee")
    return df


@pytest.fixture
def postgres_store(postgres_session: PostgresSession, _store_data: StoreData) -> PostgresDataFrame:
    postgres_store_schema = PostgresTypes.StructType(
        [
            PostgresTypes.StructField("store_id", PostgresTypes.IntegerType(), False),
            PostgresTypes.StructField("store_name", PostgresTypes.StringType(), False),
            PostgresTypes.StructField("district_id", PostgresTypes.IntegerType(), False),
            PostgresTypes.StructField("num_sales", PostgresTypes.IntegerType(), False),
        ]
    )
    df = postgres_session.createDataFrame(data=_store_data, schema=postgres_store_schema)
    df.createOrReplaceTempView("store")
    return df


@pytest.fixture
def postgres_district(
    postgres_session: PostgresSession, _district_data: DistrictData
) -> PostgresDataFrame:
    postgres_district_schema = PostgresTypes.StructType(
        [
            PostgresTypes.StructField("district_id", PostgresTypes.IntegerType(), False),
            PostgresTypes.StructField("district_name", PostgresTypes.StringType(), False),
        ]
    )
    df = postgres_session.createDataFrame(data=_district_data, schema=postgres_district_schema)
    df.createOrReplaceTempView("district")
    return df


@pytest.fixture(scope="function")
def spark_employee(spark_session: SparkSession, _employee_data: EmployeeData) -> SparkDataFrame:
    spark_employee_schema = SparkTypes.StructType(
        [
            SparkTypes.StructField("employee_id", SparkTypes.IntegerType(), False),
            SparkTypes.StructField("fname", SparkTypes.StringType(), False),
            SparkTypes.StructField("lname", SparkTypes.StringType(), False),
            SparkTypes.StructField("age", SparkTypes.IntegerType(), False),
            SparkTypes.StructField("store_id", SparkTypes.IntegerType(), False),
        ]
    )
    df = spark_session.createDataFrame(data=_employee_data, schema=spark_employee_schema)
    df.createOrReplaceTempView("employee")
    return df


@pytest.fixture(scope="function")
def spark_store(spark_session: SparkSession, _store_data: StoreData) -> SparkDataFrame:
    spark_store_schema = SparkTypes.StructType(
        [
            SparkTypes.StructField("store_id", SparkTypes.IntegerType(), False),
            SparkTypes.StructField("store_name", SparkTypes.StringType(), False),
            SparkTypes.StructField("district_id", SparkTypes.IntegerType(), False),
            SparkTypes.StructField("num_sales", SparkTypes.IntegerType(), False),
        ]
    )
    df = spark_session.createDataFrame(data=_store_data, schema=spark_store_schema)
    df.createOrReplaceTempView("store")
    return df


@pytest.fixture(scope="function")
def spark_district(spark_session: SparkSession, _district_data: DistrictData) -> SparkDataFrame:
    spark_district_schema = SparkTypes.StructType(
        [
            SparkTypes.StructField("district_id", SparkTypes.IntegerType(), False),
            SparkTypes.StructField("district_name", SparkTypes.StringType(), False),
        ]
    )
    df = spark_session.createDataFrame(data=_district_data, schema=spark_district_schema)
    df.createOrReplaceTempView("district")
    return df


@pytest.fixture
def bigquery_employee(
    bigquery_session: BigQuerySession, _employee_data: EmployeeData
) -> BigQueryDataFrame:
    bigquery_employee_schema = BigQueryTypes.StructType(
        [
            BigQueryTypes.StructField("employee_id", BigQueryTypes.IntegerType(), False),
            BigQueryTypes.StructField("fname", BigQueryTypes.StringType(), False),
            BigQueryTypes.StructField("lname", BigQueryTypes.StringType(), False),
            BigQueryTypes.StructField("age", BigQueryTypes.IntegerType(), False),
            BigQueryTypes.StructField("store_id", BigQueryTypes.IntegerType(), False),
        ]
    )
    df = bigquery_session.createDataFrame(data=_employee_data, schema=bigquery_employee_schema)
    df.createOrReplaceTempView("employee")
    return df


@pytest.fixture
def bigquery_store(bigquery_session: BigQuerySession, _store_data: StoreData) -> BigQueryDataFrame:
    bigquery_store_schema = BigQueryTypes.StructType(
        [
            BigQueryTypes.StructField("store_id", BigQueryTypes.IntegerType(), False),
            BigQueryTypes.StructField("store_name", BigQueryTypes.StringType(), False),
            BigQueryTypes.StructField("district_id", BigQueryTypes.IntegerType(), False),
            BigQueryTypes.StructField("num_sales", BigQueryTypes.IntegerType(), False),
        ]
    )
    df = bigquery_session.createDataFrame(data=_store_data, schema=bigquery_store_schema)
    df.createOrReplaceTempView("store")
    return df


@pytest.fixture
def bigquery_district(
    bigquery_session: BigQuerySession, _district_data: DistrictData
) -> BigQueryDataFrame:
    bigquery_district_schema = BigQueryTypes.StructType(
        [
            BigQueryTypes.StructField("district_id", BigQueryTypes.IntegerType(), False),
            BigQueryTypes.StructField("district_name", BigQueryTypes.StringType(), False),
        ]
    )
    df = bigquery_session.createDataFrame(data=_district_data, schema=bigquery_district_schema)
    df.createOrReplaceTempView("district")
    return df


@pytest.fixture
def redshift_employee(
    redshift_session: RedshiftSession, _employee_data: EmployeeData
) -> RedshiftDataFrame:
    redshift_employee_schema = RedshiftTypes.StructType(
        [
            RedshiftTypes.StructField("employee_id", RedshiftTypes.IntegerType(), False),
            RedshiftTypes.StructField("fname", RedshiftTypes.StringType(), False),
            RedshiftTypes.StructField("lname", RedshiftTypes.StringType(), False),
            RedshiftTypes.StructField("age", RedshiftTypes.IntegerType(), False),
            RedshiftTypes.StructField("store_id", RedshiftTypes.IntegerType(), False),
        ]
    )
    df = redshift_session.createDataFrame(data=_employee_data, schema=redshift_employee_schema)
    df.createOrReplaceTempView("employee")
    return df


@pytest.fixture
def redshift_store(redshift_session: RedshiftSession, _store_data: StoreData) -> RedshiftDataFrame:
    redshift_store_schema = RedshiftTypes.StructType(
        [
            RedshiftTypes.StructField("store_id", RedshiftTypes.IntegerType(), False),
            RedshiftTypes.StructField("store_name", RedshiftTypes.StringType(), False),
            RedshiftTypes.StructField("district_id", RedshiftTypes.IntegerType(), False),
            RedshiftTypes.StructField("num_sales", RedshiftTypes.IntegerType(), False),
        ]
    )
    df = redshift_session.createDataFrame(data=_store_data, schema=redshift_store_schema)
    df.createOrReplaceTempView("store")
    return df


@pytest.fixture
def redshift_district(
    redshift_session: RedshiftSession, _district_data: DistrictData
) -> RedshiftDataFrame:
    redshift_district_schema = RedshiftTypes.StructType(
        [
            RedshiftTypes.StructField("district_id", RedshiftTypes.IntegerType(), False),
            RedshiftTypes.StructField("district_name", RedshiftTypes.StringType(), False),
        ]
    )
    df = redshift_session.createDataFrame(data=_district_data, schema=redshift_district_schema)
    df.createOrReplaceTempView("district")
    return df


@pytest.fixture
def snowflake_employee(
    snowflake_session: SnowflakeSession, _employee_data: EmployeeData
) -> SnowflakeDataFrame:
    snowflake_employee_schema = SnowflakeTypes.StructType(
        [
            SnowflakeTypes.StructField("employee_id", SnowflakeTypes.IntegerType(), False),
            SnowflakeTypes.StructField("fname", SnowflakeTypes.StringType(), False),
            SnowflakeTypes.StructField("lname", SnowflakeTypes.StringType(), False),
            SnowflakeTypes.StructField("age", SnowflakeTypes.IntegerType(), False),
            SnowflakeTypes.StructField("store_id", SnowflakeTypes.IntegerType(), False),
        ]
    )
    df = snowflake_session.createDataFrame(data=_employee_data, schema=snowflake_employee_schema)
    df.createOrReplaceTempView("employee")
    return df


@pytest.fixture
def snowflake_store(
    snowflake_session: SnowflakeSession, _store_data: StoreData
) -> SnowflakeDataFrame:
    snowflake_store_schema = SnowflakeTypes.StructType(
        [
            SnowflakeTypes.StructField("store_id", SnowflakeTypes.IntegerType(), False),
            SnowflakeTypes.StructField("store_name", SnowflakeTypes.StringType(), False),
            SnowflakeTypes.StructField("district_id", SnowflakeTypes.IntegerType(), False),
            SnowflakeTypes.StructField("num_sales", SnowflakeTypes.IntegerType(), False),
        ]
    )
    df = snowflake_session.createDataFrame(data=_store_data, schema=snowflake_store_schema)
    df.createOrReplaceTempView("store")
    return df


@pytest.fixture
def snowflake_district(
    snowflake_session: SnowflakeSession, _district_data: DistrictData
) -> SnowflakeDataFrame:
    snowflake_district_schema = SnowflakeTypes.StructType(
        [
            SnowflakeTypes.StructField("district_id", SnowflakeTypes.IntegerType(), False),
            SnowflakeTypes.StructField("district_name", SnowflakeTypes.StringType(), False),
        ]
    )
    df = snowflake_session.createDataFrame(data=_district_data, schema=snowflake_district_schema)
    df.createOrReplaceTempView("district")
    return df


@pytest.fixture
def databricks_employee(
    databricks_session: DatabricksSession, _employee_data: EmployeeData
) -> DatabricksDataFrame:
    databricks_employee_schema = DatabricksTypes.StructType(
        [
            DatabricksTypes.StructField("employee_id", DatabricksTypes.IntegerType(), False),
            DatabricksTypes.StructField("fname", DatabricksTypes.StringType(), False),
            DatabricksTypes.StructField("lname", DatabricksTypes.StringType(), False),
            DatabricksTypes.StructField("age", DatabricksTypes.IntegerType(), False),
            DatabricksTypes.StructField("store_id", DatabricksTypes.IntegerType(), False),
        ]
    )
    df = databricks_session.createDataFrame(data=_employee_data, schema=databricks_employee_schema)
    df.createOrReplaceTempView("employee")
    return df


@pytest.fixture
def databricks_store(
    databricks_session: DatabricksSession, _store_data: StoreData
) -> DatabricksDataFrame:
    databricks_store_schema = DatabricksTypes.StructType(
        [
            DatabricksTypes.StructField("store_id", DatabricksTypes.IntegerType(), False),
            DatabricksTypes.StructField("store_name", DatabricksTypes.StringType(), False),
            DatabricksTypes.StructField("district_id", DatabricksTypes.IntegerType(), False),
            DatabricksTypes.StructField("num_sales", DatabricksTypes.IntegerType(), False),
        ]
    )
    df = databricks_session.createDataFrame(data=_store_data, schema=databricks_store_schema)
    df.createOrReplaceTempView("store")
    return df


@pytest.fixture
def databricks_district(
    databricks_session: DatabricksSession, _district_data: DistrictData
) -> DatabricksDataFrame:
    databricks_district_schema = DatabricksTypes.StructType(
        [
            DatabricksTypes.StructField("district_id", DatabricksTypes.IntegerType(), False),
            DatabricksTypes.StructField("district_name", DatabricksTypes.StringType(), False),
        ]
    )
    df = databricks_session.createDataFrame(data=_district_data, schema=databricks_district_schema)
    df.createOrReplaceTempView("district")
    return df


@pytest.fixture
def compare_frames(pyspark_session: PySparkSession) -> t.Callable:
    def _make_function(
        df: PySparkDataFrame,
        sqlf: StandaloneDataFrame,
        compare_schema: bool = True,
        no_empty: bool = True,
        sort: bool = False,
        optimize: t.Optional[bool] = None,
    ) -> t.Tuple[PySparkDataFrame, PySparkDataFrame]:
        from sqlframe.base.session import _BaseSession

        def compare_schemas(schema_1, schema_2):
            for schema in [schema_1, schema_2]:
                for struct_field in schema.fields:
                    struct_field.func_metadata = {}
            assert schema_1 == schema_2

        force_sort = sort
        if not sqlf.session._has_connection:
            for statement in sqlf.sql(pretty=False, optimize=False, as_list=True):
                sqlf_df = pyspark_session.sql(statement)
        else:
            # `schema` is not an implemented method
            compare_schema = False
            # We don't care if different engines produce different sorted results
            force_sort = True
            sqlf_df = sqlf  # type: ignore
        sqlf_df_results = sqlf_df.collect()  # type: ignore
        spark_df_results = df.collect()
        if isinstance(sqlf_df, _BaseSession) and sqlf_df.session.SANITIZE_COLUMN_NAMES:
            sanitized_spark_df_results = []
            for row in spark_df_results:
                sanitized_row = {
                    sqlf_df.session._sanitize_column_name(k): v for k, v in row.asDict().items()
                }
                sanitized_spark_df_results.append(Row(**sanitized_row))
            spark_df_results = sanitized_spark_df_results
        if compare_schema:
            compare_schemas(df.schema, sqlf_df.schema)
        if force_sort:
            spark_df_results = sorted(spark_df_results, key=lambda x: "_".join(map(str, x)))
            sqlf_df_results = sorted(sqlf_df_results, key=lambda x: "_".join(map(str, x)))
        assert len(spark_df_results) == len(sqlf_df_results)
        for spark_row, sqlf_row in zip(spark_df_results, sqlf_df_results):
            assert len(spark_row) == len(sqlf_row)
            for spark_value, sqlf_value in zip(spark_row, sqlf_row):
                if isinstance(spark_value, float):
                    assert math.isclose(spark_value, sqlf_value)
                elif isinstance(spark_value, Decimal):
                    assert math.isclose(spark_value, sqlf_value, abs_tol=10**-5)
                else:
                    assert spark_value == sqlf_value
        if no_empty:
            assert len(spark_df_results) != 0
            assert len(sqlf_df_results) != 0
        return df, sqlf_df

    return _make_function


@pytest.fixture
def get_explain_plan() -> t.Callable:
    def _make_function(df: PySparkDataFrame, mode: str = "extended") -> str:
        return df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), mode)  # type: ignore

    return _make_function


@pytest.fixture(params=ENGINE_PARAMETERS_NO_PYSPARK_STANDALONE)
def get_engine_df(request: FixtureRequest) -> t.Callable[[str], BaseDataFrame]:
    mapping = {
        "employee": f"{request.param}_employee",
        "store": f"{request.param}_store",
        "district": f"{request.param}_district",
    }

    def _get_engine_df(name: str) -> BaseDataFrame:
        return request.getfixturevalue(mapping[name])

    return _get_engine_df


@pytest.fixture(params=ENGINE_PARAMETERS_NO_PYSPARK_STANDALONE)
def get_session(request: FixtureRequest) -> t.Callable[[], _BaseSession]:
    def _get_session() -> _BaseSession:
        return request.getfixturevalue(f"{request.param}_session")

    return _get_session


@pytest.fixture(params=ENGINE_PARAMETERS_NO_PYSPARK)
def get_df(request: FixtureRequest) -> t.Callable[[str], BaseDataFrame]:
    mapping = {
        "employee": f"{request.param}_employee",
        "store": f"{request.param}_store",
        "district": f"{request.param}_district",
    }

    def _get_df(name: str) -> BaseDataFrame:
        return request.getfixturevalue(mapping[name])

    return _get_df


@pytest.fixture(params=ENGINE_PARAMETERS_NO_STANDALONE)
def get_engine_session_and_spark(
    request: FixtureRequest,
) -> t.Callable[[], t.Union[_BaseSession, PySparkSession]]:
    def _get_engine_session_and_spark() -> t.Union[_BaseSession, PySparkSession]:
        return request.getfixturevalue(f"{request.param}_session")

    return _get_engine_session_and_spark


@pytest.fixture(params=ENGINE_PARAMETERS_NO_STANDALONE)
def get_engine_df_and_pyspark(
    request: FixtureRequest,
) -> t.Callable[[str], t.Union[BaseDataFrame, PySparkDataFrame]]:
    mapping = {
        "employee": f"{request.param}_employee",
        "store": f"{request.param}_store",
        "district": f"{request.param}_district",
    }

    def _get_engine_df_and_pyspark(name: str) -> t.Union[BaseDataFrame, PySparkDataFrame]:
        return request.getfixturevalue(mapping[name])

    return _get_engine_df_and_pyspark


def get_func_from_session(name: str, session: t.Union[_BaseSession, PySparkSession]) -> t.Callable:
    return get_func_from_session_without_fallback(name, session, fallback=False)


@pytest.fixture
def get_func() -> t.Callable[[str, t.Union[_BaseSession, PySparkSession]], t.Callable]:
    def _get_func(name: str, session: t.Union[_BaseSession, PySparkSession]) -> t.Callable:
        try:
            return get_func_from_session(name, session)
        except AttributeError:
            if isinstance(session, PySparkSession):
                pytest.skip(f"pyspark does not support {name}")
            else:
                pytest.skip(f"{dialect_to_string(session.input_dialect)} does not support {name}")

    return _get_func


@pytest.fixture
def is_duckdb(request: FixtureRequest) -> t.Callable:
    def _is_duckdb() -> bool:
        return request.node.name.endswith("[duckdb]")

    return _is_duckdb


@pytest.fixture
def is_postgres(request: FixtureRequest) -> t.Callable:
    def _is_postgres() -> bool:
        return request.node.name.endswith("[postgres]")

    return _is_postgres


@pytest.fixture
def is_bigquery(request: FixtureRequest) -> t.Callable:
    def _is_bigquery() -> bool:
        return request.node.name.endswith("[bigquery]")

    return _is_bigquery


@pytest.fixture
def is_redshift(request: FixtureRequest) -> t.Callable:
    def _is_redshift() -> bool:
        return request.node.name.endswith("[redshift]")

    return _is_redshift


@pytest.fixture
def is_snowflake(request: FixtureRequest) -> t.Callable:
    def _is_snowflake() -> bool:
        return request.node.name.endswith("[snowflake]")

    return _is_snowflake


@pytest.fixture
def is_spark(request: FixtureRequest) -> t.Callable:
    def _is_spark() -> bool:
        return request.node.name.endswith("[spark]")

    return _is_spark


@pytest.fixture
def is_databricks(request: FixtureRequest) -> t.Callable:
    def _is_databricks() -> bool:
        return request.node.name.endswith("[databricks]")

    return _is_databricks


@pytest.fixture
def fixture_root_path(request: FixtureRequest) -> str:
    local_fixture_root_path = "."
    root_path_mapping = {
        "databricks": os.environ.get("DATABRICKS_ROOT_CLOUD_PATH", local_fixture_root_path)
    }
    match = re.search(r"\[(.*)]", request.node.name)
    engine = match.group(1) if match is not None else ""
    return root_path_mapping.get(engine, local_fixture_root_path)


@pytest.fixture
def tmp_root_path(request: FixtureRequest) -> str:
    local_tmp_root_path = ""
    root_path_mapping = {
        "databricks": os.environ.get("DATABRICKS_ROOT_CLOUD_PATH", local_tmp_root_path)
    }
    match = re.search(r"\[(.*)]", request.node.name)
    engine = match.group(1) if match is not None else ""
    return root_path_mapping.get(engine, local_tmp_root_path)
