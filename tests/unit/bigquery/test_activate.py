import pytest

from sqlframe import activate
from sqlframe.bigquery import (
    BigQueryCatalog,
    BigQueryDataFrame,
    BigQueryDataFrameNaFunctions,
    BigQueryDataFrameReader,
    BigQueryDataFrameStatFunctions,
    BigQueryDataFrameWriter,
    BigQueryGroupedData,
    BigQuerySession,
    BigQueryUDFRegistration,
)
from sqlframe.bigquery import Column as BigQueryColumn
from sqlframe.bigquery import Row as BigQueryRow
from sqlframe.bigquery import Window as BigQueryWindow
from sqlframe.bigquery import WindowSpec as BigQueryWindowSpec
from sqlframe.bigquery import functions as BigQueryF
from sqlframe.bigquery import types as BigQueryTypes


@pytest.mark.forked
def test_activate_bigquery(check_pyspark_imports):
    check_pyspark_imports(
        "bigquery",
        sqlf_session=BigQuerySession,
        sqlf_catalog=BigQueryCatalog,
        sqlf_column=BigQueryColumn,
        sqlf_dataframe=BigQueryDataFrame,
        sqlf_grouped_data=BigQueryGroupedData,
        sqlf_window=BigQueryWindow,
        sqlf_window_spec=BigQueryWindowSpec,
        sqlf_functions=BigQueryF,
        sqlf_types=BigQueryTypes,
        sqlf_udf_registration=BigQueryUDFRegistration,
        sqlf_dataframe_reader=BigQueryDataFrameReader,
        sqlf_dataframe_writer=BigQueryDataFrameWriter,
        sqlf_dataframe_na_functions=BigQueryDataFrameNaFunctions,
        sqlf_dataframe_stat_functions=BigQueryDataFrameStatFunctions,
        sqlf_row=BigQueryRow,
    )


@pytest.mark.forked
def test_activate_bigquery_default_dataset():
    activate("bigquery", config={"default_dataset": "sqlframe.sqlframe_test"})
    from pyspark.sql import SparkSession

    assert SparkSession == BigQuerySession
    spark = SparkSession.builder.appName("test").getOrCreate()
    assert spark.default_dataset == "sqlframe.sqlframe_test"
