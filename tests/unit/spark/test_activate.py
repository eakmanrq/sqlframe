import pytest

from sqlframe.spark import Column as SparkColumn
from sqlframe.spark import Row as SparkRow
from sqlframe.spark import (
    SparkCatalog,
    SparkDataFrame,
    SparkDataFrameNaFunctions,
    SparkDataFrameReader,
    SparkDataFrameStatFunctions,
    SparkDataFrameWriter,
    SparkGroupedData,
    SparkSession,
    SparkUDFRegistration,
)
from sqlframe.spark import Window as SparkWindow
from sqlframe.spark import WindowSpec as SparkWindowSpec
from sqlframe.spark import functions as SparkF
from sqlframe.spark import types as SparkTypes


@pytest.mark.forked
def test_replace_pyspark_spark(check_pyspark_imports):
    check_pyspark_imports(
        "spark",
        sqlf_session=SparkSession,
        sqlf_catalog=SparkCatalog,
        sqlf_column=SparkColumn,
        sqlf_dataframe=SparkDataFrame,
        sqlf_grouped_data=SparkGroupedData,
        sqlf_window=SparkWindow,
        sqlf_window_spec=SparkWindowSpec,
        sqlf_functions=SparkF,
        sqlf_types=SparkTypes,
        sqlf_udf_registration=SparkUDFRegistration,
        sqlf_dataframe_reader=SparkDataFrameReader,
        sqlf_dataframe_writer=SparkDataFrameWriter,
        sqlf_dataframe_na_functions=SparkDataFrameNaFunctions,
        sqlf_dataframe_stat_functions=SparkDataFrameStatFunctions,
        sqlf_row=SparkRow,
    )
