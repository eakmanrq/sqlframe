import pytest

from sqlframe import activate
from sqlframe.databricks import Column as DatabricksColumn
from sqlframe.databricks import (
    DatabricksCatalog,
    DatabricksDataFrame,
    DatabricksDataFrameNaFunctions,
    DatabricksDataFrameReader,
    DatabricksDataFrameStatFunctions,
    DatabricksDataFrameWriter,
    DatabricksGroupedData,
    DatabricksSession,
    DatabricksUDFRegistration,
)
from sqlframe.databricks import Row as DatabricksRow
from sqlframe.databricks import Window as DatabricksWindow
from sqlframe.databricks import WindowSpec as DatabricksWindowSpec
from sqlframe.databricks import functions as DatabricksF
from sqlframe.databricks import types as DatabricksTypes


@pytest.mark.forked
def test_activate_databricks(check_pyspark_imports):
    check_pyspark_imports(
        "databricks",
        sqlf_session=DatabricksSession,
        sqlf_catalog=DatabricksCatalog,
        sqlf_column=DatabricksColumn,
        sqlf_dataframe=DatabricksDataFrame,
        sqlf_grouped_data=DatabricksGroupedData,
        sqlf_window=DatabricksWindow,
        sqlf_window_spec=DatabricksWindowSpec,
        sqlf_functions=DatabricksF,
        sqlf_types=DatabricksTypes,
        sqlf_udf_registration=DatabricksUDFRegistration,
        sqlf_dataframe_reader=DatabricksDataFrameReader,
        sqlf_dataframe_writer=DatabricksDataFrameWriter,
        sqlf_dataframe_na_functions=DatabricksDataFrameNaFunctions,
        sqlf_dataframe_stat_functions=DatabricksDataFrameStatFunctions,
        sqlf_row=DatabricksRow,
    )


@pytest.mark.forked
def test_activate_databricks_default_dataset():
    activate("databricks")
    from pyspark.sql import SparkSession

    assert SparkSession == DatabricksSession
