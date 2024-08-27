import pytest

from sqlframe.snowflake import Column as SnowflakeColumn
from sqlframe.snowflake import Row as SnowflakeRow
from sqlframe.snowflake import (
    SnowflakeCatalog,
    SnowflakeDataFrame,
    SnowflakeDataFrameNaFunctions,
    SnowflakeDataFrameReader,
    SnowflakeDataFrameStatFunctions,
    SnowflakeDataFrameWriter,
    SnowflakeGroupedData,
    SnowflakeSession,
    SnowflakeUDFRegistration,
)
from sqlframe.snowflake import Window as SnowflakeWindow
from sqlframe.snowflake import WindowSpec as SnowflakeWindowSpec
from sqlframe.snowflake import functions as SnowflakeF
from sqlframe.snowflake import types as SnowflakeTypes


@pytest.mark.forked
def test_activate_snowflake(check_pyspark_imports):
    check_pyspark_imports(
        "snowflake",
        sqlf_session=SnowflakeSession,
        sqlf_catalog=SnowflakeCatalog,
        sqlf_column=SnowflakeColumn,
        sqlf_dataframe=SnowflakeDataFrame,
        sqlf_grouped_data=SnowflakeGroupedData,
        sqlf_window=SnowflakeWindow,
        sqlf_window_spec=SnowflakeWindowSpec,
        sqlf_functions=SnowflakeF,
        sqlf_types=SnowflakeTypes,
        sqlf_udf_registration=SnowflakeUDFRegistration,
        sqlf_dataframe_reader=SnowflakeDataFrameReader,
        sqlf_dataframe_writer=SnowflakeDataFrameWriter,
        sqlf_dataframe_na_functions=SnowflakeDataFrameNaFunctions,
        sqlf_dataframe_stat_functions=SnowflakeDataFrameStatFunctions,
        sqlf_row=SnowflakeRow,
    )
