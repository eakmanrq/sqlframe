from sqlframe.clickhouse import (
    ClickHouseCatalog,
    ClickHouseDataFrame,
    ClickHouseDataFrameNaFunctions,
    ClickHouseDataFrameReader,
    ClickHouseDataFrameStatFunctions,
    ClickHouseDataFrameWriter,
    ClickHouseGroupedData,
    ClickHouseSession,
    ClickHouseUDFRegistration,
)
from sqlframe.clickhouse import Column as ClickHouseColumn
from sqlframe.clickhouse import Row as ClickHouseRow
from sqlframe.clickhouse import Window as ClickHouseWindow
from sqlframe.clickhouse import WindowSpec as ClickHouseWindowSpec
from sqlframe.clickhouse import functions as ClickHouseF
from sqlframe.clickhouse import types as ClickHouseTypes


def test_activate_clickhouse(check_pyspark_imports):
    check_pyspark_imports(
        "clickhouse",
        sqlf_session=ClickHouseSession,
        sqlf_catalog=ClickHouseCatalog,
        sqlf_column=ClickHouseColumn,
        sqlf_dataframe=ClickHouseDataFrame,
        sqlf_grouped_data=ClickHouseGroupedData,
        sqlf_window=ClickHouseWindow,
        sqlf_window_spec=ClickHouseWindowSpec,
        sqlf_functions=ClickHouseF,
        sqlf_types=ClickHouseTypes,
        sqlf_udf_registration=ClickHouseUDFRegistration,
        sqlf_dataframe_reader=ClickHouseDataFrameReader,
        sqlf_dataframe_writer=ClickHouseDataFrameWriter,
        sqlf_dataframe_na_functions=ClickHouseDataFrameNaFunctions,
        sqlf_dataframe_stat_functions=ClickHouseDataFrameStatFunctions,
        sqlf_row=ClickHouseRow,
    )
