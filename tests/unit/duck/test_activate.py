import pytest

from sqlframe.duckdb import Column as DuckDBColumn
from sqlframe.duckdb import (
    DuckDBCatalog,
    DuckDBDataFrame,
    DuckDBDataFrameNaFunctions,
    DuckDBDataFrameReader,
    DuckDBDataFrameStatFunctions,
    DuckDBDataFrameWriter,
    DuckDBGroupedData,
    DuckDBSession,
    DuckDBUDFRegistration,
)
from sqlframe.duckdb import Row as DuckDBRow
from sqlframe.duckdb import Window as DuckDBWindow
from sqlframe.duckdb import WindowSpec as DuckDBWindowSpec
from sqlframe.duckdb import functions as DuckDBF
from sqlframe.duckdb import types as DuckDBTypes


@pytest.mark.forked
def test_activate_duckdb(check_pyspark_imports):
    check_pyspark_imports(
        "duckdb",
        sqlf_session=DuckDBSession,
        sqlf_catalog=DuckDBCatalog,
        sqlf_column=DuckDBColumn,
        sqlf_dataframe=DuckDBDataFrame,
        sqlf_grouped_data=DuckDBGroupedData,
        sqlf_window=DuckDBWindow,
        sqlf_window_spec=DuckDBWindowSpec,
        sqlf_functions=DuckDBF,
        sqlf_types=DuckDBTypes,
        sqlf_udf_registration=DuckDBUDFRegistration,
        sqlf_dataframe_reader=DuckDBDataFrameReader,
        sqlf_dataframe_writer=DuckDBDataFrameWriter,
        sqlf_dataframe_na_functions=DuckDBDataFrameNaFunctions,
        sqlf_dataframe_stat_functions=DuckDBDataFrameStatFunctions,
        sqlf_row=DuckDBRow,
    )
