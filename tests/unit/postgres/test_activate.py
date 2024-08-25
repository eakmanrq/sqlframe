import pytest

from sqlframe.postgres import Column as PostgresColumn
from sqlframe.postgres import (
    PostgresCatalog,
    PostgresDataFrame,
    PostgresDataFrameNaFunctions,
    PostgresDataFrameReader,
    PostgresDataFrameStatFunctions,
    PostgresDataFrameWriter,
    PostgresGroupedData,
    PostgresSession,
    PostgresUDFRegistration,
)
from sqlframe.postgres import Row as PostgresRow
from sqlframe.postgres import Window as PostgresWindow
from sqlframe.postgres import WindowSpec as PostgresWindowSpec
from sqlframe.postgres import functions as PostgresF
from sqlframe.postgres import types as PostgresTypes


@pytest.mark.forked
def test_activate_postgres(check_pyspark_imports):
    check_pyspark_imports(
        "postgres",
        sqlf_session=PostgresSession,
        sqlf_catalog=PostgresCatalog,
        sqlf_column=PostgresColumn,
        sqlf_dataframe=PostgresDataFrame,
        sqlf_grouped_data=PostgresGroupedData,
        sqlf_window=PostgresWindow,
        sqlf_window_spec=PostgresWindowSpec,
        sqlf_functions=PostgresF,
        sqlf_types=PostgresTypes,
        sqlf_udf_registration=PostgresUDFRegistration,
        sqlf_dataframe_reader=PostgresDataFrameReader,
        sqlf_dataframe_writer=PostgresDataFrameWriter,
        sqlf_dataframe_na_functions=PostgresDataFrameNaFunctions,
        sqlf_dataframe_stat_functions=PostgresDataFrameStatFunctions,
        sqlf_row=PostgresRow,
    )
