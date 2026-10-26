from sqlframe.trino import Column as TrinoColumn
from sqlframe.trino import Row as TrinoRow
from sqlframe.trino import (
    TrinoCatalog,
    TrinoDataFrame,
    TrinoDataFrameNaFunctions,
    TrinoDataFrameReader,
    TrinoDataFrameStatFunctions,
    TrinoDataFrameWriter,
    TrinoGroupedData,
    TrinoSession,
    TrinoUDFRegistration,
)
from sqlframe.trino import Window as TrinoWindow
from sqlframe.trino import WindowSpec as TrinoWindowSpec
from sqlframe.trino import functions as TrinoF
from sqlframe.trino import types as TrinoTypes


def test_activate_trino(check_pyspark_imports):
    check_pyspark_imports(
        "trino",
        sqlf_session=TrinoSession,
        sqlf_catalog=TrinoCatalog,
        sqlf_column=TrinoColumn,
        sqlf_dataframe=TrinoDataFrame,
        sqlf_grouped_data=TrinoGroupedData,
        sqlf_window=TrinoWindow,
        sqlf_window_spec=TrinoWindowSpec,
        sqlf_functions=TrinoF,
        sqlf_types=TrinoTypes,
        sqlf_udf_registration=TrinoUDFRegistration,
        sqlf_dataframe_reader=TrinoDataFrameReader,
        sqlf_dataframe_writer=TrinoDataFrameWriter,
        sqlf_dataframe_na_functions=TrinoDataFrameNaFunctions,
        sqlf_dataframe_stat_functions=TrinoDataFrameStatFunctions,
        sqlf_row=TrinoRow,
    )
