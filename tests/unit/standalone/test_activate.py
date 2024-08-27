import pytest

from sqlframe.standalone import Column as StandaloneColumn
from sqlframe.standalone import Row as StandaloneRow
from sqlframe.standalone import (
    StandaloneCatalog,
    StandaloneDataFrame,
    StandaloneDataFrameNaFunctions,
    StandaloneDataFrameReader,
    StandaloneDataFrameStatFunctions,
    StandaloneDataFrameWriter,
    StandaloneGroupedData,
    StandaloneSession,
    StandaloneUDFRegistration,
)
from sqlframe.standalone import Window as StandaloneWindow
from sqlframe.standalone import WindowSpec as StandaloneWindowSpec
from sqlframe.standalone import functions as StandaloneF
from sqlframe.standalone import types as StandaloneTypes


@pytest.mark.forked
def test_activate_standalone(check_pyspark_imports):
    check_pyspark_imports(
        "standalone",
        sqlf_session=StandaloneSession,
        sqlf_catalog=StandaloneCatalog,
        sqlf_column=StandaloneColumn,
        sqlf_dataframe=StandaloneDataFrame,
        sqlf_grouped_data=StandaloneGroupedData,
        sqlf_window=StandaloneWindow,
        sqlf_window_spec=StandaloneWindowSpec,
        sqlf_functions=StandaloneF,
        sqlf_types=StandaloneTypes,
        sqlf_udf_registration=StandaloneUDFRegistration,
        sqlf_dataframe_reader=StandaloneDataFrameReader,
        sqlf_dataframe_writer=StandaloneDataFrameWriter,
        sqlf_dataframe_na_functions=StandaloneDataFrameNaFunctions,
        sqlf_dataframe_stat_functions=StandaloneDataFrameStatFunctions,
        sqlf_row=StandaloneRow,
    )
