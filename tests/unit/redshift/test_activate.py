import pytest

from sqlframe.redshift import Column as RedshiftColumn
from sqlframe.redshift import (
    RedshiftCatalog,
    RedshiftDataFrame,
    RedshiftDataFrameNaFunctions,
    RedshiftDataFrameReader,
    RedshiftDataFrameStatFunctions,
    RedshiftDataFrameWriter,
    RedshiftGroupedData,
    RedshiftSession,
    RedshiftUDFRegistration,
)
from sqlframe.redshift import Row as RedshiftRow
from sqlframe.redshift import Window as RedshiftWindow
from sqlframe.redshift import WindowSpec as RedshiftWindowSpec
from sqlframe.redshift import functions as RedshiftF
from sqlframe.redshift import types as RedshiftTypes


@pytest.mark.forked
def test_activate_redshift(check_pyspark_imports):
    check_pyspark_imports(
        "redshift",
        sqlf_session=RedshiftSession,
        sqlf_catalog=RedshiftCatalog,
        sqlf_column=RedshiftColumn,
        sqlf_dataframe=RedshiftDataFrame,
        sqlf_grouped_data=RedshiftGroupedData,
        sqlf_window=RedshiftWindow,
        sqlf_window_spec=RedshiftWindowSpec,
        sqlf_functions=RedshiftF,
        sqlf_types=RedshiftTypes,
        sqlf_udf_registration=RedshiftUDFRegistration,
        sqlf_dataframe_reader=RedshiftDataFrameReader,
        sqlf_dataframe_writer=RedshiftDataFrameWriter,
        sqlf_dataframe_na_functions=RedshiftDataFrameNaFunctions,
        sqlf_dataframe_stat_functions=RedshiftDataFrameStatFunctions,
        sqlf_row=RedshiftRow,
    )
