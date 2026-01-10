import pytest

from sqlframe.gizmosql import Column as GizmoSQLColumn
from sqlframe.gizmosql import (
    GizmoSQLCatalog,
    GizmoSQLDataFrame,
    GizmoSQLDataFrameNaFunctions,
    GizmoSQLDataFrameReader,
    GizmoSQLDataFrameStatFunctions,
    GizmoSQLDataFrameWriter,
    GizmoSQLGroupedData,
    GizmoSQLSession,
    GizmoSQLUDFRegistration,
)
from sqlframe.gizmosql import Row as GizmoSQLRow
from sqlframe.gizmosql import Window as GizmoSQLWindow
from sqlframe.gizmosql import WindowSpec as GizmoSQLWindowSpec
from sqlframe.gizmosql import functions as GizmoSQLF
from sqlframe.gizmosql import types as GizmoSQLTypes


@pytest.mark.forked
def test_activate_gizmosql(check_pyspark_imports):
    check_pyspark_imports(
        "gizmosql",
        sqlf_session=GizmoSQLSession,
        sqlf_catalog=GizmoSQLCatalog,
        sqlf_column=GizmoSQLColumn,
        sqlf_dataframe=GizmoSQLDataFrame,
        sqlf_grouped_data=GizmoSQLGroupedData,
        sqlf_window=GizmoSQLWindow,
        sqlf_window_spec=GizmoSQLWindowSpec,
        sqlf_functions=GizmoSQLF,
        sqlf_types=GizmoSQLTypes,
        sqlf_udf_registration=GizmoSQLUDFRegistration,
        sqlf_dataframe_reader=GizmoSQLDataFrameReader,
        sqlf_dataframe_writer=GizmoSQLDataFrameWriter,
        sqlf_dataframe_na_functions=GizmoSQLDataFrameNaFunctions,
        sqlf_dataframe_stat_functions=GizmoSQLDataFrameStatFunctions,
        sqlf_row=GizmoSQLRow,
    )
