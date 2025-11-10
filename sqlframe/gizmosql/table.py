from sqlframe.base.mixins.table_mixins import (
    DeleteSupportMixin,
    MergeSupportMixin,
    UpdateSupportMixin,
)
from sqlframe.base.table import _BaseTable
from sqlframe.gizmosql.dataframe import GizmoSQLDataFrame


class GizmoSQLTable(
    GizmoSQLDataFrame,
    UpdateSupportMixin["GizmoSQLDataFrame"],
    DeleteSupportMixin["GizmoSQLDataFrame"],
    _BaseTable["GizmoSQLDataFrame"],
):
    _df = GizmoSQLDataFrame
