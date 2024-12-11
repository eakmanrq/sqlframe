from sqlframe.base.mixins.table_mixins import (
    DeleteSupportMixin,
    MergeSupportMixin,
    UpdateSupportMixin,
)
from sqlframe.base.table import _BaseTable
from sqlframe.databricks.dataframe import DatabricksDataFrame


class DatabricksTable(
    DatabricksDataFrame,
    UpdateSupportMixin["DatabricksDataFrame"],
    DeleteSupportMixin["DatabricksDataFrame"],
    MergeSupportMixin["DatabricksDataFrame"],
    _BaseTable["DatabricksDataFrame"],
):
    _df = DatabricksDataFrame
