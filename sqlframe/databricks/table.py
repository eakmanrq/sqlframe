from sqlframe.base.table import _BaseTable
from sqlframe.base.mixins.table_mixins import MergeSupportMixin
from sqlframe.databricks.dataframe import DatabricksDataFrame


class DatabricksTable(
    DatabricksDataFrame,
    MergeSupportMixin["DatabricksDataFrame"],
    _BaseTable["DatabricksDataFrame"]
):
    _df = DatabricksDataFrame
