from sqlframe.base.mixins.table_mixins import (
    DeleteSupportMixin,
    MergeSupportMixin,
    UpdateSupportMixin,
)
from sqlframe.base.table import (
    WhenMatched,
    WhenNotMatched,
    WhenNotMatchedBySource,
    _BaseTable,
)
from sqlframe.databricks.dataframe import DatabricksDataFrame


class DatabricksTable(
    DatabricksDataFrame,
    UpdateSupportMixin["DatabricksDataFrame"],
    DeleteSupportMixin["DatabricksDataFrame"],
    MergeSupportMixin["DatabricksDataFrame"],
    _BaseTable["DatabricksDataFrame"],
):
    _df = DatabricksDataFrame
    _merge_supported_clauses = [WhenMatched, WhenNotMatched, WhenNotMatchedBySource]
    _merge_support_star = True
