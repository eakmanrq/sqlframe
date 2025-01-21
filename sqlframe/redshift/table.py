from sqlframe.base.mixins.table_mixins import (
    DeleteSupportMixin,
    MergeSupportMixin,
    UpdateSupportMixin,
)
from sqlframe.base.table import (
    WhenMatched,
    WhenNotMatched,
    _BaseTable,
)
from sqlframe.redshift.dataframe import RedshiftDataFrame


class RedshiftTable(
    RedshiftDataFrame,
    UpdateSupportMixin["RedshiftDataFrame"],
    DeleteSupportMixin["RedshiftDataFrame"],
    MergeSupportMixin["RedshiftDataFrame"],
    _BaseTable["RedshiftDataFrame"],
):
    _df = RedshiftDataFrame
    _merge_supported_clauses = [WhenMatched, WhenNotMatched]
    _merge_support_star = False
