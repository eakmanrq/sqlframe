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
from sqlframe.bigquery.dataframe import BigQueryDataFrame


class BigQueryTable(
    BigQueryDataFrame,
    UpdateSupportMixin["BigQueryDataFrame"],
    DeleteSupportMixin["BigQueryDataFrame"],
    MergeSupportMixin["BigQueryDataFrame"],
    _BaseTable["BigQueryDataFrame"],
):
    _df = BigQueryDataFrame
    _merge_supported_clauses = [WhenMatched, WhenNotMatched, WhenNotMatchedBySource]
    _merge_support_star = False
