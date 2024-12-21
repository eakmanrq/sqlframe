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
from sqlframe.postgres.dataframe import PostgresDataFrame


class PostgresTable(
    PostgresDataFrame,
    UpdateSupportMixin["PostgresDataFrame"],
    DeleteSupportMixin["PostgresDataFrame"],
    MergeSupportMixin["PostgresDataFrame"],
    _BaseTable["PostgresDataFrame"],
):
    _df = PostgresDataFrame
    _merge_supported_clauses = [WhenMatched, WhenNotMatched, WhenNotMatchedBySource]
    _merge_support_star = False
