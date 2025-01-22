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
from sqlframe.snowflake.dataframe import SnowflakeDataFrame


class SnowflakeTable(
    SnowflakeDataFrame,
    UpdateSupportMixin["SnowflakeDataFrame"],
    DeleteSupportMixin["SnowflakeDataFrame"],
    MergeSupportMixin["SnowflakeDataFrame"],
    _BaseTable["SnowflakeDataFrame"],
):
    _df = SnowflakeDataFrame
    _merge_supported_clauses = [WhenMatched, WhenNotMatched]
    _merge_support_star = False
