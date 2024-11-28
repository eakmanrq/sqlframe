from sqlframe.base.table import _BaseTable
from sqlframe.snowflake.dataframe import SnowflakeDataFrame


class SnowflakeTable(SnowflakeDataFrame, _BaseTable["SnowflakeDataFrame"]):
    _df = SnowflakeDataFrame
