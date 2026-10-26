from sqlframe.base.table import _BaseTable
from sqlframe.clickhouse.dataframe import ClickHouseDataFrame


class ClickHouseTable(ClickHouseDataFrame, _BaseTable[ClickHouseDataFrame]):
    _df = ClickHouseDataFrame
