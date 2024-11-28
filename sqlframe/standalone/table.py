from sqlframe.base.table import _BaseTable
from sqlframe.standalone.dataframe import StandaloneDataFrame


class StandaloneTable(StandaloneDataFrame, _BaseTable["StandaloneDataFrame"]):
    _df = StandaloneDataFrame
