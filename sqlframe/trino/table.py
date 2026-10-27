from sqlframe.standalone.table import StandaloneTable
from sqlframe.trino.dataframe import TrinoDataFrame


class TrinoTable(TrinoDataFrame, StandaloneTable):
    _df = TrinoDataFrame
