from sqlframe.base.table import _BaseTable
from sqlframe.duckdb.dataframe import DuckDBDataFrame


class DuckDBTable(DuckDBDataFrame, _BaseTable["DuckDBDataFrame"]):
    _df = DuckDBDataFrame
