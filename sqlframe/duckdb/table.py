from sqlframe.base.mixins.table_mixins import (
    DeleteSupportMixin,
    MergeSupportMixin,
    UpdateSupportMixin,
)
from sqlframe.base.table import _BaseTable
from sqlframe.duckdb.dataframe import DuckDBDataFrame


class DuckDBTable(
    DuckDBDataFrame,
    UpdateSupportMixin["DuckDBDataFrame"],
    DeleteSupportMixin["DuckDBDataFrame"],
    _BaseTable["DuckDBDataFrame"],
):
    _df = DuckDBDataFrame
