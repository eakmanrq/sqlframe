from sqlframe.base.table import _BaseTable
from sqlframe.bigquery.dataframe import BigQueryDataFrame


class BigQueryTable(BigQueryDataFrame, _BaseTable["BigQueryDataFrame"]):
    _df = BigQueryDataFrame
