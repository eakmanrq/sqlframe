from sqlframe.base.table import _BaseTable
from sqlframe.spark.dataframe import SparkDataFrame


class SparkTable(SparkDataFrame, _BaseTable["SparkDataFrame"]):
    _df = SparkDataFrame
