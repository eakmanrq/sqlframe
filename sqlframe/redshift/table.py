from sqlframe.base.table import _BaseTable
from sqlframe.redshift.dataframe import RedshiftDataFrame


class RedshiftTable(RedshiftDataFrame, _BaseTable["RedshiftDataFrame"]):
    _df = RedshiftDataFrame
