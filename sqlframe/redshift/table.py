from sqlframe.base.mixins.table_mixins import (
    DeleteSupportMixin,
    UpdateSupportMixin,
)
from sqlframe.base.table import _BaseTable
from sqlframe.redshift.dataframe import RedshiftDataFrame


class RedshiftTable(
    RedshiftDataFrame,
    UpdateSupportMixin["RedshiftDataFrame"],
    DeleteSupportMixin["RedshiftDataFrame"],
    _BaseTable["RedshiftDataFrame"],
):
    _df = RedshiftDataFrame
