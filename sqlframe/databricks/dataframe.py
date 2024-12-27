from __future__ import annotations

import logging
import typing as t

from sqlframe.base.catalog import Column as CatalogColumn
from sqlframe.base.dataframe import (
    BaseDataFrame,
    _BaseDataFrameNaFunctions,
    _BaseDataFrameStatFunctions,
)
from sqlframe.base.mixins.dataframe_mixins import NoCachePersistSupportMixin
from sqlframe.base.util import normalize_string
from sqlframe.databricks.group import DatabricksGroupedData

if t.TYPE_CHECKING:
    from sqlframe.databricks.readwriter import DatabricksDataFrameWriter
    from sqlframe.databricks.session import DatabricksSession


logger = logging.getLogger(__name__)


class DatabricksDataFrameNaFunctions(_BaseDataFrameNaFunctions["DatabricksDataFrame"]):
    pass


class DatabricksDataFrameStatFunctions(_BaseDataFrameStatFunctions["DatabricksDataFrame"]):
    pass


class DatabricksDataFrame(
    NoCachePersistSupportMixin,
    BaseDataFrame[
        "DatabricksSession",
        "DatabricksDataFrameWriter",
        "DatabricksDataFrameNaFunctions",
        "DatabricksDataFrameStatFunctions",
        "DatabricksGroupedData",
    ],
):
    _na = DatabricksDataFrameNaFunctions
    _stat = DatabricksDataFrameStatFunctions
    _group_data = DatabricksGroupedData

    @property
    def _typed_columns(self) -> t.List[CatalogColumn]:
        sql = self.session._to_sql(self.expression)
        columns = []
        for row in self.session._collect(r"DESCRIBE QUERY ({sql})".format(sql=sql)):
            columns.append(
                CatalogColumn(
                    name=normalize_string(
                        row.col_name,
                        from_dialect="execution",
                        to_dialect="output",
                    ),
                    dataType=normalize_string(
                        row.data_type,
                        from_dialect="execution",
                        to_dialect="output",
                        is_datatype=True,
                    ),
                    nullable=True,
                    description=row.comment,
                    isPartition=False,
                    isBucket=False,
                )
            )
        return columns
