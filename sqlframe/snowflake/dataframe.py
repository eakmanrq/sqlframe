from __future__ import annotations

import logging
import sys
import typing as t

from sqlframe.base.catalog import Column as CatalogColumn
from sqlframe.base.dataframe import (
    _BaseDataFrame,
    _BaseDataFrameNaFunctions,
    _BaseDataFrameStatFunctions,
)
from sqlframe.base.mixins.dataframe_mixins import NoCachePersistSupportMixin
from sqlframe.snowflake.group import SnowflakeGroupedData

if t.TYPE_CHECKING:
    from sqlframe.snowflake.readwriter import SnowflakeDataFrameWriter
    from sqlframe.snowflake.session import SnowflakeSession


logger = logging.getLogger(__name__)


class SnowflakeDataFrameNaFunctions(_BaseDataFrameNaFunctions["SnowflakeDataFrame"]):
    pass


class SnowflakeDataFrameStatFunctions(_BaseDataFrameStatFunctions["SnowflakeDataFrame"]):
    pass


class SnowflakeDataFrame(
    NoCachePersistSupportMixin,
    _BaseDataFrame[
        "SnowflakeSession",
        "SnowflakeDataFrameWriter",
        "SnowflakeDataFrameNaFunctions",
        "SnowflakeDataFrameStatFunctions",
        "SnowflakeGroupedData",
    ],
):
    _na = SnowflakeDataFrameNaFunctions
    _stat = SnowflakeDataFrameStatFunctions
    _group_data = SnowflakeGroupedData

    @property
    def _typed_columns(self) -> t.List[CatalogColumn]:
        df = self._convert_leaf_to_cte()
        df = df.limit(0)
        self.session._execute(df.expression)
        query_id = self.session._cur.sfqid
        columns = []
        for row in self.session._fetch_rows(f"DESCRIBE RESULT '{query_id}'"):
            columns.append(
                CatalogColumn(
                    name=row.name,
                    dataType=row.type,
                    nullable=row["null?"] == "Y",
                    description=row.comment,
                    isPartition=False,
                    isBucket=False,
                )
            )
        return columns
