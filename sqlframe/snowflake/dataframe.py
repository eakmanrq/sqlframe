from __future__ import annotations

import logging
import sys
import typing as t

from sqlframe.base.catalog import Column as CatalogColumn
from sqlframe.base.dataframe import (
    BaseDataFrame,
    _BaseDataFrameNaFunctions,
    _BaseDataFrameStatFunctions,
)
from sqlframe.base.mixins.dataframe_mixins import NoCachePersistSupportMixin
from sqlframe.base.util import normalize_string
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
    BaseDataFrame[
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
    _EXPLAIN_PREFIX = "EXPLAIN USING TEXT"

    @property
    def _typed_columns(self) -> t.List[CatalogColumn]:
        df = self._convert_leaf_to_cte()
        df = df.limit(0)
        df.collect()
        query_id = self.session._cur.sfqid
        columns = []
        for row in self.session._collect(f"DESCRIBE RESULT '{query_id}'"):
            null_row = [x for x in row.__fields__ if "?" in x][0]
            columns.append(
                CatalogColumn(
                    name=normalize_string(row.name, from_dialect="execution", to_dialect="output"),
                    dataType=normalize_string(
                        row.type, from_dialect="execution", to_dialect="output", is_datatype=True
                    ),
                    nullable=row[null_row] == "Y",
                    description=row.comment,
                    isPartition=False,
                    isBucket=False,
                )
            )
        return columns
