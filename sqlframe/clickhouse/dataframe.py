from __future__ import annotations

import typing as t

from sqlframe.base.catalog import Column as CatalogColumn
from sqlframe.base.dataframe import (
    BaseDataFrame,
    _BaseDataFrameNaFunctions,
    _BaseDataFrameStatFunctions,
)
from sqlframe.base.mixins.dataframe_mixins import NoCachePersistSupportMixin
from sqlframe.clickhouse.group import ClickHouseGroupedData

if t.TYPE_CHECKING:
    from sqlframe.clickhouse.readwriter import ClickHouseDataFrameWriter
    from sqlframe.clickhouse.session import ClickHouseSession


class ClickHouseDataFrameNaFunctions(_BaseDataFrameNaFunctions["ClickHouseDataFrame"]):
    pass


class ClickHouseDataFrameStatFunctions(_BaseDataFrameStatFunctions["ClickHouseDataFrame"]):
    pass


class ClickHouseDataFrame(
    NoCachePersistSupportMixin,
    BaseDataFrame[
        "ClickHouseSession",
        "ClickHouseDataFrameWriter",
        "ClickHouseDataFrameNaFunctions",
        "ClickHouseDataFrameStatFunctions",
        "ClickHouseGroupedData",
    ],
):
    _na = ClickHouseDataFrameNaFunctions
    _stat = ClickHouseDataFrameStatFunctions
    _group_data = ClickHouseGroupedData

    @property
    def _typed_columns(self) -> t.List[CatalogColumn]:
        sql = self.session._to_sql(self.expression)
        return [
            self.session.catalog._column_from_describe(row)
            for row in self.session._collect(f"DESCRIBE ({sql})")
        ]
