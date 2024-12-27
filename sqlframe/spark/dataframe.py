from __future__ import annotations

import logging
import typing as t

from sqlglot import exp

from sqlframe.base.catalog import Column
from sqlframe.base.dataframe import (
    BaseDataFrame,
    _BaseDataFrameNaFunctions,
    _BaseDataFrameStatFunctions,
)
from sqlframe.base.mixins.dataframe_mixins import NoCachePersistSupportMixin
from sqlframe.spark.group import SparkGroupedData

if t.TYPE_CHECKING:
    from sqlframe.spark.readwriter import SparkDataFrameWriter
    from sqlframe.spark.session import SparkSession

logger = logging.getLogger(__name__)


class SparkDataFrameNaFunctions(_BaseDataFrameNaFunctions["SparkDataFrame"]):
    pass


class SparkDataFrameStatFunctions(_BaseDataFrameStatFunctions["SparkDataFrame"]):
    pass


class SparkDataFrame(
    NoCachePersistSupportMixin,
    BaseDataFrame[
        "SparkSession",
        "SparkDataFrameWriter",
        "SparkDataFrameNaFunctions",
        "SparkDataFrameStatFunctions",
        "SparkGroupedData",
    ],
):
    _na = SparkDataFrameNaFunctions
    _stat = SparkDataFrameStatFunctions
    _group_data = SparkGroupedData

    @property
    def _typed_columns(self) -> t.List[Column]:
        columns = []
        for field in self.session.spark_session.sql(
            self.session._to_sql(self.expression)
        ).schema.fields:
            columns.append(
                Column(
                    name=field.name,
                    dataType=exp.DataType.build(field.dataType.simpleString(), dialect="spark").sql(
                        dialect="spark"
                    ),
                    nullable=field.nullable,
                    description=None,
                    isPartition=False,
                    isBucket=False,
                )
            )
        return columns
