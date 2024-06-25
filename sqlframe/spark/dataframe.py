from __future__ import annotations

import logging
import typing as t

from sqlframe.base.dataframe import (
    _BaseDataFrame,
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
    _BaseDataFrame[
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
