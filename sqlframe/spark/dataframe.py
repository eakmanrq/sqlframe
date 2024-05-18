from __future__ import annotations

import logging
import sys
import typing as t

from sqlframe.base.dataframe import (
    _BaseDataFrame,
    _BaseDataFrameNaFunctions,
    _BaseDataFrameStatFunctions,
)
from sqlframe.spark.group import SparkGroupedData

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if t.TYPE_CHECKING:
    from sqlframe.spark.readwriter import SparkDataFrameWriter
    from sqlframe.spark.session import SparkSession


logger = logging.getLogger(__name__)


class SparkDataFrameNaFunctions(_BaseDataFrameNaFunctions["SparkDataFrame"]):
    pass


class SparkDataFrameStatFunctions(_BaseDataFrameStatFunctions["SparkDataFrame"]):
    pass


class SparkDataFrame(
    _BaseDataFrame[
        "SparkSession",
        "SparkDataFrameWriter",
        "SparkDataFrameNaFunctions",
        "SparkDataFrameStatFunctions",
        "SparkGroupedData",
    ]
):
    _na = SparkDataFrameNaFunctions
    _stat = SparkDataFrameStatFunctions
    _group_data = SparkGroupedData

    def cache(self) -> Self:
        logger.warning("Spark does not support caching. Ignoring cache() call.")
        return self

    def persist(self) -> Self:
        logger.warning("Spark does not support persist. Ignoring persist() call.")
        return self
