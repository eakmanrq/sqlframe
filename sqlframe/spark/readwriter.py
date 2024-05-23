# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import typing as t

from sqlframe.base.mixins.readwriter_mixins import PandasLoaderMixin, PandasWriterMixin
from sqlframe.base.readerwriter import (
    _BaseDataFrameReader,
    _BaseDataFrameWriter,
)

if t.TYPE_CHECKING:
    from sqlframe.spark.dataframe import SparkDataFrame
    from sqlframe.spark.session import SparkSession


class SparkDataFrameReader(
    PandasLoaderMixin["SparkSession", "SparkDataFrame"],
    _BaseDataFrameReader["SparkSession", "SparkDataFrame"],
):
    pass


class SparkDataFrameWriter(
    PandasWriterMixin["SparkSession", "SparkDataFrame"],
    _BaseDataFrameWriter["SparkSession", "SparkDataFrame"],
):
    pass
