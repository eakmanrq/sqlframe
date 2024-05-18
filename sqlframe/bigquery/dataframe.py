from __future__ import annotations

import logging
import sys
import typing as t

from sqlframe.base.dataframe import (
    _BaseDataFrame,
    _BaseDataFrameNaFunctions,
    _BaseDataFrameStatFunctions,
)
from sqlframe.bigquery.group import BigQueryGroupedData

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if t.TYPE_CHECKING:
    from sqlframe.bigquery.readwriter import BigQueryDataFrameWriter
    from sqlframe.bigquery.session import BigQuerySession


logger = logging.getLogger(__name__)


class BigQueryDataFrameNaFunctions(_BaseDataFrameNaFunctions["BigQueryDataFrame"]):
    pass


class BigQueryDataFrameStatFunctions(_BaseDataFrameStatFunctions["BigQueryDataFrame"]):
    pass


class BigQueryDataFrame(
    _BaseDataFrame[
        "BigQuerySession",
        "BigQueryDataFrameWriter",
        "BigQueryDataFrameNaFunctions",
        "BigQueryDataFrameStatFunctions",
        "BigQueryGroupedData",
    ]
):
    _na = BigQueryDataFrameNaFunctions
    _stat = BigQueryDataFrameStatFunctions
    _group_data = BigQueryGroupedData

    def cache(self) -> Self:
        logger.warning("BigQuery does not support caching. Ignoring cache() call.")
        return self

    def persist(self) -> Self:
        logger.warning("BigQuery does not support persist. Ignoring persist() call.")
        return self
