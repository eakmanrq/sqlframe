from __future__ import annotations

import logging
import sys
import typing as t

from sqlframe.base.dataframe import (
    BaseDataFrame,
    _BaseDataFrameNaFunctions,
    _BaseDataFrameStatFunctions,
)
from sqlframe.base.mixins.dataframe_mixins import (
    NoCachePersistSupportMixin,
    TypedColumnsFromTempViewMixin,
)
from sqlframe.postgres.group import PostgresGroupedData

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if t.TYPE_CHECKING:
    from sqlframe.postgres.readwriter import PostgresDataFrameWriter
    from sqlframe.postgres.session import PostgresSession


logger = logging.getLogger(__name__)


class PostgresDataFrameNaFunctions(_BaseDataFrameNaFunctions["PostgresDataFrame"]):
    pass


class PostgresDataFrameStatFunctions(_BaseDataFrameStatFunctions["PostgresDataFrame"]):
    pass


class PostgresDataFrame(
    NoCachePersistSupportMixin,
    TypedColumnsFromTempViewMixin,
    BaseDataFrame[
        "PostgresSession",
        "PostgresDataFrameWriter",
        "PostgresDataFrameNaFunctions",
        "PostgresDataFrameStatFunctions",
        "PostgresGroupedData",
    ],
):
    _na = PostgresDataFrameNaFunctions
    _stat = PostgresDataFrameStatFunctions
    _group_data = PostgresGroupedData
