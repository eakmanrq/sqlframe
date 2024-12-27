from __future__ import annotations

import logging
import sys
import typing as t

from sqlframe.base.dataframe import (
    BaseDataFrame,
    _BaseDataFrameNaFunctions,
    _BaseDataFrameStatFunctions,
)
from sqlframe.base.mixins.dataframe_mixins import NoCachePersistSupportMixin
from sqlframe.redshift.group import RedshiftGroupedData

if t.TYPE_CHECKING:
    from sqlframe.redshift.readwriter import RedshiftDataFrameWriter
    from sqlframe.redshift.session import RedshiftSession


logger = logging.getLogger(__name__)


class RedshiftDataFrameNaFunctions(_BaseDataFrameNaFunctions["RedshiftDataFrame"]):
    pass


class RedshiftDataFrameStatFunctions(_BaseDataFrameStatFunctions["RedshiftDataFrame"]):
    pass


class RedshiftDataFrame(
    NoCachePersistSupportMixin,
    BaseDataFrame[
        "RedshiftSession",
        "RedshiftDataFrameWriter",
        "RedshiftDataFrameNaFunctions",
        "RedshiftDataFrameStatFunctions",
        "RedshiftGroupedData",
    ],
):
    _na = RedshiftDataFrameNaFunctions
    _stat = RedshiftDataFrameStatFunctions
    _group_data = RedshiftGroupedData
