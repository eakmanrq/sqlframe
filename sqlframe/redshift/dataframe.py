from __future__ import annotations

import logging
import sys
import typing as t

from sqlframe.base.dataframe import (
    _BaseDataFrame,
    _BaseDataFrameNaFunctions,
    _BaseDataFrameStatFunctions,
)
from sqlframe.redshift.group import RedshiftGroupedData

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if t.TYPE_CHECKING:
    from sqlframe.redshift.readwriter import RedshiftDataFrameWriter
    from sqlframe.redshift.session import RedshiftSession


logger = logging.getLogger(__name__)


class RedshiftDataFrameNaFunctions(_BaseDataFrameNaFunctions["RedshiftDataFrame"]):
    pass


class RedshiftDataFrameStatFunctions(_BaseDataFrameStatFunctions["RedshiftDataFrame"]):
    pass


class RedshiftDataFrame(
    _BaseDataFrame[
        "RedshiftSession",
        "RedshiftDataFrameWriter",
        "RedshiftDataFrameNaFunctions",
        "RedshiftDataFrameStatFunctions",
        "RedshiftGroupedData",
    ]
):
    _na = RedshiftDataFrameNaFunctions
    _stat = RedshiftDataFrameStatFunctions
    _group_data = RedshiftGroupedData

    def cache(self) -> Self:
        logger.warning("Redshift does not support caching. Ignoring cache() call.")
        return self

    def persist(self) -> Self:
        logger.warning("Redshift does not support persist. Ignoring persist() call.")
        return self
