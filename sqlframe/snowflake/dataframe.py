from __future__ import annotations

import logging
import sys
import typing as t

from sqlframe.base.dataframe import (
    _BaseDataFrame,
    _BaseDataFrameNaFunctions,
    _BaseDataFrameStatFunctions,
)
from sqlframe.snowflake.group import SnowflakeGroupedData

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if t.TYPE_CHECKING:
    from sqlframe.snowflake.readwriter import SnowflakeDataFrameWriter
    from sqlframe.snowflake.session import SnowflakeSession


logger = logging.getLogger(__name__)


class SnowflakeDataFrameNaFunctions(_BaseDataFrameNaFunctions["SnowflakeDataFrame"]):
    pass


class SnowflakeDataFrameStatFunctions(_BaseDataFrameStatFunctions["SnowflakeDataFrame"]):
    pass


class SnowflakeDataFrame(
    _BaseDataFrame[
        "SnowflakeSession",
        "SnowflakeDataFrameWriter",
        "SnowflakeDataFrameNaFunctions",
        "SnowflakeDataFrameStatFunctions",
        "SnowflakeGroupedData",
    ]
):
    _na = SnowflakeDataFrameNaFunctions
    _stat = SnowflakeDataFrameStatFunctions
    _group_data = SnowflakeGroupedData

    def cache(self) -> Self:
        logger.warning("Snowflake does not support caching. Ignoring cache() call.")
        return self

    def persist(self) -> Self:
        logger.warning("Snowflake does not support persist. Ignoring persist() call.")
        return self
