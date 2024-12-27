from __future__ import annotations

import typing as t

from sqlframe.base.dataframe import (
    BaseDataFrame,
    _BaseDataFrameNaFunctions,
    _BaseDataFrameStatFunctions,
)
from sqlframe.standalone.group import StandaloneGroupedData

if t.TYPE_CHECKING:
    from sqlframe.standalone.readwriter import StandaloneDataFrameWriter
    from sqlframe.standalone.session import StandaloneSession


class StandaloneDataFrameNaFunctions(_BaseDataFrameNaFunctions["StandaloneDataFrame"]):
    pass


class StandaloneDataFrameStatFunctions(_BaseDataFrameStatFunctions["StandaloneDataFrame"]):
    pass


class StandaloneDataFrame(
    BaseDataFrame[
        "StandaloneSession",
        "StandaloneDataFrameWriter",
        "StandaloneDataFrameNaFunctions",
        "StandaloneDataFrameStatFunctions",
        "StandaloneGroupedData",
    ]
):
    _na = StandaloneDataFrameNaFunctions
    _stat = StandaloneDataFrameStatFunctions
    _group_data = StandaloneGroupedData
