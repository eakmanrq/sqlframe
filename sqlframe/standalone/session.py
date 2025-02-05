# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

from sqlframe.base.session import _BaseSession
from sqlframe.standalone.catalog import StandaloneCatalog
from sqlframe.standalone.dataframe import StandaloneDataFrame
from sqlframe.standalone.readwriter import (
    StandaloneDataFrameReader,
    StandaloneDataFrameWriter,
)
from sqlframe.standalone.table import StandaloneTable
from sqlframe.standalone.udf import StandaloneUDFRegistration


class StandaloneSession(
    _BaseSession[  # type: ignore
        StandaloneCatalog,
        StandaloneDataFrameReader,
        StandaloneDataFrameWriter,
        StandaloneDataFrame,
        StandaloneTable,
        object,  # type: ignore
        StandaloneUDFRegistration,
    ]
):  # type: ignore
    _catalog = StandaloneCatalog
    _reader = StandaloneDataFrameReader
    _writer = StandaloneDataFrameWriter
    _df = StandaloneDataFrame
    _table = StandaloneTable
    _udf_registration = StandaloneUDFRegistration

    @property
    def _is_standalone(self) -> bool:
        return True

    class Builder(_BaseSession.Builder):
        DEFAULT_INPUT_DIALECT = "spark"
        DEFAULT_OUTPUT_DIALECT = "spark"

        @property
        def session(self) -> StandaloneSession:
            return StandaloneSession()

        def getOrCreate(self) -> StandaloneSession:
            return super().getOrCreate()  # type: ignore

    builder = Builder()
