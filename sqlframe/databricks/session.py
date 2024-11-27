from __future__ import annotations

import typing as t
import warnings

from sqlframe.base.session import _BaseSession
from sqlframe.databricks.catalog import DatabricksCatalog
from sqlframe.databricks.dataframe import DatabricksDataFrame
from sqlframe.databricks.readwriter import (
    DatabricksDataFrameReader,
    DatabricksDataFrameWriter,
)
from sqlframe.databricks.udf import DatabricksUDFRegistration

if t.TYPE_CHECKING:
    from databricks.sql.client import Connection as DatabricksConnection
else:
    DatabricksConnection = t.Any


class DatabricksSession(
    _BaseSession[  # type: ignore
        DatabricksCatalog,
        DatabricksDataFrameReader,
        DatabricksDataFrameWriter,
        DatabricksDataFrame,
        DatabricksConnection,
        DatabricksUDFRegistration,
    ],
):
    _catalog = DatabricksCatalog
    _reader = DatabricksDataFrameReader
    _writer = DatabricksDataFrameWriter
    _df = DatabricksDataFrame
    _udf_registration = DatabricksUDFRegistration

    def __init__(
        self,
        conn: t.Optional[DatabricksConnection] = None,
        server_hostname: t.Optional[str] = None,
        http_path: t.Optional[str] = None,
        access_token: t.Optional[str] = None,
    ):
        from databricks import sql

        if not hasattr(self, "_conn"):
            super().__init__(conn or sql.connect(server_hostname, http_path, access_token))

    class Builder(_BaseSession.Builder):
        DEFAULT_EXECUTION_DIALECT = "databricks"

        @property
        def session(self) -> DatabricksSession:
            return DatabricksSession(**self._session_kwargs)

        def getOrCreate(self) -> DatabricksSession:
            return super().getOrCreate()  # type: ignore

    builder = Builder()
