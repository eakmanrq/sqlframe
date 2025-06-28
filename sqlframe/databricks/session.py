from __future__ import annotations

import logging
import typing as t

from sqlframe.base.session import _BaseSession
from sqlframe.databricks.catalog import DatabricksCatalog
from sqlframe.databricks.dataframe import DatabricksDataFrame
from sqlframe.databricks.readwriter import (
    DatabricksDataFrameReader,
    DatabricksDataFrameWriter,
)
from sqlframe.databricks.table import DatabricksTable
from sqlframe.databricks.udf import DatabricksUDFRegistration

if t.TYPE_CHECKING:
    from databricks.sql.client import Connection as DatabricksConnection
else:
    DatabricksConnection = t.Any


logger = logging.getLogger(__name__)


class DatabricksSession(
    _BaseSession[  # type: ignore
        DatabricksCatalog,
        DatabricksDataFrameReader,
        DatabricksDataFrameWriter,
        DatabricksDataFrame,
        DatabricksTable,
        DatabricksConnection,  # type: ignore
        DatabricksUDFRegistration,
    ],
):
    _catalog = DatabricksCatalog
    _reader = DatabricksDataFrameReader
    _writer = DatabricksDataFrameWriter
    _df = DatabricksDataFrame
    _table = DatabricksTable
    _udf_registration = DatabricksUDFRegistration

    def __init__(
        self,
        conn: t.Optional[DatabricksConnection] = None,
        server_hostname: t.Optional[str] = None,
        http_path: t.Optional[str] = None,
        access_token: t.Optional[str] = None,
        **kwargs: t.Any,
    ):
        from databricks import sql

        self._conn_kwargs = (
            {}
            if conn
            else {
                "server_hostname": server_hostname,
                "http_path": http_path,
                "access_token": access_token,
                "disable_pandas": True,
                **kwargs,
            }
        )

        if not hasattr(self, "_conn"):
            super().__init__(
                conn or sql.connect(**self._conn_kwargs),
            )

    def _execute(self, sql: str) -> None:
        from databricks.sql import connect
        from databricks.sql.exc import DatabaseError, RequestError

        try:
            super()._execute(sql)
        except (DatabaseError, RequestError) as e:
            logger.warning("Failed to execute query")
            if not self._is_session_expired_error(e):
                logger.error("Error is not related to session expiration, re-raising")
                raise e
            if self._conn_kwargs:
                logger.info("Attempting to reconnect with provided connection parameters")
                self._connection = connect(**self._conn_kwargs)
                # Clear the cached cursor
                if hasattr(self, "_cur"):
                    delattr(self, "_cur")
                super()._execute(sql)
            else:
                logger.error("No connection parameters provided so could not reconnect")
                raise

    def _is_session_expired_error(self, error: Exception) -> bool:
        error_str = str(error).lower()
        session_keywords = [
            "invalid sessionhandle",
            "session is closed",
            "session expired",
            "session not found",
            "sessionhandle",
        ]
        return any(keyword in error_str for keyword in session_keywords)

    @classmethod
    def _try_get_map(cls, value: t.Any) -> t.Optional[t.Dict[str, t.Any]]:
        if (
            value
            and isinstance(value, list)
            and all(isinstance(item, tuple) for item in value)
            and all(len(item) == 2 for item in value)
        ):
            return dict(value)
        return None

    @property
    def _is_databricks(self) -> bool:
        return True

    class Builder(_BaseSession.Builder):
        DEFAULT_EXECUTION_DIALECT = "databricks"

        @property
        def session(self) -> DatabricksSession:
            return DatabricksSession(**self._session_kwargs)

        def getOrCreate(self) -> DatabricksSession:
            return super().getOrCreate()  # type: ignore

    builder = Builder()
