from __future__ import annotations

import typing as t

from sqlframe.base.session import _BaseSession
from sqlframe.bigquery.catalog import BigQueryCatalog
from sqlframe.bigquery.dataframe import BigQueryDataFrame
from sqlframe.bigquery.readwriter import (
    BigQueryDataFrameReader,
    BigQueryDataFrameWriter,
)
from sqlframe.bigquery.table import BigQueryTable
from sqlframe.bigquery.udf import BigQueryUDFRegistration

if t.TYPE_CHECKING:
    from google.cloud.bigquery.client import Client as BigQueryClient
    from google.cloud.bigquery.dbapi.connection import Connection as BigQueryConnection
else:
    BigQueryClient = t.Any
    BigQueryConnection = t.Any


class BigQuerySession(
    _BaseSession[  # type: ignore
        BigQueryCatalog,
        BigQueryDataFrameReader,
        BigQueryDataFrameWriter,
        BigQueryDataFrame,
        BigQueryTable,
        BigQueryConnection,  # type: ignore
        BigQueryUDFRegistration,
    ],
):
    _catalog = BigQueryCatalog
    _reader = BigQueryDataFrameReader
    _writer = BigQueryDataFrameWriter
    _df = BigQueryDataFrame
    _table = BigQueryTable
    _udf_registration = BigQueryUDFRegistration

    QUALIFY_INFO_SCHEMA_WITH_DATABASE = True
    SANITIZE_COLUMN_NAMES = True

    def __init__(
        self, conn: t.Optional[BigQueryConnection] = None, default_dataset: t.Optional[str] = None
    ):
        from google.cloud import bigquery
        from google.cloud.bigquery.dbapi import connect

        if not hasattr(self, "_conn"):
            super().__init__(conn or connect())
            if self._client.default_query_job_config is None:
                self._client.default_query_job_config = bigquery.QueryJobConfig()
            self.default_dataset = default_dataset

    @property
    def _client(self) -> BigQueryClient:
        assert self._connection
        return self._connection._client

    @property
    def default_dataset(self) -> t.Optional[str]:
        return self._default_dataset

    @default_dataset.setter
    def default_dataset(self, dataset: str) -> None:
        self._default_dataset = dataset
        self._client.default_query_job_config.default_dataset = dataset  # type: ignore

    @property
    def default_project(self) -> str:
        return self._client.project

    @default_project.setter
    def default_project(self, project: str) -> None:
        self._client.project = project

    @classmethod
    def _try_get_map(cls, value: t.Any) -> t.Optional[t.Dict[str, t.Any]]:
        return None

    @property
    def _is_bigquery(self) -> bool:
        return True

    class Builder(_BaseSession.Builder):
        DEFAULT_EXECUTION_DIALECT = "bigquery"

        @property
        def session(self) -> BigQuerySession:
            return BigQuerySession(**self._session_kwargs)

        def getOrCreate(self) -> BigQuerySession:
            return super().getOrCreate()  # type: ignore

    builder = Builder()
