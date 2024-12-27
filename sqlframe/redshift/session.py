from __future__ import annotations

import typing as t
import warnings

from sqlframe.base.session import _BaseSession
from sqlframe.redshift.catalog import RedshiftCatalog
from sqlframe.redshift.dataframe import RedshiftDataFrame
from sqlframe.redshift.readwriter import (
    RedshiftDataFrameReader,
    RedshiftDataFrameWriter,
)
from sqlframe.redshift.table import RedshiftTable
from sqlframe.redshift.udf import RedshiftUDFRegistration

if t.TYPE_CHECKING:
    from redshift_connector.core import Connection as RedshiftConnection
else:
    RedshiftConnection = t.Any


class RedshiftSession(
    _BaseSession[  # type: ignore
        RedshiftCatalog,
        RedshiftDataFrameReader,
        RedshiftDataFrameWriter,
        RedshiftDataFrame,
        RedshiftTable,
        RedshiftConnection,
        RedshiftUDFRegistration,
    ],
):
    _catalog = RedshiftCatalog
    _reader = RedshiftDataFrameReader
    _writer = RedshiftDataFrameWriter
    _df = RedshiftDataFrame
    _table = RedshiftTable
    _udf_registration = RedshiftUDFRegistration

    def __init__(self, conn: t.Optional[RedshiftConnection] = None):
        warnings.warn(
            "RedshiftSession is still in active development. Functions may not work as expected."
        )
        if not hasattr(self, "_conn"):
            super().__init__(conn)

    @property
    def _is_redshift(self) -> bool:
        return True

    class Builder(_BaseSession.Builder):
        DEFAULT_EXECUTION_DIALECT = "redshift"

        @property
        def session(self) -> RedshiftSession:
            return RedshiftSession(**self._session_kwargs)

        def getOrCreate(self) -> RedshiftSession:
            return super().getOrCreate()  # type: ignore

    builder = Builder()
