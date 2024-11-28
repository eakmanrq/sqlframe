# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import typing as t

from sqlframe.base.mixins.readwriter_mixins import PandasLoaderMixin, PandasWriterMixin
from sqlframe.base.readerwriter import (
    _BaseDataFrameReader,
    _BaseDataFrameWriter,
)

if t.TYPE_CHECKING:
    from sqlframe.postgres.session import PostgresSession  # noqa
    from sqlframe.postgres.dataframe import PostgresDataFrame  # noqa
    from sqlframe.postgres.table import PostgresTable  # noqa


class PostgresDataFrameReader(
    PandasLoaderMixin["PostgresSession", "PostgresDataFrame"],
    _BaseDataFrameReader["PostgresSession", "PostgresDataFrame", "PostgresTable"],
):
    pass


class PostgresDataFrameWriter(
    PandasWriterMixin["PostgresSession", "PostgresDataFrame"],
    _BaseDataFrameWriter["PostgresSession", "PostgresDataFrame"],
):
    pass
