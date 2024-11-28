# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import typing as t

from sqlframe.base.mixins.readwriter_mixins import PandasLoaderMixin, PandasWriterMixin
from sqlframe.base.readerwriter import (
    _BaseDataFrameReader,
    _BaseDataFrameWriter,
)

if t.TYPE_CHECKING:
    from sqlframe.bigquery.session import BigQuerySession  # noqa
    from sqlframe.bigquery.dataframe import BigQueryDataFrame  # noqa
    from sqlframe.bigquery.table import BigQueryTable  # noqa


class BigQueryDataFrameReader(
    PandasLoaderMixin["BigQuerySession", "BigQueryDataFrame"],
    _BaseDataFrameReader["BigQuerySession", "BigQueryDataFrame", "BigQueryTable"],
):
    pass


class BigQueryDataFrameWriter(
    PandasWriterMixin["BigQuerySession", "BigQueryDataFrame"],
    _BaseDataFrameWriter["BigQuerySession", "BigQueryDataFrame"],
):
    pass
