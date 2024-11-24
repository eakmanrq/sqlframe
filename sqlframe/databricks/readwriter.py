# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import typing as t

from sqlframe.base.mixins.readwriter_mixins import PandasLoaderMixin, PandasWriterMixin
from sqlframe.base.readerwriter import (
    _BaseDataFrameReader,
    _BaseDataFrameWriter,
)

if t.TYPE_CHECKING:
    from sqlframe.databricks.session import DatabricksSession  # noqa
    from sqlframe.databricks.dataframe import DatabricksDataFrame  # noqa


class DatabricksDataFrameReader(
    PandasLoaderMixin["DatabricksSession", "DatabricksDataFrame"],
    _BaseDataFrameReader["DatabricksSession", "DatabricksDataFrame"],
):
    pass


class DatabricksDataFrameWriter(
    PandasWriterMixin["DatabricksSession", "DatabricksDataFrame"],
    _BaseDataFrameWriter["DatabricksSession", "DatabricksDataFrame"],
):
    pass
