# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import typing as t

from sqlframe.base.mixins.readwriter_mixins import PandasLoaderMixin, PandasWriterMixin
from sqlframe.base.readerwriter import (
    _BaseDataFrameReader,
    _BaseDataFrameWriter,
)

if t.TYPE_CHECKING:
    from sqlframe.snowflake.session import SnowflakeSession  # noqa
    from sqlframe.snowflake.dataframe import SnowflakeDataFrame  # noqa
    from sqlframe.snowflake.table import SnowflakeTable  # noqa


class SnowflakeDataFrameReader(
    PandasLoaderMixin["SnowflakeSession", "SnowflakeDataFrame"],
    _BaseDataFrameReader["SnowflakeSession", "SnowflakeDataFrame", "SnowflakeTable"],
):
    pass


class SnowflakeDataFrameWriter(
    PandasWriterMixin["SnowflakeSession", "SnowflakeDataFrame"],
    _BaseDataFrameWriter["SnowflakeSession", "SnowflakeDataFrame"],
):
    pass
