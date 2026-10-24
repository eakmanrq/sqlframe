# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import typing as t

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.base.mixins.readwriter_mixins import PandasLoaderMixin
from sqlframe.base.readerwriter import _BaseDataFrameReader, _BaseDataFrameWriter, _infer_format

if t.TYPE_CHECKING:
    from sqlframe.base._typing import OptionalPrimitiveType, PathOrPaths
    from sqlframe.base.types import StructType
    from sqlframe.standalone.dataframe import StandaloneDataFrame
    from sqlframe.standalone.session import StandaloneSession
    from sqlframe.standalone.table import StandaloneTable


class StandaloneDataFrameReader(
    PandasLoaderMixin["StandaloneSession", "StandaloneDataFrame"],
    _BaseDataFrameReader["StandaloneSession", "StandaloneDataFrame", "StandaloneTable"],
):
    def load(
        self,
        path: t.Optional[PathOrPaths] = None,
        format: t.Optional[str] = None,
        schema: t.Optional[t.Union[StructType, str]] = None,
        **options: OptionalPrimitiveType,
    ) -> StandaloneDataFrame:
        if path is None:
            raise ValueError("path is required")
        if not isinstance(path, str):
            raise TypeError("path must be a string")
        format = format or self.state_format_to_read or _infer_format(path)
        if format != "parquet":
            raise UnsupportedOperationError(f"Unsupported format: {format}")
        return super().load(path, format, schema, **{**self.state_options, **options})


class StandaloneDataFrameWriter(_BaseDataFrameWriter["StandaloneSession", "StandaloneDataFrame"]):
    pass
