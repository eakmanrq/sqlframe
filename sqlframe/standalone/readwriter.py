# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import typing as t

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.base.readerwriter import _BaseDataFrameReader, _BaseDataFrameWriter, _infer_format
from sqlframe.base.util import pandas_to_spark_schema, verify_pandas_installed

if t.TYPE_CHECKING:
    from sqlframe.base._typing import OptionalPrimitiveType, PathOrPaths
    from sqlframe.base.types import StructType
    from sqlframe.standalone.dataframe import StandaloneDataFrame
    from sqlframe.standalone.session import StandaloneSession
    from sqlframe.standalone.table import StandaloneTable


class StandaloneDataFrameReader(
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
        verify_pandas_installed()
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        kwargs = {k: v for k, v in {**self.state_options, **options}.items() if v is not None}
        table = pq.read_table(path, **kwargs)
        if schema is None:
            # Nullable integers must not become floats during schema inference.
            integer_dtypes = {pa.int64(): pd.Int64Dtype(), pa.int32(): pd.Int32Dtype()}
            schema = pandas_to_spark_schema(table.to_pandas(types_mapper=integer_dtypes.get))
        self.session._last_loaded_file = path
        # Arrow retains the distinction between a Parquet NULL and a floating NaN.
        return self._session.createDataFrame(table.to_pylist(), schema)


class StandaloneDataFrameWriter(_BaseDataFrameWriter["StandaloneSession", "StandaloneDataFrame"]):
    pass
