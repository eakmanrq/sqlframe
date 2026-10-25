# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import typing as t

from sqlframe.base import types
from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.base.readerwriter import _BaseDataFrameReader, _BaseDataFrameWriter

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
        format = (format or self.state_format_to_read or "parquet").lower()
        if format != "parquet":
            raise UnsupportedOperationError(f"Unsupported format: {format}")
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ModuleNotFoundError as e:
            if not e.name or (e.name != "pyarrow" and not e.name.startswith("pyarrow.")):
                raise
            raise ImportError(
                'Standalone Parquet reading requires pyarrow. Install "sqlframe[standalone]".'
            ) from e

        kwargs = {k: v for k, v in {**self.state_options, **options}.items() if v is not None}
        # These Spark options have no Arrow equivalent. Reject them rather than
        # silently changing schema merging, timestamp rebasing, or file selection.
        spark_options = {
            "mergeschema",
            "datetimerebasemode",
            "int96rebasemode",
            "pathglobfilter",
            "recursivefilelookup",
        }
        for option in kwargs:
            if option.lower() in spark_options:
                raise UnsupportedOperationError(
                    f"Unsupported standalone Parquet reader option: {option}. "
                    "Remove this option or use the Spark engine to honor its semantics."
                )
        table = pq.read_table(path, **kwargs)
        rows = table.to_pylist()
        if schema is None:

            def spark_type(dtype: pa.DataType) -> types.DataType:
                primitive_types = {
                    pa.bool_(): types.BooleanType(),
                    pa.int8(): types.ByteType(),
                    pa.int16(): types.ShortType(),
                    pa.int32(): types.IntegerType(),
                    pa.int64(): types.LongType(),
                    pa.float32(): types.FloatType(),
                    pa.float64(): types.DoubleType(),
                    pa.string(): types.StringType(),
                    pa.large_string(): types.StringType(),
                    pa.binary(): types.BinaryType(),
                    pa.large_binary(): types.BinaryType(),
                    pa.date32(): types.DateType(),
                    pa.date64(): types.DateType(),
                }
                if dtype in primitive_types:
                    return primitive_types[dtype]
                if pa.types.is_timestamp(dtype):
                    return types.TimestampType() if dtype.tz else types.TimestampNTZType()
                if pa.types.is_decimal(dtype):
                    return types.DecimalType(dtype.precision, dtype.scale)
                if (
                    pa.types.is_list(dtype)
                    or pa.types.is_large_list(dtype)
                    or pa.types.is_fixed_size_list(dtype)
                ):
                    return types.ArrayType(spark_type(dtype.value_type), dtype.value_field.nullable)
                if pa.types.is_struct(dtype):
                    return types.StructType(
                        [
                            types.StructField(field.name, spark_type(field.type), field.nullable)
                            for field in dtype
                        ]
                    )
                raise UnsupportedOperationError(f"Unsupported Parquet type: {dtype}")

            # Use the loaded table schema so projected columns match the Python rows.
            schema = types.StructType(
                [
                    types.StructField(field.name, spark_type(field.type), field.nullable)
                    for field in table.schema
                ]
            )

        def struct_rows(value: t.Any) -> t.Any:
            if isinstance(value, dict):
                # Positional construction avoids Row's decimal-to-float coercion.
                row = types.Row(*(struct_rows(v) for v in value.values()))
                row.__fields__ = list(value)
                return row
            if isinstance(value, list):
                return [struct_rows(item) for item in value]
            return value

        rows = [{name: struct_rows(value) for name, value in row.items()} for row in rows]
        self.session._last_loaded_file = path
        # Arrow retains the distinction between a Parquet NULL and a floating NaN.
        return self._session.createDataFrame(rows, schema)


class StandaloneDataFrameWriter(_BaseDataFrameWriter["StandaloneSession", "StandaloneDataFrame"]):
    pass
