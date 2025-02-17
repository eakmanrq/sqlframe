from __future__ import annotations

import pathlib
import typing as t

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.base.readerwriter import (
    DF,
    SESSION,
    _BaseDataFrameReader,
    _BaseDataFrameWriter,
    _infer_format,
)
from sqlframe.base.util import pandas_to_spark_schema, verify_pandas_installed

if t.TYPE_CHECKING:
    from sqlframe.base._typing import OptionalPrimitiveType, PathOrPaths
    from sqlframe.base.types import StructType


class PandasLoaderMixin(_BaseDataFrameReader, t.Generic[SESSION, DF]):
    def load(
        self,
        path: t.Optional[PathOrPaths] = None,
        format: t.Optional[str] = None,
        schema: t.Optional[t.Union[StructType, str]] = None,
        **options: OptionalPrimitiveType,
    ) -> DF:
        """Loads data from a data source and returns it as a :class:`DataFrame`.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str or list, t.Optional
            t.Optional string or a list of string for file-system backed data sources.
        format : str, t.Optional
            t.Optional string for format of the data source. Default to 'parquet'.
        schema : :class:`pyspark.sql.types.StructType` or str, t.Optional
            t.Optional :class:`pyspark.sql.types.StructType` for the input schema
            or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).
        **options : dict
            all other string options

        Examples
        --------
        Load a CSV file with format, schema and options specified.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a CSV file with a header
        ...     df = spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}])
        ...     df.write.option("header", True).mode("overwrite").format("csv").save(d)
        ...
        ...     # Read the CSV file as a DataFrame with 'nullValue' option set to 'Hyukjin Kwon',
        ...     # and 'header' option set to `True`.
        ...     df = spark.read.load(
        ...         d, schema=df.schema, format="csv", nullValue="Hyukjin Kwon", header=True)
        ...     df.printSchema()
        ...     df.show()
        root
         |-- age: long (nullable = true)
         |-- name: string (nullable = true)
        +---+----+
        |age|name|
        +---+----+
        |100|NULL|
        +---+----+
        """
        verify_pandas_installed()
        import pandas as pd

        assert path is not None, "path is required"
        assert isinstance(path, str), "path must be a string"
        format = format or self.state_format_to_read or _infer_format(path)
        kwargs = {k: v for k, v in options.items() if v is not None}
        if format == "json":
            df = pd.read_json(path, lines=True, **kwargs)  # type: ignore
        elif format == "parquet":
            df = pd.read_parquet(path, **kwargs)  # type: ignore
        elif format == "csv":
            kwargs.pop("inferSchema", None)
            if "header" in kwargs:
                if isinstance(kwargs["header"], bool) and kwargs["header"]:
                    kwargs["header"] = "infer"
            df = pd.read_csv(path, **kwargs)  # type: ignore
        else:
            raise UnsupportedOperationError(f"Unsupported format: {format}")
        schema = schema or pandas_to_spark_schema(df)
        self.session._last_loaded_file = path
        return self._session.createDataFrame(list(df.itertuples(index=False)), schema)


class PandasWriterMixin(_BaseDataFrameWriter, t.Generic[SESSION, DF]):
    def _write(self, path: str, mode: t.Optional[str], format: str, **options):  # type: ignore
        mode, skip = self._validate_mode(path, mode)
        if skip:
            return
        pandas_df = self._df.toPandas()
        mode = self._mode_to_pandas_mode(mode)
        kwargs = {k: v for k, v in options.items() if v is not None}
        kwargs["index"] = False
        if format == "csv":
            kwargs["mode"] = mode
            if mode == "a" and pathlib.Path(path).exists():
                kwargs["header"] = False
            pandas_df.to_csv(path, **kwargs)
        elif format == "parquet":
            if mode == "a":
                raise NotImplementedError("Append mode is not supported for parquet.")
            pandas_df.to_parquet(path, **kwargs)
        elif format == "json":
            # Pandas versions are inconsistent on how to handle True/False index so we just remove it
            # since in all versions it will not result in an index column in the output.
            del kwargs["index"]
            kwargs["mode"] = mode
            kwargs["orient"] = "records"
            pandas_df.to_json(path, lines=True, **kwargs)
        else:
            raise NotImplementedError(f"Unsupported format: {format}")
