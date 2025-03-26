# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.helper import ensure_list

from sqlframe.base.readerwriter import (
    _BaseDataFrameReader,
    _BaseDataFrameWriter,
    _infer_format,
)
from sqlframe.base.util import ensure_column_mapping, generate_random_identifier, to_csv

if t.TYPE_CHECKING:
    from sqlframe.base._typing import OptionalPrimitiveType, PathOrPaths
    from sqlframe.base.types import StructType
    from sqlframe.spark.dataframe import SparkDataFrame
    from sqlframe.spark.session import SparkSession
    from sqlframe.spark.table import SparkTable


class SparkDataFrameReader(
    _BaseDataFrameReader["SparkSession", "SparkDataFrame", "SparkTable"],
):
    def load(
        self,
        path: t.Optional[PathOrPaths] = None,
        format: t.Optional[str] = None,
        schema: t.Optional[t.Union[StructType, str]] = None,
        **options: OptionalPrimitiveType,
    ) -> SparkDataFrame:
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
        assert path is not None, "path is required"
        assert isinstance(path, str), "path must be a string"

        # Merge state_options with provided options, with provided options taking precedence
        merged_options = {**self.state_options, **options}

        format = format or self.state_format_to_read or _infer_format(path)
        if schema:
            column_mapping = ensure_column_mapping(schema)
            select_column_mapping = column_mapping.copy()
            select_columns = [x.expression for x in self._to_casted_columns(select_column_mapping)]

            if hasattr(schema, "simpleString"):
                schema = schema.simpleString()
        else:
            select_columns = [exp.Star()]

        if format == "delta":
            from_clause = f"delta.`{path}`"
        elif format:
            paths = ",".join([f"{path}" for path in ensure_list(path)])
            tmp_view_key = merged_options.get(
                "_tmp_view_key_", f"{generate_random_identifier()}_vw"
            )
            merged_options["_tmp_view_key_"] = tmp_view_key

            format_options: dict[str, OptionalPrimitiveType] = {
                k: v for k, v in merged_options.items() if v is not None
            }
            format_options.pop("_tmp_view_key_")
            format_options["path"] = paths
            if schema:
                format_options["schema"] = f"{schema}"
                format_options.pop("inferSchema", None)
            format_options = {key: f"'{val}'" for key, val in format_options.items()}
            format_options_str = to_csv(format_options, " ")

            tmp_view = f"CREATE OR REPLACE TEMPORARY VIEW {tmp_view_key} USING {format}" + (
                f" OPTIONS ({format_options_str})" if format_options_str else ""
            )
            self.session.spark_session.sql(tmp_view).collect()

            from_clause = f"{tmp_view_key}"
        else:
            from_clause = f"'{path}'"

        df = self.session.sql(
            exp.select(*select_columns).from_(from_clause, dialect=self.session.input_dialect),
            qualify=False,
        )
        if select_columns == [exp.Star()] and df.schema:
            return self.load(path=path, format=format, schema=df.schema, **merged_options)
        self.session._last_loaded_file = path  # type: ignore
        return df


class SparkDataFrameWriter(
    _BaseDataFrameWriter["SparkSession", "SparkDataFrame"],
):
    def save(
        self,
        path: str,
        mode: t.Optional[str] = None,
        format: t.Optional[str] = None,
        partitionBy: t.Optional[t.Union[str, t.List[str]]] = None,
        **options,
    ):
        format = str(format or self._state_format_to_write)
        self._write(path, mode, format, partitionBy=partitionBy, **options)

    def _write(self, path: str, mode: t.Optional[str], format: str, **options):
        spark_df = None
        expressions = self._df._get_expressions()
        for i, expression in enumerate(expressions):
            if i < len(expressions) - 1:
                self._df.session._collect(expressions)
            else:
                sql = self._df.session._to_sql(expression)
                spark_df = self._session.spark_session.sql(sql)
        if spark_df is not None:
            options = {k: v for k, v in options.items() if v is not None}
            mode = str(mode or self._mode or "default")
            spark_writer = spark_df.write.format(format).mode(mode)
            partition_columns = options.pop("partitionBy", None)
            compression = options.pop("compression", None)
            if partition_columns:
                partition_columns = options.pop("partitionBy")
                spark_writer = spark_writer.partitionBy(*partition_columns)
            if compression:
                spark_writer = spark_writer.option("compression", compression)
            spark_writer.save(path=path, **options)
