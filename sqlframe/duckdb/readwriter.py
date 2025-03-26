# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import logging
import typing as t

from sqlglot import exp
from sqlglot.helper import ensure_list

from sqlframe.base.readerwriter import _BaseDataFrameReader, _BaseDataFrameWriter
from sqlframe.base.util import ensure_column_mapping, to_csv

if t.TYPE_CHECKING:
    from sqlframe.base._typing import OptionalPrimitiveType, PathOrPaths
    from sqlframe.base.types import StructType
    from sqlframe.duckdb.dataframe import DuckDBDataFrame
    from sqlframe.duckdb.session import DuckDBSession  # noqa
    from sqlframe.duckdb.table import DuckDBTable  # noqa

logger = logging.getLogger(__name__)


class DuckDBDataFrameReader(
    _BaseDataFrameReader["DuckDBSession", "DuckDBDataFrame", "DuckDBTable"]
):
    def load(
        self,
        path: t.Optional[PathOrPaths] = None,
        format: t.Optional[str] = None,
        schema: t.Optional[t.Union[StructType, str]] = None,
        **options: OptionalPrimitiveType,
    ) -> DuckDBDataFrame:
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
        # Merge state_options with provided options, with provided options taking precedence
        merged_options = {**self.state_options, **options}

        format = format or self.state_format_to_read
        if schema:
            column_mapping = ensure_column_mapping(schema)
            select_column_mapping = column_mapping.copy()
            if merged_options.get("filename"):
                select_column_mapping["filename"] = "VARCHAR"
            select_columns = [x.expression for x in self._to_casted_columns(select_column_mapping)]
            if format == "csv":
                duckdb_columns = ", ".join(
                    [f"'{column}': '{dtype}'" for column, dtype in column_mapping.items()]
                )
                merged_options["columns"] = "{" + duckdb_columns + "}"
        else:
            select_columns = [exp.Star()]
        if format == "delta":
            from_clause = f"delta_scan('{path}')"
        elif format:
            merged_options.pop("inferSchema", None)
            paths = ",".join([f"'{path}'" for path in ensure_list(path)])
            from_clause = f"read_{format}([{paths}], {to_csv(merged_options)})"
        else:
            from_clause = f"'{path}'"
        df = self.session.sql(exp.select(*select_columns).from_(from_clause), qualify=False)
        if select_columns == [exp.Star()]:
            return self.load(path=path, format=format, schema=df.schema, **merged_options)
        self.session._last_loaded_file = path  # type: ignore
        return df


class DuckDBDataFrameWriter(_BaseDataFrameWriter["DuckDBSession", "DuckDBDataFrame"]):
    def _write(self, path: str, mode: t.Optional[str], **options):  # type: ignore
        mode, skip = self._validate_mode(path, mode)
        if skip:
            return
        if mode == "append":
            raise NotImplementedError("Append mode not supported")
        options = to_csv(options, equality_char=" ")  # type: ignore
        expressions = self._df._get_expressions()
        for i, expression in enumerate(expressions):
            if i < len(expressions) - 1:
                self._df.session._collect(expressions)
            else:
                sql = self._df.session._to_sql(expression)
                self._df.session._collect(f"COPY ({sql}) TO '{path}' ({options})")
