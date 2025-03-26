# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import sys
import typing as t

import sqlglot as sg
from databricks.sql import ServerOperationError
from sqlglot import exp
from sqlglot.helper import ensure_list

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from sqlframe.base.mixins.readwriter_mixins import PandasLoaderMixin, PandasWriterMixin
from sqlframe.base.readerwriter import (
    _BaseDataFrameReader,
    _BaseDataFrameWriter,
    _infer_format,
)
from sqlframe.base.util import (
    ensure_column_mapping,
    generate_random_identifier,
    normalize_string,
    split_filepath,
    to_csv,
)

if t.TYPE_CHECKING:
    from sqlframe.base._typing import OptionalPrimitiveType, PathOrPaths
    from sqlframe.base.types import StructType
    from sqlframe.databricks.dataframe import DatabricksDataFrame  # noqa
    from sqlframe.databricks.session import DatabricksSession  # noqa
    from sqlframe.databricks.table import DatabricksTable  # noqa


class DatabricksDataFrameReader(
    PandasLoaderMixin["DatabricksSession", "DatabricksDataFrame"],
    _BaseDataFrameReader["DatabricksSession", "DatabricksDataFrame", "DatabricksTable"],
):
    def load(
        self,
        path: t.Optional[PathOrPaths] = None,
        format: t.Optional[str] = None,
        schema: t.Optional[t.Union[StructType, str]] = None,
        **options: OptionalPrimitiveType,
    ) -> DatabricksDataFrame:
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
        fs_prefix, filepath = split_filepath(path)

        if fs_prefix == "":
            return super().load(path, format, schema, **merged_options)

        if schema:
            column_mapping = ensure_column_mapping(schema)
            select_column_mapping = column_mapping.copy()
            select_columns = [x.expression for x in self._to_casted_columns(select_column_mapping)]

            if hasattr(schema, "simpleString"):
                schema = schema.simpleString()
        else:
            select_columns = [exp.Star()]

        if format == "delta":
            from_clause = f"delta.`{fs_prefix + filepath}`"
        elif format:
            paths = ",".join([f"{path}" for path in ensure_list(path)])

            format_options: dict[str, OptionalPrimitiveType] = {
                k: v for k, v in merged_options.items() if v is not None
            }
            format_options["format"] = format
            format_options["schemaEvolutionMode"] = "none"
            if schema:
                format_options["schema"] = f"{schema}"
            if "inferSchema" in format_options:
                format_options["inferColumnTypes"] = format_options.pop("inferSchema")

            format_options = {key: f"'{val}'" for key, val in format_options.items()}
            format_options_str = to_csv(format_options, " => ")

            from_clause = f"read_files('{paths}', {format_options_str})"
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


class DatabricksDataFrameWriter(
    PandasWriterMixin["DatabricksSession", "DatabricksDataFrame"],
    _BaseDataFrameWriter["DatabricksSession", "DatabricksDataFrame"],
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

    def _write(self, path: str, mode: t.Optional[str], format: str, **options):  # type: ignore
        fs_prefix, filepath = split_filepath(path)
        if fs_prefix == "":
            super()._write(filepath, mode, format, **options)
        elif format == "delta":
            self.saveAsTable(f"delta.`{fs_prefix + filepath}`", format, mode, **options)
        else:
            mode = str(mode or self._mode or "error")
            partition_by = options.pop("partitionBy", None)
            tmp_table = f"_{generate_random_identifier()}_tmp"
            drop_expr = exp.Drop(
                this=exp.to_table(tmp_table, dialect=self._session.input_dialect),
                kind="TABLE",
                exists=True,
            )
            if mode == "append" or mode == "default":
                try:
                    self._session.catalog.createTable(
                        tmp_table,
                        path=fs_prefix + filepath,
                        source=format,
                        **options,
                    )
                    self.byName.insertInto(tmp_table)
                except ServerOperationError as e:
                    if "UNABLE_TO_INFER_SCHEMA" in str(e):
                        self.saveAsTable(
                            tmp_table,
                            format=format,
                            mode="error",
                            path=fs_prefix + filepath,
                            **options,
                        )
                    else:
                        raise e
                finally:
                    self._df.session._collect(drop_expr)
            elif mode == "error" or mode == "errorifexists":
                try:
                    self._session.catalog.createTable(
                        tmp_table,
                        path=fs_prefix + filepath,
                        source=format,
                        **options,
                    )
                    raise FileExistsError(f"Path already exists: {fs_prefix + filepath}")
                except ServerOperationError as e:
                    if "UNABLE_TO_INFER_SCHEMA" in str(e):
                        self.saveAsTable(
                            tmp_table,
                            format=format,
                            mode=mode,
                            path=fs_prefix + filepath,
                            **options,
                        )
                finally:
                    self._df.session._collect(drop_expr)
            elif mode == "overwrite":
                try:
                    self.saveAsTable(
                        tmp_table,
                        format=format,
                        mode=mode,
                        path=fs_prefix + filepath,
                        partitionBy=partition_by,
                        **options,
                    )
                finally:
                    self._df.session._collect(drop_expr)
            elif mode == "ignore":
                pass
            else:
                raise RuntimeError(f"Unssuported mode: {mode}")

    def insertInto(
        self,
        tableName: str,
        overwrite: t.Optional[bool] = None,
        replaceWhere: t.Optional[str] = None,
    ) -> Self:
        from sqlframe.base.session import _BaseSession

        tableName = normalize_string(tableName, from_dialect="input", is_table=True)
        output_expression_container = exp.Insert(
            **{
                **{
                    "this": exp.to_table(tableName, dialect=_BaseSession().input_dialect),
                    "overwrite": overwrite,
                },
                **(
                    {
                        "by_name": self._by_name,
                    }
                    if self._by_name
                    else {}
                ),
                **({"where": sg.parse_one(replaceWhere)} if replaceWhere else {}),
            }
        )
        df = self._df.copy(output_expression_container=output_expression_container)
        if self._by_name:
            columns = self._session.catalog._schema.column_names(
                tableName, only_visible=True, dialect=_BaseSession().input_dialect
            )
            df = df._convert_leaf_to_cte().select(*columns)

        if self._session._has_connection:
            df.collect()
        return self.copy(_df=df)

    def saveAsTable(
        self,
        name: str,
        format: t.Optional[str] = None,
        mode: t.Optional[str] = None,
        partitionBy: t.Optional[t.Union[str, t.List[str]]] = None,
        clusterBy: t.Optional[t.Union[str, t.List[str]]] = None,
        **options: OptionalPrimitiveType,
    ):
        format = (format or self._state_format_to_write or "delta").lower()
        table_properties: t.Union[OptionalPrimitiveType, t.Dict[str, OptionalPrimitiveType]] = (
            options.pop("properties", {})
        )
        path: OptionalPrimitiveType = options.pop("path", None)
        if path is not None and not isinstance(path, str):
            raise ValueError("path must be a string")

        replace_where: OptionalPrimitiveType = options.pop("replaceWhere", None)
        if replace_where is not None and not isinstance(replace_where, str):
            raise ValueError("replaceWhere must be a string")

        exists, replace, mode = None, None, str(mode or self._mode or "error")
        if mode == "append":
            self._session.catalog.createTable(
                name,
                path=path,
                source=format,
                schema=self._df.schema,
                partitionBy=partitionBy,
                clusterBy=clusterBy,
                exists="true",
                **options,
            )
            self.insertInto(name, replaceWhere=replace_where)
            return
        if mode == "ignore":
            exists = True
        if mode == "overwrite":
            replace = True

        name = normalize_string(name, from_dialect="input", is_table=True)

        properties: t.List[exp.Expression] = []
        if partitionBy is not None:
            if isinstance(partitionBy, str):
                partition_by = [partitionBy]
            else:
                partition_by = partitionBy
            properties.append(
                exp.PartitionedByProperty(
                    this=exp.Tuple(expressions=list(map(sg.to_identifier, partition_by)))
                )
            )
        if clusterBy is not None:
            if isinstance(clusterBy, str):
                cluster_by = [clusterBy]
            else:
                cluster_by = clusterBy
            properties.append(
                exp.Cluster(
                    expressions=[exp.Tuple(expressions=list(map(sg.to_identifier, cluster_by)))]
                )
            )

        format_options_str = ""
        if format is not None:
            properties.append(exp.FileFormatProperty(this=exp.Var(this=format.upper())))
            format_options: dict[str, OptionalPrimitiveType] = {
                key: f"'{val}'" for key, val in options.items() if val is not None
            }
            format_options_str = to_csv(format_options, " ")

        if path is not None and isinstance(path, str):
            properties.append(exp.LocationProperty(this=exp.convert(path)))
            if replace and format != "delta":
                replace = None
                drop_expression = exp.Drop(
                    this=exp.to_table(name, dialect=self._session.input_dialect),
                    kind="TABLE",
                    exists=True,
                )
                if self._session._has_connection:
                    self._session._collect(drop_expression)

        properties.extend(
            exp.Property(this=sg.to_identifier(name), value=exp.convert(value))
            for name, value in (
                (table_properties if isinstance(table_properties, dict) else {}).items()
            )
        )

        output_expression_container = exp.Create(
            this=exp.to_table(name, dialect=self._session.input_dialect),
            kind="TABLE",
            exists=exists,
            replace=replace,
            properties=exp.Properties(expressions=properties),
        )
        if self._session._has_connection:
            create_sql = self._session._to_sql(output_expression_container, quote_identifiers=True)
            df_sql = self._df.sql(self._session.execution_dialect, False, False)
            sql = (
                create_sql
                + (f" OPTIONS ({format_options_str})" if format_options_str else "")
                + " AS "
                + df_sql
            )
            self._session._collect(sql)
