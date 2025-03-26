# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import logging
import pathlib
import sys
import typing as t
from functools import reduce

from sqlglot import exp
from sqlglot.helper import object_to_dict

from sqlframe.base.util import normalize_string

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if t.TYPE_CHECKING:
    from sqlframe.base._typing import OptionalPrimitiveType, PathOrPaths
    from sqlframe.base.column import Column
    from sqlframe.base.session import DF, TABLE, _BaseSession
    from sqlframe.base.types import StructType

    SESSION = t.TypeVar("SESSION", bound=_BaseSession)
else:
    SESSION = t.TypeVar("SESSION")
    DF = t.TypeVar("DF")
    TABLE = t.TypeVar("TABLE")


logger = logging.getLogger(__name__)


class _BaseDataFrameReader(t.Generic[SESSION, DF, TABLE]):
    def __init__(self, spark: SESSION):
        self._session = spark
        self.state_format_to_read: t.Optional[str] = None
        self.state_options: t.Dict[str, OptionalPrimitiveType] = {}

    @property
    def session(self) -> SESSION:
        return self._session

    def table(self, tableName: str) -> TABLE:
        tableName = normalize_string(tableName, from_dialect="input", is_table=True)
        if df := self.session.temp_views.get(tableName):
            return df
        table = exp.to_table(tableName, dialect=self.session.input_dialect).assert_is(exp.Table)
        self.session.catalog.add_table(table)
        columns = self.session.catalog.get_columns_from_schema(table)

        return self.session._create_table(
            exp.Select()
            .from_(tableName, dialect=self.session.input_dialect)
            .select(*columns, dialect=self.session.input_dialect)
        )

    def _to_casted_columns(self, column_mapping: t.Dict) -> t.List[Column]:
        from sqlframe.base.column import Column

        return [
            Column(
                exp.cast(
                    exp.to_column(k), to=exp.DataType.build(v, dialect=self.session.input_dialect)
                ).as_(k)
            )
            for k, v in column_mapping.items()
        ]

    def format(self, source: str) -> "Self":
        """Specifies the input data source format.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        source : str
            string, name of the data source, e.g. 'json', 'parquet'.

        Examples
        --------
        >>> spark.read.format('json')
        <...readwriter.DataFrameReader object ...>

        Write a DataFrame into a JSON file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a JSON file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").format("json").save(d)
        ...
        ...     # Read the JSON file as a DataFrame.
        ...     spark.read.format('json').load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        self.state_format_to_read = source
        return self

    def options(self, **options: OptionalPrimitiveType) -> "Self":
        """Adds input options for the underlying data source.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        **options : dict
            The dictionary of string keys and primitive-type values.

        Examples
        --------
        >>> spark.read.options(inferSchema=True, header=True)
        <...readwriter.DataFrameReader object ...>

        Specify the option 'nullValue' and 'header' with reading a CSV file.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a CSV file with a header.
        ...     df = spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}])
        ...     df.write.option("header", True).mode("overwrite").format("csv").save(d)
        ...
        ...     # Read the CSV file as a DataFrame with 'nullValue' option set to 'Hyukjin Kwon',
        ...     # and 'header' option set to `True`.
        ...     spark.read.options(
        ...         nullValue="Hyukjin Kwon",
        ...         header=True
        ...     ).format('csv').load(d).show()
        +---+----+
        |age|name|
        +---+----+
        |100|NULL|
        +---+----+
        """

        self.state_options = {**self.state_options, **options}
        return self

    def option(self, key: str, value: OptionalPrimitiveType) -> "Self":
        """Adds an input option for the underlying data source.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        key : str
            The key of the option.
        value :
            The value of the option.

        Examples
        --------
        >>> spark.read.option("inferSchema", True)
        <...readwriter.DataFrameReader object ...>

        Specify the option 'nullValue' and 'header' with reading a CSV file.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a CSV file with a header.
        ...     df = spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}])
        ...     df.write.option("header", True).mode("overwrite").format("csv").save(d)
        ...
        ...     # Read the CSV file as a DataFrame with 'nullValue' option set to 'Hyukjin Kwon',
        ...     # and 'header' option set to `True`.
        ...     spark.read.option("nullValue", "Hyukjin Kwon").option("header", True).format('csv').load(d).show()
        +---+----+
        |age|name|
        +---+----+
        |100|NULL|
        +---+----+
        """
        self.state_options[key] = value
        return self

    def load(
        self,
        path: t.Optional[PathOrPaths] = None,
        format: t.Optional[str] = None,
        schema: t.Optional[t.Union[StructType, str]] = None,
        **options: OptionalPrimitiveType,
    ) -> DF:
        raise NotImplementedError()

    def json(
        self,
        path: t.Union[str, t.List[str]],
        schema: t.Optional[t.Union[StructType, str]] = None,
        primitivesAsString: t.Optional[t.Union[bool, str]] = None,
        prefersDecimal: t.Optional[t.Union[bool, str]] = None,
        allowComments: t.Optional[t.Union[bool, str]] = None,
        allowUnquotedFieldNames: t.Optional[t.Union[bool, str]] = None,
        allowSingleQuotes: t.Optional[t.Union[bool, str]] = None,
        allowNumericLeadingZero: t.Optional[t.Union[bool, str]] = None,
        allowBackslashEscapingAnyCharacter: t.Optional[t.Union[bool, str]] = None,
        mode: t.Optional[str] = None,
        columnNameOfCorruptRecord: t.Optional[str] = None,
        dateFormat: t.Optional[str] = None,
        timestampFormat: t.Optional[str] = None,
        multiLine: t.Optional[t.Union[bool, str]] = None,
        allowUnquotedControlChars: t.Optional[t.Union[bool, str]] = None,
        lineSep: t.Optional[str] = None,
        samplingRatio: t.Optional[t.Union[float, str]] = None,
        dropFieldIfAllNull: t.Optional[t.Union[bool, str]] = None,
        encoding: t.Optional[str] = None,
        locale: t.Optional[str] = None,
        pathGlobFilter: t.Optional[t.Union[bool, str]] = None,
        recursiveFileLookup: t.Optional[t.Union[bool, str]] = None,
        modifiedBefore: t.Optional[t.Union[bool, str]] = None,
        modifiedAfter: t.Optional[t.Union[bool, str]] = None,
        allowNonNumericNumbers: t.Optional[t.Union[bool, str]] = None,
    ) -> DF:
        """
        Loads JSON files and returns the results as a :class:`DataFrame`.

        `JSON Lines <http://jsonlines.org/>`_ (newline-delimited JSON) is supported by default.
        For JSON (one record per file), set the ``multiLine`` parameter to ``true``.

        If the ``schema`` parameter is not specified, this function goes
        through the input once to determine the input schema.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str, list or :class:`RDD`
            string represents path to the JSON dataset, or a list of paths,
            or RDD of Strings storing JSON objects.
        schema : :class:`pyspark.sql.types.StructType` or str, t.Optional
            an t.Optional :class:`pyspark.sql.types.StructType` for the input schema or
            a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a JSON file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a JSON file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").format("json").save(d)
        ...
        ...     # Read the JSON file as a DataFrame.
        ...     spark.read.json(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        options = dict(
            primitivesAsString=primitivesAsString,
            prefersDecimal=prefersDecimal,
            allowComments=allowComments,
            allowUnquotedFieldNames=allowUnquotedFieldNames,
            allowSingleQuotes=allowSingleQuotes,
            allowNumericLeadingZero=allowNumericLeadingZero,
            allowBackslashEscapingAnyCharacter=allowBackslashEscapingAnyCharacter,
            mode=mode,
            columnNameOfCorruptRecord=columnNameOfCorruptRecord,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            multiLine=multiLine,
            allowUnquotedControlChars=allowUnquotedControlChars,
            lineSep=lineSep,
            samplingRatio=samplingRatio,
            dropFieldIfAllNull=dropFieldIfAllNull,
            encoding=encoding,
            locale=locale,
            pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup,
            modifiedBefore=modifiedBefore,
            modifiedAfter=modifiedAfter,
            allowNonNumericNumbers=allowNonNumericNumbers,
        )
        # Apply previously set options, with method-specific options taking precedence
        all_options = {**self.state_options, **{k: v for k, v in options.items() if v is not None}}
        return self.load(path=path, format="json", schema=schema, **all_options)

    def parquet(self, *paths: str, **options: OptionalPrimitiveType) -> DF:
        """
        Loads Parquet files, returning the result as a :class:`DataFrame`.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        paths : str

        Other Parameters
        ----------------
        **options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a Parquet file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a Parquet file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").format("parquet").save(d)
        ...
        ...     # Read the Parquet file as a DataFrame.
        ...     spark.read.parquet(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        all_options = {**self.state_options, **{k: v for k, v in options.items() if v is not None}}
        dfs = [self.load(path=path, format="parquet", **all_options) for path in paths]  # type: ignore
        return reduce(lambda a, b: a.union(b), dfs)

    def csv(
        self,
        path: PathOrPaths,
        schema: t.Optional[t.Union[StructType, str]] = None,
        sep: t.Optional[str] = None,
        encoding: t.Optional[str] = None,
        quote: t.Optional[str] = None,
        escape: t.Optional[str] = None,
        comment: t.Optional[str] = None,
        header: t.Optional[t.Union[bool, str]] = None,
        inferSchema: t.Optional[t.Union[bool, str]] = None,
        ignoreLeadingWhiteSpace: t.Optional[t.Union[bool, str]] = None,
        ignoreTrailingWhiteSpace: t.Optional[t.Union[bool, str]] = None,
        nullValue: t.Optional[str] = None,
        nanValue: t.Optional[str] = None,
        positiveInf: t.Optional[str] = None,
        negativeInf: t.Optional[str] = None,
        dateFormat: t.Optional[str] = None,
        timestampFormat: t.Optional[str] = None,
        maxColumns: t.Optional[t.Union[int, str]] = None,
        maxCharsPerColumn: t.Optional[t.Union[int, str]] = None,
        maxMalformedLogPerPartition: t.Optional[t.Union[int, str]] = None,
        mode: t.Optional[str] = None,
        columnNameOfCorruptRecord: t.Optional[str] = None,
        multiLine: t.Optional[t.Union[bool, str]] = None,
        charToEscapeQuoteEscaping: t.Optional[str] = None,
        samplingRatio: t.Optional[t.Union[float, str]] = None,
        enforceSchema: t.Optional[t.Union[bool, str]] = None,
        emptyValue: t.Optional[str] = None,
        locale: t.Optional[str] = None,
        lineSep: t.Optional[str] = None,
        pathGlobFilter: t.Optional[t.Union[bool, str]] = None,
        recursiveFileLookup: t.Optional[t.Union[bool, str]] = None,
        modifiedBefore: t.Optional[t.Union[bool, str]] = None,
        modifiedAfter: t.Optional[t.Union[bool, str]] = None,
        unescapedQuoteHandling: t.Optional[str] = None,
    ) -> DF:
        r"""Loads a CSV file and returns the result as a  :class:`DataFrame`.

        This function will go through the input once to determine the input schema if
        ``inferSchema`` is enabled. To avoid going through the entire data once, disable
        ``inferSchema`` option or specify the schema explicitly using ``schema``.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str or list
            string, or list of strings, for input path(s),
            or RDD of Strings storing CSV rows.
        schema : :class:`pyspark.sql.types.StructType` or str, t.Optional
            an t.Optional :class:`pyspark.sql.types.StructType` for the input schema
            or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a CSV file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a CSV file
        ...     df = spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}])
        ...     df.write.mode("overwrite").format("csv").save(d)
        ...
        ...     # Read the CSV file as a DataFrame with 'nullValue' option set to 'Hyukjin Kwon'.
        ...     spark.read.csv(d, schema=df.schema, nullValue="Hyukjin Kwon").show()
        +---+----+
        |age|name|
        +---+----+
        |100|NULL|
        +---+----+
        """
        options = dict(
            sep=sep,
            encoding=encoding,
            quote=quote,
            escape=escape,
            comment=comment,
            header=header,
            inferSchema=inferSchema,
            ignoreLeadingWhiteSpace=ignoreLeadingWhiteSpace,
            ignoreTrailingWhiteSpace=ignoreTrailingWhiteSpace,
            nullValue=nullValue,
            nanValue=nanValue,
            positiveInf=positiveInf,
            negativeInf=negativeInf,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            maxColumns=maxColumns,
            maxCharsPerColumn=maxCharsPerColumn,
            maxMalformedLogPerPartition=maxMalformedLogPerPartition,
            mode=mode,
            columnNameOfCorruptRecord=columnNameOfCorruptRecord,
            multiLine=multiLine,
            charToEscapeQuoteEscaping=charToEscapeQuoteEscaping,
            samplingRatio=samplingRatio,
            enforceSchema=enforceSchema,
            emptyValue=emptyValue,
            locale=locale,
            lineSep=lineSep,
            pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup,
            modifiedBefore=modifiedBefore,
            modifiedAfter=modifiedAfter,
            unescapedQuoteHandling=unescapedQuoteHandling,
        )
        # Apply previously set options, with method-specific options taking precedence
        all_options = {**self.state_options, **{k: v for k, v in options.items() if v is not None}}
        return self.load(path=path, format="csv", schema=schema, **all_options)


class _BaseDataFrameWriter(t.Generic[SESSION, DF]):
    def __init__(
        self,
        df: DF,
        mode: t.Optional[str] = None,
        by_name: bool = False,
        state_format_to_write: t.Optional[str] = None,
    ):
        self._df = df
        self._mode = mode
        self._by_name = by_name
        self._state_format_to_write = state_format_to_write

    @property
    def _session(self) -> SESSION:
        return self._df.session

    def copy(self, **kwargs) -> Self:
        return self.__class__(
            **{
                k[1:] if k.startswith("_") else k: v
                for k, v in object_to_dict(self, **kwargs).items()
            }
        )

    def sql(self, **kwargs) -> t.List[str]:
        return self._df.sql(**{**dict(pretty=False, optimize=False, as_list=True), **kwargs})

    def mode(self, saveMode: t.Optional[str]) -> Self:
        return self.copy(_mode=saveMode)

    @property
    def byName(self) -> Self:
        return self.copy(by_name=True)

    def insertInto(self, tableName: str, overwrite: t.Optional[bool] = None) -> Self:
        from sqlframe.base.session import _BaseSession

        tableName = normalize_string(tableName, from_dialect="input", is_table=True)
        output_expression_container = exp.Insert(
            **{
                "this": exp.to_table(tableName, dialect=_BaseSession().input_dialect),
                "overwrite": overwrite,
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
    ) -> Self:
        if format is not None:
            raise NotImplementedError("Providing Format in the save as table is not supported")
        exists, replace, mode = None, None, mode or str(self._mode)
        if mode == "append":
            return self.insertInto(name)
        if mode == "ignore":
            exists = True
        if mode == "overwrite":
            replace = True
        name = normalize_string(name, from_dialect="input", is_table=True)
        output_expression_container = exp.Create(
            this=exp.to_table(name, dialect=self._session.input_dialect),
            kind="TABLE",
            exists=exists,
            replace=replace,
        )
        df = self._df.copy(output_expression_container=output_expression_container)
        if self._session._has_connection:
            df.collect()
        return self.copy(_df=df)

    @staticmethod
    def _mode_to_pandas_mode(mode: t.Optional[str]) -> str:
        if mode is None or mode in {"ignore", "error", "errorifexists", "overwrite"}:
            return "w"
        if mode == "append":
            return "a"
        raise ValueError(f"Unsupported mode: {mode}")

    def _validate_mode(self, path: str, mode: t.Optional[str]) -> t.Tuple[str, bool]:
        mode = mode or "error"
        if mode in {"error", "errorifexists"} and pathlib.Path(path).exists():
            raise FileExistsError(f"Path already exists: {path}")
        if mode == "ignore" and pathlib.Path(path).exists():
            return mode, True
        return mode, False

    def _write(self, path: str, mode: t.Optional[str], format: str, **options) -> None:
        raise NotImplementedError

    def format(self, source: str) -> "Self":
        """Specifies the input data source format.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        source : str
            string, name of the data source, e.g. 'json', 'parquet'.

        Examples
        --------
        >>> spark.read.format('json')
        <...readwriter.DataFrameReader object ...>

        Write a DataFrame into a JSON file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a JSON file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").format("json").save(d)
        ...
        ...     # Read the JSON file as a DataFrame.
        ...     spark.read.format('json').load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        self._state_format_to_write = source
        return self

    def json(
        self,
        path: str,
        mode: t.Optional[str] = None,
        compression: t.Optional[str] = None,
        dateFormat: t.Optional[str] = None,
        timestampFormat: t.Optional[str] = None,
        lineSep: t.Optional[str] = None,
        encoding: t.Optional[str] = None,
        ignoreNullFields: t.Optional[t.Union[bool, str]] = None,
    ) -> None:
        """Saves the content of the :class:`DataFrame` in JSON format
        (`JSON Lines text format or newline-delimited JSON <http://jsonlines.org/>`_) at the
        specified path.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str
            the path in any Hadoop supported file system
        mode : str, t.Optional
            specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a JSON file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a JSON file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.json(d, mode="overwrite")
        ...
        ...     # Read the JSON file as a DataFrame.
        ...     spark.read.format("json").load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        self._write(
            path=path,
            mode=mode,
            format="json",
            compression=compression,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            lineSep=lineSep,
            encoding=encoding,
            ignoreNullFields=ignoreNullFields,
        )

    def parquet(
        self,
        path: str,
        mode: t.Optional[str] = None,
        partitionBy: t.Optional[t.Union[str, t.List[str]]] = None,
        compression: t.Optional[str] = None,
    ) -> None:
        """Saves the content of the :class:`DataFrame` in Parquet format at the specified path.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str
            the path in any Hadoop supported file system
        mode : str, t.Optional
            specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.
        partitionBy : str or list, t.Optional
            names of partitioning columns

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a Parquet file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a Parquet file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.parquet(d, mode="overwrite")
        ...
        ...     # Read the Parquet file as a DataFrame.
        ...     spark.read.format("parquet").load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        self._write(
            path=path, mode=mode, format="parquet", compression=compression, partitionBy=partitionBy
        )

    def csv(
        self,
        path: str,
        mode: t.Optional[str] = None,
        compression: t.Optional[str] = None,
        sep: t.Optional[str] = None,
        quote: t.Optional[str] = None,
        escape: t.Optional[str] = None,
        header: t.Optional[t.Union[bool, str]] = None,
        nullValue: t.Optional[str] = None,
        escapeQuotes: t.Optional[t.Union[bool, str]] = None,
        quoteAll: t.Optional[t.Union[bool, str]] = None,
        dateFormat: t.Optional[str] = None,
        timestampFormat: t.Optional[str] = None,
        ignoreLeadingWhiteSpace: t.Optional[t.Union[bool, str]] = None,
        ignoreTrailingWhiteSpace: t.Optional[t.Union[bool, str]] = None,
        charToEscapeQuoteEscaping: t.Optional[str] = None,
        encoding: t.Optional[str] = None,
        emptyValue: t.Optional[str] = None,
        lineSep: t.Optional[str] = None,
    ) -> None:
        r"""Saves the content of the :class:`DataFrame` in CSV format at the specified path.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str
            the path in any Hadoop supported file system
        mode : str, t.Optional
            specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a CSV file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a CSV file
        ...     df = spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}])
        ...     df.write.csv(d, mode="overwrite")
        ...
        ...     # Read the CSV file as a DataFrame with 'nullValue' option set to 'Hyukjin Kwon'.
        ...     spark.read.schema(df.schema).format("csv").option(
        ...         "nullValue", "Hyukjin Kwon").load(d).show()
        +---+----+
        |age|name|
        +---+----+
        |100|NULL|
        +---+----+
        """
        self._write(
            path=path,
            mode=mode,
            format="csv",
            compression=compression,
            sep=sep,
            quote=quote,
            escape=escape,
            header=header,
            nullValue=nullValue,
            escapeQuotes=escapeQuotes,
            quoteAll=quoteAll,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            ignoreLeadingWhiteSpace=ignoreLeadingWhiteSpace,
            ignoreTrailingWhiteSpace=ignoreTrailingWhiteSpace,
            charToEscapeQuoteEscaping=charToEscapeQuoteEscaping,
            encoding=encoding,
            emptyValue=emptyValue,
            lineSep=lineSep,
        )


def _infer_format(path: str) -> str:
    if path.endswith(".json"):
        return "json"
    if path.endswith(".parquet"):
        return "parquet"
    if path.endswith(".csv"):
        return "csv"
    raise ValueError(f"Cannot infer format from path: {path}")
