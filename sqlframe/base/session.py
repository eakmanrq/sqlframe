# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import contextlib
import datetime
import logging
import sys
import typing as t
import uuid
from collections import defaultdict
from functools import cached_property

import sqlglot
from sqlglot import Dialect, exp
from sqlglot.dialects.dialect import DialectType, NormalizationStrategy
from sqlglot.expressions import parse_identifier
from sqlglot.helper import ensure_list, seq_get
from sqlglot.optimizer import RULES as OPTIMIZER_RULES
from sqlglot.optimizer import optimize
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify import qualify as qualify_func
from sqlglot.optimizer.qualify_columns import (
    quote_identifiers as quote_identifiers_func,
)
from sqlglot.schema import MappingSchema

from sqlframe.base.catalog import _BaseCatalog
from sqlframe.base.dataframe import BaseDataFrame
from sqlframe.base.normalize import normalize_dict
from sqlframe.base.readerwriter import _BaseDataFrameReader, _BaseDataFrameWriter
from sqlframe.base.table import _BaseTable
from sqlframe.base.udf import _BaseUDFRegistration
from sqlframe.base.util import (
    get_column_mapping_from_schema_input,
    normalize_string,
    verify_pandas_installed,
)

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if t.TYPE_CHECKING:
    import pandas as pd
    from _typeshed.dbapi import DBAPIConnection, DBAPICursor

    from sqlframe.base._typing import ColumnLiterals, SchemaInput
    from sqlframe.base.column import Column
    from sqlframe.base.types import Row, StructType

    class DBAPIConnectionWithPandas(DBAPIConnection):
        def cursor(self) -> DBAPICursorWithPandas: ...

    class DBAPICursorWithPandas(DBAPICursor):
        def fetchdf(self) -> pd.DataFrame: ...

    CONN = t.TypeVar("CONN", bound=DBAPIConnectionWithPandas)
else:
    CONN = t.TypeVar("CONN")


logger = logging.getLogger(__name__)


CATALOG = t.TypeVar("CATALOG", bound=_BaseCatalog)
READER = t.TypeVar("READER", bound=_BaseDataFrameReader)
WRITER = t.TypeVar("WRITER", bound=_BaseDataFrameWriter)
DF = t.TypeVar("DF", bound=BaseDataFrame)
TABLE = t.TypeVar("TABLE", bound=_BaseTable)
UDF_REGISTRATION = t.TypeVar("UDF_REGISTRATION", bound=_BaseUDFRegistration)

_MISSING = "MISSING"


class _BaseSession(t.Generic[CATALOG, READER, WRITER, DF, TABLE, CONN, UDF_REGISTRATION]):
    _instance = None
    _reader: t.Type[READER]
    _writer: t.Type[WRITER]
    _catalog: t.Type[CATALOG]
    _df: t.Type[DF]
    _table: t.Type[TABLE]
    _udf_registration: t.Type[UDF_REGISTRATION]

    SANITIZE_COLUMN_NAMES = False

    def __init__(
        self,
        conn: t.Optional[CONN] = None,
        schema: t.Optional[MappingSchema] = None,
        *args,
        **kwargs,
    ):
        if not hasattr(self, "input_dialect"):
            self.input_dialect: Dialect = Dialect.get_or_raise(self.builder.DEFAULT_INPUT_DIALECT)
            self.output_dialect: Dialect = Dialect.get_or_raise(self.builder.DEFAULT_OUTPUT_DIALECT)
            self.execution_dialect: Dialect = Dialect.get_or_raise(
                self.builder.DEFAULT_EXECUTION_DIALECT
            )
            self.known_ids: t.Set[str] = set()
            self.known_branch_ids: t.Set[str] = set()
            self.known_sequence_ids: t.Set[str] = set()
            self.name_to_sequence_id_mapping: t.Dict[str, t.List[str]] = defaultdict(list)
            self.incrementing_id: int = 1
            self._last_loaded_file: t.Optional[str] = None
            self.temp_views: t.Dict[str, DF] = {}
        if not self._has_connection or conn:
            self._connection = conn
        if not getattr(self, "schema", None) or schema:
            self._schema = schema

    # https://github.com/eakmanrq/sqlframe/issues/262
    @property
    def execution_dialect_name(self) -> str:
        return self.execution_dialect.__class__.__name__.lower()

    @property
    def read(self) -> READER:
        return self._reader(self)

    @cached_property
    def catalog(self) -> CATALOG:
        return self._catalog(self, self._schema)

    @property
    def _conn(self) -> CONN:
        if self._connection is None:
            raise ValueError("Connection not set")
        return self._connection

    @cached_property
    def _cur(self) -> DBAPICursorWithPandas:
        return self._conn.cursor()

    @property
    def default_time_format(self) -> str:
        return self.input_dialect.TIME_FORMAT.strip("'")

    def format_time(self, value: t.Optional[t.Union[Column, str]] = None) -> exp.Expression:
        from sqlframe.base.column import Column

        value = value or self.default_time_format
        if isinstance(value, Column):
            value = value.expression.this
        return self.input_dialect.format_time(f"'{value}'")  # type: ignore

    def format_execution_time(
        self, value: t.Optional[t.Union[Column, str]] = None
    ) -> exp.Expression:
        from sqlframe.base.column import Column

        if value is None:
            return exp.Literal.string(self.execution_dialect.TIME_FORMAT.strip("'"))

        if isinstance(value, Column):
            value = value.expression.this
        return exp.Literal.string(
            self.execution_dialect.generator()
            .format_time(
                exp.StrToTime(this=exp.Null(), format=self.input_dialect.format_time(f"'{value}'"))
            )
            .strip("'")  # type: ignore
        )

    def _sanitize_column_name(self, name: str) -> str:
        if self.SANITIZE_COLUMN_NAMES:
            return name.replace("(", "_").replace(")", "_")
        return name

    def table(self, tableName: str) -> TABLE:
        return self.read.table(tableName)

    def _create_df(self, *args, **kwargs) -> DF:
        return self._df(self, *args, **kwargs)

    def _create_table(self, *args, **kwargs) -> TABLE:
        return self._table(self, *args, **kwargs)

    def __new__(cls, *args, **kwargs):
        if _BaseSession._instance is None:
            _BaseSession._instance = super().__new__(cls)
        return _BaseSession._instance

    @property
    def _has_connection(self) -> bool:
        return hasattr(self, "_connection") and bool(self._connection)

    @property
    def udf(self) -> UDF_REGISTRATION:
        return self._udf_registration(self)

    def getActiveSession(self) -> Self:
        return self

    def range(
        self,
        start: int,
        end: t.Optional[int] = None,
        step: int = 1,
        numPartitions: t.Optional[int] = None,
    ):
        # Ensure end is provided by either args or kwargs
        if end is None:
            if start:
                end = start
                start = 0
            else:
                raise ValueError("range() requires an 'end' value")

        if numPartitions is not None:
            logger.warning("numPartitions is not supported")
        return self.createDataFrame([[x] for x in range(start, end, step)], schema={"id": "long"})

    def createDataFrame(
        self,
        data: t.Union[
            t.Sequence[
                t.Union[
                    t.Dict[str, ColumnLiterals],
                    t.List[ColumnLiterals],
                    t.Tuple[ColumnLiterals, ...],
                    ColumnLiterals,
                ],
            ],
            pd.DataFrame,
        ],
        schema: t.Optional[SchemaInput] = None,
        samplingRatio: t.Optional[float] = None,
        verifySchema: bool = False,
    ) -> DF:
        from sqlframe.base import functions as F
        from sqlframe.base.types import Row, StructType

        if samplingRatio is not None or verifySchema:
            raise NotImplementedError("Sampling Ratio and Verify Schema are not supported")
        if (
            schema is not None
            and not isinstance(schema, dict)
            and (
                not isinstance(schema, (StructType, str, list, tuple))
                or (isinstance(schema, (list, tuple)) and not isinstance(schema[0], str))
            )
        ):
            raise NotImplementedError("Only schema of either list or string of list supported")

        with contextlib.suppress(ImportError):
            from pandas import DataFrame as pd_DataFrame

            if isinstance(data, pd_DataFrame):
                data = data.to_dict("records")  # type: ignore

        column_mapping: t.Mapping[str, t.Optional[exp.DataType]]
        if schema is not None:
            column_mapping = get_column_mapping_from_schema_input(
                schema, dialect=self.input_dialect
            )

        elif data:
            if isinstance(data[0], Row):
                column_mapping = {col_name.strip(): None for col_name in data[0].__fields__}
            elif isinstance(data[0], dict):
                column_mapping = {col_name.strip(): None for col_name in data[0]}
            else:
                column_mapping = {f"_{i}": None for i in range(1, len(data[0]) + 1)}  # type: ignore
        else:
            column_mapping = {}

        empty_df = not data
        rows = [[None] * len(column_mapping)] if empty_df else list(data)  # type: ignore

        def get_default_data_type(value: t.Any) -> t.Optional[str]:
            if isinstance(value, Row):
                row_types = []
                for row_name, row_dtype in zip(value.__fields__, value):
                    default_type = get_default_data_type(row_dtype)
                    if not default_type:
                        continue
                    row_types.append((row_name, default_type))
                return "struct<" + ", ".join(f"{k}: {v}" for (k, v) in row_types) + ">"
            elif isinstance(value, dict):
                sample_row = seq_get(list(value.items()), 0)
                if not sample_row:
                    return None
                key, value = sample_row
                default_key = get_default_data_type(key)
                default_value = get_default_data_type(value)
                if not default_key or not default_value:
                    return None
                return f"map<{default_key}, {default_value}>"
            elif isinstance(value, (list, set, tuple)):
                if not value:
                    return None
                default_type = get_default_data_type(next(iter(value)))
                if not default_type:
                    return None
                return f"array<{default_type}>"
            elif isinstance(value, bool):
                return "boolean"
            elif isinstance(value, bytes):
                return "binary"
            elif isinstance(value, int):
                return "bigint"
            elif isinstance(value, float):
                return "double"
            elif isinstance(value, datetime.datetime):
                if value.tzinfo:
                    return "timestamptz"
                return "timestamp"
            elif isinstance(value, datetime.date):
                return "date"
            elif isinstance(value, str):
                return "string"
            return None

        updated_mapping: t.Dict[str, t.Optional[exp.DataType]] = {}
        sample_row = rows[0]
        for i, (name, dtype) in enumerate(column_mapping.items()):
            if dtype is not None:
                updated_mapping[name] = dtype
                continue
            if isinstance(sample_row, Row):
                sample_row = sample_row.asDict()
            if isinstance(sample_row, dict):
                default_data_type = get_default_data_type(sample_row[name])
                updated_mapping[name] = (
                    exp.DataType.build(default_data_type, dialect="spark")
                    if default_data_type
                    else None
                )
            else:
                default_data_type = get_default_data_type(sample_row[i])
                updated_mapping[name] = (
                    exp.DataType.build(default_data_type, dialect="spark")
                    if default_data_type
                    else None
                )
        column_mapping = updated_mapping
        data_expressions = []
        for row in rows:
            if isinstance(row, (list, tuple, dict)):
                if not row:
                    data_expressions.append(exp.tuple_(exp.Null()))
                    continue
                if isinstance(row, Row):
                    row = row.asDict()
                if isinstance(row, dict):
                    row = row.values()  # type: ignore
                data_expressions.append(exp.tuple_(*[F.lit(x).column_expression for x in row]))
            else:
                data_expressions.append(exp.tuple_(*[F.lit(row).column_expression]))

        if column_mapping:
            sel_columns = [
                (
                    F.col(name).cast(data_type).alias(name).expression
                    if data_type is not None
                    else F.col(name).expression
                )
                for name, data_type in column_mapping.items()
            ]
        else:
            sel_columns = [F.lit(None).expression]

        select_kwargs = {
            "expressions": sel_columns,
            "from": exp.From(
                this=exp.Values(
                    expressions=data_expressions,
                    alias=exp.TableAlias(
                        this=exp.to_identifier(self._auto_incrementing_name),
                        columns=[
                            exp.parse_identifier(col_name, dialect=self.input_dialect)
                            for col_name in column_mapping
                        ],
                    ),
                ),
            ),
        }

        sel_expression = exp.Select(**select_kwargs)
        if empty_df:
            sel_expression = sel_expression.where(exp.false())
        df = self._create_df(sel_expression)
        df._update_display_name_mapping(
            df._ensure_and_normalize_cols(list(column_mapping.keys())), list(column_mapping.keys())
        )
        return df

    def sql(
        self,
        sqlQuery: t.Union[str, exp.Expression],
        dialect: DialectType = None,
        qualify: bool = True,
    ) -> DF:
        dialect = Dialect.get_or_raise(dialect or self.input_dialect)
        expression = (
            sqlglot.parse_one(
                normalize_string(sqlQuery, from_dialect=dialect, is_query=True),
                read=dialect,
            )
            if isinstance(sqlQuery, str)
            else sqlQuery
        )
        expression = sqlglot.parse_one(
            normalize_string(
                expression.sql(dialect=dialect),
                from_dialect=dialect,
                to_dialect=self.input_dialect,
                is_query=True,
            ),
            dialect=self.input_dialect,
        )
        if qualify:
            expression = qualify_func(
                expression,
                dialect=dialect,
                quote_identifiers=False,
                identify=False,
                schema=self.catalog._schema,
            )
        if self.temp_views:
            replacement_mapping = {}
            for table in expression.find_all(exp.Table):
                if not (df := self.temp_views.get(table.name)):
                    continue
                expression_ctes = {cte.alias_or_name: cte for cte in expression.ctes}  # type: ignore
                replacement_mapping[table] = df.expression.ctes[-1].alias_or_name
                ctes_to_add = []
                for cte in df.expression.ctes:
                    if cte.alias_or_name not in expression_ctes:
                        ctes_to_add.append(cte)
                expression.set("with", exp.With(expressions=expression.ctes + ctes_to_add))  # type: ignore

            def replace_temp_view_name_with_cte(node: exp.Expression) -> exp.Expression:
                if isinstance(node, exp.Table):
                    if node in replacement_mapping:
                        node.set("this", exp.to_identifier(replacement_mapping[node]))
                return node

            if replacement_mapping:
                expression = expression.transform(replace_temp_view_name_with_cte)

        if isinstance(expression, exp.Select):
            df = self._create_df(expression)
            df = df._convert_leaf_to_cte()
        elif isinstance(expression, (exp.Create, exp.Insert)):
            select_expression = expression.expression.copy()
            if isinstance(expression, exp.Insert):
                select_expression.set("with", expression.args.get("with"))
                expression.set("with", None)
            del expression.args["expression"]
            df = self._create_df(select_expression, output_expression_container=expression)  # type: ignore
            df = df._convert_leaf_to_cte()
        else:
            raise ValueError(
                "Unknown expression type provided in the SQL. Please create an issue with the SQL."
            )
        return df

    @property
    def _auto_incrementing_name(self) -> str:
        name = f"a{self.incrementing_id}"
        self.incrementing_id += 1
        return name

    @property
    def _random_branch_id(self) -> str:
        id = self._random_id
        self.known_branch_ids.add(id)
        return id

    @property
    def _random_sequence_id(self):
        id = self._random_id
        self.known_sequence_ids.add(id)
        return id

    @property
    def _random_id(self) -> str:
        id = "r" + uuid.uuid4().hex
        normalized_id = self._normalize_string(id)
        self.known_ids.add(normalized_id)
        return normalized_id

    @property
    def _join_hint_names(self) -> t.Set[str]:
        return {"BROADCAST", "MERGE", "SHUFFLE_HASH", "SHUFFLE_REPLICATE_NL"}

    def _normalize_string(self, value: str) -> str:
        expression = parse_identifier(value, dialect=self.input_dialect)
        normalize_identifiers(expression, dialect=self.input_dialect)
        return expression.sql(dialect=self.input_dialect)

    def _add_alias_to_mapping(self, name: str, sequence_id: str):
        self.name_to_sequence_id_mapping[self._normalize_string(name)].append(sequence_id)

    def _collect(
        self,
        expressions: t.Union[str, exp.Expression, t.List[str], t.List[exp.Expression]],
        *,
        quote_identifiers: bool = True,
        skip_normalization: bool = False,
        skip_rows: bool = False,
    ) -> t.List[Row]:
        for expression in ensure_list(expressions):
            if isinstance(expression, exp.Expression):
                sql = (
                    expression.sql(dialect=self.execution_dialect)
                    if skip_normalization
                    else self._to_sql(expression, quote_identifiers=quote_identifiers)
                )
            else:
                sql = expression  # type: ignore
            self._execute(sql)
        if skip_rows:
            return []
        result = self._cur.fetchall()
        if not self._cur.description:
            return []
        case_sensitive_cols = []
        for col in self._cur.description:
            col_id = exp.parse_identifier(col[0], dialect=self.execution_dialect)
            col_id._meta = {"case_sensitive": True, **(col_id._meta or {})}
            case_sensitive_cols.append(col_id)
        columns = [
            normalize_string(
                x, from_dialect="execution", to_dialect="output", to_string_literal=True
            )
            for x in case_sensitive_cols
        ]
        return [self._to_row(columns, row) for row in result]

    def _fetchdf(
        self,
        expressions: t.Union[exp.Expression, t.List[exp.Expression]],
        *,
        quote_identifiers: bool = True,
    ) -> pd.DataFrame:
        verify_pandas_installed()
        from pandas.io.sql import read_sql_query

        all_expressions = [None] + ensure_list(expressions)
        for an_expression in all_expressions[:-1]:
            if an_expression:
                self._execute(self._to_sql(an_expression, quote_identifiers=quote_identifiers))  # type: ignore
        assert all_expressions[-1] is not None
        return read_sql_query(
            self._to_sql(all_expressions[-1], quote_identifiers=quote_identifiers),  # type: ignore
            self._conn,
        )

    def _to_sql(
        self,
        sql: t.Union[str, exp.Expression],
        *,
        dialect: DialectType = None,
        quote_identifiers: bool = True,
        pretty: bool = False,
        **kwargs,
    ) -> str:
        return normalize_string(
            sql,
            from_dialect=self.input_dialect,
            to_dialect=dialect or self.execution_dialect,
            is_query=True,
            quote_identifiers=quote_identifiers,
            pretty=pretty,
        )

    def _optimize(
        self,
        expression: exp.Expression,
        dialect: t.Optional[Dialect] = None,
        quote_identifiers: bool = True,
    ) -> exp.Expression:
        dialect = dialect or self.input_dialect
        normalize_identifiers(expression, dialect=dialect)
        rules = list(OPTIMIZER_RULES)
        if quote_identifiers:
            quote_identifiers_func(expression, dialect=dialect)
        else:
            rules.remove(quote_identifiers_func)
        return optimize(
            expression,
            dialect=dialect,
            schema=self.catalog._schema,
            infer_schema=True,
            quote_identifiers=quote_identifiers,
            rules=rules,  # type: ignore
        )

    def _execute(self, sql: str) -> None:
        self._cur.execute(sql)

    @classmethod
    def _try_get_map(cls, value: t.Any) -> t.Optional[t.Dict[str, t.Any]]:
        return None if not isinstance(value, dict) else value

    @classmethod
    def _to_value(cls, value: t.Any) -> t.Any:
        if (map_value := cls._try_get_map(value)) is not None:
            return {
                normalize_string(k, from_dialect="execution", to_dialect="output")
                if isinstance(k, str)
                else k: cls._to_value(v)
                for k, v in map_value.items()
            }
        elif isinstance(value, dict):
            return cls._to_row(list(value.keys()), list(value.values()))
        elif isinstance(value, (list, set, tuple)) and value:
            return [cls._to_value(x) for x in value]
        elif isinstance(value, datetime.datetime):
            return value.replace(tzinfo=None)
        return value

    @classmethod
    def _to_row(cls, columns: t.List[str], values: t.Iterable[t.Any]) -> Row:
        from sqlframe.base.types import Row, _create_row

        converted_values = []
        for value in values:
            converted_values.append(cls._to_value(value))
        return _create_row(columns, converted_values)

    @property
    def _is_bigquery(self) -> bool:
        return False

    @property
    def _is_databricks(self) -> bool:
        return False

    @property
    def _is_duckdb(self) -> bool:
        return False

    @property
    def _is_postgres(self) -> bool:
        return False

    @property
    def _is_redshift(self) -> bool:
        return False

    @property
    def _is_snowflake(self) -> bool:
        return False

    @property
    def _is_spark(self) -> bool:
        return False

    @property
    def _is_standalone(self) -> bool:
        return False

    class Builder:
        SQLFRAME_INPUT_DIALECT_KEY = "sqlframe.input.dialect"
        SQLFRAME_OUTPUT_DIALECT_KEY = "sqlframe.output.dialect"
        SQLFRAME_EXECUTION_DIALECT_KEY = "sqlframe.execution.dialect"
        SQLFRAME_CONN_KEY = "sqlframe.conn"
        SQLFRAME_SCHEMA_KEY = "sqlframe.schema"
        DEFAULT_INPUT_DIALECT = "spark"
        DEFAULT_OUTPUT_DIALECT = "spark"
        DEFAULT_EXECUTION_DIALECT = "spark"

        def __init__(self):
            self.input_dialect = self.DEFAULT_INPUT_DIALECT
            self.output_dialect = self.DEFAULT_OUTPUT_DIALECT
            self.execution_dialect = self.DEFAULT_EXECUTION_DIALECT
            self._conn = None
            self._session_kwargs = {}

        def __getattr__(self, item) -> Self:
            return self

        def __call__(self, *args, **kwargs):
            return self

        @property
        def session(self) -> _BaseSession:
            return _BaseSession(**self._session_kwargs)

        def getOrCreate(self) -> _BaseSession:
            from sqlframe import ACTIVATE_CONFIG

            for k, v in ACTIVATE_CONFIG.items():
                self._set_config(k, v)
            self._set_session_properties()
            return self.session

        def _set_config(
            self,
            key: t.Optional[str] = None,
            value: t.Optional[t.Any] = None,
            *,
            map: t.Optional[t.Dict[str, t.Any]] = None,
        ) -> None:
            if value is not None:
                if key == self.SQLFRAME_INPUT_DIALECT_KEY:
                    self.input_dialect = value
                elif key == self.SQLFRAME_OUTPUT_DIALECT_KEY:
                    self.output_dialect = value
                elif key == self.SQLFRAME_EXECUTION_DIALECT_KEY:
                    self.execution_dialect = value
                elif key == self.SQLFRAME_CONN_KEY:
                    self._session_kwargs["conn"] = value
                elif key == self.SQLFRAME_SCHEMA_KEY:
                    self._session_kwargs["schema"] = value
                else:
                    self._session_kwargs[key] = value
            if map:
                if self.SQLFRAME_INPUT_DIALECT_KEY in map:
                    self.input_dialect = map[self.SQLFRAME_INPUT_DIALECT_KEY]
                if self.SQLFRAME_OUTPUT_DIALECT_KEY in map:
                    self.output_dialect = map[self.SQLFRAME_OUTPUT_DIALECT_KEY]
                if self.SQLFRAME_EXECUTION_DIALECT_KEY in map:
                    self.execution_dialect = map[self.SQLFRAME_EXECUTION_DIALECT_KEY]
                if self.SQLFRAME_CONN_KEY in map:
                    self._session_kwargs["conn"] = map[self.SQLFRAME_CONN_KEY]
                if self.SQLFRAME_SCHEMA_KEY in map:
                    self._session_kwargs["schema"] = map[self.SQLFRAME_SCHEMA_KEY]

        def config(
            self,
            key: t.Optional[str] = None,
            value: t.Optional[t.Any] = None,
            conf: t.Optional[t.Any] = None,
            *,
            map: t.Optional[t.Dict[str, t.Any]] = None,
        ) -> Self:
            self._set_config(key, value, map=map)
            return self

        def _set_session_properties(self) -> None:
            self.session.input_dialect = Dialect.get_or_raise(self.input_dialect)
            self.session.output_dialect = Dialect.get_or_raise(self.output_dialect)
            self.session.execution_dialect = Dialect.get_or_raise(self.execution_dialect)
            if hasattr(self.session, "_connection") and not self.session._connection:
                self.session._connection = self._conn

    builder = Builder()
