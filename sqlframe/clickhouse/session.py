from __future__ import annotations

import typing as t

from sqlglot import Dialect, exp
from sqlglot.dialects.dialect import DialectType

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.base.session import _BaseSession
from sqlframe.clickhouse.catalog import ClickHouseCatalog
from sqlframe.clickhouse.dataframe import ClickHouseDataFrame
from sqlframe.clickhouse.readwriter import ClickHouseDataFrameReader, ClickHouseDataFrameWriter
from sqlframe.clickhouse.table import ClickHouseTable
from sqlframe.clickhouse.udf import ClickHouseUDFRegistration

if t.TYPE_CHECKING:
    from clickhouse_connect.dbapi.connection import Connection
else:
    Connection = t.Any


class ClickHouseSession(
    _BaseSession[
        ClickHouseCatalog,
        ClickHouseDataFrameReader,
        ClickHouseDataFrameWriter,
        ClickHouseDataFrame,
        ClickHouseTable,
        Connection,  # type: ignore[type-var]
        ClickHouseUDFRegistration,
    ]
):
    _catalog = ClickHouseCatalog
    _reader = ClickHouseDataFrameReader
    _writer = ClickHouseDataFrameWriter
    _df = ClickHouseDataFrame
    _table = ClickHouseTable
    _udf_registration = ClickHouseUDFRegistration

    def _create_df(self, *args, **kwargs) -> ClickHouseDataFrame:
        if isinstance(kwargs.get("output_expression_container"), (exp.Create, exp.Insert)):
            raise UnsupportedOperationError(
                "ClickHouse table writes are unsupported; use the ClickHouse client for inserts "
                "and table creation."
            )
        return super()._create_df(*args, **kwargs)

    def _to_sql(
        self,
        sql: t.Union[str, exp.Expr],
        *,
        dialect: DialectType = None,
        quote_identifiers: bool = True,
        pretty: bool = False,
        **kwargs,
    ) -> str:
        target = Dialect.get_or_raise(dialect or self.execution_dialect)
        if target.__class__ is self.execution_dialect.__class__ and isinstance(sql, exp.Query):
            sql = sql.copy()

            # ClickHouse requires the camel-case function name; Spark SQL uses ARRAY_SORT.
            def translate_array_sort(node: exp.Expr) -> exp.Expr:
                if not isinstance(node, exp.ArraySort):
                    return node
                if node.args.get("expression") is not None:
                    raise UnsupportedOperationError(
                        "array_sort comparator is not supported by ClickHouse"
                    )
                if node.this is None:
                    return node
                return exp.Anonymous(
                    this="arraySort",
                    expressions=[node.this.transform(translate_array_sort, copy=False)],
                )

            sql = sql.transform(translate_array_sort, copy=False)
            # Match Spark's null results for empty sum, avg, min, and max aggregates.
            required_settings = ["aggregate_functions_null_for_empty"]
            if any(
                join.args.get("side") in {"LEFT", "RIGHT", "FULL"}
                for join in sql.find_all(exp.Join)
            ):
                required_settings.append("join_use_nulls")
            settings = [
                setting.copy()
                for setting in sql.args.get("settings") or []
                if not (
                    isinstance(setting, exp.EQ)
                    and isinstance(setting.this, exp.Column)
                    and setting.this.name.lower() in required_settings
                )
            ]
            settings.extend(
                exp.EQ(this=exp.column(name), expression=exp.Literal.number(1))
                for name in required_settings
            )
            sql.set("settings", settings)
        return super()._to_sql(
            sql,
            dialect=dialect,
            quote_identifiers=quote_identifiers,
            pretty=pretty,
            **kwargs,
        )

    class Builder(_BaseSession.Builder):
        DEFAULT_EXECUTION_DIALECT = "clickhouse"

        @property
        def session(self) -> ClickHouseSession:
            return ClickHouseSession(**self._session_kwargs)

        def getOrCreate(self) -> ClickHouseSession:
            return super().getOrCreate()  # type: ignore

    builder = Builder()
