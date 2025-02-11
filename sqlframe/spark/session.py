from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.helper import classproperty, ensure_list

from sqlframe.base.session import _BaseSession
from sqlframe.base.util import normalize_string
from sqlframe.spark.catalog import SparkCatalog
from sqlframe.spark.dataframe import SparkDataFrame
from sqlframe.spark.readwriter import (
    SparkDataFrameReader,
    SparkDataFrameWriter,
)
from sqlframe.spark.table import SparkTable
from sqlframe.spark.types import Row
from sqlframe.spark.udf import SparkUDFRegistration

if t.TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql.session import SparkConf
    from pyspark.sql.session import SparkSession as PySparkSession

    from sqlframe.base._typing import OptionalPrimitiveType
else:
    PySparkSession = t.Any


class SparkSession(
    _BaseSession[  # type: ignore
        SparkCatalog,
        SparkDataFrameReader,
        SparkDataFrameWriter,
        SparkDataFrame,
        SparkTable,
        PySparkSession,  # type: ignore
        SparkUDFRegistration,
    ],
):
    _catalog = SparkCatalog
    _reader = SparkDataFrameReader
    _writer = SparkDataFrameWriter
    _df = SparkDataFrame
    _table = SparkTable
    _udf_registration = SparkUDFRegistration

    def __init__(self, conn: t.Optional[PySparkSession] = None, *args, **kwargs):
        from pyspark.sql.session import DataFrame, SparkSession

        self._last_df: t.Optional[DataFrame] = None
        if not hasattr(self, "spark_session"):
            super().__init__(conn)
            self.spark_session = conn or SparkSession.builder.getOrCreate()

    @property
    def _conn(self) -> t.Any:
        raise NotImplementedError()

    @property
    def _cur(self) -> t.Any:
        raise NotImplementedError()

    def _collect(
        self,
        expressions: t.Union[str, exp.Expression, t.List[str], t.List[exp.Expression]],
        *,
        quote_identifiers: bool = True,
        skip_normalization: bool = False,
        skip_rows: bool = False,
    ) -> t.List[Row]:
        for expression in ensure_list(expressions):
            sql = (
                self._to_sql(expression, quote_identifiers=quote_identifiers)
                if isinstance(expression, exp.Expression)
                else expression
            )
            self._execute(sql)  # type: ignore
        if skip_rows:
            return []
        assert self._last_df is not None
        results = []
        for row in self._last_df.collect():
            rows_normalized = {}
            for k, v in row.asDict().items():
                col_id = exp.parse_identifier(k, dialect=self.execution_dialect)
                col_id._meta = {"case_sensitive": True, **(col_id._meta or {})}
                col_name = normalize_string(
                    col_id, from_dialect="execution", to_dialect="output", to_string_literal=True
                )
                rows_normalized[col_name] = v
            results.append(Row(**rows_normalized))
        return results

    def _execute(self, sql: str) -> None:
        self._last_df = self.spark_session.sql(sql)

    def _fetchdf(
        self,
        expressions: t.Union[exp.Expression, t.List[exp.Expression]],
        *,
        quote_identifiers: bool = True,
    ) -> pd.DataFrame:
        for expression in ensure_list(expressions):
            sql = (
                self._to_sql(expression, quote_identifiers=quote_identifiers)
                if isinstance(expression, exp.Expression)
                else expression
            )
            self._execute(sql)  # type: ignore
        assert self._last_df is not None
        return self._last_df.toPandas()

    @property
    def _has_connection(self) -> bool:
        return True

    @classproperty
    def builder(cls) -> Builder:  # type: ignore
        """Creates a :class:`Builder` for constructing a :class:`SparkSession`.

        .. versionchanged:: 3.4.0
            Supports Spark Connect.
        """
        return cls.Builder()

    @property
    def _is_spark(self) -> bool:
        return True

    class Builder(_BaseSession.Builder):
        def __init__(self):
            from pyspark.sql.session import SparkSession

            super().__init__()
            self.builder = SparkSession.builder

        def config(
            self,
            key: t.Optional[str] = None,
            value: t.Optional[t.Any] = None,
            conf: t.Optional[SparkConf] = None,
            *,
            map: t.Optional[t.Dict[str, OptionalPrimitiveType]] = None,
        ) -> SparkSession.Builder:
            super().config(key, value, map=map)
            self.builder = self.builder.config(key, value, conf, map=map)
            return self

        def master(self, master: str) -> SparkSession.Builder:
            self.builder = self.builder.config("spark.master", master)
            return self

        def remote(self, url: str) -> SparkSession.Builder:
            self.builder = self.builder.config("spark.remote", url)
            return self

        def appName(self, name: str) -> SparkSession.Builder:
            self.builder = self.builder.appName(name)
            return self

        def enableHiveSupport(self) -> SparkSession.Builder:
            self.builder = self.builder.enableHiveSupport()
            return self

        @property
        def session(self) -> SparkSession:
            if "conn" not in self._session_kwargs:
                self._session_kwargs["conn"] = self.builder.getOrCreate()
            return SparkSession(**self._session_kwargs)

        def getOrCreate(self) -> SparkSession:
            return super().getOrCreate()  # type: ignore
