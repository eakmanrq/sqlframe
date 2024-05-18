from __future__ import annotations

import typing as t
import warnings

import pandas as pd
from sqlglot import exp

from sqlframe.base.session import _BaseSession
from sqlframe.spark.catalog import SparkCatalog
from sqlframe.spark.dataframe import SparkDataFrame
from sqlframe.spark.readwriter import (
    SparkDataFrameReader,
    SparkDataFrameWriter,
)
from sqlframe.spark.types import Row


class SparkSession(
    _BaseSession[  # type: ignore
        SparkCatalog,
        SparkDataFrameReader,
        SparkDataFrameWriter,
        SparkDataFrame,
        object,
    ],
):
    _catalog = SparkCatalog
    _reader = SparkDataFrameReader
    _writer = SparkDataFrameWriter
    _df = SparkDataFrame

    def __init__(self, conn: t.Optional[t.Any] = None):
        warnings.warn(
            "SparkSession is still in active development. Functions may not work as expected."
        )

        from pyspark.sql.session import DataFrame, SparkSession

        if not hasattr(self, "spark_session"):
            super().__init__(conn)
            self.spark_session = SparkSession.builder.getOrCreate()
            self._last_df: t.Optional[DataFrame] = None

    @property
    def _conn(self) -> t.Any:
        raise NotImplementedError()

    @property
    def _cur(self) -> t.Any:
        raise NotImplementedError()

    def _fetch_rows(
        self, sql: t.Union[str, exp.Expression], *, quote_identifiers: bool = True
    ) -> t.List[Row]:
        self._execute(sql, quote_identifiers=quote_identifiers)
        assert self._last_df is not None
        return [Row(**row.asDict()) for row in self._last_df.collect()]

    def _execute(
        self, sql: t.Union[str, exp.Expression], *, quote_identifiers: bool = True
    ) -> None:
        self._last_df = self.spark_session.sql(
            self._to_sql(sql, quote_identifiers=quote_identifiers)
        )

    def _fetchdf(
        self, sql: t.Union[str, exp.Expression], *, quote_identifiers: bool = True
    ) -> pd.DataFrame:
        self._execute(sql, quote_identifiers=quote_identifiers)
        assert self._last_df is not None
        return self._last_df.toPandas()

    @property
    def _has_connection(self) -> bool:
        return True

    class Builder(_BaseSession.Builder):
        DEFAULT_INPUT_DIALECT = "spark"
        DEFAULT_OUTPUT_DIALECT = "spark"

        @property
        def session(self) -> SparkSession:
            return SparkSession(**self._session_kwargs)

        def getOrCreate(self) -> SparkSession:
            self._set_session_properties()
            return self.session

    builder = Builder()
