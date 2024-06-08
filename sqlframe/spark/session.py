from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.helper import classproperty

from sqlframe.base.session import _BaseSession
from sqlframe.spark.catalog import SparkCatalog
from sqlframe.spark.dataframe import SparkDataFrame
from sqlframe.spark.readwriter import (
    SparkDataFrameReader,
    SparkDataFrameWriter,
)
from sqlframe.spark.types import Row

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
        PySparkSession,
    ],
):
    _catalog = SparkCatalog
    _reader = SparkDataFrameReader
    _writer = SparkDataFrameWriter
    _df = SparkDataFrame

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

    @classproperty
    def builder(cls) -> Builder:  # type: ignore
        """Creates a :class:`Builder` for constructing a :class:`SparkSession`.

        .. versionchanged:: 3.4.0
            Supports Spark Connect.
        """
        return cls.Builder()

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
            self._set_session_properties()
            return self.session
