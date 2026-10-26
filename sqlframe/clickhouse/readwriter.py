from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.base.readerwriter import _BaseDataFrameReader, _BaseDataFrameWriter

if t.TYPE_CHECKING:
    from sqlframe.clickhouse.dataframe import ClickHouseDataFrame
    from sqlframe.clickhouse.session import ClickHouseSession
    from sqlframe.clickhouse.table import ClickHouseTable


class ClickHouseDataFrameReader(
    _BaseDataFrameReader["ClickHouseSession", "ClickHouseDataFrame", "ClickHouseTable"]
):
    def table(self, tableName: str) -> ClickHouseTable:
        table = exp.to_table(tableName, dialect=self.session.input_dialect)
        for identifier in table.find_all(exp.Identifier):
            normalized = self.session.input_dialect.normalize_identifier(identifier.copy())
            if normalized.name != identifier.name:
                raise UnsupportedOperationError(
                    f"ClickHouse table {tableName!r} loses case during Spark identifier "
                    "normalization. Case-sensitive table names are unsupported; use "
                    "lowercase table and database names, or mark the table case-sensitive "
                    "in raw SQL: spark.sql('SELECT * FROM `Events` "
                    "/* sqlglot.meta case_sensitive */', qualify=False). "
                    "See docs/clickhouse.md."
                )
        return super().table(tableName)


class ClickHouseDataFrameWriter(_BaseDataFrameWriter["ClickHouseSession", "ClickHouseDataFrame"]):
    def insertInto(self, tableName: str, overwrite: t.Optional[bool] = None):
        raise UnsupportedOperationError(
            "ClickHouse table writes are unsupported; use the ClickHouse client for inserts."
        )

    def saveAsTable(self, name: str, format: t.Optional[str] = None, mode: t.Optional[str] = None):
        raise UnsupportedOperationError(
            "ClickHouse table writes are unsupported; create tables with an explicit engine "
            "using the ClickHouse client."
        )
