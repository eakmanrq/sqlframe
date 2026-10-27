from __future__ import annotations

import typing as t

from sqlframe.base.catalog import Column as CatalogColumn
from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.standalone.dataframe import StandaloneDataFrame
from sqlframe.standalone.dataframe import (
    StandaloneDataFrameNaFunctions as TrinoDataFrameNaFunctions,
)
from sqlframe.standalone.dataframe import (
    StandaloneDataFrameStatFunctions as TrinoDataFrameStatFunctions,
)

if t.TYPE_CHECKING:
    from sqlframe.base._typing import StorageLevel
    from sqlframe.trino.session import TrinoSession


class TrinoDataFrame(StandaloneDataFrame):
    @property
    def _schema_dialect(self) -> str:
        # Keep Spark type semantics independent of the dialect used by df.sql().
        return "spark"

    @property
    def _typed_columns(self) -> t.List[CatalogColumn]:
        session = t.cast("TrinoSession", self.session)
        if not session._has_connection:
            raise ValueError("Trino schema discovery requires a DB-API connection")
        # Use the same finalized query as collect(), including display names and CTEs.
        query = self.sql(dialect=session.execution_dialect, pretty=False, optimize=False)
        return [
            CatalogColumn(
                name=row[0],
                description=None,
                dataType=session.catalog._to_spark_type(row[4], row[0]).sql(
                    dialect=self._schema_dialect
                ),
                nullable=True,
                isPartition=False,
                isBucket=False,
            )
            for row in session._collect(f"DESCRIBE OUTPUT ({query})")
        ]

    def cache(self) -> t.NoReturn:
        raise UnsupportedOperationError(
            "Trino does not support DataFrame.cache(); no compatible cache operation is available"
        )

    def persist(self, storageLevel: StorageLevel = "MEMORY_AND_DISK_SER") -> t.NoReturn:
        raise UnsupportedOperationError(
            "Trino does not support DataFrame.persist(); no compatible cache operation is available"
        )


__all__ = ["TrinoDataFrame", "TrinoDataFrameNaFunctions", "TrinoDataFrameStatFunctions"]
