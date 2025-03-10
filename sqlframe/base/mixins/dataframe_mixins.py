import logging
import sys
import typing as t

from sqlglot import exp

if t.TYPE_CHECKING:
    from sqlframe.base._typing import StorageLevel

from sqlframe.base.catalog import Column
from sqlframe.base.dataframe import (
    GROUP_DATA,
    NA,
    SESSION,
    STAT,
    WRITER,
    BaseDataFrame,
)

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


logger = logging.getLogger(__name__)


class NoCachePersistSupportMixin(BaseDataFrame, t.Generic[SESSION, WRITER, NA, STAT, GROUP_DATA]):
    def cache(self) -> Self:
        logger.warning("This engine does not support caching. Ignoring cache() call.")
        return self

    def persist(self, storageLevel: "StorageLevel" = "MEMORY_AND_DISK_SER") -> Self:
        logger.warning("This engine does not support persist. Ignoring persist() call.")
        return self


class TypedColumnsFromTempViewMixin(
    BaseDataFrame, t.Generic[SESSION, WRITER, NA, STAT, GROUP_DATA]
):
    @property
    def _typed_columns(self) -> t.List[Column]:
        table = exp.to_table(self.session._random_id)
        self.session._collect(
            exp.Create(
                this=table,
                kind="VIEW",
                replace=True,
                properties=exp.Properties(expressions=[exp.TemporaryProperty()]),
                expression=self.expression,
            )
        )

        return self.session.catalog.listColumns(
            table.sql(dialect=self.session.input_dialect), include_temp=True
        )
