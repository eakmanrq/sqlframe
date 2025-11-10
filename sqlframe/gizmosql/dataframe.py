from __future__ import annotations

import logging
import typing as t
import pyarrow

from sqlframe.base.dataframe import (
    BaseDataFrame,
    _BaseDataFrameNaFunctions,
    _BaseDataFrameStatFunctions,
)
from sqlframe.base.mixins.dataframe_mixins import (
    NoCachePersistSupportMixin,
    TypedColumnsFromTempViewMixin,
)
from sqlframe.gizmosql.group import GizmoSQLGroupedData

if t.TYPE_CHECKING:
    from sqlframe.gizmosql.session import GizmoSQLSession  # noqa
    from sqlframe.gizmosql.readwriter import GizmoSQLDataFrameWriter  # noqa
    from sqlframe.gizmosql.group import GizmoSQLGroupedData  # noqa
    from pyarrow import Table as ArrowTable, RecordBatchReader

logger = logging.getLogger(__name__)


class GizmoSQLDataFrameNaFunctions(_BaseDataFrameNaFunctions["GizmoSQLDataFrame"]):
    pass


class GizmoSQLDataFrameStatFunctions(_BaseDataFrameStatFunctions["GizmoSQLDataFrame"]):
    pass


class GizmoSQLDataFrame(
    NoCachePersistSupportMixin,
    TypedColumnsFromTempViewMixin,
    BaseDataFrame[
        "GizmoSQLSession",
        "GizmoSQLDataFrameWriter",
        "GizmoSQLDataFrameNaFunctions",
        "GizmoSQLDataFrameStatFunctions",
        "GizmoSQLGroupedData",
    ],
):
    _na = GizmoSQLDataFrameNaFunctions
    _stat = GizmoSQLDataFrameStatFunctions
    _group_data = GizmoSQLGroupedData

    def explain(
        self, extended: t.Optional[t.Union[bool, str]] = None, mode: t.Optional[str] = None
    ) -> None:
        results = self._get_explain_plan_rows()
        print(results[0][1])

    @t.overload
    def toArrow(self) -> ArrowTable: ...

    @t.overload
    def toArrow(self, batch_size: int) -> RecordBatchReader: ...

    def toArrow(self, batch_size: t.Optional[int] = None) -> t.Union[ArrowTable, RecordBatchReader]:
        self._collect(skip_rows=True)
        if not batch_size:
            return self.session._last_result.fetch_arrow_table()
        table = self.session._last_result.fetch_arrow_table()
        batches = table.to_batches(max_chunksize=batch_size)
        return pyarrow.RecordBatchReader.from_batches(table.schema, batches)
