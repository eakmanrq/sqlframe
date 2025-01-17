from __future__ import annotations

import logging
import typing as t

from sqlframe.base.catalog import Column as CatalogColumn
from sqlframe.base.dataframe import (
    BaseDataFrame,
    _BaseDataFrameNaFunctions,
    _BaseDataFrameStatFunctions,
)
from sqlframe.base.mixins.dataframe_mixins import NoCachePersistSupportMixin
from sqlframe.bigquery.group import BigQueryGroupedData

if t.TYPE_CHECKING:
    from sqlframe.bigquery.readwriter import BigQueryDataFrameWriter
    from sqlframe.bigquery.session import BigQuerySession


logger = logging.getLogger(__name__)


class BigQueryDataFrameNaFunctions(_BaseDataFrameNaFunctions["BigQueryDataFrame"]):
    pass


class BigQueryDataFrameStatFunctions(_BaseDataFrameStatFunctions["BigQueryDataFrame"]):
    pass


class BigQueryDataFrame(
    NoCachePersistSupportMixin,
    BaseDataFrame[
        "BigQuerySession",
        "BigQueryDataFrameWriter",
        "BigQueryDataFrameNaFunctions",
        "BigQueryDataFrameStatFunctions",
        "BigQueryGroupedData",
    ],
):
    _na = BigQueryDataFrameNaFunctions
    _stat = BigQueryDataFrameStatFunctions
    _group_data = BigQueryGroupedData

    @property
    def _typed_columns(self) -> t.List[CatalogColumn]:
        from google.cloud import bigquery

        def field_to_column(field: bigquery.SchemaField) -> CatalogColumn:
            if field.field_type == "RECORD":
                data_type = "STRUCT<"
                for subfield in field.fields:
                    column = field_to_column(subfield)
                    data_type += f"{column.name} {column.dataType},"
                data_type += ">"
            elif field.field_type == "INTEGER":
                data_type = "INT64"
            else:
                data_type = field.field_type
            if field.mode == "REPEATED":
                data_type = f"ARRAY<{data_type}>"
            return CatalogColumn(
                name=field.name,
                dataType=data_type,
                nullable=field.is_nullable,
                description=None,
                isPartition=False,
                isBucket=False,
            )

        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        sql = self.session._to_sql(self.expression)
        query_job = self.session._client.query(sql, job_config=job_config)
        return [field_to_column(field) for field in query_job.schema]

    def explain(
        self, extended: t.Optional[t.Union[bool, str]] = None, mode: t.Optional[str] = None
    ) -> None:
        raise NotImplementedError("BigQuery does not support EXPLAIN")
