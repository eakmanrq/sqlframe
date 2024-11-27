# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import sys
import typing as t

import sqlglot as sg
from sqlglot import exp

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from sqlframe.base.mixins.readwriter_mixins import PandasLoaderMixin, PandasWriterMixin
from sqlframe.base.readerwriter import (
    _BaseDataFrameReader,
    _BaseDataFrameWriter,
)
from sqlframe.base.util import normalize_string

if t.TYPE_CHECKING:
    from sqlframe.databricks.session import DatabricksSession  # noqa
    from sqlframe.databricks.dataframe import DatabricksDataFrame  # noqa


class DatabricksDataFrameReader(
    PandasLoaderMixin["DatabricksSession", "DatabricksDataFrame"],
    _BaseDataFrameReader["DatabricksSession", "DatabricksDataFrame"],
):
    pass


class DatabricksDataFrameWriter(
    PandasWriterMixin["DatabricksSession", "DatabricksDataFrame"],
    _BaseDataFrameWriter["DatabricksSession", "DatabricksDataFrame"],
):
    def saveAsTable(
        self,
        name: str,
        format: t.Optional[str] = None,
        mode: t.Optional[str] = None,
        partitionBy: t.Optional[t.Union[str, t.List[str]]] = None,
        clusterBy: t.Optional[t.Union[str, t.List[str]]] = None,
        **options,
    ) -> Self:
        if format is not None:
            raise NotImplementedError("Providing Format in the save as table is not supported")
        exists, replace, mode = None, None, mode or str(self._mode)
        if mode == "append":
            return self.insertInto(name)
        if mode == "ignore":
            exists = True
        if mode == "overwrite":
            replace = True
        name = normalize_string(name, from_dialect="input", is_table=True)

        properties: t.List[exp.Expression] = []
        if partitionBy is not None:
            if isinstance(partitionBy, str):
                partition_by = [partitionBy]
            else:
                partition_by = partitionBy
            properties.append(
                exp.PartitionedByProperty(
                    this=exp.Tuple(expressions=list(map(sg.to_identifier, partition_by)))
                )
            )
        if clusterBy is not None:
            if isinstance(clusterBy, str):
                cluster_by = [clusterBy]
            else:
                cluster_by = clusterBy
            properties.append(
                exp.Cluster(
                    expressions=[exp.Tuple(expressions=list(map(sg.to_identifier, cluster_by)))]
                )
            )

        properties.extend(
            exp.Property(this=sg.to_identifier(name), value=exp.convert(value))
            for name, value in (options or {}).items()
        )

        output_expression_container = exp.Create(
            this=exp.to_table(name, dialect=self._session.input_dialect),
            kind="TABLE",
            exists=exists,
            replace=replace,
            properties=exp.Properties(expressions=properties),
        )
        df = self._df.copy(output_expression_container=output_expression_container)
        if self._session._has_connection:
            df.collect()
        return self.copy(_df=df)
