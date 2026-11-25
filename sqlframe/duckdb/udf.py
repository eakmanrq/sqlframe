from __future__ import annotations

import typing as t

from sqlframe.base.types import DataType, StringType
from sqlframe.base.udf import _BaseUDFRegistration
from sqlframe.base.util import spark_to_sqlglot

if t.TYPE_CHECKING:
    from sqlframe.base._typing import DataTypeOrString, UserDefinedFunctionLike
    from sqlframe.duckdb.session import DuckDBSession


class DuckDBUDFRegistration(_BaseUDFRegistration["DuckDBSession"]):
    def register(
        self,
        name: str,
        f: t.Union[t.Callable[..., t.Any], UserDefinedFunctionLike],
        returnType: t.Optional[DataTypeOrString] = None,
    ) -> UserDefinedFunctionLike:
        if returnType is None:
            returnType = StringType()
        if isinstance(returnType, DataType):
            returnType = spark_to_sqlglot(returnType).sql(dialect="duckdb")
        self.sparkSession._conn.create_function(name, f, return_type=returnType)
        return f  # type: ignore[return-value]
