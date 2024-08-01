from __future__ import annotations

import typing as t

from sqlframe.base.udf import _BaseUDFRegistration

if t.TYPE_CHECKING:
    from sqlframe.base._typing import DataTypeOrString, UserDefinedFunctionLike
    from sqlframe.duckdb.session import DuckDBSession


class DuckDBUDFRegistration(_BaseUDFRegistration["DuckDBSession"]):
    def register(  # type: ignore
        self,
        name: str,
        f: t.Union[t.Callable[..., t.Any], UserDefinedFunctionLike],
        returnType: t.Optional[DataTypeOrString] = None,
    ) -> UserDefinedFunctionLike:
        self.sparkSession._conn.create_function(name, f, return_type=returnType)  # type: ignore
