# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.
from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    from sqlframe.base._typing import DataTypeOrString, UserDefinedFunctionLike
    from sqlframe.base.session import _BaseSession

    SESSION = t.TypeVar("SESSION", bound=_BaseSession)
else:
    SESSION = t.TypeVar("SESSION")


class _BaseUDFRegistration(t.Generic[SESSION]):
    def __init__(self, sparkSession: SESSION):
        self.sparkSession = sparkSession

    def register(
        self,
        name: str,
        f: t.Union[t.Callable[..., t.Any], UserDefinedFunctionLike],
        returnType: t.Optional[DataTypeOrString] = None,
    ) -> UserDefinedFunctionLike:
        raise NotImplementedError

    def registerJavaFunction(
        self,
        name: str,
        javaClassName: str,
        returnType: t.Optional[DataTypeOrString] = None,
    ) -> None:
        raise NotImplementedError

    def registerJavaUDAF(self, name: str, javaClassName: str) -> None:
        raise NotImplementedError
