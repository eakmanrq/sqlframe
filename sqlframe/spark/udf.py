from __future__ import annotations

import typing as t

from sqlframe.base.udf import _BaseUDFRegistration

if t.TYPE_CHECKING:
    from sqlframe.base._typing import DataTypeOrString, UserDefinedFunctionLike
    from sqlframe.spark.session import SparkSession  # type: ignore


class SparkUDFRegistration(_BaseUDFRegistration["SparkSession"]):
    def register(
        self,
        name: str,
        f: t.Union[t.Callable[..., t.Any], UserDefinedFunctionLike],
        returnType: t.Optional[DataTypeOrString] = None,
    ) -> UserDefinedFunctionLike:
        return self.sparkSession.spark_session.udf.register(name, f, returnType=returnType)  # type: ignore

    def registerJavaFunction(
        self,
        name: str,
        javaClassName: str,
        returnType: t.Optional[DataTypeOrString] = None,
    ) -> None:
        self.sparkSession.spark_session.udf.registerJavaFunction(
            name,
            javaClassName,
            returnType=returnType,  # type: ignore
        )

    def registerJavaUDAF(self, name: str, javaClassName: str) -> None:
        self.sparkSession.spark_session.udf.registerJavaUDAF(name, javaClassName)
