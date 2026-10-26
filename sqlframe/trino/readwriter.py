from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.standalone.readwriter import (
    StandaloneDataFrameReader as TrinoDataFrameReader,
)
from sqlframe.standalone.readwriter import StandaloneDataFrameWriter


class TrinoDataFrameWriter(StandaloneDataFrameWriter):
    def insertInto(self, tableName: str, overwrite: bool | None = None):
        if overwrite:
            raise UnsupportedOperationError("Trino insertInto overwrite is not supported")
        return super().insertInto(tableName, overwrite=overwrite)

    def saveAsTable(self, name: str, format: str | None = None, mode: str | None = None):
        if (mode or self._mode or "").lower() == "overwrite":
            raise UnsupportedOperationError("Trino saveAsTable overwrite is not supported")
        return super().saveAsTable(name, format=format, mode=mode)


__all__ = ["TrinoDataFrameReader", "TrinoDataFrameWriter"]
