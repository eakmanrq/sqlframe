class SQLFrameException(Exception):
    pass


class UnsupportedOperationError(SQLFrameException):
    pass


class RowError(SQLFrameException):
    pass


class TableSchemaError(SQLFrameException):
    pass
