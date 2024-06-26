class SQLFrameException(Exception):
    pass


class UnsupportedOperationError(SQLFrameException):
    pass


class RowError(SQLFrameException):
    pass


class TableSchemaError(SQLFrameException):
    pass


class PandasDiffError(SQLFrameException):
    pass


class DataFrameDiffError(SQLFrameException):
    pass


class SchemaDiffError(SQLFrameException):
    pass
