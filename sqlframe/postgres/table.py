from sqlframe.base.table import _BaseTable
from sqlframe.postgres.dataframe import PostgresDataFrame


class PostgresTable(PostgresDataFrame, _BaseTable["PostgresDataFrame"]):
    _df = PostgresDataFrame
