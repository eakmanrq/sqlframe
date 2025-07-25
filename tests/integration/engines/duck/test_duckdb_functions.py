import duckdb

from sqlframe.duckdb import DuckDBSession
from sqlframe.duckdb import functions as F


def test_ends_with():
    con = duckdb.connect(database=":memory:")
    session: DuckDBSession = DuckDBSession(conn=con)

    df = session.createDataFrame([["abc"]], schema=["col1"])

    df = df.filter((F.col("col1").endswith("c")))

    sql = df.sql(dialect="duckdb")
    print(sql)

    assert "ENDS_WITH" in sql
    """
    >       assert "ENDS_WITH" in sql
E       assert 'ENDS_WITH' in 'SELECT\n  CAST("a1"."col1" AS TEXT) AS "col1"\nFROM (VALUES\n  (\'abc\')) AS "a1"("col1")\nWHERE\n  ENDSWITH(CAST("a1"."col1" AS TEXT), \'c\')'
    """
