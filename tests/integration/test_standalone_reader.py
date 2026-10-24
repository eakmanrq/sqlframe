from pathlib import Path

import duckdb
import pytest


from sqlframe import activate_context
from sqlframe.standalone import StandaloneDataFrame, StandaloneSession

PARQUET_FILE = Path(__file__).resolve().parents[1] / "fixtures" / "employee.parquet"


def test_activated_standalone_session_reads_parquet():
    with activate_context("standalone"):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("ProfilingDemo").getOrCreate()
        try:
            assert isinstance(spark, StandaloneSession)
            df = spark.read.parquet(str(PARQUET_FILE))
            assert isinstance(df, StandaloneDataFrame)

            rows = duckdb.sql(df.sql(dialect="duckdb")).fetchall()
            assert rows == [
                (1, "Jack", "Shephard", 37, 1),
                (2, "John", "Locke", 65, 1),
                (3, "Kate", "Austen", 37, 2),
                (4, "Claire", "Littleton", 27, 2),
                (5, "Hugo", "Reyes", 29, 100),
            ]
        finally:
            spark.stop()


@pytest.mark.parametrize("activated", [False, True])
@pytest.mark.parametrize("method", ["parquet", "load"])
@pytest.mark.parametrize("column", ["number", "fraction", "event_time"])
def test_standalone_parquet_preserves_nulls(tmp_path, activated, method, column):
    from contextlib import nullcontext
    from datetime import datetime

    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "nullable.parquet"
    values, dtype, sql_type = {
        "number": ([2**53 + 1, None], pa.int64(), "BIGINT"),
        "fraction": ([1.5, None], pa.float64(), "DOUBLE"),
        "event_time": ([datetime(2026, 1, 2), None], pa.timestamp("ns"), "TIMESTAMP"),
    }[column]
    pq.write_table(pa.table({column: pa.array(values, type=dtype)}), path)

    with activate_context("standalone") if activated else nullcontext():
        if activated:
            from pyspark.sql import SparkSession
        else:
            SparkSession = StandaloneSession
        spark = SparkSession.builder.getOrCreate()
        try:
            df = getattr(spark.read, method)(str(path))
            sql = df.sql(dialect="duckdb")
            result = duckdb.sql(sql)
            assert result.fetchall() == [(values[0],), (None,)]
            assert str(result.types[0]) == sql_type
            assert duckdb.sql(
                f'SELECT COUNT("{column}"), COUNT(*) FILTER (WHERE "{column}" IS NULL) '
                f'FROM ({sql})'
            ).fetchone() == (1, 1)
            assert duckdb.sql(
                f'SELECT COALESCE("{column}", CAST(NULL AS {sql_type})) FROM ({sql}) '
                f'WHERE "{column}" IS NULL'
            ).fetchall() == [(None,)]
        finally:
            spark.stop()
