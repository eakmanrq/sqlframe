from pathlib import Path

import duckdb

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
