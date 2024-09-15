import typing as t
from pathlib import Path

import duckdb
import pytest
from pyspark.sql import SparkSession as PySparkSession

from sqlframe.base.session import _BaseSession
from tests.common_fixtures import load_tpcds

pytest_plugins = ["tests.common_fixtures", "tests.integration.fixtures"]


@pytest.mark.parametrize(
    "num",
    list(range(1, 100)),
)
def test_tpcds(
    num: int,
    pyspark_session: PySparkSession,
    duckdb_session: _BaseSession,
    gen_tpcds: t.List[Path],
    compare_frames: t.Callable,
):
    if num in [16, 32, 50, 62, 92, 94, 95, 99]:
        pytest.skip(f"TPCDS{num} is not supported by PySpark due to spaces in column names")
    load_tpcds(gen_tpcds, duckdb_session)
    with open(f"tests/fixtures/tpcds/tpcds{num}.sql") as f:
        query = f.read()
    df = pyspark_session.sql(query)
    dfs = duckdb_session.sql(query)
    compare_frames(df, dfs, no_empty=False)
