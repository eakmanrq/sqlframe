import typing as t
from pathlib import Path

import pytest
from pyspark.sql import SparkSession as PySparkSession

from sqlframe.gizmosql import GizmoSQLSession

pytest_plugins = ["tests.common_fixtures", "tests.integration.fixtures"]


@pytest.mark.parametrize(
    "num",
    list(range(1, 100)),
)
def test_tpcds(
    num: int,
    pyspark_session: PySparkSession,
    gizmosql_session: GizmoSQLSession,
    compare_frames: t.Callable,
):
    if num in [16, 32, 50, 62, 92, 94, 95, 99]:
        pytest.skip(f"TPCDS{num} is not supported by PySpark due to spaces in column names")
    with open(f"tests/fixtures/tpcds/tpcds{num}.sql") as f:
        query = f.read()
    df = pyspark_session.sql(query)
    dfs = gizmosql_session.sql(sqlQuery=query, qualify=False)
    compare_frames(df, dfs, no_empty=False)
