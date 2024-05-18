import typing as t

import pytest

from sqlframe.base.dataframe import _BaseDataFrame
from sqlframe.postgres import PostgresDataFrame

pytest_plugins = ["tests.integration.fixtures"]


def test_approx_quantile(get_engine_df_and_pyspark: t.Callable[[str], _BaseDataFrame]):
    employee = get_engine_df_and_pyspark("employee")
    if isinstance(employee, PostgresDataFrame):
        pytest.skip("Approx quantile is not supported by the engine: postgres")
    results = employee.stat.approxQuantile(["employee_id", "age"], [0.1, 0.5, 1.0], 0)
    assert results == [[1.0, 3.0, 5.0], [27.0, 37.0, 65.0]]


def test_corr(get_engine_df_and_pyspark: t.Callable[[str], _BaseDataFrame]):
    employee = get_engine_df_and_pyspark("employee")
    results = employee.stat.corr("employee_id", "age")
    assert results == -0.5605569890127448


def test_cov(get_engine_df_and_pyspark: t.Callable[[str], _BaseDataFrame]):
    employee = get_engine_df_and_pyspark("employee")
    results = employee.stat.cov("employee_id", "age")
    assert results == -13.5
