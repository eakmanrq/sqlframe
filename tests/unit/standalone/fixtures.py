from __future__ import annotations

import typing as t

import pytest
from sqlglot.helper import ensure_list

from sqlframe.base import types as SQLFrameTypes
from sqlframe.base.column import Column
from sqlframe.standalone import StandaloneDataFrame
from sqlframe.standalone.session import StandaloneSession

if t.TYPE_CHECKING:
    from tests.types import EmployeeData


@pytest.fixture(scope="function")
def sqlf_sf() -> StandaloneSession:
    return StandaloneSession.builder.config(
        map={
            "sqlframe.input.dialect": "snowflake",
            "sqlframe.execution.dialect": "snowflake",
            "sqlframe.output.dialect": "snowflake",
        }
    ).getOrCreate()


@pytest.fixture(scope="function")
def _function_employee_data() -> EmployeeData:
    return [
        (1, "Jack", "Shephard", 37, 1),
        (2, "John", "Locke", 65, 1),
        (3, "Kate", "Austen", 37, 2),
        (4, "Claire", "Littleton", 27, 2),
        (5, "Hugo", "Reyes", 29, 100),
    ]


@pytest.fixture(scope="function")
def sqlf_employee(
    sqlf: StandaloneSession, _function_employee_data: EmployeeData
) -> StandaloneDataFrame:
    sqlf_employee_schema = SQLFrameTypes.StructType(
        [
            SQLFrameTypes.StructField("employee_id", SQLFrameTypes.IntegerType(), False),
            SQLFrameTypes.StructField("fname", SQLFrameTypes.StringType(), False),
            SQLFrameTypes.StructField("lname", SQLFrameTypes.StringType(), False),
            SQLFrameTypes.StructField("age", SQLFrameTypes.IntegerType(), False),
            SQLFrameTypes.StructField("store_id", SQLFrameTypes.IntegerType(), False),
        ]
    )
    df = sqlf.createDataFrame(data=_function_employee_data, schema=sqlf_employee_schema)
    sqlf.catalog.add_table("employee", sqlf_employee_schema)  # type: ignore
    return df


@pytest.fixture
def compare_sql() -> t.Callable:
    def _make_function(
        df: StandaloneDataFrame,
        expected_statements: t.Union[str, t.List[str]],
        pretty=False,
        **kwargs,
    ):
        df_kwargs = {"pretty": pretty, **kwargs}
        if not isinstance(df, Column):
            df_kwargs["optimize"] = df_kwargs.get("optimize", True)
            df_kwargs["as_list"] = df_kwargs.get("as_list", True)
        actual_sqls = ensure_list(df.sql(**df_kwargs))
        expected_statements = ensure_list(expected_statements)
        assert len(actual_sqls) == len(expected_statements)
        for expected, actual in zip(expected_statements, actual_sqls):
            assert actual == expected

    return _make_function
