from __future__ import annotations

import pathlib
import typing as t

import pytest

from sqlframe.base.session import _BaseSession
from sqlframe.base.types import Row
from sqlframe.duckdb.session import DuckDBSession

if t.TYPE_CHECKING:
    from sqlframe.base.dataframe import _BaseDataFrame

pytest_plugins = ["tests.integration.fixtures"]


@pytest.fixture
def cleanup_employee_df(
    get_engine_df: t.Callable[[str], _BaseDataFrame],
) -> t.Iterator[_BaseDataFrame]:
    df = get_engine_df("employee")
    df.session._execute("DROP TABLE IF EXISTS insert_into_employee")
    df.session._execute("DROP TABLE IF EXISTS save_as_table_employee")
    yield df
    df.session._execute("DROP TABLE IF EXISTS insert_into_employee")
    df.session._execute("DROP TABLE IF EXISTS save_as_table_employee")


def test_write_json(get_engine_df: t.Callable[[str], _BaseDataFrame], tmp_path: pathlib.Path):
    df_employee = get_engine_df("employee")
    temp_json = str(tmp_path / "employee.json")
    df_employee.write.json(temp_json)
    df2 = df_employee.session.read.json(temp_json)
    assert df2.collect() == df_employee.collect()


def test_write_json_append(get_session: t.Callable[[], _BaseSession], tmp_path: pathlib.Path):
    session = get_session()
    temp_json = tmp_path / "test.json"
    df1 = session.createDataFrame([(1,)])
    df2 = session.createDataFrame([(2,)])
    if isinstance(session, DuckDBSession):
        with pytest.raises(NotImplementedError):
            df1.write.json(str(temp_json), mode="append")
    else:
        df1.write.json(str(temp_json), mode="append")
        df2.write.json(str(temp_json), mode="append")
        df_result = session.read.json(str(temp_json))
        assert df_result.collect() == [Row(_1=1), Row(_1=2)]


def test_write_json_ignore(
    get_engine_df: t.Callable[[str], _BaseDataFrame], tmp_path: pathlib.Path
):
    df_employee = get_engine_df("employee")
    temp_json = tmp_path / "employee.json"
    temp_json.touch()
    df_employee.write.json(str(temp_json), mode="ignore")
    df2 = df_employee.session.read.json(str(temp_json))
    assert df2.collect() == []


def test_write_json_error(
    get_engine_df: t.Callable[[str], _BaseDataFrame], tmp_path: pathlib.Path, caplog
):
    df_employee = get_engine_df("employee")
    temp_json = tmp_path / "employee.json"
    temp_json.touch()
    with pytest.raises(FileExistsError):
        df_employee.write.json(temp_json, mode="error")


def test_write_parquet(get_engine_df: t.Callable[[str], _BaseDataFrame], tmp_path: pathlib.Path):
    df_employee = get_engine_df("employee")
    temp_parquet = str(tmp_path / "employee.parquet")
    df_employee.write.parquet(temp_parquet)
    df2 = df_employee.session.read.parquet(temp_parquet)
    assert df2.collect() == df_employee.collect()


def test_write_parquet_ignore(
    get_engine_df: t.Callable[[str], _BaseDataFrame], tmp_path: pathlib.Path
):
    df_employee = get_engine_df("employee")
    temp_parquet = str(tmp_path / "employee.parquet")
    df_empty = df_employee.session.createDataFrame([])
    # Write nothing
    df_empty.write.parquet(temp_parquet)
    # Try writing actual data
    df_employee.write.parquet(temp_parquet, mode="ignore")
    df2 = df_employee.session.read.parquet(temp_parquet)
    # Check that second write did not happen
    assert df2.collect() == []


def test_write_parquet_error(
    get_engine_df: t.Callable[[str], _BaseDataFrame], tmp_path: pathlib.Path, caplog
):
    df_employee = get_engine_df("employee")
    temp_parquet = tmp_path / "employee.parquet"
    temp_parquet.touch()
    with pytest.raises(FileExistsError):
        df_employee.write.json(temp_parquet, mode="error")


def test_write_parquet_unsupported_modes(
    get_engine_df: t.Callable[[str], _BaseDataFrame], tmp_path: pathlib.Path
):
    df_employee = get_engine_df("employee")
    temp_json = tmp_path / "employee.parquet"
    with pytest.raises(NotImplementedError):
        df_employee.write.parquet(str(temp_json), mode="append")


def test_write_csv(get_engine_df: t.Callable[[str], _BaseDataFrame], tmp_path: pathlib.Path):
    df_employee = get_engine_df("employee")
    temp_csv = str(tmp_path / "employee.csv")
    df_employee.write.csv(temp_csv)
    df2 = df_employee.session.read.csv(temp_csv)
    assert df2.collect() == df_employee.collect()


def test_write_csv_append(get_session: t.Callable[[], _BaseSession], tmp_path: pathlib.Path):
    session = get_session()
    temp_csv = tmp_path / "test.csv"
    df1 = session.createDataFrame([(1,)])
    df2 = session.createDataFrame([(2,)])
    if isinstance(session, DuckDBSession):
        with pytest.raises(NotImplementedError):
            df1.write.csv(str(temp_csv), mode="append")
    else:
        df1.write.csv(str(temp_csv), mode="append")
        df2.write.csv(str(temp_csv), mode="append")
        df_result = session.read.csv(str(temp_csv))
        assert df_result.collect() == [Row(_1=1), Row(_1=2)]


def test_write_csv_ignore(get_engine_df: t.Callable[[str], _BaseDataFrame], tmp_path: pathlib.Path):
    df_employee = get_engine_df("employee")
    temp_csv = str(tmp_path / "employee.csv")
    df1 = df_employee.session.createDataFrame([(1,)])
    df2 = df_employee.session.createDataFrame([(2,)])
    # Write nothing
    df1.write.csv(temp_csv)
    # Try writing actual data
    df2.write.csv(temp_csv, mode="ignore")
    df_result = df_employee.session.read.csv(temp_csv)
    # Check that second write did not happen
    assert df_result.collect() == df1.collect()


def test_write_csv_error(get_engine_df: t.Callable[[str], _BaseDataFrame], tmp_path: pathlib.Path):
    df_employee = get_engine_df("employee")
    temp_csv = tmp_path / "employee.csv"
    temp_csv.touch()
    with pytest.raises(FileExistsError):
        df_employee.write.json(temp_csv, mode="error")


def test_save_as_table(cleanup_employee_df: _BaseDataFrame, caplog):
    df_employee = cleanup_employee_df
    df_employee.write.saveAsTable("save_as_table_employee")
    df2 = df_employee.session.read.table("save_as_table_employee")
    assert sorted(df2.collect()) == sorted(df_employee.collect())


def test_insertInto(cleanup_employee_df: _BaseDataFrame, caplog):
    df_employee = cleanup_employee_df
    df = df_employee.session.createDataFrame(
        [(9, "Sayid", "Jarrah", 40, 1)], ["id", "first_name", "last_name", "age", "store_id"]
    )
    df_employee.write.saveAsTable("insert_into_employee")
    df.write.insertInto("insert_into_employee")
    df2 = df_employee.session.read.table("insert_into_employee")
    assert sorted(df_employee.union(df).collect()) == sorted(df2.collect())
