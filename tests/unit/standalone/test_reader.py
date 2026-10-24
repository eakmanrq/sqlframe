from pathlib import Path
from unittest.mock import patch

import pytest

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.standalone.session import StandaloneSession

pytest_plugins = ["tests.common_fixtures"]

PARQUET_FILE = Path(__file__).resolve().parents[2] / "fixtures" / "employee.parquet"


def test_parquet_loads_file_into_standalone_dataframe(standalone_session: StandaloneSession):
    df = standalone_session.read.parquet(str(PARQUET_FILE))

    assert df.columns == ["employee_id", "fname", "lname", "age", "store_id"]
    assert "Jack" in df.sql()
    assert "Shephard" in df.sql()


def test_load_infers_parquet_format(standalone_session: StandaloneSession):
    df = standalone_session.read.load(str(PARQUET_FILE))

    assert df.columns == ["employee_id", "fname", "lname", "age", "store_id"]


def test_load_uses_reader_format_and_explicit_schema(standalone_session: StandaloneSession):
    df = standalone_session.read.format("parquet").load(
        str(PARQUET_FILE),
        schema="employee_id STRING, fname STRING, lname STRING, age INT, store_id INT",
    )

    assert "CAST(`a1`.`employee_id` AS STRING) AS `employee_id`" in df.sql(pretty=False)


@pytest.mark.parametrize("format_name", ["csv", "json"])
@pytest.mark.parametrize("format_source", ["reader", "keyword"])
def test_load_rejects_unsupported_format(
    standalone_session: StandaloneSession, format_name, format_source
):
    reader = standalone_session.read
    kwargs = {}
    if format_source == "reader":
        reader = reader.format(format_name)
    else:
        kwargs = {"format": format_name}
    with pytest.raises(UnsupportedOperationError, match=f"Unsupported format: {format_name}"):
        reader.load(str(PARQUET_FILE), **kwargs)


def test_parquet_reports_missing_file(standalone_session: StandaloneSession, tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        standalone_session.read.parquet(str(tmp_path / "missing.parquet"))


def test_load_requires_a_path(standalone_session: StandaloneSession):
    with pytest.raises(ValueError, match="path is required"):
        standalone_session.read.load(format="parquet")


def test_parquet_missing_dependency_names_install_extra(standalone_session: StandaloneSession):
    with patch.dict("sys.modules", {"pyarrow": None}):
        with pytest.raises(ImportError, match=r"sqlframe\[standalone\]"):
            standalone_session.read.parquet(str(PARQUET_FILE))
