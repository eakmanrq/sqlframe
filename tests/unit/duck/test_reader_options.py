from unittest.mock import MagicMock, patch

import pytest

from sqlframe.base.readerwriter import _BaseDataFrameReader
from sqlframe.duckdb import DuckDBDataFrameReader


@pytest.fixture
def mock_duckdb_session():
    """Create a mock DuckDBSession for testing."""
    session = MagicMock()
    session.input_dialect = "duckdb"
    session.sql = MagicMock()
    session._last_loaded_file = None
    return session


@pytest.fixture
def reader(mock_duckdb_session):
    """Create a DataFrameReader instance for testing."""
    return DuckDBDataFrameReader(mock_duckdb_session)


def test_options_initialization(reader):
    """Test that options are correctly initialized."""
    assert reader.state_options == {}


def test_options_method(reader):
    """Test that the options method correctly stores options."""
    reader.options(inferSchema=True, header=True)
    assert reader.state_options == {"inferSchema": True, "header": True}


def test_option_method(reader):
    """Test that the option method correctly stores a single option."""
    reader.option("inferSchema", True)
    assert reader.state_options == {"inferSchema": True}


def test_options_and_option_methods_together(reader):
    """Test that option and options methods can be used together."""
    reader.options(inferSchema=True, header=True)
    reader.option("delimiter", ",")
    assert reader.state_options == {"inferSchema": True, "header": True, "delimiter": ","}


def test_options_override(reader):
    """Test that options override previous values with the same key."""
    reader.options(inferSchema=True, header=True)
    reader.options(inferSchema=False, nullValue="NULL")
    assert reader.state_options == {"inferSchema": False, "header": True, "nullValue": "NULL"}


def test_option_override(reader):
    """Test that option overrides previous values with the same key."""
    reader.option("inferSchema", True)
    reader.option("inferSchema", False)
    assert reader.state_options == {"inferSchema": False}


@patch.object(DuckDBDataFrameReader, "load")
def test_csv_uses_options(mock_load, reader):
    """Test that the csv method uses the stored options."""
    # Setup
    mock_load.return_value = MagicMock()
    reader.options(inferSchema=True, header=True, delimiter=",")

    # Execute
    reader.csv("test.csv", sep="|")

    # Assert
    call_args = mock_load.call_args
    assert call_args.kwargs["format"] == "csv"
    assert call_args.kwargs["path"] == "test.csv"
    assert call_args.kwargs["inferSchema"] is True
    assert call_args.kwargs["header"] is True
    assert call_args.kwargs["sep"] == "|"  # Method-specific overrides global
    assert call_args.kwargs["delimiter"] == ","


@patch.object(DuckDBDataFrameReader, "load")
def test_parquet_uses_options(mock_load, reader):
    """Test that the parquet method uses the stored options."""
    # Setup
    mock_load.return_value = MagicMock()
    reader.options(compression="snappy", row_group_size=1000)

    # Execute
    reader.parquet("test.parquet")

    # Assert
    call_args = mock_load.call_args
    assert call_args.kwargs["format"] == "parquet"
    assert call_args.kwargs["path"] == "test.parquet"
    assert call_args.kwargs["compression"] == "snappy"
    assert call_args.kwargs["row_group_size"] == 1000


@patch.object(DuckDBDataFrameReader, "load")
def test_method_specific_options_override_global_options(mock_load, reader):
    """Test that method-specific options override global options."""
    # Setup
    mock_load.return_value = MagicMock()
    reader.options(header=True, delimiter=",")

    # Execute
    reader.csv("test.csv", header=False)

    # Assert
    call_args = mock_load.call_args
    assert call_args.kwargs["header"] is False  # Method-specific overrides global
    assert call_args.kwargs["delimiter"] == ","
    assert call_args.kwargs["format"] == "csv"
    assert call_args.kwargs["path"] == "test.csv"
