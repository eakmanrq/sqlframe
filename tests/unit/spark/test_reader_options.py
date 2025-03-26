from unittest.mock import MagicMock, patch

import pytest

from sqlframe.base.readerwriter import _BaseDataFrameReader
from sqlframe.spark import SparkDataFrameReader, SparkSession


@pytest.fixture
def mock_spark_session():
    """Create a mock SparkSession for testing."""
    session = MagicMock()
    session.input_dialect = "spark"
    session.spark_session = MagicMock()
    session.sql = MagicMock()
    session._last_loaded_file = None
    return session


@pytest.fixture
def reader(mock_spark_session):
    """Create a DataFrameReader instance for testing."""
    return SparkDataFrameReader(mock_spark_session)


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


@patch("sqlframe.spark.readwriter.SparkDataFrameReader.load")
def test_json_uses_options(mock_load, reader):
    """Test that the json method uses the stored options."""
    reader.options(inferSchema=True, header=True)
    reader.json("test.json")

    # Get the actual call arguments from the mock
    # The call_args is a tuple (args, kwargs)
    call_args = mock_load.call_args

    # Check that the method was called with the correct options
    assert call_args.kwargs["inferSchema"] is True
    assert call_args.kwargs["header"] is True
    assert call_args.kwargs["format"] == "json"
    assert call_args.kwargs["path"] == "test.json"


@patch("sqlframe.spark.readwriter.SparkDataFrameReader.load")
def test_csv_uses_options(mock_load, reader):
    """Test that the csv method uses the stored options."""
    reader.options(inferSchema=True, header=True)
    reader.csv("test.csv", comment="TEST_COMMENT")

    # Get the actual call arguments from the mock
    call_args = mock_load.call_args

    # Check that the method was called with the correct options
    assert call_args.kwargs["inferSchema"] is True
    assert call_args.kwargs["header"] is True
    assert call_args.kwargs["comment"] == "TEST_COMMENT"
    assert call_args.kwargs["format"] == "csv"
    assert call_args.kwargs["path"] == "test.csv"


@patch("sqlframe.spark.readwriter.SparkDataFrameReader.load")
def test_parquet_uses_options(mock_load, reader):
    """Test that the parquet method uses the stored options."""
    reader.options(inferSchema=True)
    reader.parquet("test.parquet")

    # Get the actual call arguments from the mock
    call_args = mock_load.call_args

    # Check that the method was called with the correct options
    assert call_args.kwargs["inferSchema"] is True
    assert call_args.kwargs["format"] == "parquet"
    assert call_args.kwargs["path"] == "test.parquet"


@patch("sqlframe.spark.readwriter.SparkDataFrameReader.load")
def test_method_specific_options_override_global_options(mock_load, reader):
    """Test that method-specific options override global options."""
    reader.options(inferSchema=True, header=True)
    reader.csv("test.csv", inferSchema=False, sep="|")

    # Get the actual call arguments from the mock
    call_args = mock_load.call_args

    # Check that the method was called with the correct options
    # Method-specific options should take precedence
    assert call_args.kwargs["inferSchema"] is False
    assert call_args.kwargs["header"] is True
    assert call_args.kwargs["sep"] == "|"
    assert call_args.kwargs["format"] == "csv"
    assert call_args.kwargs["path"] == "test.csv"


def test_load_merges_options(reader):
    """Test that the load method merges options from state_options."""
    reader.options(inferSchema=True, header=True)

    # Need to patch at a different level since we're testing the actual load method
    with patch.object(reader.session, "sql") as mock_sql:
        mock_sql.return_value = MagicMock()
        mock_sql.return_value.schema = None

        reader.load("test.csv", format="csv", nullValue="NULL")

        # Get the actual SQL that would be executed
        # The SQL should include the options from both state_options and method arguments
        mock_session_sql_call = reader.session.spark_session.sql.call_args[0][0]

        # Check that all options are included in the SQL
        assert "inferSchema" in mock_session_sql_call
        assert "header" in mock_session_sql_call
        assert "nullValue" in mock_session_sql_call
        assert "path" in mock_session_sql_call
        assert "USING csv" in mock_session_sql_call
        assert "test.csv" in mock_session_sql_call
