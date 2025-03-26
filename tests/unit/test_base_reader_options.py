from unittest.mock import MagicMock, patch

import pytest

from sqlframe.base.readerwriter import _BaseDataFrameReader


# Mock implementation of _BaseDataFrameReader for testing
class MockReader(_BaseDataFrameReader):
    """Mock implementation of _BaseDataFrameReader for testing."""

    def load(self, path=None, format=None, schema=None, **options):
        """Mock implementation that just returns the arguments."""
        self.last_load_args = {"path": path, "format": format, "schema": schema, "options": options}
        return MagicMock()


@pytest.fixture
def mock_session():
    """Create a mock session for testing."""
    session = MagicMock()
    session.input_dialect = "test"
    return session


@pytest.fixture
def reader(mock_session):
    """Create a _BaseDataFrameReader instance for testing."""
    return MockReader(mock_session)


def test_options_initialization(reader):
    """Test that options are correctly initialized."""
    assert reader.state_options == {}


def test_options_method(reader):
    """Test that the options method correctly stores options."""
    result = reader.options(inferSchema=True, header=True)
    assert reader.state_options == {"inferSchema": True, "header": True}
    # Should return self for method chaining
    assert result is reader


def test_option_method(reader):
    """Test that the option method correctly stores a single option."""
    result = reader.option("inferSchema", True)
    assert reader.state_options == {"inferSchema": True}
    # Should return self for method chaining
    assert result is reader


def test_options_method_chaining(reader):
    """Test that options method supports chaining."""
    reader.options(inferSchema=True).options(header=True)
    assert reader.state_options == {"inferSchema": True, "header": True}


def test_option_method_chaining(reader):
    """Test that option method supports chaining."""
    reader.option("inferSchema", True).option("header", True)
    assert reader.state_options == {"inferSchema": True, "header": True}


def test_mixed_chaining(reader):
    """Test that options and option methods can be chained together."""
    reader.options(inferSchema=True).option("header", True).options(delimiter=",")
    assert reader.state_options == {"inferSchema": True, "header": True, "delimiter": ","}


def test_csv_merges_options(reader):
    """Test that csv method correctly merges options."""
    reader.options(inferSchema=True, header=True)
    reader.csv("test.csv", sep=",")

    assert reader.last_load_args["path"] == "test.csv"
    assert reader.last_load_args["format"] == "csv"
    assert reader.last_load_args["options"]["inferSchema"] is True
    assert reader.last_load_args["options"]["header"] is True
    assert reader.last_load_args["options"]["sep"] == ","


def test_json_merges_options(reader):
    """Test that json method correctly merges options."""
    reader.options(multiLine=True, dateFormat="yyyy-MM-dd")
    reader.json("test.json", encoding="UTF-8")

    assert reader.last_load_args["path"] == "test.json"
    assert reader.last_load_args["format"] == "json"
    assert reader.last_load_args["options"]["multiLine"] is True
    assert reader.last_load_args["options"]["dateFormat"] == "yyyy-MM-dd"
    assert reader.last_load_args["options"]["encoding"] == "UTF-8"


def test_parquet_merges_options(reader):
    """Test that parquet method correctly merges options."""
    reader.options(compression="snappy")
    reader.parquet("test.parquet", mergeSchema=True)

    assert reader.last_load_args["path"] == "test.parquet"
    assert reader.last_load_args["format"] == "parquet"
    assert reader.last_load_args["options"]["compression"] == "snappy"
    assert reader.last_load_args["options"]["mergeSchema"] is True


def test_format_method_preserves_options(reader):
    """Test that format method preserves options."""
    reader.options(inferSchema=True, header=True)
    reader.format("csv")

    assert reader.state_options == {"inferSchema": True, "header": True}
    assert reader.state_format_to_read == "csv"


def test_method_specific_options_override_global(reader):
    """Test that method-specific options override global options."""
    reader.options(inferSchema=True, header=True)
    reader.csv("test.csv", inferSchema=False)

    assert reader.last_load_args["options"]["inferSchema"] is False
    assert reader.last_load_args["options"]["header"] is True


def test_chained_file_operations(reader):
    """Test that options are preserved across file operations."""
    reader.options(inferSchema=True).csv("test1.csv")
    assert reader.last_load_args["options"]["inferSchema"] is True

    # Options should still be available for the next operation
    reader.json("test2.json")
    assert reader.last_load_args["options"]["inferSchema"] is True

    # Add another option and verify both are used
    reader.option("multiLine", True).json("test3.json")
    assert reader.last_load_args["options"]["inferSchema"] is True
    assert reader.last_load_args["options"]["multiLine"] is True
