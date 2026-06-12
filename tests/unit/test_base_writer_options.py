from unittest.mock import MagicMock

from sqlframe.base.readerwriter import _BaseDataFrameWriter


def test_save_uses_format_mode_and_options():
    writer = _BaseDataFrameWriter(MagicMock())
    write_mock = MagicMock()
    writer._write = write_mock  # type: ignore[method-assign]

    writer.options(header=True, nullValue="NULL").save(
        "test.csv", format="csv", mode="overwrite", header=False, emptyValue=None
    )

    write_mock.assert_called_once_with(
        path="test.csv",
        mode="overwrite",
        format="csv",
        partitionBy=None,
        header=False,
        nullValue="NULL",
    )


def test_save_uses_state_format():
    writer = _BaseDataFrameWriter(MagicMock())
    write_mock = MagicMock()
    writer._write = write_mock  # type: ignore[method-assign]

    writer.format("json").save("test.json")

    write_mock.assert_called_once_with(path="test.json", mode=None, format="json", partitionBy=None)


def test_csv_uses_writer_options():
    writer = _BaseDataFrameWriter(MagicMock())
    write_mock = MagicMock()
    writer._write = write_mock  # type: ignore[method-assign]

    writer.option("header", True).csv("test.csv", header=False, sep="|")

    write_mock.assert_called_once_with(
        path="test.csv",
        mode=None,
        format="csv",
        header=False,
        sep="|",
    )


def test_csv_uses_writer_mode():
    writer = _BaseDataFrameWriter(MagicMock())
    copied_writer = writer.mode("overwrite")
    write_mock = MagicMock()
    copied_writer._write = write_mock  # type: ignore[method-assign]

    copied_writer.csv("test.csv")

    write_mock.assert_called_once_with(
        path="test.csv",
        mode="overwrite",
        format="csv",
    )


def test_save_defaults_to_parquet():
    writer = _BaseDataFrameWriter(MagicMock())
    write_mock = MagicMock()
    writer._write = write_mock  # type: ignore[method-assign]

    writer.save("test")

    write_mock.assert_called_once_with(path="test", mode=None, format="parquet", partitionBy=None)


def test_mode_copy_preserves_options():
    writer = _BaseDataFrameWriter(MagicMock())
    copied_writer = writer.option("compression", "gzip").mode("overwrite")
    write_mock = MagicMock()
    copied_writer._write = write_mock  # type: ignore[method-assign]

    copied_writer.save("test.parquet")

    write_mock.assert_called_once_with(
        path="test.parquet",
        mode="overwrite",
        format="parquet",
        partitionBy=None,
        compression="gzip",
    )
