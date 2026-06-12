from unittest.mock import MagicMock

from sqlframe.base.readerwriter import _BaseDataFrameWriter


def test_save_uses_format_mode_and_options():
    writer = _BaseDataFrameWriter(MagicMock())
    writer._write = MagicMock()  # type: ignore[method-assign]

    writer.options(header=True, nullValue="NULL").save(
        "test.csv", format="csv", mode="overwrite", header=False, emptyValue=None
    )

    writer._write.assert_called_once_with(
        path="test.csv",
        mode="overwrite",
        format="csv",
        partitionBy=None,
        header=False,
        nullValue="NULL",
    )


def test_save_uses_state_format():
    writer = _BaseDataFrameWriter(MagicMock())
    writer._write = MagicMock()  # type: ignore[method-assign]

    writer.format("json").save("test.json")

    writer._write.assert_called_once_with(
        path="test.json", mode=None, format="json", partitionBy=None
    )


def test_csv_uses_writer_options():
    writer = _BaseDataFrameWriter(MagicMock())
    writer._write = MagicMock()  # type: ignore[method-assign]

    writer.option("header", True).csv("test.csv", header=False, sep="|")

    writer._write.assert_called_once_with(
        path="test.csv",
        mode=None,
        format="csv",
        header=False,
        sep="|",
    )


def test_csv_uses_writer_mode():
    writer = _BaseDataFrameWriter(MagicMock())
    copied_writer = writer.mode("overwrite")
    copied_writer._write = MagicMock()  # type: ignore[method-assign]

    copied_writer.csv("test.csv")

    copied_writer._write.assert_called_once_with(
        path="test.csv",
        mode="overwrite",
        format="csv",
    )


def test_save_defaults_to_parquet():
    writer = _BaseDataFrameWriter(MagicMock())
    writer._write = MagicMock()  # type: ignore[method-assign]

    writer.save("test")

    writer._write.assert_called_once_with(
        path="test", mode=None, format="parquet", partitionBy=None
    )


def test_mode_copy_preserves_options():
    writer = _BaseDataFrameWriter(MagicMock())
    copied_writer = writer.option("compression", "gzip").mode("overwrite")
    copied_writer._write = MagicMock()  # type: ignore[method-assign]

    copied_writer.save("test.parquet")

    copied_writer._write.assert_called_once_with(
        path="test.parquet",
        mode="overwrite",
        format="parquet",
        partitionBy=None,
        compression="gzip",
    )
