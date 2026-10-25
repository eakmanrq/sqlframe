from pathlib import Path

import duckdb
import pytest

from sqlframe import activate_context
from sqlframe.base import types
from sqlframe.standalone import StandaloneDataFrame, StandaloneSession

PARQUET_FILE = Path(__file__).resolve().parents[1] / "fixtures" / "employee.parquet"


def query(sql: str) -> duckdb.DuckDBPyRelation:
    result = duckdb.sql(sql)
    assert result is not None
    return result


@pytest.mark.parametrize("activated", [False, True])
def test_load_defaults_to_parquet_for_extensionless_directory(tmp_path, activated):
    from contextlib import nullcontext

    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "events"
    path.mkdir()
    pq.write_table(pa.table({"id": [1, 2]}), path / "part-00000.parquet")

    with activate_context("standalone") if activated else nullcontext():
        if activated:
            from pyspark.sql import SparkSession
        else:
            SparkSession = StandaloneSession
        spark = SparkSession.builder.getOrCreate()
        try:
            df = spark.read.load(str(path))
            assert isinstance(df, StandaloneDataFrame)
            assert query(df.sql(dialect="duckdb")).fetchall() == [(1,), (2,)]
        finally:
            spark.stop()


@pytest.mark.parametrize("format_name", ["PARQUET", "PaRqUeT"])
@pytest.mark.parametrize("format_source", ["reader", "keyword"])
def test_activated_parquet_format_is_case_insensitive(format_name, format_source):
    with activate_context("standalone"):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        try:
            reader = spark.read
            kwargs = {}
            if format_source == "reader":
                reader = reader.format(format_name)
            else:
                kwargs = {"format": format_name}
            df = reader.load(str(PARQUET_FILE), **kwargs)

            assert isinstance(df, StandaloneDataFrame)
            assert query(df.sql(dialect="duckdb")).fetchall() == [
                (1, "Jack", "Shephard", 37, 1),
                (2, "John", "Locke", 65, 1),
                (3, "Kate", "Austen", 37, 2),
                (4, "Claire", "Littleton", 27, 2),
                (5, "Hugo", "Reyes", 29, 100),
            ]
        finally:
            spark.stop()


def test_activated_standalone_session_reads_parquet():
    with activate_context("standalone"):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("ProfilingDemo").getOrCreate()
        try:
            assert isinstance(spark, StandaloneSession)
            df = spark.read.parquet(str(PARQUET_FILE))
            assert isinstance(df, StandaloneDataFrame)

            rows = query(df.sql(dialect="duckdb")).fetchall()
            assert rows == [
                (1, "Jack", "Shephard", 37, 1),
                (2, "John", "Locke", 65, 1),
                (3, "Kate", "Austen", 37, 2),
                (4, "Claire", "Littleton", 27, 2),
                (5, "Hugo", "Reyes", 29, 100),
            ]
        finally:
            spark.stop()


@pytest.mark.parametrize("activated", [False, True])
@pytest.mark.parametrize("method", ["parquet", "load"])
@pytest.mark.parametrize("column", ["number", "fraction", "event_time"])
def test_standalone_parquet_preserves_nulls(tmp_path, activated, method, column):
    from contextlib import nullcontext
    from datetime import datetime

    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "nullable.parquet"
    values, dtype, sql_type = {
        "number": ([2**53 + 1, None], pa.int64(), "BIGINT"),
        "fraction": ([1.5, None], pa.float64(), "DOUBLE"),
        "event_time": ([datetime(2026, 1, 2), None], pa.timestamp("ns"), "TIMESTAMP"),
    }[column]
    pq.write_table(pa.table({column: pa.array(values, type=dtype)}), path)

    with activate_context("standalone") if activated else nullcontext():
        if activated:
            from pyspark.sql import SparkSession
        else:
            SparkSession = StandaloneSession
        spark = SparkSession.builder.getOrCreate()
        try:
            df = getattr(spark.read, method)(str(path))
            sql = df.sql(dialect="duckdb")
            if column == "event_time":
                sql = f'SELECT CAST("{column}" AS TIMESTAMP) AS "{column}" FROM ({sql})'
            result = query(sql)
            assert result.fetchall() == [(values[0],), (None,)]
            if column == "number":
                assert str(result.types[0]) == sql_type
            assert query(
                f'SELECT COUNT("{column}"), COUNT(*) FILTER (WHERE "{column}" IS NULL) FROM ({sql})'
            ).fetchall() == [(1, 1)]
            assert query(
                f'SELECT COALESCE("{column}", (SELECT MAX("{column}") FROM ({sql}))) '
                f"FROM ({sql}) "
                f'WHERE "{column}" IS NULL'
            ).fetchall() == [(values[0],)]
        finally:
            spark.stop()


@pytest.mark.parametrize("method", ["parquet", "load"])
@pytest.mark.parametrize(
    "column", ["flag", "day", "amount", "event_time", "items", "record", "nested"]
)
def test_activated_parquet_preserves_arrow_types(tmp_path, method, column):
    from datetime import date, datetime
    from decimal import Decimal

    import pyarrow as pa
    import pyarrow.parquet as pq

    value, arrow_type, sql_type, predicate = {
        "flag": (True, pa.bool_(), "BOOLEAN", "flag = TRUE"),
        "day": (date(2024, 1, 2), pa.date32(), "DATE", "day + INTERVAL 1 DAY = DATE '2024-01-03'"),
        "amount": (Decimal("1.50"), pa.decimal128(10, 2), "DECIMAL(10,2)", "amount + 1 = 2.50"),
        "event_time": (
            datetime(2024, 1, 2, 3, 4, 5, 123456),
            pa.timestamp("us"),
            "TIMESTAMP",
            "EXTRACT(microsecond FROM event_time) = 5123456",
        ),
        "items": ([1, None, 2], pa.list_(pa.int64()), "BIGINT[]", "items[1] + items[3] = 3"),
        "nested": (
            [{"amount": Decimal("9007199254740993.50"), "flags": [True, None]}, None],
            pa.list_(
                pa.struct([("amount", pa.decimal128(20, 2)), ("flags", pa.list_(pa.bool_()))])
            ),
            "STRUCT(amount DECIMAL(20,2), flags BOOLEAN[])[]",
            "nested[1].amount + 1 = 9007199254740994.50 AND nested[1].flags[1]",
        ),
        "record": (
            {"x": 1},
            pa.struct([("x", pa.int64())]),
            "STRUCT(x BIGINT)",
            "record.x + 1 = 2",
        ),
    }[column]
    path = tmp_path / "typed.parquet"
    pq.write_table(pa.table({column: pa.array([value, None], type=arrow_type)}), path)

    with activate_context("standalone"):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        try:
            df = getattr(spark.read, method)(str(path))
            sql = df.sql(dialect="duckdb")
            result = query(sql)
            assert str(result.types[0]) == sql_type
            assert result.fetchall() == [(value,), (None,)]
            assert query(
                f'SELECT {predicate} FROM ({sql}) WHERE "{column}" IS NOT NULL'
            ).fetchall() == [(True,)]
        finally:
            spark.stop()


@pytest.mark.parametrize("column", ["record", "nested"])
def test_explicit_schema_preserves_parquet_structs(tmp_path, column):
    from decimal import Decimal

    import pyarrow as pa
    import pyarrow.parquet as pq

    value = {"amount": Decimal("9007199254740993.50"), "flags": [True, None]}
    arrow_struct = pa.struct([("amount", pa.decimal128(20, 2)), ("flags", pa.list_(pa.bool_()))])
    spark_struct = types.StructType(
        [
            types.StructField("amount", types.DecimalType(20, 2)),
            types.StructField("flags", types.ArrayType(types.BooleanType())),
        ]
    )
    arrow_type = arrow_struct if column == "record" else pa.list_(arrow_struct)
    spark_type = spark_struct if column == "record" else types.ArrayType(spark_struct)
    expected = value if column == "record" else [value, None]
    path = tmp_path / "struct.parquet"
    pq.write_table(pa.table({column: pa.array([expected], type=arrow_type)}), path)

    spark = StandaloneSession.builder.getOrCreate()
    try:
        df = spark.read.load(
            str(path), schema=types.StructType([types.StructField(column, spark_type)])
        )
        assert query(df.sql(dialect="duckdb")).fetchall() == [(expected,)]
    finally:
        spark.stop()


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
@pytest.mark.parametrize("timezone", [None, "UTC"])
def test_parquet_timestamp_units(tmp_path, unit, timezone):
    from datetime import datetime
    from datetime import timezone as dt_timezone

    import pyarrow as pa
    import pyarrow.parquet as pq

    value = datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt_timezone.utc if timezone else None)
    path = tmp_path / "timestamps.parquet"
    pq.write_table(
        pa.table({"ts": pa.array([value, None], type=pa.timestamp(unit, tz=timezone))}), path
    )
    spark = StandaloneSession.builder.getOrCreate()
    try:
        result = query(spark.read.parquet(str(path)).sql(dialect="duckdb"))
        assert str(result.types[0]) == ("TIMESTAMP WITH TIME ZONE" if timezone else "TIMESTAMP")
        assert result.fetchall() == [(value,), (None,)]
    finally:
        spark.stop()


@pytest.mark.parametrize("method", ["parquet", "load"])
@pytest.mark.parametrize("option_source", ["option", "options", "keyword"])
@pytest.mark.parametrize(
    "option,value",
    [
        ("mergeSchema", "true"),
        ("datetimeRebaseMode", "LEGACY"),
        ("int96RebaseMode", "CORRECTED"),
        ("pathGlobFilter", "*.parquet"),
        ("recursiveFileLookup", "true"),
        ("MERGESCHEMA", False),
    ],
)
def test_activated_parquet_rejects_spark_options(method, option_source, option, value):
    from sqlframe.base.exceptions import UnsupportedOperationError

    with activate_context("standalone"):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        try:
            reader = spark.read
            kwargs = {}
            if option_source == "option":
                reader = reader.option(option, value)
            elif option_source == "options":
                reader = reader.options(**{option: value})
            else:
                kwargs = {option: value}
            with pytest.raises(
                UnsupportedOperationError,
                match=f"{option}.*Remove this option or use the Spark engine",
            ):
                getattr(reader, method)(str(PARQUET_FILE), **kwargs)
        finally:
            spark.stop()


@pytest.mark.parametrize("method", ["parquet", "load"])
def test_activated_parquet_preserves_arrow_options(method):
    with activate_context("standalone"):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        try:
            reader = spark.read.options(use_threads=False, mergeSchema=None)
            df = getattr(reader, method)(
                str(PARQUET_FILE), columns=["fname"], filters=[("employee_id", "=", 1)]
            )
            assert df.columns == ["fname"]
            assert query(df.sql(dialect="duckdb")).fetchall() == [("Jack",)]
        finally:
            spark.stop()
