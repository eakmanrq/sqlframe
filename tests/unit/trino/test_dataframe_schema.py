from unittest.mock import Mock

import pytest

from sqlframe.base import types
from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.trino import TrinoDataFrame, TrinoSession
from sqlframe.trino import functions as F


@pytest.fixture
def connection():
    conn = Mock()
    conn.cursor.return_value.description = [
        (name,)
        for name in ["Column Name", "Catalog", "Schema", "Table", "Type", "Type Size", "Aliased"]
    ]
    return conn


def describe_row(name, data_type):
    return (name, "", "", "", data_type, 0, True)


def test_schema_and_print_schema_describe_the_executed_query(connection, capsys):
    cursor = connection.cursor.return_value
    cursor.fetchall.return_value = [
        describe_row("label", "varchar(20)"),
        describe_row("total", "bigint"),
    ]
    session = TrinoSession(conn=connection)
    frame = session.createDataFrame([(1, "a")], ["amount", "label"])
    frame.createOrReplaceTempView("items")
    query = session.read.table("items").groupBy("label").agg(F.sum("amount").alias("total"))

    assert isinstance(query, TrinoDataFrame)
    assert query.schema == types.StructType(
        [
            types.StructField("label", types.StringType(), True),
            types.StructField("total", types.LongType(), True),
        ]
    )
    query.printSchema()

    assert capsys.readouterr().out == (
        "root\n |-- label: string (nullable = true)\n |-- total: bigint (nullable = true)\n"
    )
    assert [call.args[0] for call in cursor.execute.call_args_list] == [
        f"DESCRIBE OUTPUT ({query.sql(pretty=False, optimize=False)})"
    ] * 2


@pytest.mark.parametrize(
    ("trino_type", "spark_type"),
    [
        ("boolean", types.BooleanType()),
        ("tinyint", types.ByteType()),
        ("smallint", types.ShortType()),
        ("integer", types.IntegerType()),
        ("bigint", types.LongType()),
        ("real", types.FloatType()),
        ("double", types.DoubleType()),
        ("decimal(38, 10)", types.DecimalType(38, 10)),
        ("varchar", types.StringType()),
        ("varchar(20)", types.StringType()),
        ("char(5)", types.StringType()),
        ("varbinary", types.BinaryType()),
        ("date", types.DateType()),
        ("timestamp(3)", types.TimestampNTZType()),
        ("timestamp(6)", types.TimestampNTZType()),
        ("timestamp(6) with time zone", types.TimestampType()),
        ("json", types.StringType()),
        ("uuid", types.StringType()),
        ("array(integer)", types.ArrayType(types.IntegerType())),
        (
            'map(varchar, array(row("A.b" decimal(12, 2), flag boolean)))',
            types.MapType(
                types.StringType(),
                types.ArrayType(
                    types.StructType(
                        [
                            types.StructField("A.b", types.DecimalType(12, 2)),
                            types.StructField("flag", types.BooleanType()),
                        ]
                    )
                ),
            ),
        ),
    ],
)
def test_query_schema_maps_trino_types(connection, trino_type, spark_type):
    connection.cursor.return_value.fetchall.return_value = [describe_row("value", trino_type)]
    session = TrinoSession(conn=connection)
    query = session.sql("SELECT 1 AS value", qualify=False)

    assert query.schema == types.StructType([types.StructField("value", spark_type, True)])


@pytest.mark.parametrize(
    "trino_type",
    [
        "time(3)",
        "time(6) with time zone",
        "timestamp(9)",
        "array(timestamp(12) with time zone)",
        "interval year to month",
        "interval day to second",
        "ipaddress",
        "HyperLogLog",
        "unknown",
        "row(integer, varchar)",
        "map(varchar, array(time(3)))",
        "some_plugin_type",
    ],
)
@pytest.mark.parametrize("operation", ["schema", "printSchema"])
def test_unsupported_query_type_names_column_and_type(connection, trino_type, operation):
    connection.cursor.return_value.fetchall.return_value = [describe_row("payload", trino_type)]
    query = TrinoSession(conn=connection).sql("SELECT 1 AS payload", qualify=False)

    with pytest.raises(UnsupportedOperationError) as error:
        if operation == "schema":
            _ = query.schema
        else:
            query.printSchema()
    assert trino_type in str(error.value)
    assert "payload" in str(error.value)


@pytest.mark.parametrize("operation", ["schema", "printSchema"])
def test_schema_requires_connection_even_with_registered_types(operation):
    session = TrinoSession()
    session.catalog.add_table("items", {"id": "int"})
    query = session.read.table("items")

    with pytest.raises(ValueError, match="Trino schema discovery requires a DB-API connection"):
        if operation == "schema":
            _ = query.schema
        else:
            query.printSchema()


def test_catalog_parses_trino_types_before_registering_table(connection):
    cursor = connection.cursor.return_value
    cursor.fetchall.return_value = [
        ("amounts", "array(decimal(12, 2))", "", "amount history"),
        ("record", 'row("A.b" integer, event_time timestamp(6))', "", ""),
        ("a.b", "varbinary", "", ""),
    ]
    session = TrinoSession(conn=connection)

    columns = session.catalog.listColumns("events")
    assert columns[0].dataType == "array(decimal(12, 2))"
    assert columns[0].description == "amount history"
    frame = session.read.table("events")
    registered = session.catalog.get_columns_from_schema("events")
    assert frame.columns == ["amounts", "record", "a.b"]
    assert registered["amounts"].sql("spark") == "ARRAY<DECIMAL(12, 2)>"
    assert registered["record"].sql("trino") == 'ROW("A.b" INTEGER, event_time TIMESTAMP(6))'
    assert registered["`a.b`"].sql("spark") == "BINARY"


@pytest.mark.parametrize("connected", [False, True])
@pytest.mark.parametrize("operation", ["cache", "persist"])
@pytest.mark.parametrize("source", ["values", "table"])
def test_cache_and_persist_fail_before_changing_plan_or_executing(
    connection, connected, operation, source
):
    session = TrinoSession(conn=connection if connected else None)
    session.catalog.add_table("items", {"id": "int"})
    query = (
        session.createDataFrame([(1,)], ["id"])
        if source == "values"
        else session.read.table("items")
    )
    original_sql = query.sql(pretty=False, optimize=False)

    with pytest.raises(UnsupportedOperationError, match=f"Trino.*{operation}"):
        getattr(query, operation)()

    assert query.sql(pretty=False, optimize=False) == original_sql
    connection.cursor.return_value.execute.assert_not_called()


def test_schema_uses_execution_dialect_when_sql_output_is_spark(connection):
    session = (
        TrinoSession.builder.config("sqlframe.conn", connection)
        .config("sqlframe.output.dialect", "spark")
        .getOrCreate()
    )
    connection.cursor.return_value.fetchall.return_value = [describe_row("value", "integer")]
    query = session.createDataFrame([(1,)], ["value"])

    assert query.schema == types.StructType([types.StructField("value", types.IntegerType())])
    connection.cursor.return_value.execute.assert_called_once_with(
        f"DESCRIBE OUTPUT ({query.sql(dialect='trino', pretty=False, optimize=False)})"
    )


def test_schema_preserves_result_names_and_prints_nested_types(connection, capsys):
    connection.cursor.return_value.fetchall.return_value = [
        describe_row("A.b", 'array(row("Mixed.Case" varbinary, t timestamp(6)))')
    ]
    query = TrinoSession(conn=connection).sql('SELECT 1 AS "A.b"', dialect="trino", qualify=False)

    assert query.schema == types.StructType(
        [
            types.StructField(
                "A.b",
                types.ArrayType(
                    types.StructType(
                        [
                            types.StructField("Mixed.Case", types.BinaryType()),
                            types.StructField("t", types.TimestampNTZType()),
                        ]
                    )
                ),
            )
        ]
    )
    query.printSchema(level=2)
    assert capsys.readouterr().out == (
        "root\n"
        " |-- A.b: array<struct<`mixed.case`: binary, t: timestamp_ntz>> (nullable = true)\n"
        " |    |-- element: struct<`mixed.case`: binary, t: timestamp_ntz> (nullable = true)\n"
    )


@pytest.mark.parametrize("native_type", ["json", "uuid", "char(5)"])
def test_native_source_type_preserves_explicit_string_cast(connection, native_type):
    connection.cursor.return_value.fetchall.return_value = [("payload", native_type, "", "")]
    session = TrinoSession(conn=connection)
    query = session.read.table("items").select(F.col("payload").cast("string"))

    assert "CAST(" in query.sql(optimize=False)
    assert "CAST(" in query.sql()


@pytest.mark.parametrize(
    "native_type",
    [
        "time(3)",
        "timestamp(9)",
        'row("A.b" timestamp(12))',
        "row(integer, varchar)",
        "interval day to second",
    ],
)
def test_mixed_table_registers_native_types_before_schema_reporting(connection, native_type):
    cursor = connection.cursor.return_value
    cursor.fetchall.return_value = [("id", "integer", "", ""), ("payload", native_type, "", "")]
    session = TrinoSession(conn=connection)
    frame = session.read.table("items")
    registered = session.catalog.get_columns_from_schema("items")
    from sqlglot import exp

    assert registered["payload"] == exp.DataType.build(native_type, dialect="trino")
    assert "id" in frame.select("id").sql()
    cursor.fetchall.return_value = [describe_row("id", "integer")]
    assert frame.select("id").schema.simpleString() == "struct<id:int>"
    cursor.fetchall.return_value = [describe_row("payload", native_type)]
    with pytest.raises(UnsupportedOperationError, match="Unsupported Trino type .*payload"):
        _ = frame.select("payload").schema


def test_table_registration_reports_native_type_parse_error(connection):
    connection.cursor.return_value.fetchall.return_value = [("payload", "row(", "", "")]
    session = TrinoSession(conn=connection)

    with pytest.raises(UnsupportedOperationError, match="Unsupported Trino type .*payload"):
        session.read.table("items")
