import pytest
from sqlglot.schema import MappingSchema

from sqlframe.base.exceptions import TableSchemaError
from sqlframe.base.session import _BaseSession
from sqlframe.standalone.session import StandaloneSession
from sqlframe.trino import TrinoSession
from sqlframe.trino import functions as F


class Cursor:
    description = [("order_id",), ("total",)]

    def __init__(self):
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)

    def fetchall(self):
        return [(7, 42)]


class Connection:
    def __init__(self):
        self._cursor = Cursor()

    def cursor(self):
        return self._cursor

    def close(self):
        pass


@pytest.fixture(autouse=True)
def reset_session():
    _BaseSession._instance = None
    TrinoSession.builder = TrinoSession.Builder()
    yield
    _BaseSession._instance = None
    TrinoSession.builder = TrinoSession.Builder()


def test_trino_session_generates_sql_and_collects_with_explicit_schema():
    conn = Connection()
    session = TrinoSession.builder.config("sqlframe.conn", conn).getOrCreate()
    assert session.execution_dialect_name == "trino"
    session.catalog.add_table("sales.orders", {"order_id": "bigint", "total": "bigint"})

    df = session.read.table("sales.orders").select(F.col("order_id"), "total").where("total > 10")
    sql = df.sql(pretty=False)

    assert '"sales"."orders"' in sql
    assert '"order_id"' in sql
    assert df.collect() == [(7, 42)]
    assert conn._cursor.statements == [df.sql(pretty=False, optimize=False)]


def test_trino_session_requires_a_connection_to_collect():
    session = TrinoSession.builder.getOrCreate()
    session.catalog.add_table("orders", {"order_id": "bigint"})

    assert '"orders"' in session.read.table("orders").sql(pretty=False)
    with pytest.raises(ValueError, match="Trino collection requires a DB-API connection"):
        session.read.table("orders").collect()


def test_trino_session_requires_explicit_table_schema():
    session = TrinoSession.builder.getOrCreate()

    with pytest.raises(TableSchemaError, match="does not have access to a catalog"):
        session.read.table("orders")


def test_standalone_session_does_not_reconfigure_trino_session():
    conn = Connection()
    trino_session = TrinoSession(conn=conn)

    standalone_session = StandaloneSession.builder.getOrCreate()

    assert type(standalone_session) is StandaloneSession
    assert standalone_session is not trino_session
    assert _BaseSession() is standalone_session
    assert trino_session.execution_dialect_name == "trino"
    assert trino_session._conn is conn


def test_trino_catalog_accepts_supplied_schema_with_mixed_table_depths():
    session = TrinoSession(schema=MappingSchema())
    session.catalog.add_table("labels", {"label": "string"})
    session.catalog.add_table("memory.sales.orders", {"order_id": "int"})

    assert list(session.catalog.get_columns_from_schema("labels")) == ["label"]
    assert list(session.catalog.get_columns_from_schema("memory.sales.orders")) == ["order_id"]


@pytest.mark.parametrize(
    ("mapping", "table_name"),
    [
        ({"orders": {"order_id": "int"}}, "orders"),
        ({"sales": {"orders": {"order_id": "int"}}}, "sales.orders"),
        ({"memory": {"sales": {"orders": {"order_id": "int"}}}}, "memory.sales.orders"),
    ],
)
def test_trino_catalog_reads_supplied_populated_schema_offline(mapping, table_name):
    session = TrinoSession(schema=MappingSchema(mapping))

    assert session.read.table(table_name).columns == ["order_id"]
    session.catalog.add_table("memory.sales.shipments", {"shipment_id": "int"})
    assert session.read.table("memory.sales.shipments").columns == ["shipment_id"]
