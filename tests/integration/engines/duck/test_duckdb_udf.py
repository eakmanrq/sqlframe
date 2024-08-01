from sqlframe.duckdb import DuckDBSession

pytest_plugins = ["tests.integration.fixtures"]


def test_udf_register(duckdb_session: DuckDBSession):
    def multi(x: int, y: int) -> int:
        return x * y

    duckdb_session.udf.register("multi", multi)

    assert duckdb_session.sql("SELECT multi(1, 2)").collect() == [(2,)]
