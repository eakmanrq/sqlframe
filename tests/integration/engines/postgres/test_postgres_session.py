from sqlglot import exp, parse_one

from sqlframe.base.types import Row
from sqlframe.postgres.session import PostgresSession

pytest_plugins = ["tests.common_fixtures"]


def test_session_from_config(function_scoped_postgres):
    function_scoped_postgres.cursor().execute(
        parse_one("CREATE TABLE test_table (cola INT, colb STRING)").sql(dialect="postgres")
    )
    session = PostgresSession.builder.config(
        "sqlframe.conn", function_scoped_postgres
    ).getOrCreate()
    columns = session.catalog.get_columns("test_table")
    assert columns == {
        "cola": exp.DataType.build("INT", dialect=session.output_dialect),
        "colb": exp.DataType.build("STRING", dialect=session.output_dialect),
    }
    assert session.execution_dialect_name == "postgres"


def test_create_dataframe_dict_as_struct(postgres_session: PostgresSession):
    df = postgres_session.createDataFrame([{"country": "DE", "city": "Berlin"}])
    result = df.collect()
    assert result == [Row(country="DE", city="Berlin")]
