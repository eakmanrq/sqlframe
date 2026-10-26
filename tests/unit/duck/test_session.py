import duckdb
import pytest
from sqlglot import parse_one

from sqlframe.duckdb import DuckDBSession


@pytest.mark.parametrize(
    "version, expected",
    [
        ("1.2.0", "SELECT LIST_TRANSFORM([1, 2, 3], x -> x * 2)"),
        ("2.0.0", "SELECT LIST_TRANSFORM([1, 2, 3], LAMBDA x : x * 2)"),
    ],
)
def test_lambda_syntax_matches_duckdb_version(monkeypatch, version, expected):
    monkeypatch.setattr(duckdb, "__version__", version)
    session = DuckDBSession.builder.getOrCreate()
    expression = parse_one("SELECT TRANSFORM(ARRAY(1, 2, 3), x -> x * 2)", read="spark")

    assert session._to_sql(expression, quote_identifiers=False) == expected
