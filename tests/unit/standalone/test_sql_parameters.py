from copy import deepcopy

import pytest
from sqlglot import exp, parse_one
from sqlglot.errors import ParseError

from sqlframe.standalone import functions as F

pytest_plugins = ["tests.common_fixtures", "tests.unit.standalone.fixtures"]


@pytest.mark.parametrize("args", [{"Value": 7}, [7]])
def test_sql_bound_literals(standalone_session, args):
    marker = ":Value" if isinstance(args, dict) else "?"
    df = standalone_session.sql(f"SELECT {marker} AS value", args=args)
    assert df.expression.ctes[-1].this.expressions[0].this == exp.Literal.number(7)


def test_sql_named_parameters_are_case_sensitive(standalone_session):
    df = standalone_session.sql("SELECT :x + :X + :x AS value", args={"x": 1, "X": 2})
    assert df.expression.ctes[-1].this.expressions[0].this == parse_one("1 + 2 + 1")


def test_sql_positional_parameters_follow_text_order(standalone_session):
    df = standalone_session.sql(
        "WITH x AS (SELECT ? AS a) SELECT ? AS b FROM x WHERE a = ?", args=[1, 2, 3]
    )
    assert df.expression.ctes[0].this.expressions[0].this == exp.Literal.number(1)
    select = df.expression.ctes[1].this
    assert select.expressions[0].this == exp.Literal.number(2)
    assert select.args["where"].this.expression == exp.Literal.number(3)


@pytest.mark.parametrize("args", [{"x": "O'Reilly ? :x"}, ["O'Reilly ? :x"]])
def test_sql_parameters_do_not_replace_strings_or_comments(standalone_session, args):
    marker = ":x" if isinstance(args, dict) else "?"
    df = standalone_session.sql(f"SELECT {marker} AS value, '? :x' AS text -- ? :x", args=args)
    expression = df.expression
    assert [literal.this for literal in expression.find_all(exp.Literal)] == [
        "O'Reilly ? :x",
        "? :x",
    ]


@pytest.mark.parametrize(
    "query,args,message",
    [
        ("SELECT :missing", {}, "missing"),
        ("SELECT ?", [], "positional"),
        ("SELECT ? + :x", [1], "mix"),
        ("SELECT ? + :x", {"x": 1}, "mix"),
    ],
)
def test_sql_invalid_parameters(standalone_session, query, args, message):
    with pytest.raises(ValueError, match=message):
        standalone_session.sql(query, args=args)


def test_sql_expression_parameters_do_not_mutate_input(standalone_session):
    query = exp.select(exp.Placeholder(this="x").as_("value"))
    df = standalone_session.sql(query, args={"x": 7})
    assert df.expression.ctes[-1].this.expressions[0].this == exp.Literal.number(7)
    assert query.find(exp.Placeholder) is not None


def test_sql_keyword_literals_and_columns(standalone_session):
    df = standalone_session.sql(
        "SELECT {column} AS value WHERE {column} IN {values}",
        column=F.lit(7),
        values=(7, 8),
    )
    assert df.expression.ctes[-1].this.expressions[0].this == exp.Literal.number(7)


def test_sql_keyword_dataframe_and_column(standalone_session):
    people = standalone_session.createDataFrame([(34,), (29,)], ["age"])
    views = dict(standalone_session.temp_views)
    df = standalone_session.sql(
        "SELECT {people.age} FROM {people} WHERE {people[age]} > :age",
        args={"age": 30},
        people=people,
    )
    assert df.sql(pretty=False, optimize=True) == (
        "SELECT CAST(`a1`.`age` AS BIGINT) AS `age` "
        "FROM VALUES (34), (29) AS `a1`(`age`) "
        "WHERE CAST(`a1`.`age` AS BIGINT) > 30"
    )
    assert standalone_session.temp_views == views


def test_sql_keyword_dataframe_cleanup_after_error(standalone_session):
    people = standalone_session.createDataFrame([(34,)], ["age"])
    views = dict(standalone_session.temp_views)
    tables = standalone_session.catalog._schema.copy()
    with pytest.raises(ParseError):
        standalone_session.sql("SELECT * FROM {people} WHERE (", people=people)
    assert standalone_session.temp_views == views
    assert standalone_session.catalog._schema.mapping == tables.mapping


def test_sql_second_positional_args(standalone_session):
    df = standalone_session.sql("SELECT :x AS value", {"x": 7})
    assert df.expression.ctes[-1].this.expressions[0].this == exp.Literal.number(7)


def test_sql_preserves_positional_dialect(standalone_session):
    df = standalone_session.sql('SELECT 7 AS "value"', "duckdb", False)
    assert df.expression.ctes[-1].this.expressions[0].this == exp.Literal.number(7)


@pytest.mark.parametrize("parse_failure", [False, True])
def test_sql_keyword_dataframe_cleanup_with_uppercase_input(sqlf_sf, parse_failure):
    people = sqlf_sf.createDataFrame([(34,)], ["age"])
    people.createOrReplaceTempView("existing")
    views = dict(sqlf_sf.temp_views)
    schema = sqlf_sf.catalog._schema
    tables = deepcopy(schema.mapping)
    if parse_failure:
        with pytest.raises(ParseError):
            sqlf_sf.sql("SELECT * FROM {people} WHERE (", people=people)
    else:
        sqlf_sf.sql("SELECT * FROM {people}", people=people)
    assert sqlf_sf.catalog._schema is schema
    assert sqlf_sf.catalog._schema.mapping == tables
    assert sqlf_sf.temp_views == views


@pytest.mark.parametrize("parse_failure", [False, True])
def test_sql_keyword_dataframe_cleanup_preserves_quoted_metadata(standalone_session, parse_failure):
    people = standalone_session.createDataFrame([(34,)], ["`Quoted Age`"])
    people.createOrReplaceTempView("existing")
    quoted_columns = standalone_session.catalog._quoted_columns
    expected = deepcopy(dict(quoted_columns))
    assert expected
    views = dict(standalone_session.temp_views)
    schema = standalone_session.catalog._schema
    for _ in range(3):
        if parse_failure:
            with pytest.raises(ParseError):
                standalone_session.sql("SELECT * FROM {people} WHERE (", people=people)
        else:
            standalone_session.sql("SELECT * FROM {people}", people=people)
        assert standalone_session.catalog._quoted_columns is quoted_columns
        assert dict(quoted_columns) == expected
        assert standalone_session.temp_views == views
        assert standalone_session.catalog._schema is schema
