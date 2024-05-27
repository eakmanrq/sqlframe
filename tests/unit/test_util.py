import typing as t

import pytest
from sqlglot import exp, parse_one

from sqlframe.base.util import quote_preserving_alias_or_name


@pytest.mark.parametrize(
    "expression, expected",
    [
        ("a", "a"),
        ("a AS b", "b"),
        ("`a`", "`a`"),
        ("`a` AS b", "b"),
        ("`a` AS `b`", "`b`"),
        ("`aB`", "`aB`"),
        ("`aB` AS c", "c"),
        ("`aB` AS `c`", "`c`"),
        ("`aB` AS `Cd`", "`Cd`"),
        # We assume inputs have been normalized so `Cd` is returned as is instead of normalized `cd`
        ("`aB` AS Cd", "Cd"),
    ],
)
def test_quote_preserving_alias_or_name(expression: t.Union[exp.Column, exp.Alias], expected: str):
    assert quote_preserving_alias_or_name(parse_one(expression, dialect="bigquery")) == expected  # type: ignore
