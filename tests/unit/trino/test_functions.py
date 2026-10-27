"""Trino SQL contracts for the bounded Spark function compatibility surface."""

import pytest

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.trino import TrinoSession, Window
from sqlframe.trino import functions as F


@pytest.fixture(autouse=True)
def session():
    return TrinoSession()


@pytest.mark.parametrize("ascending", [None, True, False])
def test_sort_array_uses_null_aware_comparator(ascending):
    result = F.sort_array("items", ascending).column_expression.sql(dialect="trino")
    direction = -1 if ascending is not False else 1
    assert result == (
        'ARRAY_SORT(items, ("left", "right") -> CASE '
        'WHEN ("left" IS NULL AND "right" IS NULL) THEN 0 '
        f'WHEN "left" IS NULL THEN {direction} WHEN "right" IS NULL THEN {-direction} '
        f'WHEN "left" < "right" THEN {direction} WHEN "left" > "right" THEN {-direction} ELSE 0 END)'
    )


@pytest.mark.parametrize(
    "name,aggregate",
    [("collect_list", "ARRAY_AGG(value)"), ("collect_set", "ARRAY_AGG(DISTINCT value)")],
)
def test_collections_remove_nulls_and_default_to_empty_array(name, aggregate):
    result = getattr(F, name)("value").column_expression.sql(dialect="trino")
    assert result == f"COALESCE(FILTER({aggregate}, x -> NOT x IS NULL), ARRAY[])"


@pytest.mark.parametrize("name", ["collect_list", "collect_set"])
def test_collection_window_wraps_aggregate_only(name):
    result = getattr(F, name)("value").over(Window.partitionBy("category")).sql(dialect="trino")
    assert "ARRAY_AGG(" in result
    assert ") OVER (PARTITION BY category), x -> NOT x IS NULL), ARRAY[])" in result


@pytest.mark.parametrize("name,native", [("greatest", "ARRAY_MAX"), ("least", "ARRAY_MIN")])
def test_extrema_skip_null_arguments(name, native):
    assert getattr(F, name)("a", "b").column_expression.sql(dialect="trino") == (
        f"{native}(FILTER(ARRAY[a, b], x -> NOT x IS NULL))"
    )


@pytest.mark.parametrize("index", [None, 0, 1, 2])
def test_regexp_extract_uses_spark_group_and_empty_string_contract(index):
    result = F.regexp_extract("value", "(a)(b)", index).column_expression.sql(dialect="trino")
    group = "" if index == 0 else f", {1 if index is None else index}"
    assert result == (
        "CASE WHEN value IS NULL THEN NULL ELSE "
        f"COALESCE(REGEXP_EXTRACT(value, '(a)(b)'{group}), '') END"
    )


@pytest.mark.parametrize("limit", [None, 0, -1, -5])
def test_split_unlimited_does_not_emit_unsupported_third_argument(limit):
    assert F.split("value", "[,;]", limit).column_expression.sql(dialect="trino") == (
        "REGEXP_SPLIT(value, '[,;]')"
    )


def test_split_positive_limit_is_explicitly_unsupported():
    with pytest.raises(UnsupportedOperationError, match="split.*positive limit"):
        F.split("value", ",", 2)


@pytest.mark.parametrize(
    "name,args",
    [
        ("get_json_object", ("value", "$.x")),
        ("from_json", ("value", "x INT")),
        ("to_json", ("value",)),
        ("json_tuple", ("value", "x")),
        ("to_timestamp", ("value",)),
    ],
)
def test_audited_incompatible_functions_fail_before_query(name, args):
    with pytest.raises(UnsupportedOperationError, match=rf"{name}.*Trino"):
        getattr(F, name)(*args)


@pytest.mark.parametrize(
    "name,args",
    [
        ("sort_array", ("items",)),
        ("collect_list", ("a",)),
        ("collect_set", ("a",)),
        ("greatest", ("a", "b")),
        ("least", ("a", "b")),
    ],
)
def test_function_lambdas_support_optimized_dataframe_sql(session, name, args):
    frame = session.createDataFrame([([3, None, 1], 1, 2)], ["items", "a", "b"])
    query = frame.select(getattr(F, name)(*args).alias("result"))
    assert 'AS "result"' in query.sql()


def test_array_sort_comparator_keeps_captured_columns_qualified(session):
    frame = session.createDataFrame([([3, 1], 2)], ["items", "threshold"])
    comparator = lambda left, right: (
        F.when(left > F.col("threshold"), 1).when(left < right, -1).otherwise(0)
    )
    query = frame.select(F.array_sort("items", comparator).alias("result"))
    sql = query.sql()
    assert '."threshold"' in sql
    assert '."left"' not in sql
    assert '."right"' not in sql


def test_array_sort_mixed_case_comparator_names_are_bound(session):
    frame = session.createDataFrame([([3, 1],)], ["items"])
    comparator = lambda leftValue, rightValue: (
        F.when(leftValue < rightValue, -1).when(leftValue > rightValue, 1).otherwise(0)
    )
    query = frame.select(F.array_sort("items", comparator).alias("sorted"))
    assert 'AS "sorted"' in query.sql()


def test_array_sort_captured_column_can_share_parameter_name(session):
    from sqlglot import exp

    comparator = lambda left, right: F.when(left > F.col("left"), 1).otherwise(0)
    expression = F.array_sort("items", comparator).column_expression
    captured = list(expression.args["expression"].find_all(exp.Column))
    assert len(captured) == 1
    assert captured[0].name == "left"
    frame = session.createDataFrame([([3, 1], 2)], ["items", "left"])
    assert '."left"' in frame.select(F.array_sort("items", comparator)).sql()


def test_sort_array_default_name_differs_from_array_sort(session):
    frame = session.createDataFrame([([3, None, 1],)], ["items"])
    query = frame.select(F.array_sort("items"), F.sort_array("items"))
    assert query.columns == ["array_sort__items__", "sort_array__items__"]
    assert 'AS "sort_array__items__"' in query.select("sort_array__items__").sql()


@pytest.mark.parametrize("name", ["collect_list", "collect_set", "greatest", "least"])
def test_wrapped_functions_keep_public_source_based_names(session, name):
    frame = session.createDataFrame([(1, 2)], ["a", "b"])
    columns = [getattr(F, name)(col) for col in ("a", "b")]
    query = frame.select(*columns)
    assert query.columns == [f"{name}__a__", f"{name}__b__"]
    assert f'AS "{name}__a__"' in query.select(f"{name}__a__").sql()


def test_collection_functions_have_distinct_default_names(session):
    frame = session.createDataFrame([(1,), (None,)], ["v"])
    assert frame.agg(F.collect_list("v"), F.collect_set("v")).columns == [
        "collect_list__v__",
        "collect_set__v__",
    ]
