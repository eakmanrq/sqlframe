"""Spark function results against the pinned Trino server."""

import datetime

import pytest

from sqlframe.base.types import Row
from sqlframe.trino import Window
from sqlframe.trino import functions as F
from tests.integration.trino.test_trino_live import trino_session as trino_session

pytestmark = pytest.mark.trino


def test_array_sort_and_sort_array_null_placement(trino_session):
    session, _ = trino_session
    frame = session.sql(
        "SELECT * FROM (VALUES (1, ARRAY[3, NULL, 1, 3, NULL]), "
        "(2, CAST(NULL AS ARRAY(INTEGER))), (3, CAST(ARRAY[] AS ARRAY(INTEGER)))) t(id, items)",
        dialect="trino",
        qualify=False,
    )
    assert frame.select(
        "id",
        F.array_sort("items").alias("native"),
        F.sort_array("items").alias("ascending"),
        F.sort_array("items", False).alias("descending"),
        F.array_sort(F.sort_array("items")).alias("nested"),
    ).orderBy("id").collect() == [
        Row(
            id=1,
            native=[1, 3, 3, None, None],
            ascending=[None, None, 1, 3, 3],
            descending=[3, 3, 1, None, None],
            nested=[1, 3, 3, None, None],
        ),
        Row(id=2, native=None, ascending=None, descending=None, nested=None),
        Row(id=3, native=[], ascending=[], descending=[], nested=[]),
    ]


def test_array_sort_comparator_and_strings(trino_session):
    session, _ = trino_session
    frame = session.createDataFrame([(["z", None, "a", "a"],)], ["items"])
    comparator = lambda left, right: (
        F.when(left.isNull(), 1)
        .when(right.isNull(), -1)
        .when(left < right, 1)
        .when(left > right, -1)
        .otherwise(0)
    )
    assert frame.select(
        F.array_sort("items", comparator).alias("custom"), F.sort_array("items").alias("ascending")
    ).collect() == [Row(custom=["z", "a", "a", None], ascending=[None, "a", "a", "z"])]


@pytest.mark.parametrize("empty", [False, True])
def test_collection_aggregates_nulls_duplicates_and_empty_input(trino_session, empty):
    session, _ = trino_session
    frame = session.createDataFrame([(1, 3), (1, None), (1, 1), (1, 3), (2, None)], ["g", "v"])
    if empty:
        frame = frame.where(F.col("g") < 0)
        result = frame.agg(
            F.collect_list("v").alias("items"), F.collect_set("v").alias("unique_items")
        ).collect()
        assert result == [Row(items=[], unique_items=[])]
    else:
        assert frame.groupBy("g").agg(
            F.array_sort(F.collect_list("v")).alias("items"),
            F.array_sort(F.collect_set("v")).alias("unique_items"),
        ).orderBy("g").collect() == [
            Row(g=1, items=[1, 3, 3], unique_items=[1, 3]),
            Row(g=2, items=[], unique_items=[]),
        ]


def test_collection_windows_include_empty_frames(trino_session):
    session, _ = trino_session
    frame = session.createDataFrame([(1, 3), (2, None), (3, 1)], ["id", "v"])
    window = Window.orderBy("id").rowsBetween(1, 1)
    assert frame.select(
        "id",
        F.collect_list("v").over(window).alias("items"),
        F.collect_set("v").over(window).alias("unique_items"),
    ).orderBy("id").collect() == [
        Row(id=1, items=[], unique_items=[]),
        Row(id=2, items=[1], unique_items=[1]),
        Row(id=3, items=[], unique_items=[]),
    ]


def test_greatest_least_skip_nulls(trino_session):
    session, _ = trino_session
    frame = session.createDataFrame([(1, None, 3), (2, 4, None), (3, None, None)], ["id", "a", "b"])
    assert frame.select(
        "id", F.greatest("a", "b").alias("hi"), F.least("a", "b").alias("lo")
    ).orderBy("id").collect() == [
        Row(id=1, hi=3, lo=3),
        Row(id=2, hi=4, lo=4),
        Row(id=3, hi=None, lo=None),
    ]


def test_regexp_extract_groups_missing_matches_and_null(trino_session):
    session, _ = trino_session
    frame = session.createDataFrame([(1, "ab"), (2, "zzz"), (3, "b"), (4, None)], ["id", "value"])
    assert frame.select(
        "id",
        F.regexp_extract("value", "(a)?(b)", 0).alias("whole"),
        F.regexp_extract("value", "(a)?(b)", 1).alias("first"),
        F.regexp_extract("value", "(a)?(b)", 2).alias("second"),
        F.regexp_extract("value", "(a)?(b)").alias("default_group"),
    ).orderBy("id").collect() == [
        Row(id=1, whole="ab", first="a", second="b", default_group="a"),
        Row(id=2, whole="", first="", second="", default_group=""),
        Row(id=3, whole="b", first="", second="b", default_group=""),
        Row(id=4, whole=None, first=None, second=None, default_group=None),
    ]


@pytest.mark.parametrize("limit", [None, 0, -1])
def test_regex_split_preserves_trailing_empty_fields(trino_session, limit):
    session, _ = trino_session
    frame = session.createDataFrame([(1, "a,b;"), (2, ""), (3, None)], ["id", "value"])
    assert frame.select("id", F.split("value", "[,;]", limit).alias("parts")).orderBy(
        "id"
    ).collect() == [Row(id=1, parts=["a", "b", ""]), Row(id=2, parts=[""]), Row(id=3, parts=None)]


def test_existing_date_string_null_order_and_join_contracts(trino_session):
    session, _ = trino_session
    frame = session.createDataFrame(
        [(1, "2024-02-29", "AbC"), (None, "2024-03-01", None)], ["id", "day", "text"]
    )
    assert frame.select(
        "id",
        F.to_date("day").alias("date"),
        F.date_format("day", "yyyy-MM-dd").alias("formatted"),
        F.lower("text").alias("lowered"),
        F.to_timestamp("day", "yyyy-MM-dd").alias("timestamp"),
    ).orderBy("id").collect() == [
        Row(
            id=None,
            date=datetime.date(2024, 3, 1),
            formatted="2024-03-01",
            lowered=None,
            timestamp=datetime.datetime(2024, 3, 1),
        ),
        Row(
            id=1,
            date=datetime.date(2024, 2, 29),
            formatted="2024-02-29",
            lowered="abc",
            timestamp=datetime.datetime(2024, 2, 29),
        ),
    ]
    other = session.createDataFrame([(1, "matched"), (2, "other")], ["id", "label"])
    assert frame.join(other, "id", "left").select("id", "label").orderBy("id").collect() == [
        Row(id=None, label=None),
        Row(id=1, label="matched"),
    ]
    assert frame.where(F.lit(False)).agg(
        F.sum("id").alias("total"), F.count("id").alias("count")
    ).collect() == [Row(total=None, count=0)]


def test_optimized_sql_preserves_function_mappings(trino_session):
    session, _ = trino_session
    frame = session.createDataFrame([(3,), (None,), (1,), (3,)], ["v"])
    query = frame.agg(
        F.sort_array(F.collect_list("v")).alias("items"),
        F.sort_array(F.collect_set("v"), False).alias("unique_items"),
    )
    session._cur.execute(query.sql())
    assert session._cur.fetchall() == [[[1, 3, 3], [3, 1]]]

    scalar_query = frame.select(
        F.greatest("v", F.lit(2)).alias("hi"),
        F.regexp_extract(F.lit("ab"), "(a)(b)", 1).alias("match"),
        F.split(F.lit("a,b,"), ",", -1).alias("parts"),
    ).orderBy("hi")
    session._cur.execute(scalar_query.sql())
    assert session._cur.fetchall() == [
        [2, "a", ["a", "b", ""]],
        [2, "a", ["a", "b", ""]],
        [3, "a", ["a", "b", ""]],
        [3, "a", ["a", "b", ""]],
    ]


def test_comparator_binding_preserves_case_and_same_named_capture(trino_session):
    session, _ = trino_session
    frame = session.createDataFrame([(1, [4, 1, 3], 4), (2, [4, 1, 3], 2)], ["id", "items", "left"])

    def mixed_case(leftValue, rightValue):
        return F.when(leftValue < rightValue, -1).when(leftValue > rightValue, 1).otherwise(0)

    def proximity(left, right):
        left_distance = F.abs(left - F.col("left"))
        right_distance = F.abs(right - F.col("left"))
        return (
            F.when(left_distance < right_distance, -1)
            .when(left_distance > right_distance, 1)
            .when(left < right, -1)
            .when(left > right, 1)
            .otherwise(0)
        )

    query = frame.select(
        "id",
        F.array_sort("items", mixed_case).alias("mixed"),
        F.array_sort("items", proximity).alias("captured"),
    ).orderBy("id")
    assert query.collect() == [
        Row(id=1, mixed=[1, 3, 4], captured=[4, 3, 1]),
        Row(id=2, mixed=[1, 3, 4], captured=[1, 3, 4]),
    ]
    session._cur.execute(query.sql())
    assert session._cur.fetchall() == [[1, [1, 3, 4], [4, 3, 1]], [2, [1, 3, 4], [1, 3, 4]]]


def test_unaliased_function_outputs_remain_distinct(trino_session):
    session, _ = trino_session
    frame = session.createDataFrame([([3, None, 1], 1, 2), ([2], 1, None)], ["items", "a", "b"])
    sorted_frame = frame.where(F.col("b").isNotNull()).select(
        F.array_sort("items"), F.sort_array("items")
    )
    assert sorted_frame.select("sort_array__items__").collect() == [
        Row(sort_array__items__=[None, 1, 3])
    ]
    assert sorted_frame.collect()[0].asDict() == {
        "array_sort__items__": [1, 3, None],
        "sort_array__items__": [None, 1, 3],
    }
    collections = frame.agg(
        F.collect_list("a"), F.collect_set("a"), F.collect_list("b"), F.collect_set("b")
    )
    assert collections.collect()[0].asDict() == {
        "collect_list__a__": [1, 1],
        "collect_set__a__": [1],
        "collect_list__b__": [2],
        "collect_set__b__": [2],
    }
    assert collections.select("collect_list__a__", "collect_set__a__").collect() == [
        Row(collect_list__a__=[1, 1], collect_set__a__=[1])
    ]
    extrema = frame.select(
        F.greatest("a", F.lit(0)),
        F.greatest("b", F.lit(0)),
        F.least("a", F.lit(3)),
        F.least("b", F.lit(3)),
    )
    assert [row.asDict() for row in extrema.orderBy("greatest__b__").collect()] == [
        {"greatest__a__": 1, "greatest__b__": 0, "least__a__": 1, "least__b__": 3},
        {"greatest__a__": 1, "greatest__b__": 2, "least__a__": 1, "least__b__": 2},
    ]


def test_unaliased_regexp_extract_remains_selectable(trino_session):
    session, _ = trino_session
    frame = session.createDataFrame([(1, "ab"), (2, "zzz"), (3, None)], ["id", "value"])
    query = frame.select(
        "id",
        F.regexp_extract("value", "(a)", 1),
        F.when(F.col("value").isNull(), "null").otherwise("text"),
    )
    assert [row.asDict() for row in query.orderBy("id").collect()] == [
        {"id": 1, "regexp_extract__value__": "a", "when__value__": "text"},
        {"id": 2, "regexp_extract__value__": "", "when__value__": "text"},
        {"id": 3, "regexp_extract__value__": None, "when__value__": "null"},
    ]
    selected = query.select("id", "regexp_extract__value__").orderBy("id")
    assert selected.collect() == [
        Row(id=1, regexp_extract__value__="a"),
        Row(id=2, regexp_extract__value__=""),
        Row(id=3, regexp_extract__value__=None),
    ]
    session._cur.execute(selected.sql())
    assert session._cur.fetchall() == [[1, "a"], [2, ""], [3, None]]


def test_comparator_defaults_and_case_distinct_parameters(trino_session):
    session, _ = trino_session
    frame = session.createDataFrame([([3, 1, 2],)], ["items"])

    def keyword_default(left, right, *, descending=True):
        direction = -1 if descending else 1
        return F.when(left > right, direction).when(left < right, -direction).otherwise(0)

    def case_distinct(x, X):
        return F.when(x < X, -1).when(x > X, 1).otherwise(0)

    query = frame.select(
        F.array_sort("items", keyword_default).alias("descending"),
        F.array_sort("items", case_distinct).alias("ascending"),
    )
    assert query.collect() == [Row(descending=[3, 2, 1], ascending=[1, 2, 3])]
    session._cur.execute(query.sql())
    assert session._cur.fetchall() == [[[3, 2, 1], [1, 2, 3]]]


def test_nested_comparators_capture_outer_arguments(trino_session):
    session, _ = trino_session
    frame = session.createDataFrame([([9, 1, 6, 2],)], ["items"])

    def outer(left, right):
        def score(pivot):
            def inner(left, right):
                left_distance = F.abs(left - pivot)
                right_distance = F.abs(right - pivot)
                return (
                    F.when(left_distance < right_distance, -1)
                    .when(left_distance > right_distance, 1)
                    .otherwise(0)
                )

            return F.array_sort(F.array(F.lit(0), F.lit(10)), inner).getItem(0)

        left_score, right_score = score(left), score(right)
        return (
            F.when(left_score > right_score, -1)
            .when(left_score < right_score, 1)
            .when(left < right, -1)
            .when(left > right, 1)
            .otherwise(0)
        )

    query = frame.select(F.array_sort("items", outer).alias("bucketed"))
    assert query.collect() == [Row(bucketed=[6, 9, 1, 2])]
    session._cur.execute(query.sql())
    assert session._cur.fetchall() == [[[6, 9, 1, 2]]]


@pytest.mark.parametrize("name", ["array_sort", "sort_array"])
def test_compound_sort_outputs_remain_selectable(trino_session, name):
    session, _ = trino_session
    frame = session.createDataFrame([(3, 1, 4, 2)], ["a", "b", "c", "d"])
    sort = getattr(F, name)
    arrays = frame.select(sort(F.array("a", "b")), sort(F.array("c", "d")))
    assert arrays.collect()[0].asDict() == {f"{name}__a__": [1, 3], f"{name}__c__": [2, 4]}
    assert arrays.select(f"{name}__c__").collect()[0][0] == [2, 4]
    aggregates = frame.agg(sort(F.collect_list("a")), sort(F.collect_list("b")))
    assert aggregates.collect()[0].asDict() == {f"{name}__a__": [3], f"{name}__b__": [1]}
    assert aggregates.select(f"{name}__b__").collect()[0][0] == [1]
