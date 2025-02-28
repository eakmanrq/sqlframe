import datetime

from sqlframe.standalone import functions as F
from sqlframe.standalone.window import Window


def test_simple():
    assert (F.col("cola") == 1).sql() == "cola = 1"


def test_neq():
    assert (F.col("cola") != 1).sql() == "cola <> 1"


def test_gt():
    assert (F.col("cola") > 1).sql() == "cola > 1"


def test_lt():
    assert (F.col("cola") < 1).sql() == "cola < 1"


def test_le():
    assert (F.col("cola") <= 1).sql() == "cola <= 1"


def test_ge():
    assert (F.col("cola") >= 1).sql() == "cola >= 1"


def test_and():
    assert (
        (F.col("cola") == F.col("colb")) & (F.col("colc") == F.col("cold"))
    ).sql() == "(cola = colb AND colc = cold)"


def test_or():
    assert (
        (F.col("cola") == F.col("colb")) | (F.col("colc") == F.col("cold"))
    ).sql() == "(cola = colb OR colc = cold)"


def test_mod():
    assert (F.col("cola") % 2).sql() == "(cola % 2)"


def test_add():
    assert (F.col("cola") + 1).sql() == "(cola + 1)"


def test_sub():
    assert (F.col("cola") - 1).sql() == "(cola - 1)"


def test_mul():
    assert (F.col("cola") * 2).sql() == "(cola * 2)"


def test_div():
    assert (F.col("cola") / 2).sql() == "(cola / 2)"


def test_radd():
    assert (1 + F.col("cola")).sql() == "(1 + cola)"


def test_rsub():
    assert (1 - F.col("cola")).sql() == "(1 - cola)"


def test_rmul():
    assert (1 * F.col("cola")).sql() == "(1 * cola)"


def test_rdiv():
    assert (1 / F.col("cola")).sql() == "(1 / cola)"


def test_pow():
    assert (F.col("cola") ** 2).sql() == "POWER(cola, 2)"


def test_rpow():
    assert (2 ** F.col("cola")).sql() == "POWER(2, cola)"


def test_invert():
    assert (~F.col("cola")).sql() == "NOT (cola)"


def test_invert_conjuction():
    assert (~(F.col("cola") | F.col("colb"))).sql() == "NOT ((cola OR colb))"


def test_paren():
    assert ((F.col("cola") - F.col("colb")) / F.col("colc")).sql() == "((cola - colb) / colc)"


def test_implicit_paren():
    assert (F.col("cola") - F.col("colb") / F.col("colc")).sql() == "(cola - (colb / colc))"


def test_startswith():
    assert (F.col("cola").startswith("test")).sql() == "STARTSWITH(cola, 'test')"


def test_endswith():
    assert (F.col("cola").endswith("test")).sql() == "ENDSWITH(cola, 'test')"


def test_rlike():
    assert (F.col("cola").rlike("foo")).sql() == "cola RLIKE 'foo'"


def test_like():
    assert (F.col("cola").like("foo%")).sql() == "cola LIKE 'foo%'"


def test_ilike():
    assert (F.col("cola").ilike("foo%")).sql() == "cola ILIKE 'foo%'"


def test_substring():
    assert (F.col("cola").substr(2, 3)).sql() == "SUBSTRING(cola, 2, 3)"


def test_isin():
    assert (F.col("cola").isin([1, 2, 3])).sql() == "cola IN (1, 2, 3)"
    assert (F.col("cola").isin(1, 2, 3)).sql() == "cola IN (1, 2, 3)"


def test_asc():
    assert (F.col("cola").asc()).sql() == "cola ASC"


def test_desc():
    assert (F.col("cola").desc()).sql() == "cola DESC"


def test_asc_nulls_first():
    assert (F.col("cola").asc_nulls_first()).sql() == "cola ASC"


def test_asc_nulls_last():
    assert (F.col("cola").asc_nulls_last()).sql() == "cola ASC NULLS LAST"


def test_desc_nulls_first():
    assert (F.col("cola").desc_nulls_first()).sql() == "cola DESC NULLS FIRST"


def test_desc_nulls_last():
    assert (F.col("cola").desc_nulls_last()).sql() == "cola DESC"


def test_when_otherwise():
    assert (F.when(F.col("cola") == 1, 2)).sql() == "CASE WHEN cola = 1 THEN 2 END AS when__cola__"
    assert (
        F.col("cola").when(F.col("cola") == 1, 2)
    ).sql() == "CASE WHEN cola = 1 THEN 2 END AS when__cola__"
    assert (
        F.when(F.col("cola") == 1, 2).when(F.col("colb") == 2, 3)
    ).sql() == "CASE WHEN cola = 1 THEN 2 WHEN colb = 2 THEN 3 END AS when__cola__"
    assert (
        F.col("cola").when(F.col("cola") == 1, 2).when(F.col("colb") == 2, 3).sql()
        == "CASE WHEN cola = 1 THEN 2 WHEN colb = 2 THEN 3 END AS when__cola__"
    )
    assert (
        F.when(F.col("cola") == 1, 2).when(F.col("colb") == 2, 3).otherwise(4).sql()
        == "CASE WHEN cola = 1 THEN 2 WHEN colb = 2 THEN 3 ELSE 4 END AS when__cola__"
    )


def test_is_null():
    assert (F.col("cola").isNull()).sql() == "cola IS NULL"


def test_is_not_null():
    assert (F.col("cola").isNotNull()).sql() == "NOT cola IS NULL"


def test_cast():
    assert (F.col("cola").cast("INT")).sql() == "CAST(cola AS INT)"


def test_alias():
    assert (F.col("cola").alias("new_name")).sql() == "cola AS new_name"


def test_between():
    assert (F.col("cola").between(1, 3)).sql() == "cola BETWEEN 1 AND 3"
    assert (F.col("cola").between(10.1, 12.1)).sql() == "cola BETWEEN 10.1 AND 12.1"
    assert (
        F.col("cola").between(datetime.date(2022, 1, 1), datetime.date(2022, 3, 1))
    ).sql() == "cola BETWEEN CAST('2022-01-01' AS DATE) AND CAST('2022-03-01' AS DATE)"
    assert (
        (
            F.col("cola").between(
                datetime.datetime(2022, 1, 1, 1, 1, 1), datetime.datetime(2022, 3, 1, 1, 1, 1)
            )
        ).sql()
        == "cola BETWEEN CAST('2022-01-01 01:01:01' AS TIMESTAMP) AND CAST('2022-03-01 01:01:01' AS TIMESTAMP)"
    )


def test_over():
    assert (
        (
            F.sum("cola").over(
                Window.partitionBy("colb")
                .orderBy("colc")
                .rowsBetween(-1, Window.unboundedFollowing)
            )
        ).sql()
        == "SUM(cola) OVER (PARTITION BY colb ORDER BY colc ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)"
    )
    assert (
        (
            F.sum("cola").over(
                Window.partitionBy("colb")
                .orderBy("colc")
                .rangeBetween(-1, Window.unboundedFollowing)
            )
        ).sql()
        == "SUM(cola) OVER (PARTITION BY colb ORDER BY colc RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)"
    )


def test_get_item():
    assert F.col("cola").getItem(1).sql() == "ELEMENT_AT(cola, (1 + 1)) AS element_at__cola__"
