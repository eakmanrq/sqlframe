import pytest

from sqlframe.standalone import functions as F
from sqlframe.standalone.window import Window, WindowSpec


@pytest.mark.parametrize(
    "func, expected",
    [
        (WindowSpec().partitionBy(F.col("cola"), F.col("colb")), "OVER (PARTITION BY cola, colb)"),
        (Window.partitionBy(F.col("cola"), F.col("colb")), "OVER (PARTITION BY cola, colb)"),
        (WindowSpec().orderBy("cola", "colb"), "OVER (ORDER BY cola, colb)"),
        (Window.orderBy("cola", "colb"), "OVER (ORDER BY cola, colb)"),
        (WindowSpec().rowsBetween(3, 5), "OVER (ROWS BETWEEN 3 PRECEDING AND 5 FOLLOWING)"),
        (Window.rowsBetween(3, 5), "OVER (ROWS BETWEEN 3 PRECEDING AND 5 FOLLOWING)"),
        (WindowSpec().rangeBetween(3, 5), "OVER (RANGE BETWEEN 3 PRECEDING AND 5 FOLLOWING)"),
        (Window.rangeBetween(3, 5), "OVER (RANGE BETWEEN 3 PRECEDING AND 5 FOLLOWING)"),
        (
            Window.rowsBetween(Window.unboundedPreceding, 2),
            "OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING)",
        ),
        (
            Window.rowsBetween(1, Window.unboundedFollowing),
            "OVER (ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)",
        ),
        (
            Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing),
            "OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
        ),
        (
            Window.rangeBetween(Window.unboundedPreceding, 2),
            "OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING)",
        ),
        (
            Window.rangeBetween(1, Window.unboundedFollowing),
            "OVER (RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)",
        ),
        (
            Window.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing),
            "OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
        ),
    ],
)
def test_window_spec_partition_by(func, expected):
    assert func.sql() == expected
