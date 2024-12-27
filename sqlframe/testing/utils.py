# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.
from __future__ import annotations

import difflib
import os
import typing as t
from itertools import zip_longest

from sqlframe.base import types
from sqlframe.base.dataframe import BaseDataFrame
from sqlframe.base.exceptions import (
    DataFrameDiffError,
    SchemaDiffError,
    SQLFrameException,
)
from sqlframe.base.util import verify_pandas_installed

if t.TYPE_CHECKING:
    import pandas as pd


def _terminal_color_support():
    try:
        # determine if environment supports color
        script = "$(test $(tput colors)) && $(test $(tput colors) -ge 8) && echo true || echo false"
        return os.popen(script).read()
    except Exception:
        return False


def _context_diff(actual: t.List[str], expected: t.List[str], n: int = 3):
    """
    Modified from difflib context_diff API,
    see original code here: https://github.com/python/cpython/blob/main/Lib/difflib.py#L1180
    """

    def red(s: str) -> str:
        red_color = "\033[31m"
        no_color = "\033[0m"
        return red_color + str(s) + no_color

    prefix = dict(insert="+ ", delete="- ", replace="! ", equal="  ")
    for group in difflib.SequenceMatcher(None, actual, expected).get_grouped_opcodes(n):
        yield "*** actual ***"
        if any(tag in {"replace", "delete"} for tag, _, _, _, _ in group):
            for tag, i1, i2, _, _ in group:
                for line in actual[i1:i2]:
                    if tag != "equal" and _terminal_color_support():
                        yield red(prefix[tag] + str(line))
                    else:
                        yield prefix[tag] + str(line)

        yield "\n"

        yield "*** expected ***"
        if any(tag in {"replace", "insert"} for tag, _, _, _, _ in group):
            for tag, _, _, j1, j2 in group:
                for line in expected[j1:j2]:
                    if tag != "equal" and _terminal_color_support():
                        yield red(prefix[tag] + str(line))
                    else:
                        yield prefix[tag] + str(line)


# Source: https://github.com/apache/spark/blob/master/python/pyspark/testing/utils.py#L519
def assertDataFrameEqual(
    actual: t.Union[BaseDataFrame, pd.DataFrame, t.List[types.Row]],
    expected: t.Union[BaseDataFrame, pd.DataFrame, t.List[types.Row]],
    checkRowOrder: bool = False,
    rtol: float = 1e-5,
    atol: float = 1e-8,
):
    r"""
    A util function to assert equality between `actual` and `expected`
    (DataFrames or lists of Rows), with optional parameters `checkRowOrder`, `rtol`, and `atol`.

    Supports Spark, Spark Connect, pandas, and pandas-on-Spark DataFrames.
    For more information about pandas-on-Spark DataFrame equality, see the docs for
    `assertPandasOnSparkEqual`.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    actual : DataFrame (Spark, Spark Connect, pandas, or pandas-on-Spark) or list of Rows
        The DataFrame that is being compared or tested.
    expected : DataFrame (Spark, Spark Connect, pandas, or pandas-on-Spark) or list of Rows
        The expected result of the operation, for comparison with the actual result.
    checkRowOrder : bool, optional
        A flag indicating whether the order of rows should be considered in the comparison.
        If set to `False` (default), the row order is not taken into account.
        If set to `True`, the order of rows is important and will be checked during comparison.
        (See Notes)
    rtol : float, optional
        The relative tolerance, used in asserting approximate equality for float values in actual
        and expected. Set to 1e-5 by default. (See Notes)
    atol : float, optional
        The absolute tolerance, used in asserting approximate equality for float values in actual
        and expected. Set to 1e-8 by default. (See Notes)

    Notes
    -----
    When `assertDataFrameEqual` fails, the error message uses the Python `difflib` library to
    display a diff log of each row that differs in `actual` and `expected`.

    For `checkRowOrder`, note that PySpark DataFrame ordering is non-deterministic, unless
    explicitly sorted.

    Note that schema equality is checked only when `expected` is a DataFrame (not a list of Rows).

    For DataFrames with float values, assertDataFrame asserts approximate equality.
    Two float values a and b are approximately equal if the following equation is True:

    ``absolute(a - b) <= (atol + rtol * absolute(b))``.

    Examples
    --------
    >>> df1 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
    >>> df2 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
    >>> assertDataFrameEqual(df1, df2)  # pass, DataFrames are identical

    >>> df1 = spark.createDataFrame(data=[("1", 0.1), ("2", 3.23)], schema=["id", "amount"])
    >>> df2 = spark.createDataFrame(data=[("1", 0.109), ("2", 3.23)], schema=["id", "amount"])
    >>> assertDataFrameEqual(df1, df2, rtol=1e-1)  # pass, DataFrames are approx equal by rtol

    >>> df1 = spark.createDataFrame(data=[(1, 1000), (2, 3000)], schema=["id", "amount"])
    >>> list_of_rows = [Row(1, 1000), Row(2, 3000)]
    >>> assertDataFrameEqual(df1, list_of_rows)  # pass, actual and expected data are equal

    >>> import pyspark.pandas as ps
    >>> df1 = ps.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [7, 8, 9]})
    >>> df2 = ps.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [7, 8, 9]})
    >>> assertDataFrameEqual(df1, df2)  # pass, pandas-on-Spark DataFrames are equal

    >>> df1 = spark.createDataFrame(
    ...     data=[("1", 1000.00), ("2", 3000.00), ("3", 2000.00)], schema=["id", "amount"])
    >>> df2 = spark.createDataFrame(
    ...     data=[("1", 1001.00), ("2", 3000.00), ("3", 2003.00)], schema=["id", "amount"])
    >>> assertDataFrameEqual(df1, df2)  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    PySparkAssertionError: [DIFFERENT_ROWS] Results do not match: ( 66.66667 % )
    *** actual ***
    ! Row(id='1', amount=1000.0)
    Row(id='2', amount=3000.0)
    ! Row(id='3', amount=2000.0)
    *** expected ***
    ! Row(id='1', amount=1001.0)
    Row(id='2', amount=3000.0)
    ! Row(id='3', amount=2003.0)
    """
    import pandas as pd

    if actual is None and expected is None:
        return True
    elif actual is None or expected is None:
        raise SQLFrameException("Missing required arguments: actual and expected")

    def compare_rows(r1: types.Row, r2: types.Row):
        def compare_vals(val1, val2):
            if isinstance(val1, list) and isinstance(val2, list):
                return len(val1) == len(val2) and all(
                    compare_vals(x, y) for x, y in zip(val1, val2)
                )
            elif isinstance(val1, types.Row) and isinstance(val2, types.Row):
                return all(compare_vals(x, y) for x, y in zip(val1, val2))
            elif isinstance(val1, dict) and isinstance(val2, dict):
                return (
                    len(val1.keys()) == len(val2.keys())
                    and val1.keys() == val2.keys()
                    and all(compare_vals(val1[k], val2[k]) for k in val1.keys())
                )
            elif isinstance(val1, float) and isinstance(val2, float):
                if abs(val1 - val2) > (atol + rtol * abs(val2)):
                    return False
            else:
                if val1 != val2:
                    return False
            return True

        if r1 is None and r2 is None:
            return True
        elif r1 is None or r2 is None:
            return False

        return compare_vals(r1, r2)

    def assert_rows_equal(rows1: t.List[types.Row], rows2: t.List[types.Row]):
        zipped = list(zip_longest(rows1, rows2))
        diff_rows_cnt = 0
        diff_rows = False

        rows_str1 = ""
        rows_str2 = ""

        # count different rows
        for r1, r2 in zipped:
            rows_str1 += str(r1) + "\n"
            rows_str2 += str(r2) + "\n"
            if not compare_rows(r1, r2):
                diff_rows_cnt += 1
                diff_rows = True

        generated_diff = _context_diff(
            actual=rows_str1.splitlines(), expected=rows_str2.splitlines(), n=len(zipped)
        )

        if diff_rows:
            error_msg = "Results do not match: "
            percent_diff = (diff_rows_cnt / len(zipped)) * 100
            error_msg += "( %.5f %% )" % percent_diff
            error_msg += "\n" + "\n".join(generated_diff)
            raise DataFrameDiffError("Rows are different:\n%s" % error_msg)

    # convert actual and expected to list
    if not isinstance(actual, list) and not isinstance(expected, list):
        # only compare schema if expected is not a List
        assertSchemaEqual(actual.schema, expected.schema)  # type: ignore

    if not isinstance(actual, list):
        actual_list = actual.collect()  # type: ignore
    else:
        actual_list = actual

    if not isinstance(expected, list):
        expected_list = expected.collect()  # type: ignore
    else:
        expected_list = expected

    if not checkRowOrder:
        # rename duplicate columns for sorting
        actual_list = sorted(actual_list, key=lambda x: str(x))
        expected_list = sorted(expected_list, key=lambda x: str(x))

    assert_rows_equal(actual_list, expected_list)


def assertSchemaEqual(actual: types.StructType, expected: types.StructType):
    r"""
    A util function to assert equality between DataFrame schemas `actual` and `expected`.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    actual : StructType
        The DataFrame schema that is being compared or tested.
    expected : StructType
        The expected schema, for comparison with the actual schema.

    Notes
    -----
    When assertSchemaEqual fails, the error message uses the Python `difflib` library to display
    a diff log of the `actual` and `expected` schemas.

    Examples
    --------
    >>> from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType, DoubleType
    >>> s1 = StructType([StructField("names", ArrayType(DoubleType(), True), True)])
    >>> s2 = StructType([StructField("names", ArrayType(DoubleType(), True), True)])
    >>> assertSchemaEqual(s1, s2)  # pass, schemas are identical

    >>> df1 = spark.createDataFrame(data=[(1, 1000), (2, 3000)], schema=["id", "number"])
    >>> df2 = spark.createDataFrame(data=[("1", 1000), ("2", 5000)], schema=["id", "amount"])
    >>> assertSchemaEqual(df1.schema, df2.schema)  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    PySparkAssertionError: [DIFFERENT_SCHEMA] Schemas do not match.
    --- actual
    +++ expected
    - StructType([StructField('id', LongType(), True), StructField('number', LongType(), True)])
    ?                               ^^                               ^^^^^
    + StructType([StructField('id', StringType(), True), StructField('amount', LongType(), True)])
    ?                               ^^^^                              ++++ ^
    """
    if not isinstance(actual, types.StructType):
        raise RuntimeError("actual must be a StructType")
    if not isinstance(expected, types.StructType):
        raise RuntimeError("expected must be a StructType")

    def compare_schemas_ignore_nullable(s1: types.StructType, s2: types.StructType):
        if len(s1) != len(s2):
            return False
        zipped = zip_longest(s1, s2)
        for sf1, sf2 in zipped:
            if not compare_structfields_ignore_nullable(sf1, sf2):
                return False
        return True

    def compare_structfields_ignore_nullable(
        actualSF: types.StructField, expectedSF: types.StructField
    ):
        if actualSF is None and expectedSF is None:
            return True
        elif actualSF is None or expectedSF is None:
            return False
        if actualSF.name != expectedSF.name:
            return False
        else:
            return compare_datatypes_ignore_nullable(actualSF.dataType, expectedSF.dataType)

    def compare_datatypes_ignore_nullable(dt1: t.Any, dt2: t.Any):
        # checks datatype equality, using recursion to ignore nullable
        if dt1.typeName() == dt2.typeName():
            if dt1.typeName() == "array":
                return compare_datatypes_ignore_nullable(dt1.elementType, dt2.elementType)
            elif dt1.typeName() == "struct":
                return compare_schemas_ignore_nullable(dt1, dt2)
            else:
                return True
        else:
            return False

    # ignore nullable flag by default
    if not compare_schemas_ignore_nullable(actual, expected):
        generated_diff = difflib.ndiff(str(actual).splitlines(), str(expected).splitlines())

        error_msg = "\n".join(generated_diff)

        raise SchemaDiffError(error_msg)
