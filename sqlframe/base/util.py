from __future__ import annotations

import importlib
import typing as t
import unicodedata

from sqlglot import expressions as exp
from sqlglot.dialects.dialect import Dialect, DialectType
from sqlglot.schema import ensure_column_mapping as sqlglot_ensure_column_mapping

if t.TYPE_CHECKING:
    from pandas.core.frame import DataFrame as PandasDataFrame
    from pyspark.sql.dataframe import SparkSession as PySparkSession

    from sqlframe.base import types
    from sqlframe.base._typing import OptionalPrimitiveType, SchemaInput
    from sqlframe.base.session import _BaseSession
    from sqlframe.base.types import StructType


def decoded_str(value: t.Union[str, bytes]) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def schema_(
    db: exp.Identifier | str,
    catalog: t.Optional[exp.Identifier | str] = None,
    quoted: t.Optional[bool] = None,
) -> exp.Table:
    """Build a Schema.

    Args:
        db: Database name.
        catalog: Catalog name.
        quoted: Whether to force quotes on the schema's identifiers.

    Returns:
        The new Schema instance.
    """
    return exp.Table(
        this=None,
        db=exp.to_identifier(db, quoted=quoted) if db else None,
        catalog=exp.to_identifier(catalog, quoted=quoted) if catalog else None,
    )


def to_schema(
    sql_path: t.Union[str, exp.Table], dialect: t.Optional[DialectType] = None
) -> exp.Table:
    if isinstance(sql_path, exp.Table) and sql_path.this is None:
        return sql_path
    table = exp.to_table(
        sql_path.copy() if isinstance(sql_path, exp.Table) else sql_path, dialect=dialect
    )
    table.set("catalog", table.args.get("db"))
    table.set("db", table.args.get("this"))
    table.set("this", None)
    return table


def get_column_mapping_from_schema_input(
    schema: SchemaInput, dialect: DialectType = None
) -> t.Dict[str, t.Optional[exp.DataType]]:
    from sqlframe.base import types

    if isinstance(schema, dict):
        value = schema
    elif isinstance(schema, str):
        col_name_type_strs = [x.strip() for x in schema.split(",")]
        if len(col_name_type_strs) == 1 and len(col_name_type_strs[0].split(" ")) == 1:
            value = {"value": col_name_type_strs[0].strip()}
        else:
            value = {
                name_type_str.split(" ")[0].strip(): name_type_str.split(" ")[1].strip()
                for name_type_str in col_name_type_strs
            }
    elif isinstance(schema, types.StructType):
        value = {struct_field.name: struct_field.dataType.simpleString() for struct_field in schema}
    else:
        value = {x.strip(): None for x in schema}
    return {
        exp.to_column(k).sql(dialect=dialect): exp.DataType.build(v, dialect=dialect)
        if v is not None
        else v
        for k, v in value.items()
    }
    # return {x.strip(): None for x in schema}  # type: ignore


def get_tables_from_expression_with_join(expression: exp.Select) -> t.List[exp.Table]:
    if not expression.args.get("joins"):
        return []

    left_table = expression.args["from"].this
    other_tables = [join.this for join in expression.args["joins"]]
    return [left_table] + other_tables


def to_csv(options: t.Dict[str, OptionalPrimitiveType], equality_char: str = "=") -> str:
    return ", ".join(
        [f"{k}{equality_char}{v}" for k, v in (options or {}).items() if v is not None]
    )


def ensure_column_mapping(schema: t.Union[str, StructType]) -> t.Dict:
    if isinstance(schema, str):
        col_name_type_strs = [x.strip() for x in schema.split(",")]
        schema = {  # type: ignore
            name_type_str.split(" ")[0].strip(): name_type_str.split(" ")[1].strip()
            for name_type_str in col_name_type_strs
        }
    # TODO: Make a protocol with a `simpleString` attribute as what it looks for instead of the actual
    # `StructType` object.
    elif hasattr(schema, "simpleString"):
        return {struct_field.name: struct_field.dataType.simpleString() for struct_field in schema}
    return sqlglot_ensure_column_mapping(schema)  # type: ignore


# SO: https://stackoverflow.com/questions/37513355/converting-pandas-dataframe-into-spark-dataframe-error
def get_equivalent_spark_type(pandas_type) -> types.DataType:
    """
    This method will retrieve the corresponding spark type given a pandas
    type.

    Args:
        pandas_type (str): pandas data type

    Returns:
        spark data type
    """
    from sqlframe.base import types

    type_map = {
        "datetime64[ns]": types.TimestampType(),
        "int64": types.LongType(),
        "int32": types.IntegerType(),
        "float64": types.DoubleType(),
        "float32": types.FloatType(),
    }
    return type_map.get(str(pandas_type).lower(), types.StringType())


def pandas_to_spark_schema(pandas_df: PandasDataFrame) -> types.StructType:
    """
    This method will return a spark dataframe schema given a pandas dataframe.

    Args:
        pandas_df (pandas.core.frame.DataFrame): pandas DataFrame

    Returns:
        equivalent spark DataFrame schema
    """
    from sqlframe.base import types

    columns = list(
        [
            x.replace("?column?", f"unknown_column_{i}").replace("NULL", f"unknown_column_{i}")
            for i, x in enumerate(pandas_df.columns)
        ]
    )
    d_types = list(pandas_df.dtypes)
    p_schema = types.StructType(
        [
            types.StructField(column, get_equivalent_spark_type(pandas_type))
            for column, pandas_type in zip(columns, d_types)
        ]
    )
    return p_schema


def dialect_to_string(dialect: Dialect) -> str:
    mapping = {v: k for k, v in Dialect.classes.items()}
    return mapping[type(dialect)]


def get_func_from_session(
    name: str,
    session: t.Optional[t.Union[_BaseSession, PySparkSession]] = None,
    fallback: bool = True,
) -> t.Callable:
    from sqlframe.base.session import _BaseSession

    session = session if session else _BaseSession()

    if isinstance(session, _BaseSession):
        dialect_str = dialect_to_string(session.input_dialect)
        import_path = f"sqlframe.{dialect_str}.functions"
    else:
        import_path = "pyspark.sql.functions"
    try:
        func = getattr(importlib.import_module(import_path), name)
    except AttributeError as e:
        if not fallback:
            raise e
        func = getattr(importlib.import_module("sqlframe.base.functions"), name)
        if session.output_dialect in func.unsupported_engines:  # type: ignore
            raise NotImplementedError(
                f"{name} is not supported by the engine: {session.output_dialect}"  # type: ignore
            )
    return func


def soundex(s):
    if not s:
        return ""

    s = unicodedata.normalize("NFKD", s)
    s = s.upper()

    replacements = (
        ("BFPV", "1"),
        ("CGJKQSXZ", "2"),
        ("DT", "3"),
        ("L", "4"),
        ("MN", "5"),
        ("R", "6"),
    )
    result = [s[0]]
    count = 1

    # find would-be replacment for first character
    for lset, sub in replacements:
        if s[0] in lset:
            last = sub
            break
    else:
        last = None

    for letter in s[1:]:
        for lset, sub in replacements:
            if letter in lset:
                if sub != last:
                    result.append(sub)
                    count += 1
                last = sub
                break
        else:
            if letter != "H" and letter != "W":
                # leave last alone if middle letter is H or W
                last = None
        if count == 4:
            break

    result += "0" * (4 - count)
    return "".join(result)


def verify_pandas_installed():
    try:
        import pandas  # noqa
    except ImportError:
        raise ImportError(
            """Pandas is required for this functionality. `pip install "sqlframe[pandas]"` (also include your engine if needed) to install pandas."""
        )


def verify_openai_installed():
    try:
        import openai  # noqa
    except ImportError:
        raise ImportError(
            """OpenAI is required for this functionality. `pip install "sqlframe[openai]"` (also include your engine if needed) to install openai."""
        )


def verify_numpy_installed():
    try:
        import numpy  # noqa
    except ImportError:
        raise ImportError(
            """Numpy is required for this functionality. `pip install "sqlframe[pandas]"` (also include your engine if needed) to install pandas/numpy."""
        )


def quote_preserving_alias_or_name(col: t.Union[exp.Column, exp.Alias]) -> str:
    from sqlframe.base.session import _BaseSession

    if isinstance(col, exp.Alias):
        col = col.args["alias"]
    if isinstance(col, exp.Column):
        col = col.copy()
        col.set("table", None)
    if isinstance(col, (exp.Identifier, exp.Column)):
        return col.sql(dialect=_BaseSession().input_dialect)
    # We may get things like `Null()` expression or maybe literals so we just return the alias or name in those cases
    return col.alias_or_name
