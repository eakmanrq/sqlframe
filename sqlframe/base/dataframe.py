# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import enum
import functools
import json
import logging
import sys
import typing as t
import uuid
import zlib
from copy import copy
from dataclasses import dataclass
from uuid import uuid4

import sqlglot
from prettytable import PrettyTable
from sqlglot import Dialect, maybe_parse
from sqlglot import expressions as exp
from sqlglot import lineage as sqlglot_lineage
from sqlglot.helper import ensure_list, object_to_dict, seq_get
from sqlglot.optimizer.pushdown_projections import pushdown_projections
from sqlglot.optimizer.qualify import qualify

from sqlframe.base.catalog import Column as CatalogColumn
from sqlframe.base.operations import Operation
from sqlframe.base.plan import (
    AliasNode,
    CacheNode,
    DistinctNode,
    DropDuplicatesNode,
    DropNaNode,
    DropNode,
    FillNaNode,
    FilterNode,
    GroupByNode,
    HintNode,
    JoinNode,
    LimitNode,
    PlanNode,
    RenameColumnsNode,
    ReplaceNode,
    SelectNode,
    SetOpNode,
    SortNode,
    SourceNode,
    ToDFNode,
    UnionByNameNode,
    UnpivotNode,
    WithColumnsNode,
)
from sqlframe.base.transforms import replace_id_value
from sqlframe.base.util import (
    get_func_from_session,
    get_tables_from_expression_with_join,
    normalize_string,
    partition_to,
    quote_preserving_alias_or_name,
    sqlglot_to_spark,
    verify_openai_installed,
)

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if t.TYPE_CHECKING:
    import pandas as pd
    from pyarrow import RecordBatchReader
    from pyarrow import Table as ArrowTable
    from sqlglot.dialects.dialect import DialectType

    from sqlframe.base._typing import (
        ColumnOrLiteral,
        ColumnOrName,
        OutputExpressionContainer,
        PrimitiveType,
        StorageLevel,
    )
    from sqlframe.base.column import Column
    from sqlframe.base.group import _BaseGroupedData
    from sqlframe.base.session import WRITER, _BaseSession
    from sqlframe.base.types import Row, StructType

    SESSION = t.TypeVar("SESSION", bound=_BaseSession)
    GROUP_DATA = t.TypeVar("GROUP_DATA", bound=_BaseGroupedData)
else:
    WRITER = t.TypeVar("WRITER")
    SESSION = t.TypeVar("SESSION")
    GROUP_DATA = t.TypeVar("GROUP_DATA")

logger = logging.getLogger(__name__)

JOIN_HINTS = {
    "BROADCAST",
    "BROADCASTJOIN",
    "MAPJOIN",
    "MERGE",
    "SHUFFLEMERGE",
    "MERGEJOIN",
    "SHUFFLE_HASH",
    "SHUFFLE_REPLICATE_NL",
}

JOIN_TYPE_MAPPING = {
    "outer": "full_outer",
    "full": "full_outer",
    "fullouter": "full_outer",
    "left": "left_outer",
    "leftouter": "left_outer",
    "right": "right_outer",
    "rightouter": "right_outer",
    "semi": "left_semi",
    "leftsemi": "left_semi",
    "anti": "left_anti",
    "leftanti": "left_anti",
}

DF = t.TypeVar("DF", bound="BaseDataFrame")


class OpenAIMode(enum.Enum):
    CTE_ONLY = "cte_only"
    FULL = "full"

    @property
    def is_cte_only(self) -> bool:
        return self == OpenAIMode.CTE_ONLY

    @property
    def is_full(self) -> bool:
        return self == OpenAIMode.FULL


@dataclass
class OpenAIConfig:
    mode: OpenAIMode = OpenAIMode.CTE_ONLY
    model: str = "gpt-4o"
    prompt_override: t.Optional[str] = None

    @classmethod
    def from_dict(cls, config: t.Dict[str, t.Any]) -> OpenAIConfig:
        if "mode" in config:
            config["mode"] = OpenAIMode(config["mode"].lower())
        return cls(**config)

    def get_prompt(self, dialect: Dialect) -> str:
        if self.prompt_override:
            return self.prompt_override
        if self.mode.is_cte_only:
            return f"You are a backend tool that creates unique CTE alias names match what a human would write and in snake case. You respond without code blocks and only a json payload with the key being the CTE name that is being replaced and the value being the new CTE human readable name."
        return f"""
        You are a backend tool that converts correct {dialect} SQL to simplified and more human readable version.
        You respond without code block with rewritten {dialect} SQL.
        You don't change any column names in the final select because the user expects those to remain the same.
        You make unique CTE alias names match what a human would write and in snake case.
        You improve formatting with spacing and line-breaks.
        You remove redundant parenthesis and aliases.
        When remove extra quotes, make sure to keep quotes around words that could be reserved words"""


class _BaseDataFrameNaFunctions(t.Generic[DF]):
    def __init__(self, df: DF):
        self.df = df

    def drop(
        self,
        how: str = "any",
        thresh: t.Optional[int] = None,
        subset: t.Optional[t.Union[str, t.Tuple[str, ...], t.List[str]]] = None,
    ) -> DF:
        return self.df.dropna(how=how, thresh=thresh, subset=subset)

    @t.overload
    def fill(self, value: PrimitiveType, subset: t.Optional[t.List[str]] = ...) -> DF: ...

    @t.overload
    def fill(self, value: t.Dict[str, PrimitiveType]) -> DF: ...

    def fill(
        self,
        value: t.Union[PrimitiveType, t.Dict[str, PrimitiveType]],
        subset: t.Optional[t.Union[str, t.Tuple[str, ...], t.List[str]]] = None,
    ) -> DF:
        return self.df.fillna(value=value, subset=subset)

    def replace(
        self,
        to_replace: t.Union[bool, int, float, str, t.List, t.Dict],
        value: t.Optional[t.Union[bool, int, float, str, t.List]] = None,
        subset: t.Optional[t.Union[str, t.List[str]]] = None,
    ) -> DF:
        return self.df.replace(to_replace=to_replace, value=value, subset=subset)


NA = t.TypeVar("NA", bound=_BaseDataFrameNaFunctions)


class _BaseDataFrameStatFunctions(t.Generic[DF]):
    def __init__(self, df: DF):
        self.df = df

    @t.overload
    def approxQuantile(
        self,
        col: str,
        probabilities: t.Union[t.List[float], t.Tuple[float]],
        relativeError: float,
    ) -> t.List[float]: ...

    @t.overload
    def approxQuantile(
        self,
        col: t.Union[t.List[str], t.Tuple[str]],
        probabilities: t.Union[t.List[float], t.Tuple[float]],
        relativeError: float,
    ) -> t.List[t.List[float]]: ...

    def approxQuantile(
        self,
        col: t.Union[str, t.List[str], t.Tuple[str]],
        probabilities: t.Union[t.List[float], t.Tuple[float]],
        relativeError: float,
    ) -> t.Union[t.List[float], t.List[t.List[float]]]:
        return self.df.approxQuantile(col, probabilities, relativeError)

    def corr(self, col1: str, col2: str, method: str = "pearson") -> float:
        return self.df.corr(col1, col2, method)

    def cov(self, col1: str, col2: str) -> float:
        return self.df.cov(col1, col2)


STAT = t.TypeVar("STAT", bound=_BaseDataFrameStatFunctions)


class BaseDataFrame(t.Generic[SESSION, WRITER, NA, STAT, GROUP_DATA]):
    _na: t.Type[NA]
    _stat: t.Type[STAT]
    _group_data: t.Type[GROUP_DATA]
    _EXPLAIN_PREFIX = "EXPLAIN"

    def __init__(
        self,
        session: SESSION,
        expression: t.Optional[exp.Select] = None,
        branch_id: t.Optional[str] = None,
        sequence_id: t.Optional[str] = None,
        join_on_uuid: t.Optional[str] = None,
        known_uuids: t.Optional[t.Set[str]] = None,
        last_op: Operation = Operation.INIT,
        pending_hints: t.Optional[t.List[exp.Expression]] = None,
        output_expression_container: t.Optional[OutputExpressionContainer] = None,
        display_name_mapping: t.Optional[t.Dict[str, str]] = None,
        _plan: t.Optional[PlanNode] = None,
        **kwargs,
    ):
        self.session = session
        self.branch_id = branch_id or self.session._random_branch_id
        self.sequence_id = sequence_id or self.session._random_sequence_id
        self.join_on_uuid = join_on_uuid or str(uuid4())
        self.known_uuids = known_uuids or set()
        self.known_uuids.add(self.join_on_uuid)
        self.last_op = last_op
        self.pending_hints = pending_hints or []
        self.output_expression_container = output_expression_container or exp.Select()
        self.temp_views: t.List[exp.Select] = []
        self.display_name_mapping = display_name_mapping or {}
        # Plan infrastructure: store the logical plan and lazily compiled expression
        if _plan is not None:
            self._plan: PlanNode = _plan
            self._cached_expression: t.Optional[exp.Select] = None
        elif expression is not None:
            self._plan = SourceNode(
                expression=expression,
                branch_id=self.branch_id,
                sequence_id=self.sequence_id,
                last_op=self.last_op,
            )
            self._cached_expression = expression
        else:
            raise ValueError("Either expression or _plan must be provided")

    @property
    def expression(self) -> exp.Select:
        if self._cached_expression is None:
            from sqlframe.base.compiler import PlanCompiler

            state = PlanCompiler(self.session).compile_with_state(self._plan)
            self._cached_expression = state.expression
            # Flow back metadata computed during compilation
            self.display_name_mapping.update(state.display_name_mapping)
        return self._cached_expression

    @expression.setter
    def expression(self, value: exp.Select) -> None:
        self._cached_expression = value
        self._plan = SourceNode(
            expression=value,
            branch_id=self.branch_id,
            sequence_id=self.sequence_id,
            last_op=self.last_op,
        )

    def __getattr__(self, column_name: str) -> Column:
        return self[column_name]

    def __getitem__(self, column_name: str) -> Column:
        from sqlframe.base.util import get_func_from_session

        col_func = get_func_from_session("col", self.session)

        column_name = f"{self.branch_id}.{column_name}"
        col = col_func(column_name)
        col.expression.meta["join_on_uuid"] = self.join_on_uuid
        return col

    def __copy__(self):
        return self.copy()

    def _display_(self) -> str:
        return self.__repr__()

    @property
    def _typed_columns(self) -> t.List[CatalogColumn]:
        raise NotImplementedError

    @property
    def write(self) -> WRITER:
        return self.session._writer(self)

    @property
    def latest_cte_name(self) -> str:
        if not self.expression.ctes:
            from_exp = self.expression.args["from_"]
            if from_exp.alias_or_name:
                return from_exp.alias_or_name
            table_alias = from_exp.find(exp.TableAlias)
            if not table_alias:
                raise RuntimeError(
                    f"Could not find an alias name for this expression: {self.expression}"
                )
            return table_alias.alias_or_name
        return self.expression.ctes[-1].alias

    @property
    def pending_join_hints(self):
        return [hint for hint in self.pending_hints if isinstance(hint, exp.JoinHint)]

    @property
    def pending_partition_hints(self):
        return [hint for hint in self.pending_hints if isinstance(hint, exp.Anonymous)]

    @property
    def columns(self) -> t.List[str]:
        expression_display_names = self.expression.copy()
        self._set_display_names(expression_display_names)
        return expression_display_names.named_selects

    @property
    def _columns(self) -> t.List[str]:
        return self.expression.named_selects

    @property
    def na(self) -> NA:
        return self._na(self)

    @property
    def stat(self) -> STAT:
        return self._stat(self)

    @property
    def schema(self) -> StructType:
        """Returns the schema of this :class:`DataFrame` as a :class:`pyspark.sql.types.StructType`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Returns
        -------
        :class:`StructType`

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])

        Retrieve the schema of the current DataFrame.

        >>> df.schema
        StructType([StructField('age', LongType(), True),
                    StructField('name', StringType(), True)])
        """
        from sqlframe.base import types

        try:
            return types.StructType(
                [
                    types.StructField(
                        self.display_name_mapping.get(c.name, c.name),
                        sqlglot_to_spark(
                            exp.DataType.build(c.dataType, dialect=self.session.output_dialect)
                        ),
                    )
                    for c in self._typed_columns
                ]
            )
        except NotImplementedError as e:
            raise NotImplementedError(
                "This engine does not support schema inference likely since it does not have an active connection."
            ) from e

    @property
    def sparkSession(self) -> SESSION:
        return self.session

    def _replace_cte_names_with_hashes(self, expression: exp.Select):
        replacement_mapping = {}
        seen_hashes: t.Dict[str, exp.Identifier] = {}
        cte_indices_to_remove = []

        for i, cte in enumerate(expression.ctes):
            old_name_id = cte.args["alias"].this
            cte_hash = self._create_hash_from_expression(cte.this)

            if cte_hash in seen_hashes:
                # Duplicate CTE found - map its old name to the existing hash
                replacement_mapping[old_name_id] = seen_hashes[cte_hash]
                cte_indices_to_remove.append(i)
            else:
                # New unique CTE - process normally
                new_hashed_id = exp.to_identifier(cte_hash, quoted=old_name_id.args["quoted"])
                seen_hashes[cte_hash] = new_hashed_id
                replacement_mapping[old_name_id] = new_hashed_id

            expression = expression.transform(replace_id_value, replacement_mapping).assert_is(
                exp.Select
            )

        # Remove duplicate CTEs by index in reverse order to avoid index shifting
        for idx in reversed(cte_indices_to_remove):
            del expression.args["with_"].expressions[idx]

        return expression

    def _create_cte_from_expression(
        self,
        expression: exp.Expression,
        branch_id: str,
        sequence_id: str,
        name: t.Optional[str] = None,
        **kwargs,
    ) -> t.Tuple[exp.CTE, str]:
        name = name or self._create_hash_from_expression(expression)
        expression_to_cte = expression.copy()
        expression_to_cte.set("with_", None)
        cte = exp.Select().with_(name, as_=expression_to_cte, **kwargs).ctes[0]
        cte.set("branch_id", branch_id)
        cte.set("sequence_id", sequence_id)
        return cte, name

    def _convert_leaf_to_cte(
        self, sequence_id: t.Optional[str] = None, name: t.Optional[str] = None
    ) -> Self:
        df = self._resolve_pending_hints()
        sequence_id = sequence_id or df.sequence_id
        expression = df.expression.copy()
        # When wrapping a join expression into a CTE, collect the sequence_ids of CTEs
        # in the FROM clause so we can propagate alias mappings to the wrapper CTE.
        has_joins = bool(expression.args.get("joins"))
        from_sequence_ids: t.Set[str] = set()
        if has_joins:
            from_table_names = {
                table.alias_or_name for table in get_tables_from_expression_with_join(expression)
            }
            from_sequence_ids = {
                cte.args["sequence_id"]
                for cte in expression.ctes
                if cte.alias_or_name in from_table_names and "sequence_id" in cte.args
            }
        cte_expression, cte_name = df._create_cte_from_expression(
            expression=expression, branch_id=self.branch_id, sequence_id=sequence_id, name=name
        )
        new_expression = df._add_ctes_to_expression(
            exp.Select(), expression.ctes + [cte_expression]
        )
        sel_columns = df._get_outer_select_columns(cte_expression)
        new_expression = new_expression.from_(cte_name).select(*[x.expression for x in sel_columns])
        # Propagate alias mappings: alias names that pointed to inner FROM-clause CTEs
        # should also point to the wrapper CTE so they resolve correctly in subsequent operations.
        # Only needed when wrapping a join expression, as that's when alias names reference
        # multiple CTEs that get folded into the wrapper.
        if from_sequence_ids:
            for seq_ids in df.session.name_to_sequence_id_mapping.values():
                if any(sid in from_sequence_ids for sid in seq_ids) and sequence_id not in seq_ids:
                    seq_ids.append(sequence_id)
        return df.copy(expression=new_expression, sequence_id=sequence_id)

    def _resolve_pending_hints(self) -> Self:
        df = self.copy()
        if not self.pending_hints:
            return df
        expression = df.expression
        hint_expression = expression.args.get("hint") or exp.Hint(expressions=[])
        for hint in df.pending_partition_hints:
            hint_expression.append("expressions", hint)
            df.pending_hints.remove(hint)

        join_aliases = {
            join_table.alias_or_name
            for join_table in get_tables_from_expression_with_join(expression)
        }
        if join_aliases:
            for hint in df.pending_join_hints:
                for sequence_id_expression in hint.expressions:
                    sequence_id_or_name = sequence_id_expression.alias_or_name
                    sequence_ids_to_match = [sequence_id_or_name]
                    if sequence_id_or_name in df.session.name_to_sequence_id_mapping:
                        sequence_ids_to_match = df.session.name_to_sequence_id_mapping[
                            sequence_id_or_name
                        ]
                    matching_ctes = [
                        cte
                        for cte in reversed(expression.ctes)
                        if cte.args["sequence_id"] in sequence_ids_to_match
                    ]
                    for matching_cte in matching_ctes:
                        if matching_cte.alias_or_name in join_aliases:
                            sequence_id_expression.set("this", matching_cte.args["alias"].this)
                            df.pending_hints.remove(hint)
                            break
                hint_expression.append("expressions", hint)
        if hint_expression.expressions:
            expression.set("hint", hint_expression)
        return df

    def _build_hint_expression(self, hint_name: str, args: t.List[Column]) -> exp.Expression:
        hint_name = hint_name.upper()
        return (
            exp.JoinHint(
                this=hint_name,
                expressions=[exp.to_table(parameter.alias_or_name) for parameter in args],
            )
            if hint_name in JOIN_HINTS
            else exp.Anonymous(
                this=hint_name, expressions=[parameter.expression for parameter in args]
            )
        )

    def _set_operation(self, klass: t.Callable, other: Self, distinct: bool) -> Self:
        other_df = other._convert_leaf_to_cte()
        base_expression = self.expression.copy()
        base_expression = self._add_ctes_to_expression(base_expression, other_df.expression.ctes)
        all_ctes = base_expression.ctes
        other_df.expression.set("with_", None)
        base_expression.set("with_", None)
        operation = klass(this=base_expression, distinct=distinct, expression=other_df.expression)
        operation.set("with_", exp.With(expressions=all_ctes))
        return self.copy(expression=operation)._convert_leaf_to_cte()

    def _add_ctes_to_expression(self, expression: exp.Select, ctes: t.List[exp.CTE]) -> exp.Select:
        expression = expression.copy()
        with_expression = expression.args.get("with_")
        if with_expression:
            existing_ctes = with_expression.expressions
            existing_cte_names = {x.alias_or_name for x in existing_ctes}
            replaced_cte_names = {}
            for cte in ctes:
                if replaced_cte_names:
                    cte = cte.transform(replace_id_value, replaced_cte_names)
                if cte.alias_or_name in existing_cte_names:
                    existing_cte = next(
                        c for c in existing_ctes if c.alias_or_name == cte.alias_or_name
                    )
                    if self._create_hash_from_expression(
                        existing_cte.this
                    ) == self._create_hash_from_expression(cte.this) and existing_cte.args.get(
                        "sequence_id"
                    ) == cte.args.get("sequence_id"):
                        # Same content and same origin: this is a common ancestor CTE shared by
                        # both branches — reuse the existing one instead of creating a duplicate.
                        continue
                    random_filter = exp.Literal.string(uuid.uuid4().hex)
                    # Add unique where filter to ensure that the hash of the CTE is unique
                    cte.set(
                        "this",
                        cte.this.where(
                            exp.EQ(
                                this=random_filter,
                                expression=random_filter,
                            )
                        ),
                    )
                    new_cte_alias = self._create_hash_from_expression(cte.this)
                    replaced_cte_names[cte.args["alias"].this] = maybe_parse(
                        new_cte_alias, dialect=self.session.input_dialect, into=exp.Identifier
                    )
                    cte.set(
                        "alias",
                        maybe_parse(
                            new_cte_alias, dialect=self.session.input_dialect, into=exp.TableAlias
                        ),
                    )
                    existing_cte_names.add(new_cte_alias)
                existing_ctes.append(cte)
        else:
            existing_ctes = ctes
        expression.set("with_", exp.With(expressions=existing_ctes))
        return expression

    @classmethod
    def _get_outer_select_columns(cls, item: exp.Expression) -> t.List[Column]:
        from sqlframe.base.session import _BaseSession

        col = get_func_from_session("col", _BaseSession())

        outer_select = item.find(exp.Select)
        if outer_select:
            return [col(quote_preserving_alias_or_name(x)) for x in outer_select.expressions]
        return []

    def _create_hash_from_expression(self, expression: exp.Expression) -> str:
        from sqlframe.base.session import _BaseSession

        value = expression.sql(dialect=_BaseSession().input_dialect).encode("utf-8")
        hash = f"t{zlib.crc32(value)}"[:9]
        return self.session._normalize_string(hash)

    def _get_select_expressions(
        self,
    ) -> t.List[t.Tuple[t.Union[t.Type[exp.Cache], OutputExpressionContainer], exp.Select]]:
        select_expressions: t.List[
            t.Tuple[t.Union[t.Type[exp.Cache], OutputExpressionContainer], exp.Select]
        ] = []
        main_select_ctes: t.List[exp.CTE] = []
        for cte in self.expression.ctes:
            cache_storage_level = cte.args.get("cache_storage_level")
            if cache_storage_level:
                select_expression = cte.this.copy()
                select_expression.set("with_", exp.With(expressions=copy(main_select_ctes)))
                select_expression.set("cte_alias_name", cte.alias_or_name)
                select_expression.set("cache_storage_level", cache_storage_level)
                select_expressions.append((exp.Cache, select_expression))
            else:
                main_select_ctes.append(cte)
        main_select = self.expression.copy()
        if main_select_ctes:
            main_select.set("with_", exp.With(expressions=main_select_ctes))
        expression_select_pair = (type(self.output_expression_container), main_select)
        select_expressions.append(expression_select_pair)  # type: ignore
        return select_expressions

    def _set_display_names(self, select_expression: exp.Select) -> None:
        for index, column in enumerate(select_expression.expressions):
            column_name = quote_preserving_alias_or_name(column)
            if column_name in self.display_name_mapping:
                display_name_identifier = exp.to_identifier(
                    self.display_name_mapping[column_name], quoted=True
                )
                display_name_identifier._meta = {"case_sensitive": True, **(column._meta or {})}
                select_expression.expressions[index] = exp.alias_(
                    column.unalias(), display_name_identifier, quoted=True
                )

    def _get_expressions(
        self,
        optimize: bool = False,
        openai_config: t.Optional[t.Union[t.Dict[str, t.Any], OpenAIConfig]] = None,
        quote_identifiers: bool = True,
    ) -> t.List[exp.Expression]:
        df = self._resolve_pending_hints()
        select_expressions = df._get_select_expressions()
        output_expressions: t.List[exp.Expression] = []
        replacement_mapping: t.Dict[exp.Identifier, exp.Identifier] = {}
        if openai_config is not None and isinstance(openai_config, dict):
            openai_config = OpenAIConfig.from_dict(t.cast(t.Dict[str, t.Any], openai_config))

        for expression_type, select_expression in select_expressions:
            select_expression = select_expression.transform(
                replace_id_value, replacement_mapping
            ).assert_is(exp.Select)
            df._set_display_names(select_expression)
            if optimize:
                select_expression = t.cast(
                    exp.Select,
                    self.session._optimize(select_expression, quote_identifiers=quote_identifiers),
                )
            elif openai_config:
                qualify(
                    select_expression,
                    dialect=self.session.input_dialect,
                    schema=self.session.catalog._schema,
                )
                pushdown_projections(select_expression, schema=self.session.catalog._schema)

            select_expression = df._replace_cte_names_with_hashes(select_expression)

            expression: exp.Expression
            if expression_type == exp.Cache:
                cache_table_name = df._create_hash_from_expression(select_expression)
                cache_table = exp.to_table(cache_table_name)
                original_alias_name = select_expression.args["cte_alias_name"]

                replacement_mapping[exp.to_identifier(original_alias_name)] = exp.to_identifier(
                    cache_table_name
                )
                self.session.catalog.add_table(
                    cache_table_name,
                    {
                        quote_preserving_alias_or_name(expression): expression.type.sql(
                            dialect=self.session.input_dialect
                        )
                        if expression.type
                        else "UNKNOWN"
                        for expression in select_expression.expressions
                    },
                )

                cache_storage_level = select_expression.args["cache_storage_level"]
                options = [
                    exp.Literal.string("storageLevel"),
                    exp.Literal.string(cache_storage_level),
                ]
                expression = exp.Cache(
                    this=cache_table, expression=select_expression, lazy=True, options=options
                )

                # We will drop the "view" if it exists before running the cache table
                output_expressions.append(exp.Drop(this=cache_table, exists=True, kind="VIEW"))
            elif expression_type == exp.Create:
                expression = df.output_expression_container.copy()
                expression.set("expression", select_expression)
            elif expression_type == exp.Insert:
                expression = df.output_expression_container.copy()
                select_without_ctes = select_expression.copy()
                select_without_ctes.set("with_", None)
                expression.set("expression", select_without_ctes)

                if select_expression.ctes:
                    expression.set("with_", exp.With(expressions=select_expression.ctes))
            elif expression_type == exp.Select:
                expression = select_expression
            else:
                raise ValueError(f"Invalid expression type: {expression_type}")

            output_expressions.append(expression)
        return output_expressions

    @t.overload
    def sql(
        self,
        dialect: DialectType = ...,
        optimize: bool = ...,
        pretty: bool = ...,
        quote_identifiers: bool = ...,
        *,
        as_list: t.Literal[False] = False,
        **kwargs: t.Any,
    ) -> str: ...

    @t.overload
    def sql(
        self,
        dialect: DialectType = ...,
        optimize: bool = ...,
        pretty: bool = ...,
        quote_identifiers: bool = ...,
        *,
        as_list: t.Literal[True],
        **kwargs: t.Any,
    ) -> t.List[str]: ...

    def sql(
        self,
        dialect: DialectType = None,
        optimize: bool = True,
        pretty: bool = True,
        quote_identifiers: bool = True,
        openai_config: t.Optional[t.Union[t.Dict[str, t.Any], OpenAIConfig]] = None,
        as_list: bool = False,
        **kwargs,
    ) -> t.Union[str, t.List[str]]:
        dialect = Dialect.get_or_raise(dialect) if dialect else self.session.output_dialect
        results = []
        for expression in self._get_expressions(
            optimize=optimize, openai_config=openai_config, quote_identifiers=quote_identifiers
        ):
            sql = self.session._to_sql(
                expression,
                dialect=dialect,
                pretty=pretty,
                quote_identifiers=quote_identifiers,
                **kwargs,
            )
            if openai_config:
                assert isinstance(openai_config, OpenAIConfig)
                verify_openai_installed()
                from openai import OpenAI

                client = OpenAI()
                chat_completed = client.chat.completions.create(
                    messages=[
                        {
                            "role": "system",
                            "content": openai_config.get_prompt(dialect),
                        },
                        {
                            "role": "user",
                            "content": sql,
                        },
                    ],
                    model=openai_config.model,
                )
                assert chat_completed.choices[0].message.content is not None
                if openai_config.mode.is_cte_only:
                    cte_replacement_mapping = json.loads(chat_completed.choices[0].message.content)
                    for old_name, new_name in cte_replacement_mapping.items():
                        sql = sql.replace(old_name, new_name)
                else:
                    sql = chat_completed.choices[0].message.content
            results.append(sql)

        if as_list:
            return results
        return ";\n".join(results)

    def copy(self, **kwargs) -> Self:
        kwargs["join_on_uuid"] = str(uuid4())
        # When a new expression is provided, clear the plan so __init__ wraps it in SourceNode
        if "expression" in kwargs:
            kwargs["_plan"] = None
        return self.__class__(**object_to_dict(self, **kwargs))

    def _with_plan(self, plan: PlanNode, **overrides) -> Self:
        """Create a new DataFrame with a plan node, inheriting metadata from self.

        Unlike copy(), this does NOT deep-copy the expression or use object_to_dict.
        It creates a lightweight DataFrame that records the plan node and defers
        expression building to the compiler.
        """
        # Compute effective last_op matching @operation decorator behavior:
        # - If plan.operation is not NO_OP, use it directly
        # - If NO_OP, inherit parent's last_op (with INIT → NO_OP adjustment)
        parent_last_op = self.last_op
        if parent_last_op == Operation.INIT:
            parent_last_op = Operation.NO_OP
        if plan.operation != Operation.NO_OP:
            new_last_op = plan.operation
        else:
            new_last_op = parent_last_op

        return self.__class__(
            session=self.session,
            branch_id=overrides.pop("branch_id", self.branch_id),
            sequence_id=overrides.pop("sequence_id", self.sequence_id),
            known_uuids=overrides.pop("known_uuids", self.known_uuids.copy()),
            last_op=new_last_op,
            pending_hints=overrides.pop("pending_hints", list(self.pending_hints)),
            output_expression_container=self.output_expression_container,
            display_name_mapping=overrides.pop(
                "display_name_mapping", dict(self.display_name_mapping)
            ),
            _plan=plan,
        )

    def select(self, *cols, **kwargs) -> Self:
        if not cols:
            return self
        return self._with_plan(SelectNode(parent=self._plan, cols=cols, kwargs=kwargs))

    def alias(self, name: str, **kwargs) -> Self:
        from sqlframe.base.column import Column

        new_sequence_id = self.session._random_sequence_id
        # Update pending join hints eagerly (they reference the old sequence_id)
        pending_join_hints = list(self.pending_join_hints)
        for join_hint in pending_join_hints:
            for expression in join_hint.expressions:
                if expression.alias_or_name == self.sequence_id:
                    expression.set("this", Column.ensure_col(new_sequence_id).expression)
        # Register the alias mapping eagerly (needed for column resolution)
        self.session._add_alias_to_mapping(name, new_sequence_id)
        return self._with_plan(
            AliasNode(parent=self._plan, name=name, new_sequence_id=new_sequence_id),
            sequence_id=new_sequence_id,
        )

    def where(self, column: t.Union[Column, str, bool], **kwargs) -> Self:
        return self._with_plan(FilterNode(parent=self._plan, condition=column))

    filter = where

    def groupBy(self, *cols, **kwargs) -> GROUP_DATA:
        if cols and isinstance(cols[0], list):
            cols = cols[0]
        return self._group_data(self, list(cols), self.last_op)

    groupby = groupBy

    def agg(self, *exprs, **kwargs) -> Self:
        from sqlframe.base.plan import GroupAggNode

        return self._with_plan(
            GroupAggNode(
                parent=GroupByNode(parent=self._plan, cols=()),
                group_by_cols=[],
                agg_exprs=tuple(exprs),
                df_agg=True,
            )
        )

    def crossJoin(self, other: DF) -> Self:
        """Returns the cartesian product with another :class:`DataFrame`.

        .. versionadded:: 2.1.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other : :class:`DataFrame`
            Right side of the cartesian product.

        Returns
        -------
        :class:`DataFrame`
            Joined DataFrame.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> df2 = spark.createDataFrame(
        ...     [Row(height=80, name="Tom"), Row(height=85, name="Bob")])
        >>> df.crossJoin(df2.select("height")).select("age", "name", "height").show()
        +---+-----+------+
        |age| name|height|
        +---+-----+------+
        | 14|  Tom|    80|
        | 14|  Tom|    85|
        | 23|Alice|    80|
        | 23|Alice|    85|
        | 16|  Bob|    80|
        | 16|  Bob|    85|
        +---+-----+------+
        """
        return self.join(other, how="cross")

    def _handle_self_join(self, other_df: DF, join_columns: t.List[Column]):
        # If the two dataframes being joined come from the same branch, we then check if they have any columns that
        # were created using the "branch_id" (df["column_name"]). If so, we know that we need to differentiate
        # the two columns since they would end up with the same table name. We do this by checking for the unique
        # uuids in the other df and finding columns that have metadata on them that match the uuids. If so, we know
        # it comes from the other df and we change the table name to the other df's table name.
        # See `test_self_join` for an example of this.
        if self.branch_id == other_df.branch_id:
            other_df_unique_uuids = other_df.known_uuids - self.known_uuids
            for col in join_columns:
                for col_expr in col.expression.find_all(exp.Column):
                    if (
                        "join_on_uuid" in col_expr.meta
                        and col_expr.meta["join_on_uuid"] in other_df_unique_uuids
                    ):
                        col_expr.set("table", exp.to_identifier(other_df.latest_cte_name))

    @staticmethod
    def _handle_join_column_names_only(
        join_columns: t.List[Column],
        join_expression: exp.Select,
        other_df: DF,
        table_names: t.List[str],
    ):
        potential_ctes = [
            cte
            for cte in join_expression.ctes
            if cte.alias_or_name in table_names and cte.alias_or_name != other_df.latest_cte_name
        ]
        # Determine the table to reference for the left side of the join by checking each of the left side
        # tables and see if they have the column being referenced.
        join_column_pairs = []
        for join_column in join_columns:
            num_matching_ctes = 0
            for cte in potential_ctes:
                if join_column.alias_or_name in cte.this.named_selects or any(
                    isinstance(sel, exp.Star) for sel in cte.this.expressions
                ):
                    left_column = join_column.copy().set_table_name(cte.alias_or_name)
                    right_column = join_column.copy().set_table_name(other_df.latest_cte_name)
                    join_column_pairs.append((left_column, right_column))
                    num_matching_ctes += 1
                    # We only want to match one table to the column and that should be matched left -> right
                    # so we break after the first match
                    break
            if num_matching_ctes == 0:
                raise ValueError(
                    f"Column `{join_column.alias_or_name}` does not exist in any of the tables."
                )
        join_clause = functools.reduce(
            lambda x, y: x & y,
            [left_column == right_column for left_column, right_column in join_column_pairs],
        )
        return join_column_pairs, join_clause

    def _normalize_join_clause(
        self,
        join_columns: t.List[Column],
        join_expression: t.Optional[exp.Select],
        *,
        remove_identifier_if_possible: bool = True,
    ) -> Column:
        from sqlframe.base.normalize import ensure_and_normalize_cols

        join_columns = ensure_and_normalize_cols(
            self.session,
            join_expression or self.expression,
            join_columns,
            remove_identifier_if_possible=remove_identifier_if_possible,
        )
        if len(join_columns) > 1:
            join_columns = [functools.reduce(lambda x, y: x & y, join_columns)]
        join_clause = join_columns[0]
        return join_clause

    def join(
        self,
        other: Self,
        on: t.Optional[t.Union[str, t.List[str], Column, t.List[Column]]] = None,
        how: str = "inner",
        **kwargs,
    ) -> Self:
        if (on is None) and ("cross" not in how):
            logger.warning("Got no value for on. This appears to change the join to a cross join.")
            how = "cross"
        if (on is not None) and ("cross" in how):
            # Not a lot of doc, but Spark handles cross with predicate as an inner join
            # https://learn.microsoft.com/en-us/dotnet/api/microsoft.spark.sql.dataframe.join
            logger.warning("Got cross join with an 'on' value. This will result in an inner join.")
            how = "inner"

        return self._with_plan(
            JoinNode(
                parent=self._plan,
                other=other._plan,
                on=on,
                how=how,
                left_branch_id=self.branch_id,
                right_branch_id=other.branch_id,
                left_known_uuids=self.known_uuids.copy(),
                right_known_uuids=other.known_uuids.copy(),
                left_pending_hints=list(self.pending_hints),
                right_pending_hints=list(other.pending_hints),
            ),
            pending_hints=[],
        )

    def orderBy(
        self,
        *cols: t.Union[str, Column],
        ascending: t.Optional[t.Union[t.Any, t.List[t.Any]]] = None,
    ) -> Self:
        return self._with_plan(SortNode(parent=self._plan, cols=cols, ascending=ascending))

    sort = orderBy

    def union(self, other: Self) -> Self:
        return self._with_plan(
            SetOpNode(
                parent=self._plan,
                other=other._plan,
                op_class=exp.Union,
                distinct=False,
            )
        )

    unionAll = union

    def unionByName(self, other: Self, allowMissingColumns: bool = False) -> Self:
        return self._with_plan(
            UnionByNameNode(
                parent=self._plan,
                other=other._plan,
                allow_missing_columns=allowMissingColumns,
            )
        )

    def intersect(self, other: Self) -> Self:
        return self._with_plan(
            SetOpNode(
                parent=self._plan,
                other=other._plan,
                op_class=exp.Intersect,
                distinct=True,
            )
        )

    def intersectAll(self, other: Self) -> Self:
        return self._with_plan(
            SetOpNode(
                parent=self._plan,
                other=other._plan,
                op_class=exp.Intersect,
                distinct=False,
            )
        )

    def exceptAll(self, other: Self) -> Self:
        return self._with_plan(
            SetOpNode(
                parent=self._plan,
                other=other._plan,
                op_class=exp.Except,
                distinct=False,
            )
        )

    def distinct(self) -> Self:
        return self._with_plan(DistinctNode(parent=self._plan))

    def dropDuplicates(self, subset: t.Optional[t.List[str]] = None):
        return self._with_plan(DropDuplicatesNode(parent=self._plan, subset=subset))

    drop_duplicates = dropDuplicates

    def dropna(
        self,
        how: str = "any",
        thresh: t.Optional[int] = None,
        subset: t.Optional[t.Union[str, t.Tuple[str, ...], t.List[str]]] = None,
    ) -> Self:
        return self._with_plan(
            DropNaNode(
                parent=self._plan,
                how=how,
                thresh=thresh,
                subset=subset,
            )
        )

    def _get_explain_plan_rows(self) -> t.List[Row]:
        sql_queries = self.sql(
            pretty=False, optimize=False, as_list=True, dialect=self.session.execution_dialect
        )
        if len(sql_queries) > 1:
            raise ValueError("Cannot explain a DataFrame with multiple queries")
        sql_query = " ".join([self._EXPLAIN_PREFIX, sql_queries[0]])
        results = self.session._collect(sql_query)
        if len(results) != 1:
            raise ValueError("Got more than one result from explain query")
        return results

    def explain(
        self, extended: t.Optional[t.Union[bool, str]] = None, mode: t.Optional[str] = None
    ) -> None:
        """Prints the (logical and physical) plans to the console for debugging purposes.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        extended : bool, optional
            default ``False``. If ``False``, prints only the physical plan.
            When this is a string without specifying the ``mode``, it works as the mode is
            specified.
        mode : str, optional
            specifies the expected output format of plans.

            * ``simple``: Print only a physical plan.
            * ``extended``: Print both logical and physical plans.
            * ``codegen``: Print a physical plan and generated codes if they are available.
            * ``cost``: Print a logical plan and statistics if they are available.
            * ``formatted``: Split explain output into two sections: a physical plan outline \
                and node details.

            .. versionchanged:: 3.0.0
               Added optional argument `mode` to specify the expected output format of plans.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])

        Print out the physical plan only (default).

        >>> df.explain()  # doctest: +SKIP
        == Physical Plan ==
        *(1) Scan ExistingRDD[age...,name...]

        Print out all of the parsed, analyzed, optimized and physical plans.

        >>> df.explain(True)
        == Parsed Logical Plan ==
        ...
        == Analyzed Logical Plan ==
        ...
        == Optimized Logical Plan ==
        ...
        == Physical Plan ==
        ...

        Print out the plans with two sections: a physical plan outline and node details

        >>> df.explain(mode="formatted")  # doctest: +SKIP
        == Physical Plan ==
        * Scan ExistingRDD (...)
        (1) Scan ExistingRDD [codegen id : ...]
        Output [2]: [age..., name...]
        ...

        Print a logical plan and statistics if they are available.

        >>> df.explain("cost")
        == Optimized Logical Plan ==
        ...Statistics...
        ...
        """
        results = self._get_explain_plan_rows()
        print(results[0][0])

    def fillna(
        self,
        value: t.Union[PrimitiveType, t.Dict[str, PrimitiveType]],
        subset: t.Optional[t.Union[str, t.Tuple[str, ...], t.List[str]]] = None,
    ) -> Self:
        return self._with_plan(
            FillNaNode(
                parent=self._plan,
                value=value,
                subset=subset,
            )
        )

    def replace(
        self,
        to_replace: t.Union[bool, int, float, str, t.List, t.Dict],
        value: t.Optional[t.Union[bool, int, float, str, t.List]] = None,
        subset: t.Optional[t.Collection[ColumnOrName] | ColumnOrName] = None,
    ) -> Self:
        return self._with_plan(
            ReplaceNode(
                parent=self._plan,
                to_replace=to_replace,
                value=value,
                subset=subset,
            )
        )

    def transform(self, func: t.Callable[..., DF], *args: t.Any, **kwargs: t.Any) -> Self:
        """Returns a new :class:`DataFrame`. Concise syntax for chaining custom transformations.

        .. versionadded:: 3.0.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        func : function
            a function that takes and returns a :class:`DataFrame`.
        *args
            Positional arguments to pass to func.

            .. versionadded:: 3.3.0
        **kwargs
            Keyword arguments to pass to func.

            .. versionadded:: 3.3.0

        Returns
        -------
        :class:`DataFrame`
            Transformed DataFrame.

        Examples
        --------
        >>> from pyspark.sql.functions import col
        >>> df = spark.createDataFrame([(1, 1.0), (2, 2.0)], ["int", "float"])
        >>> def cast_all_to_int(input_df):
        ...     return input_df.select([col(col_name).cast("int") for col_name in input_df.columns])
        ...
        >>> def sort_columns_asc(input_df):
        ...     return input_df.select(*sorted(input_df.columns))
        ...
        >>> df.transform(cast_all_to_int).transform(sort_columns_asc).show()
        +-----+---+
        |float|int|
        +-----+---+
        |    1|  1|
        |    2|  2|
        +-----+---+

        >>> def add_n(input_df, n):
        ...     return input_df.select([(col(col_name) + n).alias(col_name)
        ...                             for col_name in input_df.columns])
        >>> df.transform(add_n, 1).transform(add_n, n=10).show()
        +---+-----+
        |int|float|
        +---+-----+
        | 12| 12.0|
        | 13| 13.0|
        +---+-----+
        """
        return func(self, *args, **kwargs)  # type: ignore

    def withColumn(self, colName: str, col: Column) -> Self:
        return self.withColumns({colName: col})

    def _rename_columns(self, cols_map: t.Dict[str, str], raise_on_missing: bool) -> Self:
        return self._with_plan(
            RenameColumnsNode(
                parent=self._plan,
                cols_map=cols_map,
                raise_on_missing=raise_on_missing,
            )
        )

    def withColumnRenamed(self, existing: str, new: str) -> Self:
        return self._rename_columns({existing: new}, raise_on_missing=True)

    def withColumnsRenamed(self, colsMap: t.Dict[str, str]) -> Self:
        """
        Returns a new :class:`DataFrame` by renaming multiple columns. If a non-existing column is
        provided, it will be silently ignored.

        .. versionadded:: 3.5.0

        Parameters
        ----------
        colsMap : dict
            a dict of column name and new column name.

        Returns
        -------
        :class:`DataFrame`
            DataFrame with renamed columns.

        Examples
        --------
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
        >>> df.withColumnsRenamed({"age": "years", "name": "firstName"}).show()
        +-----+---------+
        |years|firstName|
        +-----+---------+
        |    2|    Alice|
        |    5|      Bob|
        +-----+---------+
        """
        return self._rename_columns(colsMap, raise_on_missing=False)

    def withColumns(self, *colsMap: t.Dict[str, Column]) -> Self:
        """
        Returns a new :class:`DataFrame` by adding multiple columns or replacing the
        existing columns that have the same names.

        The colsMap is a map of column name and column, the column must only refer to attributes
        supplied by this Dataset. It is an error to add columns that refer to some other Dataset.

        .. versionadded:: 3.3.0
           Added support for multiple columns adding

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        colsMap : dict
            a dict of column name and :class:`Column`. Currently, only a single map is supported.

        Returns
        -------
        :class:`DataFrame`
            DataFrame with new or replaced columns.

        Examples
        --------
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
        >>> df.withColumns({'age2': df.age + 2, 'age3': df.age + 3}).show()
        +---+-----+----+----+
        |age| name|age2|age3|
        +---+-----+----+----+
        |  2|Alice|   4|   5|
        |  5|  Bob|   7|   8|
        +---+-----+----+----+
        """
        return self._with_plan(WithColumnsNode(parent=self._plan, cols_map=colsMap))

    def drop(self, *cols: t.Union[str, Column]) -> Self:
        return self._with_plan(DropNode(parent=self._plan, cols=cols))

    def limit(self, num: int) -> Self:
        return self._with_plan(LimitNode(parent=self._plan, num=num))

    def toDF(self, *cols: str) -> Self:
        """Returns a new :class:`DataFrame` that with new specified column names

        .. versionadded:: 1.6.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        *cols : tuple
            a tuple of string new column name. The length of the
            list needs to be the same as the number of columns in the initial
            :class:`DataFrame`

        Returns
        -------
        :class:`DataFrame`
            DataFrame with new column names.

        Examples
        --------
        >>> df = spark.createDataFrame([(14, "Tom"), (23, "Alice"),
        ...     (16, "Bob")], ["age", "name"])
        >>> df.toDF('f1', 'f2').show()
        +---+-----+
        | f1|   f2|
        +---+-----+
        | 14|  Tom|
        | 23|Alice|
        | 16|  Bob|
        +---+-----+
        """
        return self._with_plan(ToDFNode(parent=self._plan, col_names=cols))

    def hint(self, name: str, *parameters: t.Optional[t.Union[str, int]]) -> Self:
        from sqlframe.base.column import Column
        from sqlframe.base.normalize import ensure_list_of_columns

        parameter_list = ensure_list(parameters)
        parameter_columns = (
            ensure_list_of_columns(parameter_list)
            if parameters
            else Column.ensure_cols([self.sequence_id])
        )
        hint_expression = self._build_hint_expression(name, parameter_columns)
        return self._with_plan(HintNode(parent=self._plan, hint_expression=hint_expression))

    def repartition(self, numPartitions: t.Union[int, ColumnOrName], *cols: ColumnOrName) -> Self:
        from sqlframe.base.normalize import ensure_list_of_columns

        num_partition_cols = ensure_list_of_columns(numPartitions)
        columns = ensure_list_of_columns(cols)
        args = num_partition_cols + columns
        hint_expression = self._build_hint_expression("repartition", args)
        return self._with_plan(HintNode(parent=self._plan, hint_expression=hint_expression))

    def coalesce(self, numPartitions: int) -> Self:
        lit = get_func_from_session("lit")
        num_partitions = lit(numPartitions)
        hint_expression = self._build_hint_expression("coalesce", [num_partitions])
        return self._with_plan(HintNode(parent=self._plan, hint_expression=hint_expression))

    def cache(self) -> Self:
        return self._with_plan(CacheNode(parent=self._plan, storage_level="MEMORY_AND_DISK"))

    def persist(self, storageLevel: StorageLevel = "MEMORY_AND_DISK_SER") -> Self:
        """
        Storage Level Options: https://spark.apache.org/docs/3.0.0-preview/sql-ref-syntax-aux-cache-cache-table.html
        """
        return self._with_plan(CacheNode(parent=self._plan, storage_level=storageLevel))

    @t.overload
    def cube(self, *cols: ColumnOrName) -> GROUP_DATA: ...

    @t.overload
    def cube(self, __cols: t.Union[t.List[Column], t.List[str]]) -> GROUP_DATA: ...

    def cube(self, *cols: ColumnOrName) -> GROUP_DATA:
        """
        Create a multi-dimensional cube for the current :class:`DataFrame` using
        the specified columns, so we can run aggregations on them.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        cols : list, str or :class:`Column`
            columns to create cube by.
            Each element should be a column name (string) or an expression (:class:`Column`)
            or list of them.

        Returns
        -------
        :class:`GroupedData`
            Cube of the data by given columns.

        Examples
        --------
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
        >>> df.cube("name", df.age).count().orderBy("name", "age").show()
        +-----+----+-----+
        | name| age|count|
        +-----+----+-----+
        | NULL|NULL|    2|
        | NULL|   2|    1|
        | NULL|   5|    1|
        |Alice|NULL|    1|
        |Alice|   2|    1|
        |  Bob|NULL|    1|
        |  Bob|   5|    1|
        +-----+----+-----+
        """

        from sqlframe.base.normalize import ensure_list_of_columns

        columns = ensure_list_of_columns(cols)
        return self._group_data(self, columns, self.last_op, is_cube=True)

    def unpivot(
        self,
        ids: t.Union[ColumnOrName, t.Collection[ColumnOrName]],
        values: t.Optional[t.Union[ColumnOrName, t.Collection[ColumnOrName]]],
        variableColumnName: str,
        valueColumnName: str,
    ) -> Self:
        """
        Unpivot a DataFrame from wide format to long format, optionally leaving
        identifier columns set. This is the reverse to `groupBy(...).pivot(...).agg(...)`,
        except for the aggregation, which cannot be reversed.

        This function is useful to massage a DataFrame into a format where some
        columns are identifier columns ("ids"), while all other columns ("values")
        are "unpivoted" to the rows, leaving just two non-id columns, named as given
        by `variableColumnName` and `valueColumnName`.

        When no "id" columns are given, the unpivoted DataFrame consists of only the
        "variable" and "value" columns.

        The `values` columns must not be empty so at least one value must be given to be unpivoted.
        When `values` is `None`, all non-id columns will be unpivoted.

        All "value" columns must share a least common data type. Unless they are the same data type,
        all "value" columns are cast to the nearest common data type. For instance, types
        `IntegerType` and `LongType` are cast to `LongType`, while `IntegerType` and `StringType`
        do not have a common data type and `unpivot` fails.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        ids : str, Column, tuple, list
            Column(s) to use as identifiers. Can be a single column or column name,
            or a list or tuple for multiple columns.
        values : str, Column, tuple, list, optional
            Column(s) to unpivot. Can be a single column or column name, or a list or tuple
            for multiple columns. If specified, must not be empty. If not specified, uses all
            columns that are not set as `ids`.
        variableColumnName : str
            Name of the variable column.
        valueColumnName : str
            Name of the value column.

        Returns
        -------
        :class:`DataFrame`
            Unpivoted DataFrame.

        Notes
        -----
        Supports Spark Connect.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...     [(1, 11, 1.1), (2, 12, 1.2)],
        ...     ["id", "int", "double"],
        ... )
        >>> df.show()
        +---+---+------+
        | id|int|double|
        +---+---+------+
        |  1| 11|   1.1|
        |  2| 12|   1.2|
        +---+---+------+

        >>> df.unpivot("id", ["int", "double"], "var", "val").show()
        +---+------+----+
        | id|   var| val|
        +---+------+----+
        |  1|   int|11.0|
        |  1|double| 1.1|
        |  2|   int|12.0|
        |  2|double| 1.2|
        +---+------+----+

        See Also
        --------
        DataFrame.melt
        """
        return self._with_plan(
            UnpivotNode(
                parent=self._plan,
                ids=ids,
                values=values,
                variable_column_name=variableColumnName,
                value_column_name=valueColumnName,
            )
        )

    def collect(self) -> t.List[Row]:
        return self._collect()

    def _collect(self, **kwargs) -> t.List[Row]:
        return self.session._collect(self._get_expressions(optimize=False), **kwargs)

    @t.overload
    def head(self) -> t.Optional[Row]: ...

    @t.overload
    def head(self, n: int) -> t.List[Row]: ...

    def head(self, n: t.Optional[int] = None) -> t.Union[t.Optional[Row], t.List[Row]]:
        df = self.limit(n or 1)
        collected = df.collect()
        if n is None:
            return seq_get(collected, 0)
        return collected

    def first(self) -> t.Optional[Row]:
        return self.head()

    def show(
        self, n: int = 20, truncate: t.Optional[t.Union[bool, int]] = None, vertical: bool = False
    ):
        if vertical:
            raise NotImplementedError("Vertical show is not yet supported")
        if truncate:
            logger.warning("Truncate is ignored so full results will be displayed")
        # Make sure that the limit we add doesn't affect the results
        df = self._convert_leaf_to_cte()
        result = df.limit(n).collect()
        table = PrettyTable()
        if row := seq_get(result, 0):
            table.field_names = row._unique_field_names
            for row in result:
                table.add_row(list(row))
        print(table)

    def printSchema(self, level: t.Optional[int] = None) -> None:
        def print_schema(
            column_name: str, column_type: exp.DataType, nullable: bool, current_level: int
        ):
            if level and current_level >= level:
                return
            if current_level > 0:
                print(" |   " * current_level, end="")
            print(
                f" |-- {column_name}: {column_type.sql(self.session.output_dialect).lower()} (nullable = {str(nullable).lower()})"
            )
            if column_type.this in (exp.DataType.Type.STRUCT, exp.DataType.Type.OBJECT):
                for column_def in column_type.expressions:
                    print_schema(column_def.name, column_def.args["kind"], True, current_level + 1)
            if column_type.this == exp.DataType.Type.ARRAY:
                for data_type in column_type.expressions:
                    print_schema("element", data_type, True, current_level + 1)
            if column_type.this == exp.DataType.Type.MAP:
                print_schema("key", column_type.expressions[0], True, current_level + 1)
                print_schema("value", column_type.expressions[1], True, current_level + 1)

        print("root")
        for column in self._typed_columns:
            print_schema(
                self.display_name_mapping.get(column.name, column.name),
                exp.DataType.build(column.dataType, dialect=self.session.output_dialect),
                column.nullable,
                0,
            )

    def lineage(self, col: ColumnOrName, optimize: bool = True) -> sqlglot_lineage.Node:
        from sqlframe.base.normalize import ensure_and_normalize_col

        return sqlglot_lineage.lineage(
            column=ensure_and_normalize_col(self.session, self.expression, col).alias_or_name,
            sql=self._get_expressions(optimize=optimize)[0],
            schema=self.session.catalog._schema,
        )

    def toPandas(self) -> pd.DataFrame:
        return self.session._fetchdf(self._get_expressions(optimize=False))

    def createOrReplaceTempView(self, name: str) -> None:
        name = normalize_string(name, from_dialect="input")
        df = self.copy()._convert_leaf_to_cte()
        self.session.temp_views[name] = df
        self.session.catalog.add_table(
            name, [x.alias_or_name for x in self._get_outer_select_columns(df.expression)]
        )

    def count(self) -> int:
        if not self.session._has_connection:
            raise RuntimeError("Cannot count without a connection")

        df = self._convert_leaf_to_cte()
        df = self.copy(expression=df.expression.select("count(*)", append=False))
        return df.collect()[0][0]

    def createGlobalTempView(self, name: str) -> None:
        raise NotImplementedError("Global temp views are not yet supported")

    def isEmpty(self) -> bool:
        from sqlframe.base import functions as F

        return not bool(self.select(F.lit(True)).head())

    """
    Stat Functions
    """

    @t.overload
    def approxQuantile(
        self,
        col: str,
        probabilities: t.Union[t.List[float], t.Tuple[float]],
        relativeError: float,
    ) -> t.List[float]: ...

    @t.overload
    def approxQuantile(
        self,
        col: t.Union[t.List[str], t.Tuple[str]],
        probabilities: t.Union[t.List[float], t.Tuple[float]],
        relativeError: float,
    ) -> t.List[t.List[float]]: ...

    def approxQuantile(
        self,
        col: t.Union[str, t.List[str], t.Tuple[str]],
        probabilities: t.Union[t.List[float], t.Tuple[float]],
        relativeError: float,
    ) -> t.Union[t.List[float], t.List[t.List[float]]]:
        """
        Calculates the approximate quantiles of numerical columns of a
        :class:`DataFrame`.

        The result of this algorithm has the following deterministic bound:
        If the :class:`DataFrame` has N elements and if we request the quantile at
        probability `p` up to error `err`, then the algorithm will return
        a sample `x` from the :class:`DataFrame` so that the *exact* rank of `x` is
        close to (p * N). More precisely,

          floor((p - err) * N) <= rank(x) <= ceil((p + err) * N).

        This method implements a variation of the Greenwald-Khanna
        algorithm (with some speed optimizations). The algorithm was first
        present in [[https://doi.org/10.1145/375663.375670
        Space-efficient Online Computation of Quantile Summaries]]
        by Greenwald and Khanna.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        col: str, tuple or list
            Can be a single column name, or a list of names for multiple columns.

            .. versionchanged:: 2.2.0
               Added support for multiple columns.
        probabilities : list or tuple
            a list of quantile probabilities
            Each number must belong to [0, 1].
            For example 0 is the minimum, 0.5 is the median, 1 is the maximum.
        relativeError : float
            The relative target precision to achieve
            (>= 0). If set to zero, the exact quantiles are computed, which
            could be very expensive. Note that values greater than 1 are
            accepted but gives the same result as 1.

        Returns
        -------
        list
            the approximate quantiles at the given probabilities.

            * If the input `col` is a string, the output is a list of floats.

            * If the input `col` is a list or tuple of strings, the output is also a
                list, but each element in it is a list of floats, i.e., the output
                is a list of list of floats.

        Notes
        -----
        Null values will be ignored in numerical columns before calculation.
        For columns only containing null values, an empty list is returned.
        """

        percentile_approx = get_func_from_session("percentile_approx")
        col_func = get_func_from_session("col")

        accuracy = 1.0 / relativeError if relativeError > 0.0 else 10000

        df = self.select(
            *[
                percentile_approx(col_func(x), probabilities, accuracy).alias(f"val_{i}")
                for i, x in enumerate(ensure_list(col))
            ]
        )
        rows = df.collect()
        return [[float(y) for y in x] for row in rows for x in row.asDict().values()]

    def corr(self, col1: str, col2: str, method: t.Optional[str] = None) -> float:
        """
        Calculates the correlation of two columns of a :class:`DataFrame` as a double value.
        Currently only supports the Pearson Correlation Coefficient.
        :func:`DataFrame.corr` and :func:`DataFrameStatFunctions.corr` are aliases of each other.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        col1 : str
            The name of the first column
        col2 : str
            The name of the second column
        method : str, optional
            The correlation method. Currently only supports "pearson"

        Returns
        -------
        float
            Pearson Correlation Coefficient of two columns.

        Examples
        --------
        >>> df = spark.createDataFrame([(1, 12), (10, 1), (19, 8)], ["c1", "c2"])
        >>> df.corr("c1", "c2")
        -0.3592106040535498
        >>> df = spark.createDataFrame([(11, 12), (10, 11), (9, 10)], ["small", "bigger"])
        >>> df.corr("small", "bigger")
        1.0
        """
        if method != "pearson":
            raise ValueError(f"Currently only the Pearson Correlation Coefficient is supported")

        corr = get_func_from_session("corr")
        col_func = get_func_from_session("col")

        return self.select(corr(col_func(col1), col_func(col2))).collect()[0][0]

    def cov(self, col1: str, col2: str) -> float:
        """
        Calculate the sample covariance for the given columns, specified by their names, as a
        double value. :func:`DataFrame.cov` and :func:`DataFrameStatFunctions.cov` are aliases.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        col1 : str
            The name of the first column
        col2 : str
            The name of the second column

        Returns
        -------
        float
            Covariance of two columns.

        Examples
        --------
        >>> df = spark.createDataFrame([(1, 12), (10, 1), (19, 8)], ["c1", "c2"])
        >>> df.cov("c1", "c2")
        -18.0
        >>> df = spark.createDataFrame([(11, 12), (10, 11), (9, 10)], ["small", "bigger"])
        >>> df.cov("small", "bigger")
        1.0

        """
        covar_samp = get_func_from_session("covar_samp")
        col_func = get_func_from_session("col")

        return self.select(covar_samp(col_func(col1), col_func(col2))).collect()[0][0]

    @t.overload
    def toArrow(self) -> ArrowTable: ...

    @t.overload
    def toArrow(self, batch_size: int) -> RecordBatchReader: ...

    def toArrow(self, batch_size: t.Optional[int] = None) -> t.Union[ArrowTable, RecordBatchReader]:
        """
        `batch_size` and `RecordBatchReader` are not part of the PySpark API
        """
        raise NotImplementedError("Arrow conversion is not supported by this engine")
