# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import typing as t

from sqlglot import expressions as exp
from sqlglot.helper import ensure_list

from sqlframe.base.column import Column
from sqlframe.base.util import get_tables_from_expression_with_join

if t.TYPE_CHECKING:
    from sqlframe.base.dataframe import SESSION

    NORMALIZE_INPUT = t.TypeVar("NORMALIZE_INPUT", bound=t.Union[str, exp.Expression, Column])


def normalize(session: SESSION, expression_context: exp.Select, expr: t.List[NORMALIZE_INPUT]):
    expr = ensure_list(expr)
    expressions = _ensure_expressions(expr)
    for expression in expressions:
        identifiers = expression.find_all(exp.Identifier)
        for identifier in identifiers:
            identifier.transform(session.input_dialect.normalize_identifier)
            replace_alias_name_with_cte_name(session, expression_context, identifier)
            replace_branch_and_sequence_ids_with_cte_name(session, expression_context, identifier)


def replace_alias_name_with_cte_name(
    session: SESSION, expression_context: exp.Select, id: exp.Identifier
):
    normalized_id = session._normalize_string(id.alias_or_name)
    if normalized_id in session.name_to_sequence_id_mapping:
        for cte in reversed(expression_context.ctes):
            if cte.args["sequence_id"] in session.name_to_sequence_id_mapping[normalized_id]:
                _set_alias_name(id, cte.alias_or_name)
                break


def replace_branch_and_sequence_ids_with_cte_name(
    session: SESSION, expression_context: exp.Select, id: exp.Identifier
):
    normalized_id = session._normalize_string(id.alias_or_name)
    if normalized_id in session.known_ids:
        # Check if we have a join and if both the tables in that join share a common branch id
        # If so we need to have this reference the left table by default unless the id is a sequence
        # id then it keeps that reference. This handles the weird edge case in spark that shouldn't
        # be common in practice
        if expression_context.args.get("joins") and normalized_id in session.known_branch_ids:
            join_table_aliases = [
                x.alias_or_name for x in get_tables_from_expression_with_join(expression_context)
            ]
            ctes_in_join = [
                cte for cte in expression_context.ctes if cte.alias_or_name in join_table_aliases
            ]
            if ctes_in_join[0].args["branch_id"] == ctes_in_join[1].args["branch_id"]:
                assert len(ctes_in_join) == 2
                _set_alias_name(id, ctes_in_join[0].alias_or_name)
                return

        for cte in reversed(expression_context.ctes):
            if normalized_id in (cte.args["branch_id"], cte.args["sequence_id"]):
                _set_alias_name(id, cte.alias_or_name)
                return


def normalize_dict(session: SESSION, data: t.Dict) -> t.Dict:
    if isinstance(data, dict):
        return {session._normalize_string(k): normalize_dict(session, v) for k, v in data.items()}
    elif isinstance(data, list):
        return [normalize_dict(session, v) for v in data]
    else:
        return data


def _set_alias_name(id: exp.Identifier, name: str):
    id.set("this", name)
    id.set("quoted", False)


def _ensure_expressions(values: t.List[NORMALIZE_INPUT]) -> t.List[exp.Expression]:
    results = []
    for value in values:
        if isinstance(value, str):
            results.append(Column.ensure_col(value).expression)
        elif isinstance(value, Column):
            results.append(value.expression)
        elif isinstance(value, exp.Expression):
            results.append(value)
        else:
            raise ValueError(f"Got an invalid type to normalize: {type(value)}")
    return results
