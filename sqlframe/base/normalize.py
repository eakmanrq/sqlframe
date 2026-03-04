# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import typing as t

from sqlglot import expressions as exp
from sqlglot.helper import ensure_list, flatten, seq_get

from sqlframe.base.column import Column
from sqlframe.base.util import (
    get_tables_from_expression_with_join,
    quote_preserving_alias_or_name,
)

if t.TYPE_CHECKING:
    from sqlframe.base.dataframe import SESSION

    NORMALIZE_INPUT = t.TypeVar("NORMALIZE_INPUT", bound=t.Union[str, exp.Expression, Column])


def normalize(
    session: SESSION,
    expression_context: exp.Select,
    expr: t.List[NORMALIZE_INPUT],
    *,
    remove_identifier_if_possible: bool = True,
):
    expr = ensure_list(expr)
    expressions = _ensure_expressions(expr)
    for expression in expressions:
        identifiers = expression.find_all(exp.Identifier)
        for identifier in identifiers:
            identifier.transform(session.input_dialect.normalize_identifier)
            replace_alias_name_with_cte_name(
                session,
                expression_context,
                identifier,
                remove_identifier_if_possible=remove_identifier_if_possible,
            )
            replace_branch_and_sequence_ids_with_cte_name(
                session,
                expression_context,
                identifier,
                remove_identifier_if_possible=remove_identifier_if_possible,
            )


def replace_alias_name_with_cte_name(
    session: SESSION,
    expression_context: exp.Select,
    id: exp.Identifier,
    *,
    remove_identifier_if_possible: bool,
):
    normalized_id = session._normalize_string(id.alias_or_name)
    if normalized_id in session.name_to_sequence_id_mapping:
        # Get CTEs that are referenced in the FROM clause
        referenced_cte_names = get_cte_names_from_from_clause(expression_context)

        # Filter CTEs to only include those defined and referenced by the FROM clause
        filtered_ctes = [
            cte
            for cte in reversed(expression_context.ctes)
            if cte.alias_or_name in referenced_cte_names
        ]

        for cte in filtered_ctes:
            if cte.args["sequence_id"] in session.name_to_sequence_id_mapping[normalized_id]:
                _set_alias_name(id, cte.alias_or_name)
                break
        else:
            # Fallback: If not found in filtered CTEs, search through ALL CTEs unfiltered
            for cte in reversed(expression_context.ctes):
                if cte.args["sequence_id"] in session.name_to_sequence_id_mapping[normalized_id]:
                    _set_alias_name(id, cte.alias_or_name)
                    break
            else:
                # Final fallback: If this is a qualified column reference (table.column)
                # and the table doesn't exist in FROM clause, remove the qualifier IF the column is unambiguously available
                parent = id.parent
                if parent and isinstance(parent, exp.Column) and remove_identifier_if_possible:
                    # Check if this table is not available in current context
                    current_tables = get_cte_names_from_from_clause(expression_context)
                    if normalized_id not in current_tables:
                        # Check if this table ID matches any CTE name directly (cross-context CTE reference)
                        cte_exists = any(
                            cte.alias_or_name == normalized_id for cte in expression_context.ctes
                        )

                        if cte_exists:
                            # This is a reference to a CTE that exists but is not in the current FROM clause
                            # Get the column name being referenced
                            column_name = (
                                _extract_column_name(parent.this)
                                if hasattr(parent, "this")
                                else None
                            )

                            # Only remove qualifier if the column is unambiguously available in current context
                            if column_name and is_column_unambiguously_available(
                                expression_context, column_name
                            ):
                                parent.set("table", None)


def replace_branch_and_sequence_ids_with_cte_name(
    session: SESSION,
    expression_context: exp.Select,
    id: exp.Identifier,
    *,
    remove_identifier_if_possible: bool,
):
    normalized_id = session._normalize_string(id.alias_or_name)
    if normalized_id in session.known_ids:
        # Get CTEs that are referenced in the FROM clause
        referenced_cte_names = get_cte_names_from_from_clause(expression_context)

        # Check if we have a join and if both the tables in that join share a common branch id
        # If so we need to have this reference the left table by default unless the id is a sequence
        # id then it keeps that reference. This handles the weird edge case in spark that shouldn't
        # be common in practice
        if expression_context.args.get("joins") and normalized_id in session.known_branch_ids:
            join_table_aliases = [
                x.alias_or_name for x in get_tables_from_expression_with_join(expression_context)
            ]
            # Filter CTEs to only include those referenced in the FROM clause
            ctes_in_join = [
                cte
                for cte in expression_context.ctes
                if cte.alias_or_name in join_table_aliases
                and cte.alias_or_name in referenced_cte_names
            ]
            if (
                len(ctes_in_join) >= 2
                and ctes_in_join[0].args["branch_id"] == ctes_in_join[1].args["branch_id"]
            ):
                assert len(ctes_in_join) == 2
                _set_alias_name(id, ctes_in_join[0].alias_or_name)
                return

        # Filter CTEs to only include those defined and referenced by the FROM clause
        filtered_ctes = [
            cte
            for cte in reversed(expression_context.ctes)
            if cte.alias_or_name in referenced_cte_names
        ]

        for cte in filtered_ctes:
            if normalized_id in (cte.args["branch_id"], cte.args["sequence_id"]):
                _set_alias_name(id, cte.alias_or_name)
                return

        # Fallback: If not found in filtered CTEs, search through ALL CTEs unfiltered
        for cte in reversed(expression_context.ctes):
            if normalized_id in (cte.args["branch_id"], cte.args["sequence_id"]):
                _set_alias_name(id, cte.alias_or_name)
                return

    # Final fallback: If this is a qualified column reference (table.column)
    # and the table doesn't exist in FROM clause, remove the qualifier IF the column is unambiguously available
    parent = id.parent
    if parent and isinstance(parent, exp.Column) and remove_identifier_if_possible:
        # Check if this table is not available in current context
        current_tables = get_cte_names_from_from_clause(expression_context)
        if normalized_id not in current_tables:
            # Check if this table ID matches any CTE name directly (cross-context CTE reference)
            cte_exists = any(cte.alias_or_name == normalized_id for cte in expression_context.ctes)

            if cte_exists:
                # This is a reference to a CTE that exists but is not in the current FROM clause
                # Get the column name being referenced
                column_name = _extract_column_name(parent.this) if hasattr(parent, "this") else None

                # Only remove qualifier if the column is unambiguously available in current context
                if column_name and is_column_unambiguously_available(
                    expression_context, column_name
                ):
                    parent.set("table", None)


def is_column_unambiguously_available(expression_context: exp.Select, column_name: str) -> bool:
    """
    Check if a column name is unambiguously available in the current context.
    Returns True if the column appears exactly once across all accessible CTEs.

    Enhanced to handle more column expression types and edge cases.
    """
    current_tables = get_cte_names_from_from_clause(expression_context)
    column_count_in_from = 0

    # If no tables in FROM clause, be conservative
    if not current_tables:
        return False

    # Count how many times this column appears in accessible CTEs
    for cte in expression_context.ctes:
        if cte.alias_or_name in current_tables:
            if hasattr(cte, "this") and hasattr(cte.this, "expressions"):
                for expr in cte.this.expressions:
                    expr_column_name = _extract_column_name(expr)

                    # Case-insensitive comparison for robustness
                    if expr_column_name and expr_column_name.lower() == column_name.lower():
                        column_count_in_from += 1

    # Column is unambiguous if it appears exactly once in the FROM clause CTEs
    return column_count_in_from == 1


def _extract_column_name(expr) -> str:
    """
    Extract column name from various expression types.
    Enhanced to handle more SQLGlot expression types.
    """
    if hasattr(expr, "alias_or_name") and expr.alias_or_name:
        return expr.alias_or_name
    elif hasattr(expr, "this"):
        if hasattr(expr.this, "this"):
            return str(expr.this.this)
        elif hasattr(expr.this, "name"):
            return str(expr.this.name)
        else:
            return str(expr.this)
    elif hasattr(expr, "name"):
        return str(expr.name)
    else:
        return str(expr)


def get_cte_names_from_from_clause(expression_context: exp.Select) -> t.Set[str]:
    """
    Get the set of CTE names that are referenced in the FROM clause of the expression.

    Args:
        expression_context: The SELECT expression to analyze

    Returns:
        Set of CTE alias names referenced in the FROM clause (including joins)
    """
    referenced_cte_names = set()

    # Get the main table from FROM clause
    from_clause = expression_context.args.get("from_")
    if from_clause and from_clause.this:
        main_table = from_clause.this
        if hasattr(main_table, "alias_or_name") and main_table.alias_or_name:
            referenced_cte_names.add(main_table.alias_or_name)

    # Get tables from joins
    if expression_context.args.get("joins"):
        join_tables = get_tables_from_expression_with_join(expression_context)
        for table in join_tables:
            if hasattr(table, "alias_or_name") and table.alias_or_name:
                referenced_cte_names.add(table.alias_or_name)

    return referenced_cte_names


def normalize_dict(session: SESSION, data: t.Dict) -> t.Any:
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


# ---------------------------------------------------------------------------
# Standalone normalization helpers (no DataFrame required)
# ---------------------------------------------------------------------------


def ensure_list_of_columns(
    cols: t.Optional[t.Union[t.Any, t.Collection[t.Any]]],
) -> t.List[Column]:
    return Column.ensure_cols(ensure_list(cols))  # type: ignore


def ensure_and_normalize_cols(
    session: SESSION,
    expression: exp.Select,
    cols: t.Any,
    *,
    skip_star_expansion: bool = False,
    remove_identifier_if_possible: bool = True,
) -> t.List[Column]:
    cols = ensure_list_of_columns(cols)
    normalize(
        session,
        expression,
        cols,
        remove_identifier_if_possible=remove_identifier_if_possible,
    )
    if not skip_star_expansion:
        cols = list(flatten([expand_star(expression, col) for col in cols]))
    resolve_ambiguous_columns(expression, cols)
    return cols


def ensure_and_normalize_col(
    session: SESSION,
    expression: exp.Select,
    col: t.Any,
) -> Column:
    col = Column.ensure_col(col)
    normalize(session, expression, [col])
    resolve_ambiguous_columns(expression, col)
    return col


def expand_star(expression: exp.Select, col: Column) -> t.List[Column]:
    from sqlframe.base.util import get_func_from_session

    if isinstance(col.column_expression, exp.Star):
        return _get_outer_select_columns(expression)
    elif (
        isinstance(col.column_expression, exp.Column)
        and isinstance(col.column_expression.this, exp.Star)
        and col.column_expression.args.get("table")
    ):
        for cte in expression.ctes:
            if cte.alias_or_name == col.column_expression.args["table"].this:
                return [
                    Column.ensure_col(exp.column(x.column_alias_or_name, cte.alias_or_name))
                    for x in _get_outer_select_columns(cte)
                ]
        raise ValueError(
            f"Could not find table to expand star: {col.column_expression.args['table']}"
        )
    return [col]


def resolve_ambiguous_columns(
    expression: exp.Select,
    columns: t.Union[Column, t.List[Column]],
) -> None:
    if "joins" not in expression.args:
        return

    col_list: t.List[Column] = t.cast(t.List[Column], ensure_list(columns))
    ambiguous_cols: t.List[exp.Column] = list(
        flatten(
            [
                sub_col
                for col in col_list
                for sub_col in col.expression.find_all(exp.Column)
                if not sub_col.table
            ]
        )
    )
    if ambiguous_cols:
        join_table_identifiers = [x.this for x in get_tables_from_expression_with_join(expression)]
        cte_names_in_join = [x.this for x in join_table_identifiers]
        resolved_column_position: t.Dict[exp.Column, int] = {
            col.copy(): -1 for col in ambiguous_cols
        }
        cte_lookup = {cte.alias_or_name: cte for cte in expression.ctes}
        for ambiguous_col in ambiguous_cols:
            join_side = expression.args["joins"][0].args.get("side", "")
            ordered_join_cte_names = (
                list(reversed(cte_names_in_join)) if join_side == "right" else cte_names_in_join
            )
            ctes_with_column = [
                cte_lookup[name]
                for name in ordered_join_cte_names
                if name in cte_lookup
                and (
                    ambiguous_col.alias_or_name in cte_lookup[name].this.named_selects
                    or any(isinstance(sel, exp.Star) for sel in cte_lookup[name].this.expressions)
                )
            ]
            cte = seq_get(ctes_with_column, resolved_column_position[ambiguous_col] + 1)
            if cte:
                resolved_column_position[ambiguous_col] += 1
            else:
                cte = seq_get(ctes_with_column, resolved_column_position[ambiguous_col])
            if cte:
                ambiguous_col.set("table", exp.to_identifier(cte.alias_or_name))


def update_display_name_mapping(
    mapping: t.Dict[str, str],
    normalized_columns: t.List[Column],
    user_input: t.Iterable[t.Any],
) -> None:
    normalized_aliases = [x.alias_or_name for x in normalized_columns]
    user_display_names = [
        x.expression.meta.get("display_name") if isinstance(x, Column) else x for x in user_input
    ]
    zipped = {
        k: v for k, v in dict(zip(normalized_aliases, user_display_names)).items() if v is not None
    }
    mapping.update(zipped)


def _get_outer_select_columns(item: exp.Expression) -> t.List[Column]:
    from sqlframe.base.session import _BaseSession
    from sqlframe.base.util import get_func_from_session

    col = get_func_from_session("col", _BaseSession())
    outer_select = item.find(exp.Select)
    if outer_select:
        return [col(quote_preserving_alias_or_name(x)) for x in outer_select.expressions]
    return []
