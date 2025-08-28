# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

import typing as t

from sqlglot import expressions as exp


def replace_id_value(
    node: exp.Expression, replacement_mapping: t.Dict[exp.Identifier, exp.Identifier]
) -> exp.Expression:
    if isinstance(node, exp.Identifier) and node in replacement_mapping:
        node = node.replace(replacement_mapping[node].copy())
    return node


def expand_column_references_with_expressions(
    node: exp.Expression, replacement_mapping: t.Dict[str, exp.Expression]
) -> exp.Expression:
    """
    Checks if an expression contains a column expression that has an alias or name
    that matches the alias or names in the replacement mapping. If so, it replaces
    the column expression with the corresponding expression from the mapping.

    This is useful if you want to take a select column and replace it with new logic
    without having to convert the select to a CTE.

    If the node contains a table reference, it maintains that table reference in the
    new expression.

    Ex:
    replacement: {blah: "CAST(x AS INT) as blah"}
    node: CASE WHEN source_table.blah > 0 THEN 'A' ELSE 'B' END
    becomes: CASE WHEN CAST(source_table.x AS INT) > 0 THEN 'A' ELSE 'B' END

    If the
    """
    if isinstance(node, exp.Column) and node.alias_or_name in replacement_mapping:
        new_expression = replacement_mapping[node.alias_or_name].copy().unalias()
        if node.table:
            new_expression = new_expression.transform(replace_table, node.args["table"].copy())
        node = node.replace(new_expression)
    return node


def replace_table(node: exp.Expression, new_table: exp.Identifier) -> exp.Expression:
    if isinstance(node, exp.Column):
        node.set("table", new_table)
    return node
