# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

import typing as t

from sqlglot import expressions as exp


def replace_id_value(
    node: exp.Expression, replacement_mapping: t.Dict[exp.Identifier, exp.Identifier]
) -> exp.Expression:
    if isinstance(node, exp.Identifier) and node in replacement_mapping:
        node = node.replace(replacement_mapping[node].copy())
    return node
