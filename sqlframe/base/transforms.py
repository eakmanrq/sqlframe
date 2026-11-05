# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

import typing as t

from sqlglot import expressions as exp
from sqlglot.optimizer.annotate_types import TypeAnnotator


class ContainerTypeAnnotator(TypeAnnotator):
    def _annotate_bracket(self, expression: exp.Bracket) -> exp.Bracket:
        # SQLGlot infers array elements but not values read from typed map columns.
        # Propagate the value type so chained lookups retain their container types.
        container_type = expression.this.type
        if container_type and container_type.is_type(exp.DataType.Type.MAP):
            self._set_type(expression, container_type.expressions[1])
            return expression
        return super()._annotate_bracket(expression)


def replace_id_value(
    node: exp.Expression, replacement_mapping: t.Dict[exp.Identifier, exp.Identifier]
) -> exp.Expression:
    if isinstance(node, exp.Identifier) and node in replacement_mapping:
        node = node.replace(replacement_mapping[node].copy())
    return node
