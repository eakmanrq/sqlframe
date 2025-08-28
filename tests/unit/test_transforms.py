import pytest
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlframe.base.transforms import expand_column_references_with_expressions


@pytest.mark.parametrize(
    "node, replacement_mapping, expected_sql",
    [
        # Basic column replacement
        (
            parse_one("blah", into=exp.Column),
            {"blah": parse_one("CAST(x AS INT)")},
            "CAST(x AS INT)",
        ),
        # Column with table reference
        (
            parse_one("source_table.blah", into=exp.Column),
            {"blah": parse_one("CAST(x AS INT)")},
            "CAST(source_table.x AS INT)",
        ),
        # Complex expression with column replacement
        (
            parse_one("CASE WHEN blah > 0 THEN 'A' ELSE 'B' END"),
            {"blah": parse_one("CAST(x AS INT)")},
            "CASE WHEN CAST(x AS INT) > 0 THEN 'A' ELSE 'B' END",
        ),
        # Complex expression with table-qualified column replacement
        (
            parse_one("CASE WHEN source_table.blah > 0 THEN 'A' ELSE 'B' END"),
            {"blah": parse_one("CAST(x AS INT)")},
            "CASE WHEN CAST(source_table.x AS INT) > 0 THEN 'A' ELSE 'B' END",
        ),
        # Multiple column references in same expression
        (
            parse_one("blah + other_col"),
            {"blah": parse_one("CAST(x AS INT)"), "other_col": parse_one("y * 2")},
            "CAST(x AS INT) + y * 2",
        ),
        # Multiple column references with table qualification
        (
            parse_one("t1.blah + t1.other_col"),
            {"blah": parse_one("CAST(x AS INT)"), "other_col": parse_one("y * 2")},
            "CAST(t1.x AS INT) + t1.y * 2",
        ),
        # No matching columns - should return unchanged
        (parse_one("no_match", into=exp.Column), {"blah": parse_one("CAST(x AS INT)")}, "no_match"),
        # Non-column expression - should return unchanged
        (parse_one("'literal_string'"), {"blah": parse_one("CAST(x AS INT)")}, "'literal_string'"),
        # Empty replacement mapping
        (parse_one("blah", into=exp.Column), {}, "blah"),
        # Column with alias in replacement
        (
            parse_one("col1 AS new_value"),
            {"col1": parse_one("UPPER(name) AS col1")},
            "UPPER(name) AS new_value",
        ),
        # Table-qualified column with alias in replacement
        (
            parse_one("t.col1", into=exp.Column),
            {"col1": parse_one("UPPER(name) AS col1")},
            "UPPER(t.name)",
        ),
        # Function call with column replacement
        (
            parse_one("SUM(amount)"),
            {"amount": parse_one("price * quantity")},
            "SUM(price * quantity)",
        ),
        # Function call with table-qualified column replacement
        (
            parse_one("SUM(orders.amount)"),
            {"amount": parse_one("price * quantity")},
            "SUM(orders.price * orders.quantity)",
        ),
        # Nested function with column replacement
        (
            parse_one("COALESCE(value, 0)"),
            {"value": parse_one("CAST(raw_value AS DECIMAL(10,2))")},
            "COALESCE(CAST(raw_value AS DECIMAL(10, 2)), 0)",
        ),
        # Binary operation with column replacement
        (
            parse_one("profit_margin > 0.1"),
            {"profit_margin": parse_one("(revenue - cost) / revenue")},
            "(revenue - cost) / revenue > 0.1",
        ),
        # Binary operation with table-qualified column replacement
        (
            parse_one("sales.profit_margin > 0.1"),
            {"profit_margin": parse_one("(revenue - cost) / revenue")},
            "(sales.revenue - sales.cost) / sales.revenue > 0.1",
        ),
        # Complex nested expression
        (
            parse_one("CASE WHEN status = 'active' THEN calculated_value ELSE 0 END"),
            {"calculated_value": parse_one("base_value * multiplier + bonus")},
            "CASE WHEN status = 'active' THEN base_value * multiplier + bonus ELSE 0 END",
        ),
        # Subquery with column replacement (column inside subquery)
        (
            parse_one("EXISTS (SELECT 1 FROM table2 WHERE table2.id = parent_id)"),
            {"parent_id": parse_one("main_table.id")},
            "EXISTS(SELECT 1 FROM table2 WHERE table2.id = main_table.id)",
        ),
        # Window function with column replacement
        (
            parse_one("ROW_NUMBER() OVER (ORDER BY score DESC)"),
            {"score": parse_one("points + bonus_points")},
            "ROW_NUMBER() OVER (ORDER BY points + bonus_points DESC)",
        ),
    ],
)
def test_expand_column_references_with_expressions(node, replacement_mapping, expected_sql):
    """Test the expand_column_references_with_expressions function with various scenarios."""
    result = node.transform(expand_column_references_with_expressions, replacement_mapping)
    assert result.sql() == expected_sql
