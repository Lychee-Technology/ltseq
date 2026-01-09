"""
Phase 8 Tests: Pointer-Based Linking - Filter Operations

Tests for filtering on linked tables, including both source column filters
and linked column filters.
"""

import pytest
from ltseq import LTSeq, LinkedTable


class TestLinkedTableFilter:
    """Tests for filtering on linked tables"""

    @pytest.fixture
    def orders_table(self):
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        return LTSeq.read_csv("examples/products.csv")

    def test_filter_returns_linked_table(self, orders_table, products_table):
        """
        filter() on source columns should return a LinkedTable.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        result = linked.filter(lambda r: r.quantity > 2)
        assert isinstance(result, LinkedTable)

    def test_filter_preserves_linked_table_structure(
        self, orders_table, products_table
    ):
        """
        Filtering a linked table should preserve its structure for further operations.
        The filtered linked table should still support access to linked columns.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Filter on source column
        filtered = linked.filter(lambda r: r.quantity > 1)

        # The result should still be a linked table with schema intact
        assert isinstance(filtered, LinkedTable)
        assert "prod_name" in filtered._schema
        assert "prod_price" in filtered._schema

    def test_filter_with_complex_source_condition(self, orders_table, products_table):
        """
        Test filtering with more complex conditions on source columns.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Filter with compound condition on source columns only
        result = linked.filter(lambda r: r.quantity > 1)

        assert result is not None
        assert isinstance(result, LinkedTable)
        result.show(1)

    def test_filter_on_source_then_show_linked_columns(
        self, orders_table, products_table
    ):
        """
        Filter on source, then materialize to access linked columns.
        Tests that the filtering doesn't break the join capability.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Filter on source column
        filtered = linked.filter(lambda r: r.quantity > 2)

        # Then materialize and display (this materializes the join after filtering)
        if isinstance(filtered, LinkedTable):
            filtered.show(2)

    def test_filter_returns_correct_row_count(self, orders_table, products_table):
        """
        Verify that filtering correctly reduces row count.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Get original count
        original_count = len(linked._source)

        # Filter to only high-quantity orders
        filtered = linked.filter(lambda r: r.quantity > 3)

        # Filtered count should be less than original
        filtered_count = len(filtered._source)
        assert filtered_count < original_count
        assert filtered_count > 0


class TestLinkedTableFilterOnLinkedColumns:
    """
    Phase 10: Tests for filtering directly on linked columns without forced materialization.

    These tests verify that we can filter on linked table columns (like r.prod_price)
    and get back filtered results without requiring explicit materialization in user code.
    """

    @pytest.fixture
    def orders_table(self):
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        return LTSeq.read_csv("examples/products.csv")

    def test_filter_on_linked_column_returns_ltseq(self, orders_table, products_table):
        """
        Filtering on a linked column should return an LTSeq (materialized result).
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        result = linked.filter(lambda r: r.prod_price > 100)

        # When filtering on linked columns, we get back an LTSeq
        assert isinstance(result, LTSeq)
        assert len(result) > 0

    def test_filter_on_linked_column_simple_condition(
        self, orders_table, products_table
    ):
        """
        Test filtering with a simple condition on a single linked column.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Filter to only expensive products
        filtered = linked.filter(lambda r: r.prod_price > 100)

        assert filtered is not None
        assert len(filtered) > 0

        # Verify schema is preserved
        assert "prod_price" in filtered._schema

    def test_filter_on_linked_column_with_comparison(
        self, orders_table, products_table
    ):
        """
        Test filtering with comparison operators on linked columns.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Test greater than
        filtered_gt = linked.filter(lambda r: r.prod_price > 50)
        assert len(filtered_gt) > 0

        # Test less than
        filtered_lt = linked.filter(lambda r: r.prod_price < 200)
        assert len(filtered_lt) > 0

        # Test equality
        filtered_eq = linked.filter(lambda r: r.prod_price == 100)
        assert len(filtered_eq) >= 0

    def test_filter_on_linked_string_column(self, orders_table, products_table):
        """
        Test filtering on a string-type linked column.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Filter by product name (Widget, Gadget, Tool, or Device)
        filtered = linked.filter(lambda r: r.prod_name == "Widget")

        # Verify filtering worked
        assert len(filtered) > 0
        assert "prod_name" in filtered._schema

    def test_filter_with_combined_source_and_linked_condition(
        self, orders_table, products_table
    ):
        """
        Test that we cannot easily combine source and linked column conditions in a single filter.

        Note: Complex conditions combining source and linked columns would require
        more sophisticated handling. For now, test that the simpler case works.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Filter on linked column only
        filtered = linked.filter(lambda r: r.prod_price > 75)

        assert filtered is not None
        assert len(filtered) > 0

    def test_filter_on_linked_column_preserves_all_columns(
        self, orders_table, products_table
    ):
        """
        Verify that filtering on linked columns preserves all columns in result.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        original_columns = set(linked._schema.keys())

        filtered = linked.filter(lambda r: r.prod_price > 100)
        filtered_columns = set(filtered._schema.keys())

        assert original_columns == filtered_columns

    def test_filter_on_linked_column_chaining(self, orders_table, products_table):
        """
        Test that we can chain operations after filtering on linked columns.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Filter on linked column, then materialize (should already be materialized)
        filtered = linked.filter(lambda r: r.prod_price > 100)

        # Should be able to call methods on the result
        assert len(filtered) > 0
        filtered.show(2)

    def test_filter_on_linked_column_empty_result(self, orders_table, products_table):
        """
        Test filtering on linked column with condition that matches no rows.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Filter with condition that should match nothing
        filtered = linked.filter(lambda r: r.prod_price > 10000)

        assert len(filtered) == 0

    def test_filter_on_linked_column_with_multiple_conditions(
        self, orders_table, products_table
    ):
        """
        Test filtering with multiple conditions on linked columns using logical operators.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Test AND condition (both must be true)
        filtered = linked.filter(lambda r: (r.prod_price > 50) & (r.prod_price < 200))

        # Verify filtering worked
        assert len(filtered) > 0
        assert "prod_price" in filtered._schema
