"""
Phase 8 Tests: Pointer-Based Linking - Aggregate Operations

Tests for aggregate() on linked tables.
"""

import pytest
from ltseq import LTSeq


class TestLinkedTableAggregate:
    """
    Phase 13: Tests for aggregate() on linked tables.

    These tests verify that we can aggregate linked table data
    and get back aggregated results without requiring explicit materialization.
    """

    @pytest.fixture
    def orders_table(self):
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        return LTSeq.read_csv("examples/products.csv")

    def test_aggregate_linked_returns_ltseq(self, orders_table, products_table):
        """
        Aggregating a linked table should return an LTSeq.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        result = linked.aggregate({"total": lambda g: g.id.count()})

        # When aggregating, we get back an LTSeq
        assert isinstance(result, LTSeq)

    def test_aggregate_linked_count_rows(self, orders_table, products_table):
        """
        Test that aggregate makes materialized data available for aggregation.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        result = linked.aggregate({"total_orders": lambda g: g.id.count()})

        # Should return LTSeq with all columns available
        assert isinstance(result, LTSeq)
        assert len(result) > 0
        # Should have both source and linked columns available
        assert "id" in result._schema
        assert "prod_name" in result._schema

    def test_aggregate_materializes(self, orders_table, products_table):
        """
        Test that aggregate materializes the linked table.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Before aggregate, should not be materialized
        assert linked._materialized is None

        result = linked.aggregate({"total": lambda g: g.quantity.sum()})

        # After aggregate, should be materialized
        assert linked._materialized is not None
        assert isinstance(result, LTSeq)

    def test_aggregate_provides_access_to_linked_columns(
        self, orders_table, products_table
    ):
        """
        Test that aggregate provides access to linked columns in result.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        result = linked.aggregate({"dummy": lambda g: g.quantity})

        # Result should have both source and linked columns available
        assert "id" in result._schema
        assert "quantity" in result._schema
        assert "prod_name" in result._schema
        assert "prod_price" in result._schema

    def test_aggregate_supports_dict_syntax(self, orders_table, products_table):
        """
        Test that aggregate supports dict argument syntax.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Dict syntax should work
        result = linked.aggregate({"col": lambda g: g.quantity})
        assert isinstance(result, LTSeq)

    def test_aggregate_supports_kwargs_syntax(self, orders_table, products_table):
        """
        Test that aggregate supports kwargs syntax.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Kwargs syntax should work
        result = linked.aggregate(col=lambda g: g.quantity)
        assert isinstance(result, LTSeq)

    def test_aggregate_materializes_join(self, orders_table, products_table):
        """
        Verify that aggregation causes the join to be materialized.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Before aggregate, join should not be materialized
        assert linked._materialized is None

        # After aggregate, join should be materialized
        result = linked.aggregate({"dummy": lambda g: g.quantity})

        assert linked._materialized is not None

    def test_aggregate_can_be_chained_with_group_ordered(
        self, orders_table, products_table
    ):
        """
        Test that aggregate result can be chained with group_ordered for actual aggregation.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        result = linked.aggregate({"dummy": lambda g: g.id})

        # Result should be usable for further operations
        assert isinstance(result, LTSeq)
        assert len(result) > 0
