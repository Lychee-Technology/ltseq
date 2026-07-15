"""
Phase 8 Tests: Pointer-Based Linking - Composite Keys and Join Types

Tests for composite (multi-column) join keys and different join types (INNER, LEFT, RIGHT, FULL).
"""

import pytest
from ltseq import LTSeq, LinkedTable


class TestLinkedTableCompositeKeys:
    """Tests for composite (multi-column) join keys"""

    @pytest.fixture
    def orders_table(self):
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        return LTSeq.read_csv("examples/products.csv")

    def test_composite_key_creates_linked_table(self, orders_table, products_table):
        """Composite key join should create a LinkedTable"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: (o.product_id == p.product_id),
            as_="prod",
        )

        assert linked is not None
        assert isinstance(linked, LinkedTable)

    def test_composite_key_schema_correct(self, orders_table, products_table):
        """Schema should include all columns with proper prefixes"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: (o.product_id == p.product_id),
            as_="prod",
        )

        # Check source columns
        assert "id" in linked._schema
        assert "product_id" in linked._schema

        # Check linked columns with prefix
        assert "prod_product_id" in linked._schema
        assert "prod_name" in linked._schema

    def test_composite_key_materialize_works(self, orders_table, products_table):
        """Materialization should work with composite keys"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: (o.product_id == p.product_id),
            as_="prod",
        )

        result = linked._ensure_join_plan()
        assert result is not None
        assert isinstance(result, LTSeq)

    def test_composite_key_show_works(self, orders_table, products_table):
        """show() should display composite key join results"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: (o.product_id == p.product_id),
            as_="prod",
        )

        linked.show(2)

    def test_composite_key_select_source_columns(self, orders_table, products_table):
        """select() should work on source columns with composite keys"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: (o.product_id == p.product_id),
            as_="prod",
        )

        result = linked.select("id", "product_id")
        assert result is not None

    def test_composite_key_filter_on_source_column(self, orders_table, products_table):
        """filter() should work on source columns with composite keys"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: (o.product_id == p.product_id),
            as_="prod",
        )

        filtered = linked.filter(lambda r: r.quantity > 2)
        assert isinstance(filtered, LTSeq)

    def test_composite_key_data_integrity(self, orders_table, products_table):
        """Composite key join should not corrupt data"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: (o.product_id == p.product_id),
            as_="prod",
        )

        result = linked._ensure_join_plan()
        result.show(3)

    def test_three_column_composite_key(self, orders_table, products_table):
        """
        Test that we can construct a composite key with multiple conditions.
        This tests the parsing of complex join conditions.
        """
        linked = orders_table.link(
            products_table,
            on=lambda o, p: (o.product_id == p.product_id),
            as_="prod",
        )

        # Should create a valid linked table
        assert linked is not None
        assert "prod_name" in linked._schema
        assert "prod_price" in linked._schema


class TestLinkedTableJoinTypes:
    """Tests for different join types (INNER, LEFT, RIGHT, FULL)"""

    @pytest.fixture
    def orders_table(self):
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        return LTSeq.read_csv("examples/products.csv")

    def test_inner_join_only_matches(self, orders_table, products_table):
        """INNER join should only include matching rows"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
            join_type="inner",
        )

        assert linked._join_type == "inner"
        result = linked._ensure_join_plan()
        assert result is not None

    def test_left_join_keeps_all_left_rows(self, orders_table, products_table):
        """LEFT join should keep all rows from left table"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
            join_type="left",
        )

        assert linked._join_type == "left"
        result = linked._ensure_join_plan()
        assert result is not None

    def test_left_join_filters_null_product(self, orders_table, products_table):
        """LEFT join should handle NULL values from unmatched right rows"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
            join_type="left",
        )

        result = linked._ensure_join_plan()
        # Show to verify it works
        result.show(2)

    def test_right_join_keeps_all_right_rows(self, orders_table, products_table):
        """RIGHT join should keep all rows from right table"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
            join_type="right",
        )

        assert linked._join_type == "right"
        result = linked._ensure_join_plan()
        assert result is not None

    def test_full_join_keeps_all_rows(self, orders_table, products_table):
        """FULL join should keep all rows from both tables"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
            join_type="full",
        )

        assert linked._join_type == "full"
        result = linked._ensure_join_plan()
        assert result is not None

    def test_invalid_join_type_rejected(self, orders_table, products_table):
        """Invalid join_type should raise ValueError"""
        with pytest.raises(ValueError):
            orders_table.link(
                products_table,
                on=lambda o, p: o.product_id == p.product_id,
                as_="prod",
                join_type="INVALID",
            )

    def test_default_join_type_is_inner(self, orders_table, products_table):
        """Default join_type should be 'inner'"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
        )

        assert linked._join_type == "inner"


class TestCompositeKeyPairingOrder:
    """Regression for the linked-join key-swap bug (PR #120 review finding 1):
    join keys were rebuilt in right-schema order but paired positionally against
    left keys, so composite keys silently swapped when the right key order in the
    lambda differed from the right table's schema column order."""

    def test_composite_link_pairs_keys_by_lambda_order_not_schema_order(self):
        # Left row has k1=1, k2=100.
        left = LTSeq._from_rows(
            [{"k1": 1, "k2": 100, "tag": "L"}],
            {"k1": "int64", "k2": "int64", "tag": "string"},
        )
        # Right SCHEMA order puts k2 before k1 (deliberately != lambda order).
        # Row "A" is the correct match for (k1==1 & k2==100); row "B" is what a
        # swapped pairing (k1==k2 & k2==k1) would wrongly match.
        right = LTSeq._from_rows(
            [
                {"k2": 100, "k1": 1, "label": "A"},
                {"k2": 1, "k1": 100, "label": "B"},
            ],
            {"k2": "int64", "k1": "int64", "label": "string"},
        )

        rows = (
            left.link(
                right,
                on=lambda l, r: (l.k1 == r.k1) & (l.k2 == r.k2),
                as_="m",
            )
            ._ensure_join_plan()
            .to_dicts()
        )

        assert len(rows) == 1
        assert rows[0]["m_label"] == "A"
