"""
Phase 8 Tests: Pointer-Based Linking - Basic Operations

Tests for link() operator which implements pointer-based foreign key relationships.
Covers basic linking, edge cases, and show() operation.
"""

import pytest
from ltseq import LTSeq, LinkedTable


class TestLinkBasic:
    """Basic link() functionality tests"""

    @pytest.fixture
    def orders_table(self):
        """Orders table: id, product_id, quantity"""
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        """Products table: id, name, price"""
        return LTSeq.read_csv("examples/products.csv")

    def test_link_creates_pointer_reference(self, orders_table, products_table):
        """link() should create a LinkedTable object"""
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        assert linked is not None
        assert linked._alias == "prod"
        assert linked._source is orders_table
        assert linked._target is products_table

    def test_link_schema_includes_target_columns(self, orders_table, products_table):
        """link() should update schema to include target columns with prefix"""
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Check that schema includes prefixed target columns
        assert "id" in linked._schema  # from source
        assert "prod_product_id" in linked._schema  # from target with prefix
        assert "prod_name" in linked._schema  # from target with prefix
        assert "prod_price" in linked._schema  # from target with prefix

    def test_link_delegates_select_to_source(self, orders_table, products_table):
        """select() should work (delegates to source in MVP)"""
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Select from source columns
        result = linked.select("id", "product_id")
        assert result is not None
        assert "id" in result._schema

    def test_link_with_filter(self, orders_table, products_table):
        """filter() should work and return a new LinkedTable"""
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Filter on source columns
        result = linked.filter(lambda r: r.quantity > 2)
        assert result is not None


class TestLinkEdgeCases:
    """Edge cases and error conditions"""

    @pytest.fixture
    def orders_table(self):
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        return LTSeq.read_csv("examples/products.csv")

    def test_link_nonexistent_column_in_source(self, orders_table, products_table):
        """Should raise error if joining on non-existent column in source"""
        with pytest.raises((AttributeError, TypeError)):
            orders_table.link(
                products_table,
                on=lambda o, p: o.nonexistent == p.product_id,
                as_="prod",
            )

    def test_link_nonexistent_column_in_target(self, orders_table, products_table):
        """Should raise error if joining on non-existent column in target"""
        with pytest.raises((AttributeError, TypeError)):
            orders_table.link(
                products_table,
                on=lambda o, p: o.product_id == p.nonexistent,
                as_="prod",
            )


class TestLinkShowOperation:
    """Tests for show() on linked tables"""

    @pytest.fixture
    def orders_table(self):
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        return LTSeq.read_csv("examples/products.csv")

    def test_link_show_displays_data(self, orders_table, products_table, capsys):
        """
        show() should display the linked table with joined data.
        Tests that show() works without raising exceptions.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # This should not raise an exception
        linked.show(3)

        # Check that output was printed
        captured = capsys.readouterr()
        assert len(captured.out) > 0


class TestLinkLenOperation:
    """Tests for len() on linked tables"""

    @pytest.fixture
    def orders_table(self):
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        return LTSeq.read_csv("examples/products.csv")

    def test_link_len_returns_row_count(self, orders_table, products_table):
        """
        len() should return the number of rows in the materialized join.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # len() should work and return a positive count
        count = len(linked)
        assert isinstance(count, int)
        assert count > 0

    def test_link_len_matches_materialized_count(self, orders_table, products_table):
        """
        len(linked) should equal len(linked._materialize()).
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Compare len() with explicit materialization
        linked_len = len(linked)
        materialized_len = len(linked._materialize())
        assert linked_len == materialized_len

    def test_link_len_after_filter_source_columns(self, orders_table, products_table):
        """
        len() should work on filtered LinkedTable (source columns only).
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Filter on source column returns LinkedTable
        filtered = linked.filter(lambda r: r.quantity > 2)

        # len() should work on the filtered LinkedTable
        count = len(filtered)
        assert isinstance(count, int)
        assert count >= 0

    def test_link_len_after_filter_linked_columns(self, orders_table, products_table):
        """
        len() should work on filtered result (linked columns, returns LTSeq).
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Filter on linked column returns LTSeq
        filtered = linked.filter(lambda r: r.prod_price > 50)

        # len() should work on the result
        count = len(filtered)
        assert isinstance(count, int)
        assert count >= 0


class TestLinkedTableSortSliceDistinct:
    """Tests for LinkedTable.sort(), .slice(), .distinct() (T33)."""

    @pytest.fixture
    def orders_table(self):
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        return LTSeq.read_csv("examples/products.csv")

    def _make_linked(self, orders_table, products_table):
        return orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
        )

    def test_sort_returns_linked_table(self, orders_table, products_table):
        """sort() on LinkedTable returns a LinkedTable."""
        linked = self._make_linked(orders_table, products_table)
        sorted_linked = linked.sort("quantity")
        assert isinstance(sorted_linked, LinkedTable)

    def test_sort_preserves_schema(self, orders_table, products_table):
        """sort() preserves schema including linked columns."""
        linked = self._make_linked(orders_table, products_table)
        sorted_linked = linked.sort("quantity")
        assert "prod_name" in sorted_linked._schema
        assert "id" in sorted_linked._schema

    def test_sort_preserves_row_count(self, orders_table, products_table):
        """sort() does not change the number of rows."""
        linked = self._make_linked(orders_table, products_table)
        original_len = len(linked)
        sorted_linked = linked.sort("quantity")
        assert len(sorted_linked) == original_len

    def test_slice_returns_linked_table(self, orders_table, products_table):
        """slice() on LinkedTable returns a LinkedTable."""
        linked = self._make_linked(orders_table, products_table)
        sliced = linked.slice(0, 2)
        assert isinstance(sliced, LinkedTable)

    def test_slice_reduces_rows(self, orders_table, products_table):
        """slice(0, 2) returns at most 2 rows."""
        linked = self._make_linked(orders_table, products_table)
        sliced = linked.slice(0, 2)
        assert len(sliced) <= 2

    def test_distinct_returns_linked_table(self, orders_table, products_table):
        """distinct() on LinkedTable returns a LinkedTable."""
        linked = self._make_linked(orders_table, products_table)
        distinct_linked = linked.distinct()
        assert isinstance(distinct_linked, LinkedTable)

    def test_distinct_preserves_schema(self, orders_table, products_table):
        """distinct() preserves schema including linked columns."""
        linked = self._make_linked(orders_table, products_table)
        distinct_linked = linked.distinct()
        assert "prod_name" in distinct_linked._schema
