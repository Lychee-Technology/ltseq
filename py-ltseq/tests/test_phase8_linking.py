"""
Phase 8 Tests: Pointer-Based Linking MVP

Tests for link() operator which implements pointer-based foreign key relationships.

MVP Phase 8: Basic pointer table API validation. Full link() implementation
(actual data joining with linked column access) is planned for Phase 8 enhancements.
"""

import pytest
from ltseq import LTSeq


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
            products_table, on=lambda o, p: o.product_id == p.id, as_="prod"
        )

        assert linked is not None
        assert linked._alias == "prod"
        assert linked._source is orders_table
        assert linked._target is products_table

    def test_link_schema_includes_target_columns(self, orders_table, products_table):
        """link() should update schema to include target columns with prefix"""
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.id, as_="prod"
        )

        # Check that schema includes prefixed target columns
        assert "id" in linked._schema  # from source
        assert "prod_id" in linked._schema  # from target with prefix
        assert "prod_name" in linked._schema  # from target with prefix
        assert "prod_price" in linked._schema  # from target with prefix

    def test_link_delegates_select_to_source(self, orders_table, products_table):
        """select() should work (delegates to source in MVP)"""
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.id, as_="prod"
        )

        # Select from source columns
        result = linked.select("id", "product_id")
        assert result is not None
        assert "id" in result._schema

    def test_link_with_filter(self, orders_table, products_table):
        """filter() should work and return a new LinkedTable"""
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.id, as_="prod"
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
                on=lambda o, p: o.nonexistent == p.id,
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

    def test_link_preserves_original_data(self, orders_table, products_table):
        """Linking should not modify original table"""
        original_len = len(orders_table)

        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.id, as_="prod"
        )

        # Original table should be unchanged
        assert len(orders_table) == original_len


class TestLinkChaining:
    """Test chaining operations after link()"""

    @pytest.fixture
    def orders_table(self):
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        return LTSeq.read_csv("examples/products.csv")

    def test_link_then_filter(self, orders_table, products_table):
        """Can chain filter() after link()"""
        result = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.id, as_="prod"
        ).filter(lambda r: r.quantity > 1)
        assert result is not None

    def test_link_then_slice(self, orders_table, products_table):
        """Can chain slice() after link()"""
        result = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.id, as_="prod"
        ).slice(0, 5)
        assert result is not None

    def test_link_then_derive(self, orders_table, products_table):
        """Can chain derive() after link()"""
        result = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.id, as_="prod"
        ).derive(lambda r: {"qty_x2": r.quantity * 2})
        assert result is not None

    def test_link_multiple_chained_operations(self, orders_table, products_table):
        """Can chain multiple operations"""
        result = (
            orders_table.link(
                products_table, on=lambda o, p: o.product_id == p.id, as_="prod"
            )
            .filter(lambda r: r.quantity > 1)
            .derive(lambda r: {"qty_doubled": r.quantity * 2})
            .slice(0, 3)
        )
        assert result is not None


class TestLinkMultiple:
    """Test linking to multiple tables"""

    @pytest.fixture
    def orders_table(self):
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        return LTSeq.read_csv("examples/products.csv")

    def test_link_multiple_times(self, orders_table, products_table):
        """Should be able to link the same table multiple times with different aliases"""
        result = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.id, as_="prod1"
        ).link(products_table, on=lambda o, p: o.product_id == p.id, as_="prod2")

        # Should have columns from both links
        assert "prod1_name" in result._schema
        assert "prod1_price" in result._schema
        assert "prod2_name" in result._schema
        assert "prod2_price" in result._schema


class TestLinkShowOperation:
    """Test show() method on linked tables"""

    @pytest.fixture
    def orders_table(self):
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        return LTSeq.read_csv("examples/products.csv")

    def test_link_show_works(self, orders_table, products_table):
        """show() should work on linked table"""
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.id, as_="prod"
        )

        # Should not raise an error
        linked.show(5)


class TestLinkLazy:
    """Test laziness of link() operation"""

    @pytest.fixture
    def orders_table(self):
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        return LTSeq.read_csv("examples/products.csv")

    def test_link_is_lazy(self, orders_table, products_table):
        """link() should be lazy - no data materialization until accessed"""
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.id, as_="prod"
        )

        # Just creating the link shouldn't materialize anything
        assert linked is not None
        assert (
            linked._materialized is None if hasattr(linked, "_materialized") else True
        )
