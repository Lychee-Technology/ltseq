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

    def test_link_preserves_original_data(self, orders_table, products_table):
        """Linking should not modify original table"""
        original_len = len(orders_table)

        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
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
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        ).filter(lambda r: r.quantity > 1)
        assert result is not None

    def test_link_then_slice(self, orders_table, products_table):
        """Can chain slice() after link()"""
        result = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        ).slice(0, 5)
        assert result is not None

    def test_link_then_derive(self, orders_table, products_table):
        """Can chain derive() after link()"""
        result = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        ).derive(lambda r: {"qty_x2": r.quantity * 2})
        assert result is not None

    def test_link_multiple_chained_operations(self, orders_table, products_table):
        """Can chain multiple operations"""
        result = (
            orders_table.link(
                products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
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
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod1"
        ).link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod2"
        )

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
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
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
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Just creating the link shouldn't materialize anything
        assert linked is not None
        assert (
            linked._materialized is None if hasattr(linked, "_materialized") else True
        )


class TestExtractJoinKeys:
    """Tests for Phase 8A: _extract_join_keys() function"""

    def test_extract_simple_equality_join(self):
        """Should extract join keys from simple lambda: lambda o, p: o.product_id == p.product_id"""
        from ltseq import _extract_join_keys

        orders_schema = {"id": "int64", "product_id": "int64", "quantity": "int64"}
        products_schema = {"product_id": "int64", "name": "string", "price": "float64"}

        left_key, right_key, join_type = _extract_join_keys(
            lambda o, p: o.product_id == p.product_id, orders_schema, products_schema
        )

        assert left_key == {"type": "Column", "name": "product_id"}
        assert right_key == {"type": "Column", "name": "product_id"}
        assert join_type == "inner"

    def test_extract_reversed_join_keys(self):
        """Should handle reversed comparison: lambda o, p: p.id == o.product_id"""
        from ltseq import _extract_join_keys

        orders_schema = {"id": "int64", "product_id": "int64", "quantity": "int64"}
        products_schema = {"id": "int64", "name": "string", "price": "float64"}

        # Note: When we write lambda o, p: p.id == o.product_id
        # The result is a BinOpExpr with left=p.id (target) and right=o.product_id (source)
        # But our validation expects left to be from source and right from target
        # This is the user's responsibility to write the condition correctly
        with pytest.raises(ValueError):
            _extract_join_keys(
                lambda o, p: p.id == o.product_id,
                orders_schema,
                products_schema,
            )

    def test_extract_rejects_non_equality_operator(self):
        """Should reject comparisons with non-equality operators like >"""
        from ltseq import _extract_join_keys

        orders_schema = {"id": "int64", "amount": "float64"}
        products_schema = {"id": "int64", "price": "float64"}

        with pytest.raises(TypeError, match="equality"):
            _extract_join_keys(
                lambda o, p: o.amount > p.price, orders_schema, products_schema
            )

    def test_extract_rejects_complex_expression(self):
        """Should reject complex expressions like o.id + 1 == p.product_id"""
        from ltseq import _extract_join_keys

        orders_schema = {"id": "int64", "product_id": "int64"}
        products_schema = {"product_id": "int64", "name": "string"}

        with pytest.raises(TypeError, match="directly"):
            _extract_join_keys(
                lambda o, p: o.id + 1 == p.product_id, orders_schema, products_schema
            )

    def test_extract_rejects_nonexistent_column_source(self):
        """Should reject when left column doesn't exist in source schema"""
        from ltseq import _extract_join_keys

        orders_schema = {"id": "int64", "product_id": "int64"}
        products_schema = {"id": "int64", "name": "string"}

        with pytest.raises((ValueError, AttributeError, TypeError)):
            _extract_join_keys(
                lambda o, p: o.nonexistent == p.product_id,
                orders_schema,
                products_schema,
            )

    def test_extract_rejects_nonexistent_column_target(self):
        """Should reject when right column doesn't exist in target schema"""
        from ltseq import _extract_join_keys

        orders_schema = {"id": "int64", "product_id": "int64"}
        products_schema = {"id": "int64", "name": "string"}

        with pytest.raises((ValueError, AttributeError, TypeError)):
            _extract_join_keys(
                lambda o, p: o.product_id == p.nonexistent,
                orders_schema,
                products_schema,
            )

    def test_extract_with_multiple_valid_columns(self):
        """Should work with any valid columns from the schemas"""
        from ltseq import _extract_join_keys

        orders_schema = {
            "order_id": "int64",
            "customer_id": "int64",
            "product_id": "int64",
        }
        products_schema = {
            "product_id": "int64",
            "category_id": "int64",
            "name": "string",
        }

        # Join on customer_id (which doesn't actually make semantic sense, but should be allowed)
        left_key, right_key, join_type = _extract_join_keys(
            lambda o, p: o.customer_id == p.product_id, orders_schema, products_schema
        )

        assert left_key == {"type": "Column", "name": "customer_id"}
        assert right_key == {"type": "Column", "name": "product_id"}
        assert join_type == "inner"

    def test_extract_rejects_literal_on_left(self):
        """Should reject joins like lambda o, p: 42 == p.product_id"""
        from ltseq import _extract_join_keys

        orders_schema = {"id": "int64", "product_id": "int64"}
        products_schema = {"product_id": "int64", "name": "string"}

        with pytest.raises(TypeError, match="directly"):
            _extract_join_keys(
                lambda o, p: 42 == p.product_id, orders_schema, products_schema
            )

    def test_extract_rejects_literal_on_right(self):
        """Should reject joins like lambda o, p: o.id == 42"""
        from ltseq import _extract_join_keys

        orders_schema = {"id": "int64"}
        products_schema = {"id": "int64"}

        with pytest.raises(TypeError, match="directly"):
            _extract_join_keys(lambda o, p: o.id == 42, orders_schema, products_schema)

    def test_extract_preserves_column_names_with_special_chars(self):
        """Should work with column names containing underscores"""
        from ltseq import _extract_join_keys

        orders_schema = {"order_id": "int64", "product_id": "int64"}
        products_schema = {"product_id": "int64", "product_name": "string"}

        left_key, right_key, join_type = _extract_join_keys(
            lambda o, p: o.product_id == p.product_id, orders_schema, products_schema
        )

        assert left_key == {"type": "Column", "name": "product_id"}
        assert right_key == {"type": "Column", "name": "product_id"}
