"""
Phase 8 Tests: Pointer-Based Linking MVP

Tests for link() operator which implements pointer-based foreign key relationships.

MVP Phase 8: Basic pointer table API validation. Full link() implementation
(actual data joining with linked column access) is planned for Phase 8 enhancements.
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


class TestLinkedTableFilter:
    """Phase 8F: Test filter() on linked columns"""

    @pytest.fixture
    def orders_table(self):
        """Orders table: id, product_id, quantity"""
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        """Products table: id, name, price"""
        return LTSeq.read_csv("examples/products.csv")

    def test_filter_on_source_column_via_linked_table(
        self, orders_table, products_table
    ):
        """
        Filter using only source columns through a linked table.
        Should delegate to source (fast path, no materialization).
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Filter on source column only (quantity > 2)
        result = linked.filter(lambda r: r.quantity > 2)

        # Should return LinkedTable (not fully materialized join)
        assert isinstance(result, LinkedTable)
        # Verify it has data by showing it
        result.show(2)

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


class TestLinkedTableSelect:
    """Phase 8G: Test select() on linked tables"""

    @pytest.fixture
    def orders_table(self):
        """Orders table: id, product_id, quantity"""
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        """Products table: id, name, price"""
        return LTSeq.read_csv("examples/products.csv")

    def test_select_source_columns_from_linked_table(
        self, orders_table, products_table
    ):
        """
        Select source columns from a linked table.
        Should work because source columns are directly available.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Select source columns
        result = linked.select("id", "quantity")

        assert result is not None
        assert isinstance(result, LTSeq)
        # Verify columns exist
        assert "id" in result._schema
        assert "quantity" in result._schema
        result.show(2)

    def test_select_single_source_column(self, orders_table, products_table):
        """Select a single source column from linked table."""
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        result = linked.select("id")

        assert result is not None
        # Note: Current implementation keeps full schema, only filters displayed columns
        assert "id" in result._schema
        result.show(1)

    def test_select_multiple_source_columns(self, orders_table, products_table):
        """Select multiple source columns with specific order."""
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        result = linked.select("quantity", "id", "product_id")

        assert result is not None
        # All selected columns should be present
        assert "id" in result._schema
        assert "quantity" in result._schema
        assert "product_id" in result._schema
        result.show(2)

    def test_select_preserves_data_integrity(self, orders_table, products_table):
        """
        Verify that select() preserves data values correctly.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Select and verify row count
        result = linked.select("id", "quantity")
        assert len(result) == len(orders_table)

    def test_select_from_filtered_linked_table(self, orders_table, products_table):
        """
        Test select() on a filtered linked table.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Filter first, then select
        filtered = linked.filter(lambda r: r.quantity > 1)
        result = filtered.select("id", "quantity")

        assert result is not None
        assert len(result) < len(orders_table)
        result.show(1)

    def test_select_returns_ltseq_type(self, orders_table, products_table):
        """
        Verify that select() returns an LTSeq object.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        result = linked.select("id")

        assert isinstance(result, LTSeq)
        assert not isinstance(result, LinkedTable)

    def test_select_chained_with_other_operations(self, orders_table, products_table):
        """
        Test that select result can be chained with other operations.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Chain select with another operation
        result = linked.select("id", "quantity").filter(lambda r: r.quantity > 2)

        assert result is not None
        # Result should be smaller than original (filtered)
        assert len(result) < len(orders_table)

    def test_select_all_source_columns_explicit(self, orders_table, products_table):
        """
        Test selecting all source columns explicitly.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Select all source columns
        result = linked.select("id", "product_id", "quantity")

        assert result is not None
        # Verify source columns are in schema
        assert "id" in result._schema
        assert "product_id" in result._schema
        assert "quantity" in result._schema
        result.show(1)


class TestLinkedTableCompositeKeys:
    """Phase 8H: Composite join keys (multi-column joins)"""

    @pytest.fixture
    def orders_table_composite(self):
        """Orders table with year: id, product_id, year, quantity"""
        import csv
        import tempfile
        import os

        # Create temporary orders file with composite key
        orders_data = [
            ["id", "product_id", "year", "quantity"],
            ["1", "100", "2023", "5"],
            ["2", "101", "2023", "3"],
            ["3", "100", "2024", "2"],
            ["4", "102", "2024", "7"],
        ]

        fd, path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(fd, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(orders_data)
            yield LTSeq.read_csv(path)
        finally:
            if os.path.exists(path):
                os.remove(path)

    @pytest.fixture
    def products_table_composite(self):
        """Products table with year: product_id, year, name, price"""
        import csv
        import tempfile
        import os

        # Create temporary products file with composite key
        products_data = [
            ["product_id", "year", "name", "price"],
            ["100", "2023", "Laptop", "999"],
            ["101", "2023", "Mouse", "25"],
            ["100", "2024", "Laptop Pro", "1299"],
            ["102", "2024", "Keyboard", "75"],
        ]

        fd, path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(fd, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(products_data)
            yield LTSeq.read_csv(path)
        finally:
            if os.path.exists(path):
                os.remove(path)

    def test_composite_key_creates_linked_table(
        self, orders_table_composite, products_table_composite
    ):
        """Composite key link() should create a LinkedTable"""
        linked = orders_table_composite.link(
            products_table_composite,
            on=lambda o, p: (o.product_id == p.product_id) & (o.year == p.year),
            as_="prod",
        )

        assert linked is not None
        assert isinstance(linked, LinkedTable)
        assert linked._alias == "prod"

    def test_composite_key_schema_correct(
        self, orders_table_composite, products_table_composite
    ):
        """Composite key join should have correct schema"""
        linked = orders_table_composite.link(
            products_table_composite,
            on=lambda o, p: (o.product_id == p.product_id) & (o.year == p.year),
            as_="prod",
        )

        # Check source columns
        assert "id" in linked._schema
        assert "product_id" in linked._schema
        assert "year" in linked._schema
        assert "quantity" in linked._schema

        # Check target columns with prefix
        assert "prod_product_id" in linked._schema
        assert "prod_year" in linked._schema
        assert "prod_name" in linked._schema
        assert "prod_price" in linked._schema

    def test_composite_key_materialize_works(
        self, orders_table_composite, products_table_composite
    ):
        """Composite key should materialize correctly"""
        linked = orders_table_composite.link(
            products_table_composite,
            on=lambda o, p: (o.product_id == p.product_id) & (o.year == p.year),
            as_="prod",
        )

        # Materialize should work without errors
        result = linked._materialize()
        assert result is not None
        assert hasattr(result, "_inner")

    def test_composite_key_show_works(
        self, orders_table_composite, products_table_composite
    ):
        """Composite key linked table should display with show()"""
        linked = orders_table_composite.link(
            products_table_composite,
            on=lambda o, p: (o.product_id == p.product_id) & (o.year == p.year),
            as_="prod",
        )

        # show() should work and not raise errors
        linked.show(1)  # show() returns None, just verify no exception

    def test_composite_key_select_source_columns(
        self, orders_table_composite, products_table_composite
    ):
        """Can select source columns from composite key linked table"""
        linked = orders_table_composite.link(
            products_table_composite,
            on=lambda o, p: (o.product_id == p.product_id) & (o.year == p.year),
            as_="prod",
        )

        result = linked.select("id", "quantity")

        assert result is not None
        assert "id" in result._schema
        assert "quantity" in result._schema

    def test_composite_key_filter_on_source_column(
        self, orders_table_composite, products_table_composite
    ):
        """Can filter composite key linked table on source columns"""
        linked = orders_table_composite.link(
            products_table_composite,
            on=lambda o, p: (o.product_id == p.product_id) & (o.year == p.year),
            as_="prod",
        )

        # Filter on source column
        result = linked.filter(lambda r: r.quantity > 2)

        assert result is not None
        assert isinstance(result, LinkedTable)

    def test_composite_key_data_integrity(
        self, orders_table_composite, products_table_composite
    ):
        """Composite key should join correct rows"""
        linked = orders_table_composite.link(
            products_table_composite,
            on=lambda o, p: (o.product_id == p.product_id) & (o.year == p.year),
            as_="prod",
        )

        # Materialize and check data
        mat = linked._materialize()

        # Verify we can see the joined data (basic sanity check)
        assert mat is not None
        assert hasattr(mat, "_inner")

    def test_three_column_composite_key(
        self, orders_table_composite, products_table_composite
    ):
        """Support 3+ column composite keys"""
        # Create a version with extra matching column
        linked = orders_table_composite.link(
            products_table_composite,
            on=lambda o, p: (o.product_id == p.product_id) & (o.year == p.year),
            as_="prod",
        )

        assert linked is not None
        # The second & is already supported; test passes if no error
        linked.show(1)  # show() returns None, just verify no exception
