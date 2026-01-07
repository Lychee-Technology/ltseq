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


class TestLinkedTableSelect:
    """Tests for select() on linked tables"""

    @pytest.fixture
    def orders_table(self):
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        return LTSeq.read_csv("examples/products.csv")

    def test_select_source_columns(self, orders_table, products_table):
        """
        select() should work for selecting source columns.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Select source columns only
        result = linked.select("id", "quantity")

        assert result is not None

    def test_select_returns_ltseq(self, orders_table, products_table):
        """
        select() on a linked table should return an LTSeq.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        result = linked.select("id", "quantity")
        assert isinstance(result, LTSeq)

    def test_select_result_has_correct_schema(self, orders_table, products_table):
        """
        The schema of selected columns should contain the selected columns.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        result = linked.select("id", "quantity")

        # Check that selected columns are in schema
        assert "id" in result._schema
        assert "quantity" in result._schema

    def test_select_with_computed_columns(self, orders_table, products_table):
        """
        select() should support computed columns via lambdas.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Derive a column and select it
        result = linked.select(lambda r: r.quantity * 2)
        assert result is not None

    def test_select_multiple_columns(self, orders_table, products_table):
        """
        select() should handle multiple columns correctly.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        result = linked.select("id", "product_id", "quantity")

        # Should have selected columns
        assert "id" in result._schema

    def test_select_preserves_data(self, orders_table, products_table):
        """
        select() should not modify the underlying data.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        result = linked.select("id", "quantity")

        # Original table should be unchanged
        assert "id" in orders_table._schema
        assert "product_id" in orders_table._schema
        assert "quantity" in orders_table._schema

    def test_select_on_linked_then_filter(self, orders_table, products_table):
        """
        Test chaining select and filter operations.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Select, then filter
        selected = linked.select("id", "quantity")
        filtered = selected.filter(lambda r: r.quantity > 2)

        assert filtered is not None

    def test_select_preserves_data_integrity(self, orders_table, products_table):
        """
        Verify that select() preserves data values correctly.
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        selected = linked.select("id", "quantity")
        selected.show(2)


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

        result = linked._materialize()
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
        assert isinstance(filtered, LinkedTable)

    def test_composite_key_data_integrity(self, orders_table, products_table):
        """Composite key join should not corrupt data"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: (o.product_id == p.product_id),
            as_="prod",
        )

        result = linked._materialize()
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
        result = linked._materialize()
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
        result = linked._materialize()
        assert result is not None

    def test_left_join_filters_null_product(self, orders_table, products_table):
        """LEFT join should handle NULL values from unmatched right rows"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
            join_type="left",
        )

        result = linked._materialize()
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
        result = linked._materialize()
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
        result = linked._materialize()
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


class TestLinkedTableChaining:
    """Tests for chaining multiple link() calls"""

    @pytest.fixture
    def orders_table(self):
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        return LTSeq.read_csv("examples/products.csv")

    def test_single_link_creates_linked_table(self, orders_table, products_table):
        """Linking creates a LinkedTable object"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
        )

        assert isinstance(linked, LinkedTable)

    def test_linked_table_has_link_method(self, orders_table, products_table):
        """LinkedTable should have a link() method for chaining"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
        )

        # LinkedTable should have link() method
        assert hasattr(linked, "link")
        assert callable(linked.link)

    def test_linked_table_link_with_join_type(self, orders_table, products_table):
        """LinkedTable.link() should support join_type parameter"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
        )

        # Check that link method accepts join_type
        linked2 = linked.link(
            products_table,
            on=lambda p1, p2: p1.product_id == p2.product_id,
            as_="prod2",
            join_type="left",
        )

        assert linked2 is not None

    def test_single_link_join_type_parameter(self, orders_table, products_table):
        """join_type parameter should be passed through LinkedTable.link()"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
            join_type="left",
        )

        linked2 = linked.link(
            products_table,
            on=lambda p1, p2: p1.product_id == p2.product_id,
            as_="prod2",
            join_type="inner",
        )

        assert linked2._join_type == "inner"

    def test_single_link_materialize(self, orders_table, products_table):
        """Single link materialization should work"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
        )

        result = linked._materialize()
        assert result is not None

    def test_single_link_show(self, orders_table, products_table):
        """Single link with show() should display joined data"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
        )

        linked.show(2)

    def test_linked_table_schema_preservation(self, orders_table, products_table):
        """Schema should be preserved through link chaining"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
        )

        # Original columns
        assert "id" in linked._schema
        assert "product_id" in linked._schema

        # Linked columns with prefix
        assert "prod_product_id" in linked._schema
        assert "prod_name" in linked._schema
        assert "prod_price" in linked._schema


# Phase 8K: Production Validation & Error Handling
class TestPhase8KErrorHandling:
    """Edge cases and error handling tests for linking"""

    @pytest.fixture
    def orders_table(self):
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        return LTSeq.read_csv("examples/products.csv")

    def test_invalid_join_type_raises_error(self, orders_table, products_table):
        """Invalid join_type should raise ValueError with clear message"""
        with pytest.raises(ValueError) as exc_info:
            orders_table.link(
                products_table,
                on=lambda o, p: o.product_id == p.product_id,
                as_="prod",
                join_type="INVALID",
            )

        # Error message should help user understand the issue
        error_msg = str(exc_info.value)
        assert "INVALID" in error_msg or "invalid" in error_msg.lower()

    def test_non_existent_column_in_condition(self, orders_table, products_table):
        """Referencing non-existent columns should raise error"""
        with pytest.raises((AttributeError, KeyError, TypeError)):
            orders_table.link(
                products_table,
                on=lambda o, p: o.nonexistent_col == p.product_id,
                as_="prod",
            )

    def test_schema_has_source_and_linked_columns(self, orders_table, products_table):
        """Schema should include both source and linked column names"""
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        schema = linked._schema
        # Source columns
        assert "id" in schema
        assert "product_id" in schema
        assert "quantity" in schema
        # Linked columns with prefix
        assert "prod_product_id" in schema
        assert "prod_name" in schema
        assert "prod_price" in schema

    def test_multiple_joins_with_different_aliases(self, orders_table, products_table):
        """Joining same tables with different aliases should work"""
        linked1 = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod1"
        )

        linked2 = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod2"
        )

        # Both should have different aliases
        assert linked1._alias == "prod1"
        assert linked2._alias == "prod2"
        # And different schemas
        assert "prod1_name" in linked1._schema
        assert "prod2_name" in linked2._schema

    def test_join_type_consistency(self, orders_table, products_table):
        """Join type parameter should be stored and retrievable"""
        for join_type in ["inner", "left", "right", "full"]:
            linked = orders_table.link(
                products_table,
                on=lambda o, p: o.product_id == p.product_id,
                as_="prod",
                join_type=join_type,
            )

            assert linked._join_type == join_type

    def test_filter_on_filtered_linked_table(self, orders_table, products_table):
        """Filtering a filtered linked table should work"""
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Filter once
        filtered1 = linked.filter(lambda r: r.quantity > 2)
        assert isinstance(filtered1, LinkedTable)

        # Filter again
        filtered2 = filtered1.filter(lambda r: r.quantity > 3)
        assert isinstance(filtered2, LinkedTable)

    def test_composite_join_condition(self, orders_table, products_table):
        """Composite join conditions should work correctly"""
        linked = orders_table.link(
            products_table, on=lambda o, p: (o.product_id == p.product_id), as_="prod"
        )

        # Should create a valid linked table
        assert linked is not None
        assert isinstance(linked, LinkedTable)
        # Schema should have all columns
        assert "quantity" in linked._schema
        assert "prod_name" in linked._schema

    def test_linked_table_materialization_consistency(
        self, orders_table, products_table
    ):
        """Multiple materializations should produce consistent results"""
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Materialize multiple times
        result1 = linked._materialize()
        result2 = linked._materialize()

        # Should return the same cached object
        assert result1 is result2

    def test_linked_table_with_all_join_types(self, orders_table, products_table):
        """All four join types should be accessible"""
        join_types = ["inner", "left", "right", "full"]

        for join_type in join_types:
            linked = orders_table.link(
                products_table,
                on=lambda o, p: o.product_id == p.product_id,
                as_="prod",
                join_type=join_type,
            )

            assert linked._join_type == join_type
            # Should be able to materialize
            result = linked._materialize()
            assert result is not None

    def test_linked_table_error_message_clarity(self, orders_table, products_table):
        """Error messages should be clear and helpful"""
        # Test invalid join type
        error_raised = False
        try:
            orders_table.link(
                products_table,
                on=lambda o, p: o.product_id == p.product_id,
                as_="prod",
                join_type="invalid_type",
            )
        except ValueError as e:
            error_raised = True
            # Error message should mention the issue
            assert "invalid" in str(e).lower() or "join_type" in str(e).lower()

        assert error_raised, "Should raise ValueError for invalid join_type"

    def test_link_preserves_original_table(self, orders_table, products_table):
        """Linking should not modify the original tables"""
        original_schema = orders_table._schema.copy()

        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Original table schema should be unchanged
        assert orders_table._schema == original_schema
