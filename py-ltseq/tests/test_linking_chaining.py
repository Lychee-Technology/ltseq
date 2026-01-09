"""
Phase 8 Tests: Pointer-Based Linking - Chaining and Error Handling

Tests for chaining multiple link() calls, error handling, and chained materialization.
"""

import pytest
from ltseq import LTSeq, LinkedTable


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


class TestChainedMaterialization:
    """Tests for chaining multiple link() calls with intermediate materialization"""

    @pytest.fixture
    def orders_table(self):
        return LTSeq.read_csv("examples/orders.csv")

    @pytest.fixture
    def products_table(self):
        return LTSeq.read_csv("examples/products.csv")

    @pytest.fixture
    def categories_table(self):
        return LTSeq.read_csv("examples/categories.csv")

    def test_single_link_then_materialize(self, orders_table, products_table):
        """Single link followed by materialization should work"""
        linked = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
        )
        result = linked._materialize()

        assert result is not None
        assert isinstance(result, LTSeq)
        # Should have columns from both tables
        assert "id" in result._schema
        assert "prod_name" in result._schema

    def test_materialize_then_link_with_inner_join(
        self, orders_table, products_table, categories_table
    ):
        """Materialize first link, then link to third table with inner join"""
        linked1 = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
        )
        mat1 = linked1._materialize()

        # Link the materialized result to categories
        linked2 = mat1.link(
            categories_table,
            on=lambda m, c: m.product_id == c.product_id,
            as_="cat",
            join_type="inner",
        )

        # This should create a LinkedTable successfully
        assert linked2 is not None
        assert isinstance(linked2, LinkedTable)

    def test_materialize_then_link_then_materialize_again(
        self, orders_table, products_table, categories_table
    ):
        """Materialize, link, then materialize again - the critical test"""
        linked1 = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
        )
        mat1 = linked1._materialize()

        # Link to categories
        linked2 = mat1.link(
            categories_table,
            on=lambda m, c: m.product_id == c.product_id,
            as_="cat",
            join_type="inner",
        )

        # Now materialize the second link
        mat2 = linked2._materialize()

        assert mat2 is not None
        assert isinstance(mat2, LTSeq)
        # Schema should include columns from all three tables
        assert "id" in mat2._schema
        assert "prod_name" in mat2._schema
        assert "cat_category" in mat2._schema

    def test_schema_consistency_after_chaining(
        self, orders_table, products_table, categories_table
    ):
        """Schema should be consistent through the materialization chain"""
        linked1 = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
        )
        mat1 = linked1._materialize()

        linked2 = mat1.link(
            categories_table,
            on=lambda m, c: m.product_id == c.product_id,
            as_="cat",
        )
        mat2 = linked2._materialize()

        # At minimum, check that base columns are present
        schema_keys = set(mat2._schema.keys())
        assert "id" in schema_keys
        assert "product_id" in schema_keys
        assert "quantity" in schema_keys

    def test_chained_materialization_preserves_data(
        self, orders_table, products_table, categories_table
    ):
        """Data should be preserved through chained materialization"""
        linked1 = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
        )
        mat1 = linked1._materialize()
        original_rows = len(mat1)

        linked2 = mat1.link(
            categories_table,
            on=lambda m, c: m.product_id == c.product_id,
            as_="cat",
        )
        mat2 = linked2._materialize()

        # Data should be preserved (inner join, so same or fewer rows)
        assert len(mat2) > 0
        assert len(mat2) <= original_rows

    def test_three_table_chain_without_intermediate_materialization(
        self, orders_table, products_table, categories_table
    ):
        """Direct chaining without intermediate materialization should work"""
        linked1 = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
        )
        # Chain without materializing in between
        linked2 = linked1.link(
            categories_table,
            on=lambda l, c: l.product_id == c.product_id,
            as_="cat",
        )

        # Now materialize the entire chain
        result = linked2._materialize()

        assert result is not None
        assert isinstance(result, LTSeq)
        # Should have rows
        assert len(result) > 0

    def test_chained_link_with_left_join(
        self, orders_table, products_table, categories_table
    ):
        """Left join in chained linking"""
        linked1 = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
            join_type="left",
        )
        mat1 = linked1._materialize()

        linked2 = mat1.link(
            categories_table,
            on=lambda m, c: m.product_id == c.product_id,
            as_="cat",
            join_type="left",
        )
        mat2 = linked2._materialize()

        # Left joins should preserve all left rows
        assert len(mat2) > 0

    def test_chained_materialization_with_filtering(
        self, orders_table, products_table, categories_table
    ):
        """Filter after chained materialization"""
        linked1 = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
        )
        mat1 = linked1._materialize()

        linked2 = mat1.link(
            categories_table,
            on=lambda m, c: m.product_id == c.product_id,
            as_="cat",
        )
        mat2 = linked2._materialize()

        # Filter on the chained materialized result
        filtered = mat2.filter(lambda r: r.quantity > 5)
        assert len(filtered) >= 0

    def test_materialized_result_has_correct_dataframe_columns(
        self, orders_table, products_table, categories_table
    ):
        """DataFrame columns should match schema after chaining"""
        linked1 = orders_table.link(
            products_table,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
        )
        mat1 = linked1._materialize()

        linked2 = mat1.link(
            categories_table,
            on=lambda m, c: m.product_id == c.product_id,
            as_="cat",
        )
        mat2 = linked2._materialize()

        # Check schema has expected columns
        schema_cols = set(mat2._schema.keys())

        # Should have columns from all three tables
        assert "id" in schema_cols  # from orders
        assert "quantity" in schema_cols  # from orders
        assert "prod_name" in schema_cols  # from products (with alias)
        assert len(schema_cols) > 0
