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
        len(linked) should equal len(linked._ensure_join_plan()).
        """
        linked = orders_table.link(
            products_table, on=lambda o, p: o.product_id == p.product_id, as_="prod"
        )

        # Compare len() with explicit materialization
        linked_len = len(linked)
        materialized_len = len(linked._ensure_join_plan())
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

    def test_sort_returns_ltseq(self, orders_table, products_table):
        """sort() runs on the joined plan and returns a plain LTSeq (issue #125)."""
        linked = self._make_linked(orders_table, products_table)
        sorted_linked = linked.sort("quantity")
        assert isinstance(sorted_linked, LTSeq)

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

    def test_slice_returns_ltseq(self, orders_table, products_table):
        """slice() runs on the joined plan and returns a plain LTSeq (issue #125)."""
        linked = self._make_linked(orders_table, products_table)
        sliced = linked.slice(0, 2)
        assert isinstance(sliced, LTSeq)

    def test_slice_reduces_rows(self, orders_table, products_table):
        """slice(0, 2) returns at most 2 rows."""
        linked = self._make_linked(orders_table, products_table)
        sliced = linked.slice(0, 2)
        assert len(sliced) <= 2

    def test_distinct_returns_ltseq(self, orders_table, products_table):
        """distinct() runs on the joined plan and returns a plain LTSeq (issue #125)."""
        linked = self._make_linked(orders_table, products_table)
        distinct_linked = linked.distinct()
        assert isinstance(distinct_linked, LTSeq)

    def test_distinct_preserves_schema(self, orders_table, products_table):
        """distinct() preserves schema including linked columns."""
        linked = self._make_linked(orders_table, products_table)
        distinct_linked = linked.distinct()
        assert "prod_name" in distinct_linked._schema


class TestLinkedTableJoinTypePropagation:
    """Regression tests: transforms must honor join_type (issues #92, #125).

    Previously filter/derive/sort/slice/distinct rebuilt the LinkedTable
    without passing join_type, silently downgrading left joins to inner
    (#92). After #125 the transforms run on the joined plan itself and
    return a plain LTSeq, so the join type is baked into the plan — the
    guarantee is now checked through the surviving unmatched row, not a
    ``_join_type`` attribute on the result.
    """

    @pytest.fixture
    def orders_with_unmatched(self, tmp_path):
        """Orders table containing a product_id absent from products."""
        p = tmp_path / "orders_unmatched.csv"
        p.write_text(
            "id,product_id,quantity\n"
            "1,101,5\n"
            "2,102,3\n"
            "3,999,7\n"  # 999 has no match in products.csv
        )
        return LTSeq.read_csv(str(p))

    @pytest.fixture
    def products_table(self):
        return LTSeq.read_csv("examples/products.csv")

    def _make_left_linked(self, orders, products):
        return orders.link(
            products,
            on=lambda o, p: o.product_id == p.product_id,
            as_="prod",
            join_type="left",
        )

    def test_transforms_return_ltseq_and_keep_left_semantics(
        self, orders_with_unmatched, products_table
    ):
        linked = self._make_left_linked(orders_with_unmatched, products_table)
        assert linked._join_type == "left"

        # Each transform runs on the left-joined plan (3 rows, unmatched
        # id=3 kept) and returns a plain LTSeq — never silently downgrading
        # to an inner join that would drop id=3.
        derived = linked.derive(qty2=lambda r: r.quantity * 2)
        assert isinstance(derived, LTSeq)
        assert len(derived) == 3

        assert len(linked.sort("id")) == 3
        assert len(linked.distinct()) == 3

        filtered = linked.filter(lambda r: r.quantity > 0)
        assert isinstance(filtered, LTSeq)
        assert len(filtered) == 3

    def test_left_join_row_kept_after_transform(
        self, orders_with_unmatched, products_table
    ):
        """The unmatched row must survive materialization after a transform."""
        linked = self._make_left_linked(orders_with_unmatched, products_table)

        # Baseline: left join keeps all 3 orders
        assert len(linked) == 3

        # After sort (a rebuilding transform), left semantics must survive.
        # Before the fix this materialized as an inner join and dropped id=3.
        assert len(linked.sort("id")) == 3
        assert len(linked.filter(lambda r: r.quantity > 0)) == 3
