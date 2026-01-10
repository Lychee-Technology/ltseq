"""Tests for pointer syntax (r.prod.name) with linked tables."""

import csv
import os
import tempfile

import pytest


class TestPointerSyntax:
    """Tests for r.prod.name nested attribute access on linked tables."""

    @pytest.fixture
    def linked_tables(self):
        """Create test orders and products tables, return linked result."""
        from ltseq import LTSeq

        # Create orders CSV
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['order_id', 'product_id', 'quantity'])
            writer.writerow([1, 101, 2])
            writer.writerow([2, 102, 1])
            writer.writerow([3, 101, 3])
            orders_path = f.name

        # Create products CSV
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['product_id', 'name', 'price'])
            writer.writerow([101, 'Widget', 10])
            writer.writerow([102, 'Gadget', 25])
            products_path = f.name

        orders = LTSeq.read_csv(orders_path)
        products = LTSeq.read_csv(products_path)

        linked = orders.link(products, on=lambda o, p: o.product_id == p.product_id, as_='prod')

        yield linked, orders_path, products_path

        os.unlink(orders_path)
        os.unlink(products_path)

    def test_pointer_select_single_column(self, linked_tables):
        """Test selecting a single linked column with pointer syntax."""
        linked, _, _ = linked_tables
        result = linked.select(lambda r: r.prod.name)
        df = result.to_pandas()
        assert 'prod_name' in df.columns
        assert set(df['prod_name'].tolist()) == {'Widget', 'Gadget'}

    def test_pointer_select_multiple_columns(self, linked_tables):
        """Test selecting multiple linked columns with pointer syntax."""
        linked, _, _ = linked_tables
        result = linked.select(
            lambda r: r.order_id,
            lambda r: r.prod.name,
            lambda r: r.prod.price
        )
        df = result.to_pandas()
        assert list(df.columns) == ['order_id', 'prod_name', 'prod_price']
        assert len(df) == 3

    def test_pointer_in_filter(self, linked_tables):
        """Test using pointer syntax in filter predicate."""
        linked, _, _ = linked_tables
        result = linked.filter(lambda r: r.prod.price > 15)
        df = result.to_pandas()
        assert len(df) == 1  # Only Gadget has price > 15
        assert df.iloc[0]['prod_name'] == 'Gadget'

    def test_pointer_in_derive(self, linked_tables):
        """Test using pointer syntax in derive expression."""
        linked, _, _ = linked_tables
        materialized = linked._materialize()
        result = materialized.derive(total=lambda r: r.quantity * r.prod.price)
        df = result.to_pandas()
        assert 'total' in df.columns
        # Order 1: 2 * 10 = 20, Order 2: 1 * 25 = 25, Order 3: 3 * 10 = 30
        totals = sorted(df['total'].tolist())
        assert totals == [20, 25, 30]

    def test_pointer_invalid_column_error(self, linked_tables):
        """Test that accessing non-existent linked column raises AttributeError."""
        linked, _, _ = linked_tables

        with pytest.raises(AttributeError) as exc_info:
            linked.select(lambda r: r.prod.nonexistent)

        assert 'nonexistent' in str(exc_info.value)
        assert 'prod' in str(exc_info.value)

    def test_pointer_invalid_alias_error(self, linked_tables):
        """Test that accessing non-existent alias raises AttributeError."""
        linked, _, _ = linked_tables

        with pytest.raises(AttributeError) as exc_info:
            linked.select(lambda r: r.other.name)

        assert 'other' in str(exc_info.value)
