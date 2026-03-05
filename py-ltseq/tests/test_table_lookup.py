"""
Tests for table-level LTSeq.lookup() method (5.3).

lookup() performs an eager LEFT JOIN with a dimension table, returning
a plain LTSeq with dimension columns prefixed by the alias.

This is distinct from the expression-level r.col.lookup() tested in
test_lookup.py.
"""

import csv
import os
import pytest
from ltseq import LTSeq


@pytest.fixture
def orders_csv(tmp_path):
    """Create a temporary orders CSV."""
    csv_file = str(tmp_path / "orders.csv")
    with open(csv_file, "w") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "product_id", "quantity"])
        writer.writerow(["1", "101", "2"])
        writer.writerow(["2", "102", "1"])
        writer.writerow(["3", "101", "3"])
        writer.writerow(["4", "103", "1"])
        writer.writerow(["5", "999", "5"])  # No matching product
    yield csv_file


@pytest.fixture
def products_csv(tmp_path):
    """Create a temporary products CSV."""
    csv_file = str(tmp_path / "products.csv")
    with open(csv_file, "w") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "price"])
        writer.writerow(["101", "Widget", "9.99"])
        writer.writerow(["102", "Gadget", "19.99"])
        writer.writerow(["103", "Doohickey", "4.99"])
    yield csv_file


class TestTableLookupBasic:
    """Test basic table-level lookup() functionality."""

    def test_lookup_returns_ltseq(self, orders_csv, products_csv):
        """lookup() should return an LTSeq instance."""
        orders = LTSeq.read_csv(orders_csv)
        products = LTSeq.read_csv(products_csv)
        result = orders.lookup(
            products, on=lambda o, p: o.product_id == p.id, as_="prod"
        )
        assert isinstance(result, LTSeq)

    def test_lookup_preserves_all_rows(self, orders_csv, products_csv):
        """LEFT JOIN should preserve all rows from the left table."""
        orders = LTSeq.read_csv(orders_csv)
        products = LTSeq.read_csv(products_csv)
        result = orders.lookup(
            products, on=lambda o, p: o.product_id == p.id, as_="prod"
        )
        assert len(result) == len(orders)

    def test_lookup_adds_dim_columns(self, orders_csv, products_csv):
        """Dimension columns should appear with alias prefix."""
        orders = LTSeq.read_csv(orders_csv)
        products = LTSeq.read_csv(products_csv)
        result = orders.lookup(
            products, on=lambda o, p: o.product_id == p.id, as_="prod"
        )
        assert "prod_id" in result._schema
        assert "prod_name" in result._schema
        assert "prod_price" in result._schema

    def test_lookup_keeps_original_columns(self, orders_csv, products_csv):
        """Original columns should still be present."""
        orders = LTSeq.read_csv(orders_csv)
        products = LTSeq.read_csv(products_csv)
        result = orders.lookup(
            products, on=lambda o, p: o.product_id == p.id, as_="prod"
        )
        assert "order_id" in result._schema
        assert "product_id" in result._schema
        assert "quantity" in result._schema

    def test_lookup_matched_values(self, orders_csv, products_csv):
        """Matched rows should have dimension values populated."""
        orders = LTSeq.read_csv(orders_csv)
        products = LTSeq.read_csv(products_csv)
        result = orders.lookup(
            products, on=lambda o, p: o.product_id == p.id, as_="prod"
        )
        df = result.to_pandas()
        # First order has product_id 101 -> Widget
        row0 = df.iloc[0]
        assert row0["prod_name"] == "Widget"

    def test_lookup_unmatched_rows_have_nulls(self, orders_csv, products_csv):
        """Unmatched rows (product_id=999) should have NULL dim columns."""
        orders = LTSeq.read_csv(orders_csv)
        products = LTSeq.read_csv(products_csv)
        result = orders.lookup(
            products, on=lambda o, p: o.product_id == p.id, as_="prod"
        )
        df = result.to_pandas()
        # Last order has product_id 999 -> no match
        unmatched = df[df["product_id"] == 999]
        assert len(unmatched) == 1
        val = unmatched.iloc[0]["prod_name"]
        assert val is None or str(val) in ("None", "nan", "")


class TestTableLookupChaining:
    """Test that table-level lookup results can be used with other operations."""

    def test_lookup_then_filter(self, orders_csv, products_csv):
        """Should be able to filter on lookup columns."""
        orders = LTSeq.read_csv(orders_csv)
        products = LTSeq.read_csv(products_csv)
        result = (
            orders.lookup(
                products, on=lambda o, p: o.product_id == p.id, as_="prod"
            )
            .filter(lambda r: r.prod_name == "Widget")
        )
        assert len(result) == 2  # Two orders for product 101 (Widget)

    def test_lookup_then_select(self, orders_csv, products_csv):
        """Should be able to select from lookup result."""
        orders = LTSeq.read_csv(orders_csv)
        products = LTSeq.read_csv(products_csv)
        result = (
            orders.lookup(
                products, on=lambda o, p: o.product_id == p.id, as_="prod"
            )
            .select(lambda r: [r.order_id, r.prod_name, r.prod_price])
        )
        df = result.to_pandas()
        # select() should produce the 3 requested columns
        assert "order_id" in df.columns
        assert "prod_name" in df.columns
        assert "prod_price" in df.columns
        assert len(df) == 5

    def test_lookup_then_sort(self, orders_csv, products_csv):
        """Should be able to sort by lookup columns."""
        orders = LTSeq.read_csv(orders_csv)
        products = LTSeq.read_csv(products_csv)
        result = (
            orders.lookup(
                products, on=lambda o, p: o.product_id == p.id, as_="prod"
            )
            .sort("prod_price")
        )
        assert result is not None


class TestTableLookupMultiple:
    """Test lookup with multiple dimension tables."""

    def test_double_lookup(self, orders_csv, products_csv, tmp_path):
        """Should be able to chain multiple lookups."""
        cats_csv = str(tmp_path / "categories.csv")
        with open(cats_csv, "w") as f:
            writer = csv.writer(f)
            writer.writerow(["cat_id", "cat_name"])
            writer.writerow(["101", "Electronics"])
            writer.writerow(["102", "Tools"])
            writer.writerow(["103", "Hardware"])

        orders = LTSeq.read_csv(orders_csv)
        products = LTSeq.read_csv(products_csv)
        categories = LTSeq.read_csv(cats_csv)

        result = (
            orders.lookup(
                products, on=lambda o, p: o.product_id == p.id, as_="prod"
            )
            .lookup(
                categories, on=lambda r, c: r.product_id == c.cat_id, as_="cat"
            )
        )
        assert "prod_name" in result._schema
        assert "cat_cat_name" in result._schema


class TestTableLookupErrors:
    """Test error handling for table-level lookup()."""

    def test_lookup_non_ltseq_raises(self, orders_csv):
        """Passing non-LTSeq as dim_table should raise TypeError."""
        orders = LTSeq.read_csv(orders_csv)
        with pytest.raises(TypeError, match="dim_table must be LTSeq"):
            orders.lookup("not_a_table", on=lambda a, b: a.id == b.id, as_="x")

    def test_lookup_non_callable_on_raises(self, orders_csv, products_csv):
        """Passing non-callable as on should raise TypeError."""
        orders = LTSeq.read_csv(orders_csv)
        products = LTSeq.read_csv(products_csv)
        with pytest.raises(TypeError, match="on must be a callable"):
            orders.lookup(products, on="not_callable", as_="prod")

    def test_lookup_empty_alias_raises(self, orders_csv, products_csv):
        """Empty as_ string should raise TypeError."""
        orders = LTSeq.read_csv(orders_csv)
        products = LTSeq.read_csv(products_csv)
        with pytest.raises(TypeError, match="as_ must be a non-empty string"):
            orders.lookup(
                products, on=lambda o, p: o.product_id == p.id, as_=""
            )

    def test_lookup_non_string_alias_raises(self, orders_csv, products_csv):
        """Non-string as_ should raise TypeError."""
        orders = LTSeq.read_csv(orders_csv)
        products = LTSeq.read_csv(products_csv)
        with pytest.raises(TypeError, match="as_ must be a non-empty string"):
            orders.lookup(
                products, on=lambda o, p: o.product_id == p.id, as_=123
            )

    def test_lookup_invalid_column_raises(self, orders_csv, products_csv):
        """Referencing non-existent column in on should raise."""
        orders = LTSeq.read_csv(orders_csv)
        products = LTSeq.read_csv(products_csv)
        with pytest.raises((TypeError, ValueError)):
            orders.lookup(
                products,
                on=lambda o, p: o.nonexistent == p.id,
                as_="prod",
            )
