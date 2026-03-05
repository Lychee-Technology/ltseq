"""
Tests for property-style group aggregation access (5.2).

Both styles should be supported:
    g.avg('price')   # string-style (existing)
    g.price.avg()    # property-style (new)

This tests two proxy systems:
- GroupProxy (System A): used in filter(), computes real values via pandas fallback
- DeriveGroupProxy (System B): used in derive(), captures expressions as GroupAggExpr
"""

import csv
import os
import pytest
from ltseq import LTSeq


@pytest.fixture
def trades_csv(tmp_path):
    """Create a CSV with trade data for group testing."""
    csv_file = str(tmp_path / "trades.csv")
    with open(csv_file, "w") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "symbol", "price", "quantity", "side"])
        # Group 1: 3 buys
        writer.writerow(["2024-01-01", "AAPL", "150", "10", "buy"])
        writer.writerow(["2024-01-02", "AAPL", "155", "20", "buy"])
        writer.writerow(["2024-01-03", "AAPL", "160", "15", "buy"])
        # Group 2: 2 sells
        writer.writerow(["2024-01-04", "AAPL", "158", "25", "sell"])
        writer.writerow(["2024-01-05", "AAPL", "152", "30", "sell"])
        # Group 3: 4 buys
        writer.writerow(["2024-01-06", "AAPL", "154", "12", "buy"])
        writer.writerow(["2024-01-07", "AAPL", "157", "18", "buy"])
        writer.writerow(["2024-01-08", "AAPL", "161", "22", "buy"])
        writer.writerow(["2024-01-09", "AAPL", "165", "8", "buy"])
        # Group 4: 1 sell
        writer.writerow(["2024-01-10", "AAPL", "163", "35", "sell"])
    yield csv_file


class TestGroupProxyPropertyStyle:
    """Test property-style access on GroupProxy (System A, used in filter)."""

    def test_filter_property_max(self, trades_csv):
        """g.price.max() in filter should work like g.max('price')."""
        t = LTSeq.read_csv(trades_csv).sort("date")
        # Property-style
        r1 = t.group_ordered(lambda r: r.side).filter(
            lambda g: g.price.max() > 160
        )
        # String-style
        r2 = t.group_ordered(lambda r: r.side).filter(
            lambda g: g.max("price") > 160
        )
        assert len(r1) == len(r2)

    def test_filter_property_min(self, trades_csv):
        """g.price.min() in filter should work like g.min('price')."""
        t = LTSeq.read_csv(trades_csv).sort("date")
        r1 = t.group_ordered(lambda r: r.side).filter(
            lambda g: g.price.min() < 155
        )
        r2 = t.group_ordered(lambda r: r.side).filter(
            lambda g: g.min("price") < 155
        )
        assert len(r1) == len(r2)

    def test_filter_property_sum(self, trades_csv):
        """g.quantity.sum() in filter should work like g.sum('quantity')."""
        t = LTSeq.read_csv(trades_csv).sort("date")
        r1 = t.group_ordered(lambda r: r.side).filter(
            lambda g: g.quantity.sum() > 50
        )
        r2 = t.group_ordered(lambda r: r.side).filter(
            lambda g: g.sum("quantity") > 50
        )
        assert len(r1) == len(r2)

    def test_filter_property_avg(self, trades_csv):
        """g.price.avg() in filter should work like g.avg('price')."""
        t = LTSeq.read_csv(trades_csv).sort("date")
        r1 = t.group_ordered(lambda r: r.side).filter(
            lambda g: g.price.avg() > 155
        )
        r2 = t.group_ordered(lambda r: r.side).filter(
            lambda g: g.avg("price") > 155
        )
        assert len(r1) == len(r2)

    def test_filter_property_median(self, trades_csv):
        """g.price.median() should work in filter."""
        t = LTSeq.read_csv(trades_csv).sort("date")
        result = t.group_ordered(lambda r: r.side).filter(
            lambda g: g.price.median() > 155
        )
        assert result is not None

    def test_filter_property_variance(self, trades_csv):
        """g.price.variance() and g.price.var() should work in filter."""
        t = LTSeq.read_csv(trades_csv).sort("date")
        r1 = t.group_ordered(lambda r: r.side).filter(
            lambda g: g.price.variance() is not None
        )
        r2 = t.group_ordered(lambda r: r.side).filter(
            lambda g: g.price.var() is not None
        )
        assert len(r1) == len(r2)

    def test_filter_property_std(self, trades_csv):
        """g.price.std() and g.price.stddev() should work in filter."""
        t = LTSeq.read_csv(trades_csv).sort("date")
        r1 = t.group_ordered(lambda r: r.side).filter(
            lambda g: g.price.std() is not None
        )
        r2 = t.group_ordered(lambda r: r.side).filter(
            lambda g: g.price.stddev() is not None
        )
        assert len(r1) == len(r2)

    def test_filter_property_mode(self, trades_csv):
        """g.side.mode() should work in filter (all same within each group)."""
        t = LTSeq.read_csv(trades_csv).sort("date")
        result = t.group_ordered(lambda r: r.side).filter(
            lambda g: g.side.mode() == "buy"
        )
        # Groups 1 and 3 are buy groups
        assert result is not None

    def test_filter_property_concat_agg(self, trades_csv):
        """g.symbol.concat_agg() should work in filter."""
        t = LTSeq.read_csv(trades_csv).sort("date")
        result = t.group_ordered(lambda r: r.side).filter(
            lambda g: len(g.symbol.concat_agg(",")) > 0
        )
        assert len(result) == 10  # All groups pass

    def test_filter_invalid_column_raises(self, trades_csv):
        """Accessing a non-existent column should raise AttributeError."""
        t = LTSeq.read_csv(trades_csv).sort("date")
        with pytest.raises((ValueError, AttributeError)):
            t.group_ordered(lambda r: r.side).filter(
                lambda g: g.nonexistent.avg() > 0
            )

    def test_filter_property_combined_with_count(self, trades_csv):
        """Property-style can be combined with other group methods."""
        t = LTSeq.read_csv(trades_csv).sort("date")
        result = t.group_ordered(lambda r: r.side).filter(
            lambda g: g.count() > 2 and g.price.avg() > 155
        )
        assert result is not None


class TestDeriveGroupProxyPropertyStyle:
    """Test property-style access on DeriveGroupProxy (System B, used in derive)."""

    def test_derive_property_max(self, trades_csv):
        """g.price.max() in derive should produce same result as g.max('price')."""
        t = LTSeq.read_csv(trades_csv).sort("date")
        r1 = t.group_ordered(lambda r: r.side).derive(
            lambda g: {"max_price": g.price.max()}
        )
        r2 = t.group_ordered(lambda r: r.side).derive(
            lambda g: {"max_price": g.max("price")}
        )
        assert "max_price" in r1._schema
        df1 = r1.to_pandas()
        df2 = r2.to_pandas()
        assert list(df1["max_price"]) == list(df2["max_price"])

    def test_derive_property_min(self, trades_csv):
        """g.price.min() in derive should produce same result as g.min('price')."""
        t = LTSeq.read_csv(trades_csv).sort("date")
        r1 = t.group_ordered(lambda r: r.side).derive(
            lambda g: {"min_price": g.price.min()}
        )
        r2 = t.group_ordered(lambda r: r.side).derive(
            lambda g: {"min_price": g.min("price")}
        )
        df1 = r1.to_pandas()
        df2 = r2.to_pandas()
        assert list(df1["min_price"]) == list(df2["min_price"])

    def test_derive_property_sum(self, trades_csv):
        """g.quantity.sum() in derive should produce same result as g.sum('quantity')."""
        t = LTSeq.read_csv(trades_csv).sort("date")
        r1 = t.group_ordered(lambda r: r.side).derive(
            lambda g: {"total_qty": g.quantity.sum()}
        )
        r2 = t.group_ordered(lambda r: r.side).derive(
            lambda g: {"total_qty": g.sum("quantity")}
        )
        df1 = r1.to_pandas()
        df2 = r2.to_pandas()
        assert list(df1["total_qty"]) == list(df2["total_qty"])

    def test_derive_property_avg(self, trades_csv):
        """g.price.avg() in derive should produce same result as g.avg('price')."""
        t = LTSeq.read_csv(trades_csv).sort("date")
        r1 = t.group_ordered(lambda r: r.side).derive(
            lambda g: {"avg_price": g.price.avg()}
        )
        r2 = t.group_ordered(lambda r: r.side).derive(
            lambda g: {"avg_price": g.avg("price")}
        )
        df1 = r1.to_pandas()
        df2 = r2.to_pandas()
        assert list(df1["avg_price"]) == list(df2["avg_price"])

    def test_derive_property_mixed_with_count(self, trades_csv):
        """Property-style can be mixed with other derive expressions."""
        t = LTSeq.read_csv(trades_csv).sort("date")
        result = t.group_ordered(lambda r: r.side).derive(
            lambda g: {
                "group_size": g.count(),
                "max_price": g.price.max(),
                "start_date": g.first().date,
            }
        )
        assert "group_size" in result._schema
        assert "max_price" in result._schema
        assert "start_date" in result._schema

    def test_derive_property_arithmetic(self, trades_csv):
        """Property-style expressions should support arithmetic."""
        t = LTSeq.read_csv(trades_csv).sort("date")
        result = t.group_ordered(lambda r: r.side).derive(
            lambda g: {"price_range": g.price.max() - g.price.min()}
        )
        assert "price_range" in result._schema
        df = result.to_pandas()
        # All price_range values should be >= 0
        assert all(v >= 0 for v in df["price_range"])


class TestDeriveColumnProxyReturnsGroupAggExpr:
    """Test that DeriveColumnProxy correctly returns GroupAggExpr objects."""

    def test_derive_column_proxy_types(self):
        """DeriveColumnProxy methods should return GroupAggExpr."""
        from ltseq.grouping.proxies.derive_proxy import DeriveColumnProxy, DeriveGroupProxy
        from ltseq.grouping.expr import GroupAggExpr

        proxy = DeriveGroupProxy()
        col = proxy.price  # returns DeriveColumnProxy

        assert isinstance(col.max(), GroupAggExpr)
        assert isinstance(col.min(), GroupAggExpr)
        assert isinstance(col.sum(), GroupAggExpr)
        assert isinstance(col.avg(), GroupAggExpr)

    def test_derive_column_proxy_serialization(self):
        """DeriveColumnProxy expressions should serialize correctly."""
        from ltseq.grouping.proxies.derive_proxy import DeriveGroupProxy

        proxy = DeriveGroupProxy()

        max_expr = proxy.price.max()
        assert max_expr.serialize() == {
            "type": "GroupAgg",
            "func": "max",
            "column": "price",
        }

        min_expr = proxy.quantity.min()
        assert min_expr.serialize() == {
            "type": "GroupAgg",
            "func": "min",
            "column": "quantity",
        }

    def test_derive_column_proxy_arithmetic_serialization(self):
        """Arithmetic on property-style expressions should serialize correctly."""
        from ltseq.grouping.proxies.derive_proxy import DeriveGroupProxy

        proxy = DeriveGroupProxy()
        expr = proxy.price.max() - proxy.price.min()
        serialized = expr.serialize()

        assert serialized["type"] == "BinOp"
        assert serialized["op"] == "-"
        assert serialized["left"] == {
            "type": "GroupAgg",
            "func": "max",
            "column": "price",
        }
        assert serialized["right"] == {
            "type": "GroupAgg",
            "func": "min",
            "column": "price",
        }

    def test_derive_group_proxy_getattr_underscore(self):
        """Accessing _private attributes should raise AttributeError."""
        from ltseq.grouping.proxies.derive_proxy import DeriveGroupProxy

        proxy = DeriveGroupProxy()
        with pytest.raises(AttributeError):
            _ = proxy._internal

    def test_derive_group_proxy_string_vs_property_equivalence(self):
        """String-style and property-style should produce identical expressions."""
        from ltseq.grouping.proxies.derive_proxy import DeriveGroupProxy

        proxy = DeriveGroupProxy()

        # String-style
        s1 = proxy.max("price").serialize()
        # Property-style
        s2 = proxy.price.max().serialize()

        assert s1 == s2


class TestGroupColumnProxyDirect:
    """Test GroupColumnProxy directly with mock group data."""

    def test_group_column_proxy_delegates(self):
        """GroupColumnProxy methods should delegate to GroupProxy string-style."""
        from ltseq.grouping.proxies.aggregations import GroupColumnProxy, GroupAggregationMixin

        class MockGroupProxy(GroupAggregationMixin):
            def __init__(self, data):
                self._group_data = data

        data = [
            {"price": 100, "qty": 10},
            {"price": 200, "qty": 20},
            {"price": 150, "qty": 15},
        ]
        gp = MockGroupProxy(data)
        col = GroupColumnProxy(gp, "price")

        assert col.max() == 200
        assert col.min() == 100
        assert col.sum() == 450
        assert col.avg() == 150.0
        assert col.median() == 150

    def test_group_column_proxy_var_alias(self):
        """var() should be an alias for variance()."""
        from ltseq.grouping.proxies.aggregations import GroupColumnProxy, GroupAggregationMixin

        class MockGroupProxy(GroupAggregationMixin):
            def __init__(self, data):
                self._group_data = data

        data = [{"v": 10}, {"v": 20}, {"v": 30}]
        gp = MockGroupProxy(data)
        col = GroupColumnProxy(gp, "v")

        assert col.var() == col.variance()

    def test_group_column_proxy_stddev_alias(self):
        """stddev() should be an alias for std()."""
        from ltseq.grouping.proxies.aggregations import GroupColumnProxy, GroupAggregationMixin

        class MockGroupProxy(GroupAggregationMixin):
            def __init__(self, data):
                self._group_data = data

        data = [{"v": 10}, {"v": 20}, {"v": 30}]
        gp = MockGroupProxy(data)
        col = GroupColumnProxy(gp, "v")

        assert col.stddev() == col.std()

    def test_group_column_proxy_concat_agg(self):
        """concat_agg() should join string values."""
        from ltseq.grouping.proxies.aggregations import GroupColumnProxy, GroupAggregationMixin

        class MockGroupProxy(GroupAggregationMixin):
            def __init__(self, data):
                self._group_data = data

        data = [{"tag": "a"}, {"tag": "b"}, {"tag": "c"}]
        gp = MockGroupProxy(data)
        col = GroupColumnProxy(gp, "tag")

        assert col.concat_agg(",") == "a,b,c"
        assert col.concat_agg(" | ") == "a | b | c"
