"""Tests for Phase 3.2: top_k aggregate function.

Tests the top_k() aggregate which returns the K highest values
from a column within each group as a semicolon-delimited string.
"""

import pytest
import tempfile
import os
from ltseq import LTSeq


def parse_top_k(value) -> list:
    """Parse top_k result string into a list of floats."""
    # Handle None/empty
    if value is None:
        return []
    # Handle already-numeric (single value with no semicolon)
    if isinstance(value, (int, float)):
        import math

        if math.isnan(value):
            return []
        return [float(value)]
    # Handle string
    if not value or value == "":
        return []
    return [float(x) for x in str(value).split(";")]


class TestTopKBasic:
    """Basic top_k functionality tests."""

    @pytest.fixture
    def sample_table(self):
        """Create sample table with regional sales data."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("region,sales\n")
        csv.write("East,100\n")
        csv.write("East,200\n")
        csv.write("East,150\n")
        csv.write("West,300\n")
        csv.write("West,250\n")
        csv.write("West,400\n")
        csv.write("West,350\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_top_k_with_groupby(self, sample_table):
        """top_k should return string of top values per group."""
        result = sample_table.agg(
            by=lambda r: r.region, top_sales=lambda g: g.sales.top_k(2)
        )
        df = result.to_pandas()

        # Should have 2 groups
        assert len(df) == 2

        # Check that we have the expected regions
        regions = set(df["region"].tolist())
        assert regions == {"East", "West"}

    def test_top_k_returns_correct_count(self, sample_table):
        """top_k(k) should return at most k values."""
        result = sample_table.agg(
            by=lambda r: r.region, top_sales=lambda g: g.sales.top_k(2)
        )
        df = result.to_pandas()

        # Each group's top_sales should have at most 2 values
        for _, row in df.iterrows():
            top_values = parse_top_k(row["top_sales"])
            assert len(top_values) <= 2

    def test_top_k_values_descending(self, sample_table):
        """top_k should return values in descending order."""
        result = sample_table.agg(
            by=lambda r: r.region, top_sales=lambda g: g.sales.top_k(3)
        )
        df = result.to_pandas()

        for _, row in df.iterrows():
            top_values = parse_top_k(row["top_sales"])
            # Values should be in descending order
            for i in range(len(top_values) - 1):
                assert top_values[i] >= top_values[i + 1]


class TestTopKEdgeCases:
    """Edge case tests for top_k."""

    @pytest.fixture
    def small_table(self):
        """Create small table for edge case testing."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("group,value\n")
        csv.write("A,10\n")
        csv.write("A,20\n")
        csv.write("B,100\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_top_k_larger_than_group_size(self, small_table):
        """top_k(k) where k > group size should return all values."""
        result = small_table.agg(
            by=lambda r: r.group, top_values=lambda g: g.value.top_k(10)
        )
        df = result.to_pandas()

        # Group A has 2 rows, should return 2 values
        a_row = df[df["group"] == "A"].iloc[0]
        a_values = parse_top_k(a_row["top_values"])
        assert len(a_values) == 2

        # Group B has 1 row, should return 1 value
        b_row = df[df["group"] == "B"].iloc[0]
        b_values = parse_top_k(b_row["top_values"])
        assert len(b_values) == 1

    def test_top_k_single_value(self, small_table):
        """top_k(1) should return single highest value."""
        result = small_table.agg(
            by=lambda r: r.group, top_value=lambda g: g.value.top_k(1)
        )
        df = result.to_pandas()

        # Group A's top 1 should be [20]
        a_row = df[df["group"] == "A"].iloc[0]
        a_values = parse_top_k(a_row["top_value"])
        assert len(a_values) == 1
        assert a_values[0] == 20.0

        # Group B's top 1 should be [100]
        b_row = df[df["group"] == "B"].iloc[0]
        b_values = parse_top_k(b_row["top_value"])
        assert len(b_values) == 1
        assert b_values[0] == 100.0


class TestTopKNoGroupBy:
    """Tests for top_k without GROUP BY (full table aggregation)."""

    @pytest.fixture
    def simple_table(self):
        """Create simple table without groups."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("id,score\n")
        csv.write("1,85\n")
        csv.write("2,92\n")
        csv.write("3,78\n")
        csv.write("4,95\n")
        csv.write("5,88\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_top_k_full_table(self, simple_table):
        """top_k without group by should aggregate entire table."""
        result = simple_table.agg(top_scores=lambda g: g.score.top_k(3))
        df = result.to_pandas()

        # Should have 1 row (full table aggregation)
        assert len(df) == 1

        # Should have top 3 scores
        top_scores = parse_top_k(df["top_scores"].iloc[0])
        assert len(top_scores) == 3
        # Top 3 should be 95, 92, 88
        assert top_scores == [95.0, 92.0, 88.0]


class TestTopKGroupProxy:
    """Tests for top_k via GroupProxy (Python-side NestedTable)."""

    def test_top_k_via_group_proxy(self):
        """top_k should work via GroupProxy in group_ordered().derive()."""
        # Test the GroupProxy.top_k method directly via Python-side evaluation
        from ltseq.grouping.proxies import GroupProxy

        # Create a mock group
        group_data = [
            {"price": 100},
            {"price": 150},
            {"price": 120},
        ]
        proxy = GroupProxy(group_data, None)

        result = proxy.top_k("price", 2)
        assert result == [150, 120]

    def test_top_k_group_proxy_descending(self):
        """GroupProxy.top_k should return values in descending order."""
        from ltseq.grouping.proxies import GroupProxy

        group_data = [
            {"value": 5},
            {"value": 1},
            {"value": 9},
            {"value": 3},
            {"value": 7},
        ]
        proxy = GroupProxy(group_data, None)

        result = proxy.top_k("value", 3)
        assert result == [9, 7, 5]

    def test_top_k_group_proxy_k_larger_than_data(self):
        """GroupProxy.top_k with k > data size should return all values."""
        from ltseq.grouping.proxies import GroupProxy

        group_data = [{"x": 10}, {"x": 20}]
        proxy = GroupProxy(group_data, None)

        result = proxy.top_k("x", 5)
        assert result == [20, 10]


class TestTopKWithOtherAggregates:
    """Tests for top_k combined with other aggregate functions."""

    @pytest.fixture
    def sales_table(self):
        """Create sales table for multi-aggregate testing."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("region,quarter,revenue\n")
        csv.write("East,Q1,1000\n")
        csv.write("East,Q2,1500\n")
        csv.write("East,Q3,1200\n")
        csv.write("West,Q1,2000\n")
        csv.write("West,Q2,1800\n")
        csv.write("West,Q3,2200\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_top_k_with_sum_and_max(self, sales_table):
        """top_k should work alongside other aggregates."""
        result = sales_table.agg(
            by=lambda r: r.region,
            total_revenue=lambda g: g.revenue.sum(),
            max_revenue=lambda g: g.revenue.max(),
            top_quarters=lambda g: g.revenue.top_k(2),
        )
        df = result.to_pandas()

        assert len(df) == 2  # Two regions

        # Verify all columns exist
        assert "total_revenue" in df.columns
        assert "max_revenue" in df.columns
        assert "top_quarters" in df.columns

        # Check top_quarters values
        for _, row in df.iterrows():
            top_values = parse_top_k(row["top_quarters"])
            assert len(top_values) == 2
