"""Tests for Phase 4.1: Statistical Aggregation Functions.

Tests median(), percentile(p), variance(), var(), std(), stddev(), and mode()
aggregate functions.
"""

import math
import os
import tempfile

import pytest

from ltseq import LTSeq


class TestMedian:
    """Tests for median() aggregate function."""

    @pytest.fixture
    def sample_table(self):
        """Create sample table with numeric data."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("region,value\n")
        csv.write("East,10\n")
        csv.write("East,20\n")
        csv.write("East,30\n")
        csv.write("West,100\n")
        csv.write("West,200\n")
        csv.write("West,300\n")
        csv.write("West,400\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_median_with_groupby(self, sample_table):
        """median() should return middle value for each group."""
        result = sample_table.agg(
            by=lambda r: r.region, median_value=lambda g: g.value.median()
        )
        df = result.to_pandas()

        assert len(df) == 2

        east_row = df[df["region"] == "East"].iloc[0]
        west_row = df[df["region"] == "West"].iloc[0]

        # East: [10, 20, 30] -> median = 20
        assert east_row["median_value"] == 20.0

        # West: [100, 200, 300, 400] -> median = (200 + 300) / 2 = 250
        assert west_row["median_value"] == 250.0

    def test_median_without_groupby(self, sample_table):
        """median() should work for full table aggregation."""
        result = sample_table.agg(median_value=lambda g: g.value.median())
        df = result.to_pandas()

        assert len(df) == 1
        # Values: [10, 20, 30, 100, 200, 300, 400] -> median = 100
        assert df["median_value"].iloc[0] == 100.0


class TestPercentile:
    """Tests for percentile(p) aggregate function."""

    @pytest.fixture
    def sample_table(self):
        """Create sample table for percentile tests."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("group,score\n")
        for i in range(1, 101):
            csv.write(f"A,{i}\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_percentile_50th(self, sample_table):
        """percentile(0.5) should approximate median."""
        result = sample_table.agg(p50=lambda g: g.score.percentile(0.5))
        df = result.to_pandas()

        # 50th percentile of 1-100 should be around 50
        assert abs(df["p50"].iloc[0] - 50.5) < 1.0

    def test_percentile_95th(self, sample_table):
        """percentile(0.95) should return value near 95."""
        result = sample_table.agg(p95=lambda g: g.score.percentile(0.95))
        df = result.to_pandas()

        # 95th percentile of 1-100 should be around 95
        assert abs(df["p95"].iloc[0] - 95.0) < 2.0

    def test_percentile_25th(self, sample_table):
        """percentile(0.25) should return first quartile."""
        result = sample_table.agg(p25=lambda g: g.score.percentile(0.25))
        df = result.to_pandas()

        # 25th percentile of 1-100 should be around 25
        assert abs(df["p25"].iloc[0] - 25.0) < 2.0


class TestVariance:
    """Tests for variance() / var() aggregate function."""

    @pytest.fixture
    def sample_table(self):
        """Create sample table with known variance."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("group,value\n")
        # Group A: [2, 4, 4, 4, 5, 5, 7, 9] - known variance = 4.571...
        csv.write("A,2\n")
        csv.write("A,4\n")
        csv.write("A,4\n")
        csv.write("A,4\n")
        csv.write("A,5\n")
        csv.write("A,5\n")
        csv.write("A,7\n")
        csv.write("A,9\n")
        # Group B: [1, 2, 3] - known variance = 1.0
        csv.write("B,1\n")
        csv.write("B,2\n")
        csv.write("B,3\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_variance_with_groupby(self, sample_table):
        """variance() should return sample variance for each group."""
        result = sample_table.agg(
            by=lambda r: r.group, var_value=lambda g: g.value.variance()
        )
        df = result.to_pandas()

        assert len(df) == 2

        a_row = df[df["group"] == "A"].iloc[0]
        b_row = df[df["group"] == "B"].iloc[0]

        # Group A: sample variance = 4.571... (using n-1 denominator)
        assert abs(a_row["var_value"] - 4.571) < 0.1

        # Group B: [1, 2, 3] -> sample variance = 1.0
        assert abs(b_row["var_value"] - 1.0) < 0.1

    def test_var_alias(self, sample_table):
        """var() should be an alias for variance()."""
        result = sample_table.agg(
            by=lambda r: r.group, var_value=lambda g: g.value.var()
        )
        df = result.to_pandas()

        b_row = df[df["group"] == "B"].iloc[0]
        assert abs(b_row["var_value"] - 1.0) < 0.1


class TestStdDev:
    """Tests for std() / stddev() aggregate function."""

    @pytest.fixture
    def sample_table(self):
        """Create sample table with known standard deviation."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("group,value\n")
        # Group A: [2, 4, 4, 4, 5, 5, 7, 9] - known std = 2.138...
        csv.write("A,2\n")
        csv.write("A,4\n")
        csv.write("A,4\n")
        csv.write("A,4\n")
        csv.write("A,5\n")
        csv.write("A,5\n")
        csv.write("A,7\n")
        csv.write("A,9\n")
        # Group B: constant values -> std = 0
        csv.write("B,5\n")
        csv.write("B,5\n")
        csv.write("B,5\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_std_with_groupby(self, sample_table):
        """std() should return sample standard deviation for each group."""
        result = sample_table.agg(
            by=lambda r: r.group, std_value=lambda g: g.value.std()
        )
        df = result.to_pandas()

        assert len(df) == 2

        a_row = df[df["group"] == "A"].iloc[0]
        b_row = df[df["group"] == "B"].iloc[0]

        # Group A: sample std = sqrt(4.571) = 2.138...
        assert abs(a_row["std_value"] - 2.138) < 0.1

        # Group B: constant values -> std = 0
        assert abs(b_row["std_value"] - 0.0) < 0.01

    def test_stddev_alias(self, sample_table):
        """stddev() should be an alias for std()."""
        result = sample_table.agg(
            by=lambda r: r.group, std_value=lambda g: g.value.stddev()
        )
        df = result.to_pandas()

        a_row = df[df["group"] == "A"].iloc[0]
        assert abs(a_row["std_value"] - 2.138) < 0.1


class TestMode:
    """Tests for mode() aggregate function."""

    @pytest.fixture
    def sample_table(self):
        """Create sample table with clear mode values."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("group,category\n")
        csv.write("A,red\n")
        csv.write("A,red\n")
        csv.write("A,red\n")
        csv.write("A,blue\n")
        csv.write("A,green\n")
        csv.write("B,x\n")
        csv.write("B,y\n")
        csv.write("B,y\n")
        csv.write("B,y\n")
        csv.write("B,z\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_mode_via_group_proxy(self):
        """Test mode() via GroupProxy directly."""
        from ltseq.grouping.proxies import GroupProxy

        group_data = [
            {"category": "red"},
            {"category": "red"},
            {"category": "red"},
            {"category": "blue"},
            {"category": "green"},
        ]
        proxy = GroupProxy(group_data, None)

        result = proxy.mode("category")
        assert result == "red"

    def test_mode_numeric(self):
        """Test mode() with numeric data."""
        from ltseq.grouping.proxies import GroupProxy

        group_data = [
            {"value": 1},
            {"value": 2},
            {"value": 2},
            {"value": 2},
            {"value": 3},
        ]
        proxy = GroupProxy(group_data, None)

        result = proxy.mode("value")
        assert result == 2


class TestGroupProxyStatistical:
    """Tests for statistical functions via GroupProxy (Python-side)."""

    def test_median_via_group_proxy(self):
        """Test median() via GroupProxy directly."""
        from ltseq.grouping.proxies import GroupProxy

        group_data = [{"value": 10}, {"value": 20}, {"value": 30}]
        proxy = GroupProxy(group_data, None)

        result = proxy.median("value")
        assert result == 20

    def test_median_even_count(self):
        """Test median() with even number of values."""
        from ltseq.grouping.proxies import GroupProxy

        group_data = [{"value": 10}, {"value": 20}, {"value": 30}, {"value": 40}]
        proxy = GroupProxy(group_data, None)

        result = proxy.median("value")
        assert result == 25.0

    def test_percentile_via_group_proxy(self):
        """Test percentile() via GroupProxy directly."""
        from ltseq.grouping.proxies import GroupProxy

        group_data = [{"value": i} for i in range(1, 101)]
        proxy = GroupProxy(group_data, None)

        result = proxy.percentile("value", 0.5)
        assert abs(result - 50.5) < 0.1

    def test_variance_via_group_proxy(self):
        """Test variance() via GroupProxy directly."""
        from ltseq.grouping.proxies import GroupProxy

        group_data = [{"value": 1}, {"value": 2}, {"value": 3}]
        proxy = GroupProxy(group_data, None)

        result = proxy.variance("value")
        assert abs(result - 1.0) < 0.01

    def test_std_via_group_proxy(self):
        """Test std() via GroupProxy directly."""
        from ltseq.grouping.proxies import GroupProxy

        group_data = [{"value": 1}, {"value": 2}, {"value": 3}]
        proxy = GroupProxy(group_data, None)

        result = proxy.std("value")
        assert abs(result - 1.0) < 0.01

    def test_var_alias_via_group_proxy(self):
        """Test var() alias via GroupProxy directly."""
        from ltseq.grouping.proxies import GroupProxy

        group_data = [{"value": 1}, {"value": 2}, {"value": 3}]
        proxy = GroupProxy(group_data, None)

        result = proxy.var("value")
        assert abs(result - 1.0) < 0.01


class TestStatisticalWithOtherAggregates:
    """Tests for statistical functions combined with other aggregates."""

    @pytest.fixture
    def sales_table(self):
        """Create sales table for multi-aggregate testing."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("region,sales\n")
        csv.write("East,100\n")
        csv.write("East,200\n")
        csv.write("East,300\n")
        csv.write("West,150\n")
        csv.write("West,250\n")
        csv.write("West,350\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_combined_aggregates(self, sales_table):
        """Statistical functions should work alongside other aggregates."""
        result = sales_table.agg(
            by=lambda r: r.region,
            total=lambda g: g.sales.sum(),
            avg_sales=lambda g: g.sales.avg(),
            median_sales=lambda g: g.sales.median(),
            std_sales=lambda g: g.sales.std(),
        )
        df = result.to_pandas()

        assert len(df) == 2

        # Verify all columns exist
        assert "total" in df.columns
        assert "avg_sales" in df.columns
        assert "median_sales" in df.columns
        assert "std_sales" in df.columns

        # Check East values
        east_row = df[df["region"] == "East"].iloc[0]
        assert east_row["total"] == 600
        assert east_row["avg_sales"] == 200.0
        assert east_row["median_sales"] == 200.0


class TestStatisticalEdgeCases:
    """Edge case tests for statistical functions."""

    @pytest.fixture
    def single_value_table(self):
        """Create table with single values per group."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("group,value\n")
        csv.write("A,100\n")
        csv.write("B,200\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_median_single_value(self, single_value_table):
        """median() with single value should return that value."""
        result = single_value_table.agg(
            by=lambda r: r.group, median_value=lambda g: g.value.median()
        )
        df = result.to_pandas()

        a_row = df[df["group"] == "A"].iloc[0]
        assert a_row["median_value"] == 100.0

    def test_percentile_single_value(self, single_value_table):
        """percentile() with single value should return that value."""
        result = single_value_table.agg(
            by=lambda r: r.group, p50=lambda g: g.value.percentile(0.5)
        )
        df = result.to_pandas()

        a_row = df[df["group"] == "A"].iloc[0]
        assert a_row["p50"] == 100.0

    def test_variance_single_value_via_proxy(self):
        """variance() with single value should return None (needs 2+ values)."""
        from ltseq.grouping.proxies import GroupProxy

        group_data = [{"value": 100}]
        proxy = GroupProxy(group_data, None)

        result = proxy.variance("value")
        assert result is None

    def test_std_single_value_via_proxy(self):
        """std() with single value should return None (needs 2+ values)."""
        from ltseq.grouping.proxies import GroupProxy

        group_data = [{"value": 100}]
        proxy = GroupProxy(group_data, None)

        result = proxy.std("value")
        assert result is None
