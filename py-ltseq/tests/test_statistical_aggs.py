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


class TestSkew:
    """Numeric tests for skew() (added with the native migration, #91 PR 2).

    Expected values follow the population-moment formula the implementation
    (and the legacy SQL before it) uses:
        (E[x**3] - 3*E[x**2]*E[x] + 2*E[x]**3) / stddev_pop(x)**3
    """

    @pytest.fixture
    def grouped_table(self):
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("g,x\n")
        for row in ["a,1", "a,2", "a,9", "b,1", "b,2", "b,3"]:
            csv.write(row + "\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    @staticmethod
    def _expected_skew(values):
        n = len(values)
        m1 = sum(values) / n
        m2 = sum(v * v for v in values) / n
        m3 = sum(v * v * v for v in values) / n
        sd = (m2 - m1 * m1) ** 0.5
        return (m3 - 3 * m2 * m1 + 2 * m1**3) / sd**3

    def test_skew_method_form(self, grouped_table):
        """g.x.skew(): symmetric group -> 0, right-skewed group -> positive."""
        df = (
            grouped_table.agg(by=lambda r: r.g, s=lambda g: g.x.skew())
            .to_pandas()
            .sort_values("g")
            .reset_index(drop=True)
        )
        assert df["s"][0] == pytest.approx(self._expected_skew([1, 2, 9]))
        assert df["s"][0] > 0  # right-skewed
        assert df["s"][1] == pytest.approx(0.0)  # symmetric

    def test_skew_free_function_form(self, grouped_table):
        """skew(g.x): the exported free function must match the method form.

        (Previously broken on both the SQL and native paths: the free
        function carries the column in args[0], not `on`.)
        """
        from ltseq import skew

        df = (
            grouped_table.agg(by=lambda r: r.g, s=lambda g: skew(g.x))
            .to_pandas()
            .sort_values("g")
            .reset_index(drop=True)
        )
        assert df["s"][0] == pytest.approx(self._expected_skew([1, 2, 9]))
        assert df["s"][1] == pytest.approx(0.0)
