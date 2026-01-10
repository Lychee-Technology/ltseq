"""Tests for Phase 4.3: Conditional Aggregation Functions.

Tests count_if(predicate), sum_if(predicate, col), avg_if(predicate, col),
min_if(predicate, col), max_if(predicate, col) aggregate functions.
"""

import os
import tempfile

import pytest

from ltseq import LTSeq, count_if, sum_if, avg_if, min_if, max_if


class TestCountIf:
    """Tests for count_if() aggregate function."""

    @pytest.fixture
    def sample_table(self):
        """Create sample table with mixed data."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("region,category,price,quantity\n")
        csv.write("East,A,100,10\n")
        csv.write("East,A,200,20\n")
        csv.write("East,B,150,15\n")
        csv.write("West,A,300,30\n")
        csv.write("West,B,50,5\n")
        csv.write("West,B,75,8\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_count_if_with_groupby(self, sample_table):
        """count_if should count rows matching predicate per group."""
        result = sample_table.agg(
            by=lambda r: r.region,
            high_price_count=lambda g: count_if(g.price > 100),
        )
        df = result.to_pandas()

        assert len(df) == 2

        east_row = df[df["region"] == "East"].iloc[0]
        west_row = df[df["region"] == "West"].iloc[0]

        # East: 200 > 100, 150 > 100 -> 2
        assert east_row["high_price_count"] == 2

        # West: 300 > 100 -> 1
        assert west_row["high_price_count"] == 1

    def test_count_if_no_groupby(self, sample_table):
        """count_if should work for full table aggregation."""
        result = sample_table.agg(
            high_price_count=lambda g: count_if(g.price > 100),
        )
        df = result.to_pandas()

        assert len(df) == 1
        # Prices > 100: 200, 150, 300 -> 3
        assert df["high_price_count"].iloc[0] == 3

    def test_count_if_equality(self, sample_table):
        """count_if should work with equality predicates."""
        result = sample_table.agg(
            by=lambda r: r.region,
            category_a_count=lambda g: count_if(g.category == "A"),
        )
        df = result.to_pandas()

        east_row = df[df["region"] == "East"].iloc[0]
        west_row = df[df["region"] == "West"].iloc[0]

        # East has 2 category A items
        assert east_row["category_a_count"] == 2

        # West has 1 category A item
        assert west_row["category_a_count"] == 1


class TestSumIf:
    """Tests for sum_if() aggregate function."""

    @pytest.fixture
    def sample_table(self):
        """Create sample table for sum_if tests."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("region,is_active,sales\n")
        csv.write("East,true,100\n")
        csv.write("East,true,200\n")
        csv.write("East,false,50\n")
        csv.write("West,true,300\n")
        csv.write("West,false,150\n")
        csv.write("West,false,100\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_sum_if_with_groupby(self, sample_table):
        """sum_if should sum column where predicate is True per group."""
        result = sample_table.agg(
            by=lambda r: r.region,
            active_sales=lambda g: sum_if(g.is_active == True, g.sales),  # noqa: E712
        )
        df = result.to_pandas()

        assert len(df) == 2

        east_row = df[df["region"] == "East"].iloc[0]
        west_row = df[df["region"] == "West"].iloc[0]

        # East active: 100 + 200 = 300
        assert east_row["active_sales"] == 300

        # West active: 300
        assert west_row["active_sales"] == 300

    def test_sum_if_no_groupby(self, sample_table):
        """sum_if should work for full table aggregation."""
        result = sample_table.agg(
            active_sales=lambda g: sum_if(g.is_active == True, g.sales),  # noqa: E712
        )
        df = result.to_pandas()

        assert len(df) == 1
        # Active sales: 100 + 200 + 300 = 600
        assert df["active_sales"].iloc[0] == 600


class TestAvgIf:
    """Tests for avg_if() aggregate function."""

    @pytest.fixture
    def sample_table(self):
        """Create sample table for avg_if tests."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("region,grade,score\n")
        csv.write("East,A,90\n")
        csv.write("East,A,80\n")
        csv.write("East,B,70\n")
        csv.write("West,A,100\n")
        csv.write("West,B,60\n")
        csv.write("West,B,50\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_avg_if_with_groupby(self, sample_table):
        """avg_if should average column where predicate is True per group."""
        result = sample_table.agg(
            by=lambda r: r.region,
            avg_grade_a=lambda g: avg_if(g.grade == "A", g.score),
        )
        df = result.to_pandas()

        assert len(df) == 2

        east_row = df[df["region"] == "East"].iloc[0]
        west_row = df[df["region"] == "West"].iloc[0]

        # East grade A: (90 + 80) / 2 = 85
        assert east_row["avg_grade_a"] == 85.0

        # West grade A: 100 / 1 = 100
        assert west_row["avg_grade_a"] == 100.0

    def test_avg_if_no_groupby(self, sample_table):
        """avg_if should work for full table aggregation."""
        result = sample_table.agg(
            avg_grade_a=lambda g: avg_if(g.grade == "A", g.score),
        )
        df = result.to_pandas()

        assert len(df) == 1
        # Grade A scores: 90, 80, 100 -> avg = 90
        assert df["avg_grade_a"].iloc[0] == 90.0


class TestMinIf:
    """Tests for min_if() aggregate function."""

    @pytest.fixture
    def sample_table(self):
        """Create sample table for min_if tests."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("region,is_valid,value\n")
        csv.write("East,true,100\n")
        csv.write("East,true,50\n")
        csv.write("East,false,10\n")
        csv.write("West,true,200\n")
        csv.write("West,false,5\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_min_if_with_groupby(self, sample_table):
        """min_if should return minimum where predicate is True per group."""
        result = sample_table.agg(
            by=lambda r: r.region,
            min_valid=lambda g: min_if(g.is_valid == True, g.value),  # noqa: E712
        )
        df = result.to_pandas()

        east_row = df[df["region"] == "East"].iloc[0]
        west_row = df[df["region"] == "West"].iloc[0]

        # East valid min: min(100, 50) = 50
        assert east_row["min_valid"] == 50

        # West valid min: 200
        assert west_row["min_valid"] == 200


class TestMaxIf:
    """Tests for max_if() aggregate function."""

    @pytest.fixture
    def sample_table(self):
        """Create sample table for max_if tests."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("region,is_valid,value\n")
        csv.write("East,true,100\n")
        csv.write("East,true,50\n")
        csv.write("East,false,200\n")
        csv.write("West,true,150\n")
        csv.write("West,false,300\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_max_if_with_groupby(self, sample_table):
        """max_if should return maximum where predicate is True per group."""
        result = sample_table.agg(
            by=lambda r: r.region,
            max_valid=lambda g: max_if(g.is_valid == True, g.value),  # noqa: E712
        )
        df = result.to_pandas()

        east_row = df[df["region"] == "East"].iloc[0]
        west_row = df[df["region"] == "West"].iloc[0]

        # East valid max: max(100, 50) = 100
        assert east_row["max_valid"] == 100

        # West valid max: 150
        assert west_row["max_valid"] == 150


class TestGroupProxyConditionalAggs:
    """Tests for conditional aggregations via GroupProxy (Python-side)."""

    def test_count_if_via_group_proxy(self):
        """count_if should work via GroupProxy directly."""
        from ltseq.grouping.proxies import GroupProxy

        group_data = [
            {"price": 50},
            {"price": 150},
            {"price": 200},
            {"price": 80},
        ]
        proxy = GroupProxy(group_data, None)

        result = proxy.count_if(lambda r: r.price > 100)
        assert result == 2

    def test_sum_if_via_group_proxy(self):
        """sum_if should work via GroupProxy directly."""
        from ltseq.grouping.proxies import GroupProxy

        group_data = [
            {"is_active": True, "sales": 100},
            {"is_active": False, "sales": 50},
            {"is_active": True, "sales": 200},
        ]
        proxy = GroupProxy(group_data, None)

        result = proxy.sum_if(lambda r: r.is_active, "sales")
        assert result == 300

    def test_avg_if_via_group_proxy(self):
        """avg_if should work via GroupProxy directly."""
        from ltseq.grouping.proxies import GroupProxy

        group_data = [
            {"grade": "A", "score": 90},
            {"grade": "B", "score": 70},
            {"grade": "A", "score": 80},
        ]
        proxy = GroupProxy(group_data, None)

        result = proxy.avg_if(lambda r: r.grade == "A", "score")
        assert result == 85.0

    def test_min_if_via_group_proxy(self):
        """min_if should work via GroupProxy directly."""
        from ltseq.grouping.proxies import GroupProxy

        group_data = [
            {"valid": True, "value": 100},
            {"valid": False, "value": 10},
            {"valid": True, "value": 50},
        ]
        proxy = GroupProxy(group_data, None)

        result = proxy.min_if(lambda r: r.valid, "value")
        assert result == 50

    def test_max_if_via_group_proxy(self):
        """max_if should work via GroupProxy directly."""
        from ltseq.grouping.proxies import GroupProxy

        group_data = [
            {"valid": True, "value": 100},
            {"valid": False, "value": 200},
            {"valid": True, "value": 50},
        ]
        proxy = GroupProxy(group_data, None)

        result = proxy.max_if(lambda r: r.valid, "value")
        assert result == 100


class TestConditionalAggsWithOtherAggregates:
    """Tests for conditional aggregations combined with other aggregates."""

    @pytest.fixture
    def sales_table(self):
        """Create sales table for multi-aggregate testing."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("region,category,revenue\n")
        csv.write("East,premium,500\n")
        csv.write("East,standard,200\n")
        csv.write("East,premium,300\n")
        csv.write("West,standard,150\n")
        csv.write("West,premium,600\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_combined_aggregates(self, sales_table):
        """Conditional aggregates should work alongside regular aggregates."""
        result = sales_table.agg(
            by=lambda r: r.region,
            total_revenue=lambda g: g.revenue.sum(),
            premium_count=lambda g: count_if(g.category == "premium"),
            premium_revenue=lambda g: sum_if(g.category == "premium", g.revenue),
        )
        df = result.to_pandas()

        assert len(df) == 2

        # Verify all columns exist
        assert "total_revenue" in df.columns
        assert "premium_count" in df.columns
        assert "premium_revenue" in df.columns

        # Check East values
        east_row = df[df["region"] == "East"].iloc[0]
        assert east_row["total_revenue"] == 1000  # 500 + 200 + 300
        assert east_row["premium_count"] == 2
        assert east_row["premium_revenue"] == 800  # 500 + 300


class TestConditionalAggsEdgeCases:
    """Edge case tests for conditional aggregations."""

    @pytest.fixture
    def edge_table(self):
        """Create table for edge case testing."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("group,flag,value\n")
        csv.write("A,true,100\n")
        csv.write("A,false,200\n")
        csv.write("B,false,50\n")
        csv.write("B,false,75\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_count_if_no_matches(self, edge_table):
        """count_if should return 0 when no rows match."""
        result = edge_table.agg(
            by=lambda r: r.group,
            true_count=lambda g: count_if(g.flag == True),  # noqa: E712
        )
        df = result.to_pandas()

        # Group B has no true flags
        b_row = df[df["group"] == "B"].iloc[0]
        assert b_row["true_count"] == 0

    def test_sum_if_no_matches(self, edge_table):
        """sum_if should return 0 when no rows match."""
        result = edge_table.agg(
            by=lambda r: r.group,
            true_sum=lambda g: sum_if(g.flag == True, g.value),  # noqa: E712
        )
        df = result.to_pandas()

        # Group B has no true flags
        b_row = df[df["group"] == "B"].iloc[0]
        assert b_row["true_sum"] == 0

    def test_avg_if_no_matches_via_proxy(self):
        """avg_if via GroupProxy should return None when no rows match."""
        from ltseq.grouping.proxies import GroupProxy

        group_data = [
            {"flag": False, "value": 100},
            {"flag": False, "value": 200},
        ]
        proxy = GroupProxy(group_data, None)

        result = proxy.avg_if(lambda r: r.flag, "value")
        assert result is None
