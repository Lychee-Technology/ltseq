"""Tests for the agg() GROUP BY aggregation method."""

import os
import tempfile

import pandas as pd
import pytest

from ltseq import LTSeq


class TestAggBasic:
    """Test basic agg() functionality with single column grouping."""

    @pytest.fixture
    def sales_data(self):
        """Create sample sales data for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "sales.csv")
            with open(path, "w") as f:
                f.write("region,product,sales,quantity\n")
                f.write("West,Widget,100,10\n")
                f.write("East,Gadget,200,20\n")
                f.write("West,Gadget,150,15\n")
                f.write("East,Widget,300,30\n")
                f.write("West,Widget,125,12\n")
            yield LTSeq.read_csv(path)

    def test_agg_sum(self, sales_data):
        """Test aggregation with sum."""
        result = sales_data.agg(by=lambda r: r.region, total=lambda g: g.sales.sum())
        df = result.to_pandas()

        assert len(df) == 2
        assert set(df["region"]) == {"West", "East"}

        west_total = df[df["region"] == "West"]["total"].iloc[0]
        east_total = df[df["region"] == "East"]["total"].iloc[0]
        assert west_total == 375  # 100 + 150 + 125
        assert east_total == 500  # 200 + 300

    def test_agg_count(self, sales_data):
        """Test aggregation with count."""
        result = sales_data.agg(
            by=lambda r: r.region, num_sales=lambda g: g.sales.count()
        )
        df = result.to_pandas()

        assert len(df) == 2
        west_count = df[df["region"] == "West"]["num_sales"].iloc[0]
        east_count = df[df["region"] == "East"]["num_sales"].iloc[0]
        assert west_count == 3
        assert east_count == 2

    def test_agg_min(self, sales_data):
        """Test aggregation with min."""
        result = sales_data.agg(by=lambda r: r.region, min_sale=lambda g: g.sales.min())
        df = result.to_pandas()

        assert len(df) == 2
        west_min = df[df["region"] == "West"]["min_sale"].iloc[0]
        east_min = df[df["region"] == "East"]["min_sale"].iloc[0]
        assert west_min == 100
        assert east_min == 200

    def test_agg_max(self, sales_data):
        """Test aggregation with max."""
        result = sales_data.agg(by=lambda r: r.region, max_sale=lambda g: g.sales.max())
        df = result.to_pandas()

        assert len(df) == 2
        west_max = df[df["region"] == "West"]["max_sale"].iloc[0]
        east_max = df[df["region"] == "East"]["max_sale"].iloc[0]
        assert west_max == 150
        assert east_max == 300

    def test_agg_avg(self, sales_data):
        """Test aggregation with avg."""
        result = sales_data.agg(by=lambda r: r.region, avg_sale=lambda g: g.sales.avg())
        df = result.to_pandas()

        assert len(df) == 2
        west_avg = df[df["region"] == "West"]["avg_sale"].iloc[0]
        east_avg = df[df["region"] == "East"]["avg_sale"].iloc[0]
        assert abs(west_avg - 125.0) < 0.01  # 375 / 3
        assert abs(east_avg - 250.0) < 0.01  # 500 / 2


class TestAggMultipleAggregates:
    """Test agg() with multiple aggregations in a single call."""

    @pytest.fixture
    def sales_data(self):
        """Create sample sales data for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "sales.csv")
            with open(path, "w") as f:
                f.write("region,sales,quantity\n")
                f.write("West,100,10\n")
                f.write("East,200,20\n")
                f.write("West,150,15\n")
                f.write("East,300,30\n")
            yield LTSeq.read_csv(path)

    def test_multiple_aggregates_same_column(self, sales_data):
        """Test multiple aggregations on the same column."""
        result = sales_data.agg(
            by=lambda r: r.region,
            total=lambda g: g.sales.sum(),
            avg_sales=lambda g: g.sales.avg(),
            min_sales=lambda g: g.sales.min(),
            max_sales=lambda g: g.sales.max(),
        )
        df = result.to_pandas()

        assert len(df) == 2
        assert "total" in df.columns
        assert "avg_sales" in df.columns
        assert "min_sales" in df.columns
        assert "max_sales" in df.columns

        west = df[df["region"] == "West"].iloc[0]
        assert west["total"] == 250
        assert abs(west["avg_sales"] - 125.0) < 0.01
        assert west["min_sales"] == 100
        assert west["max_sales"] == 150

    def test_multiple_aggregates_different_columns(self, sales_data):
        """Test aggregations on different columns."""
        result = sales_data.agg(
            by=lambda r: r.region,
            total_sales=lambda g: g.sales.sum(),
            total_qty=lambda g: g.quantity.sum(),
        )
        df = result.to_pandas()

        assert len(df) == 2
        west = df[df["region"] == "West"].iloc[0]
        assert west["total_sales"] == 250
        assert west["total_qty"] == 25  # 10 + 15


class TestAggNoGrouping:
    """Test agg() without grouping (full-table aggregation)."""

    @pytest.fixture
    def simple_data(self):
        """Create simple data for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write("value\n")
                f.write("10\n")
                f.write("20\n")
                f.write("30\n")
                f.write("40\n")
            yield LTSeq.read_csv(path)

    def test_agg_without_grouping(self, simple_data):
        """Test aggregation without a group-by clause."""
        result = simple_data.agg(total=lambda g: g.value.sum())
        df = result.to_pandas()

        # Should return a single row with the total
        assert len(df) == 1
        assert df["total"].iloc[0] == 100

    def test_agg_multiple_without_grouping(self, simple_data):
        """Test multiple aggregations without grouping."""
        result = simple_data.agg(
            total=lambda g: g.value.sum(),
            average=lambda g: g.value.avg(),
            minimum=lambda g: g.value.min(),
            maximum=lambda g: g.value.max(),
            cnt=lambda g: g.value.count(),
        )
        df = result.to_pandas()

        assert len(df) == 1
        assert df["total"].iloc[0] == 100
        assert df["average"].iloc[0] == 25.0
        assert df["minimum"].iloc[0] == 10
        assert df["maximum"].iloc[0] == 40
        assert df["cnt"].iloc[0] == 4


class TestAggEdgeCases:
    """Test edge cases for agg()."""

    def test_agg_empty_table(self):
        """Test aggregation on an empty table raises error due to null column type."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "empty.csv")
            with open(path, "w") as f:
                f.write("region,sales\n")  # Headers only
            t = LTSeq.read_csv(path)
            # Empty tables have Null column types, which can't be aggregated
            # This is expected DataFusion behavior
            with pytest.raises(RuntimeError, match="Sum not supported for Null"):
                t.agg(by=lambda r: r.region, total=lambda g: g.sales.sum())

    def test_agg_single_row(self):
        """Test aggregation on a single row table."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "single.csv")
            with open(path, "w") as f:
                f.write("region,sales\n")
                f.write("West,100\n")
            t = LTSeq.read_csv(path)
            result = t.agg(by=lambda r: r.region, total=lambda g: g.sales.sum())
            df = result.to_pandas()
            assert len(df) == 1
            assert df["total"].iloc[0] == 100

    def test_agg_all_same_group(self):
        """Test aggregation where all rows are in the same group."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "same.csv")
            with open(path, "w") as f:
                f.write("region,sales\n")
                f.write("West,100\n")
                f.write("West,200\n")
                f.write("West,300\n")
            t = LTSeq.read_csv(path)
            result = t.agg(by=lambda r: r.region, total=lambda g: g.sales.sum())
            df = result.to_pandas()
            assert len(df) == 1
            assert df["region"].iloc[0] == "West"
            assert df["total"].iloc[0] == 600

    def test_agg_each_row_different_group(self):
        """Test aggregation where each row is in a different group."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "different.csv")
            with open(path, "w") as f:
                f.write("id,value\n")
                f.write("A,10\n")
                f.write("B,20\n")
                f.write("C,30\n")
            t = LTSeq.read_csv(path)
            result = t.agg(by=lambda r: r.id, total=lambda g: g.value.sum())
            df = result.to_pandas()
            assert len(df) == 3
            # Each sum should equal the original value
            for _, row in df.iterrows():
                expected = {"A": 10, "B": 20, "C": 30}[row["id"]]
                assert row["total"] == expected


class TestAggWithVariableAssignment:
    """Test agg() with lambdas assigned to variables (different closure behavior)."""

    def test_agg_with_predefined_lambdas(self):
        """Test agg() using pre-defined lambda variables."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write("category,amount\n")
                f.write("A,100\n")
                f.write("B,200\n")
                f.write("A,150\n")
            t = LTSeq.read_csv(path)

            # Pre-define lambdas
            by_fn = lambda r: r.category
            agg_fn = lambda g: g.amount.sum()

            result = t.agg(by=by_fn, total=agg_fn)
            df = result.to_pandas()

            assert len(df) == 2
            a_total = df[df["category"] == "A"]["total"].iloc[0]
            b_total = df[df["category"] == "B"]["total"].iloc[0]
            assert a_total == 250
            assert b_total == 200


class TestAggDocExamples:
    """Test examples from documentation."""

    def test_sales_by_region_example(self):
        """Test the canonical sales by region example."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "sales.csv")
            with open(path, "w") as f:
                f.write("region,product,amount\n")
                f.write("North,Widget,100\n")
                f.write("South,Widget,150\n")
                f.write("North,Gadget,200\n")
                f.write("South,Gadget,250\n")
                f.write("North,Widget,120\n")
            t = LTSeq.read_csv(path)

            # Group by region and sum amounts
            result = t.agg(
                by=lambda r: r.region,
                total_sales=lambda g: g.amount.sum(),
                num_transactions=lambda g: g.amount.count(),
            )
            df = result.to_pandas()

            north = df[df["region"] == "North"].iloc[0]
            south = df[df["region"] == "South"].iloc[0]

            assert north["total_sales"] == 420  # 100 + 200 + 120
            assert north["num_transactions"] == 3
            assert south["total_sales"] == 400  # 150 + 250
            assert south["num_transactions"] == 2
