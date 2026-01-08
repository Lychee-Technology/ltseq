"""
Phase B.3: DSL Showcase Integration Tests

Complete end-to-end tests for the group_ordered showcase from api.md.
This validates that filter() and derive() work together correctly
for the "Find all time intervals where a stock rose for more than 3 consecutive days" task.

Tests validate:
1. Row-level derive (is_up column)
2. Group-ordered grouping
3. Group-level filter with multiple predicates
4. Group-level derive with g.first(), g.last(), and arithmetic
"""

import pytest
import pandas as pd
import tempfile
import os

# Import LTSeq - tests are run from project root
try:
    from ltseq import LTSeq
except ImportError:
    import sys

    sys.path.insert(0, "/Users/ruoshi/code/github/ltseq/py-ltseq")
    from ltseq import LTSeq


class TestGroupOrderedShowcase:
    """Test the complete group_ordered showcase from api.md."""

    def test_showcase_stock_price_analysis(self):
        """
        Test: Find all time intervals where a stock rose for more than 3 consecutive days.

        This is the exact example from api.md, validating:
        1. Procedural derive to calculate is_up
        2. Group-ordered grouping
        3. Filtering on sub-tables (Groups)
        4. Extract info from the group with group-level derive
        """
        # Create test data: stock prices by date
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("date,price\n")
            # 4 consecutive up days
            f.write("2020-01-01,100\n")
            f.write("2020-01-02,102\n")
            f.write("2020-01-03,105\n")
            f.write("2020-01-04,108\n")
            f.write("2020-01-05,110\n")
            # 2 down days (filtered out by count > 3)
            f.write("2020-01-06,107\n")
            f.write("2020-01-07,105\n")
            # 4 consecutive up days
            f.write("2020-01-08,110\n")
            f.write("2020-01-09,112\n")
            f.write("2020-01-10,115\n")
            f.write("2020-01-11,120\n")
            # 1 down day (filtered out)
            f.write("2020-01-12,118\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)

            result = (
                t.sort(lambda r: r.date)
                # 1. Procedural: Calculate rise/fall status relative to previous row
                .derive(lambda r: {"is_up": r.price > r.price.shift(1)})
                # 2. Ordered Grouping: Cut a new group only when 'is_up' changes
                .group_ordered(lambda r: r.is_up)
                # 3. Filtering on Sub-tables (Groups)
                .filter(
                    lambda g: (g.first().is_up == True)  # Must be a rising group
                    & (g.count() > 3)  # Must last > 3 days
                )
                # 4. Extract info from the group
                .derive(
                    lambda g: {
                        "start": g.first().date,
                        "end": g.last().date,
                        "gain": (g.last().price - g.first().price) / g.first().price,
                    }
                )
            )

            df = result.to_pandas()

            # Verify: Should have 8 rows total (2 groups of 4 rows each)
            assert len(df) == 8, f"Expected 8 rows, got {len(df)}"

            # Verify: All rows in first group have is_up=True
            group1 = df[
                df["date"].isin(
                    ["2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05"]
                )
            ]
            assert len(group1) == 4
            assert (group1["is_up"] == True).all()

            # Verify: All rows in second group have is_up=True
            group2 = df[df["date"].isin(["2020-01-09", "2020-01-10", "2020-01-11"])]
            assert len(group2) == 3

            # Verify: Start and end dates are correct
            group1_starts = group1["start"].unique()
            assert len(group1_starts) == 1
            assert group1_starts[0] == "2020-01-02"

            group1_ends = group1["end"].unique()
            assert len(group1_ends) == 1
            assert group1_ends[0] == "2020-01-05"

            # Verify: Gain is calculated correctly
            # Group 1: (110 - 102) / 102 = 0.0784...
            group1_gains = group1["gain"].unique()
            assert len(group1_gains) == 1
            assert abs(group1_gains[0] - (110 - 102) / 102) < 0.01

        finally:
            os.unlink(fname)

    def test_showcase_filter_single_predicate(self):
        """Test group filter with a single predicate: g.count() > 2."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,val\n")
            f.write("A,1\n")
            f.write("A,2\n")
            f.write("A,3\n")
            f.write("B,10\n")  # Single row, will be filtered out
            f.write("C,20\n")
            f.write("C,30\n")
            f.write("C,40\n")  # Add third row to group C so count > 2
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            result = t.group_ordered(lambda r: r.id).filter(lambda g: g.count() > 2)

            df = result.to_pandas()

            # Only groups A and C have > 2 rows (group B has 1 row)
            assert len(df) == 6  # 3 from A + 3 from C
            assert set(df["id"].unique()) == {"A", "C"}

        finally:
            os.unlink(fname)

    def test_showcase_derive_single_operation(self):
        """Test group derive with a single operation: g.count()."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,val\n")
            f.write("A,1\n")
            f.write("A,2\n")
            f.write("B,10\n")
            f.write("B,20\n")
            f.write("B,30\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            result = t.group_ordered(lambda r: r.id).derive(
                lambda g: {"group_size": g.count()}
            )

            df = result.to_pandas()

            # Group A should have group_size=2 for both rows
            assert (df[df["id"] == "A"]["group_size"] == 2).all()
            # Group B should have group_size=3 for all rows
            assert (df[df["id"] == "B"]["group_size"] == 3).all()

        finally:
            os.unlink(fname)

    def test_showcase_first_last_access(self):
        """Test group derive with g.first() and g.last() access."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,val\n")
            f.write("A,1\n")
            f.write("A,2\n")
            f.write("A,3\n")
            f.write("B,10\n")
            f.write("B,20\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            result = t.group_ordered(lambda r: r.id).derive(
                lambda g: {
                    "first_val": g.first().val,
                    "last_val": g.last().val,
                    "range": g.last().val - g.first().val,
                }
            )

            df = result.to_pandas()

            # Group A
            group_a = df[df["id"] == "A"]
            assert (group_a["first_val"] == 1).all()
            assert (group_a["last_val"] == 3).all()
            assert (group_a["range"] == 2).all()

            # Group B
            group_b = df[df["id"] == "B"]
            assert (group_b["first_val"] == 10).all()
            assert (group_b["last_val"] == 20).all()
            assert (group_b["range"] == 10).all()

        finally:
            os.unlink(fname)

    def test_showcase_aggregate_functions_in_filter(self):
        """Test group filter using aggregate functions: g.sum(), g.avg()."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,val\n")
            f.write("A,1\n")
            f.write("A,2\n")
            f.write("A,3\n")  # sum=6, avg=2
            f.write("B,10\n")  # sum=10, avg=10
            f.write("C,5\n")
            f.write("C,5\n")
            f.write("C,5\n")  # sum=15, avg=5
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)

            # Filter groups where sum > 8
            result = t.group_ordered(lambda r: r.id).filter(lambda g: g.sum("val") > 8)

            df = result.to_pandas()

            # Groups B and C pass (sum=10 and sum=15)
            assert set(df["id"].unique()) == {"B", "C"}

        finally:
            os.unlink(fname)

    def test_showcase_chained_operations(self):
        """Test chained filter and derive operations."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,val\n")
            f.write("A,1\n")
            f.write("A,2\n")
            f.write("A,3\n")
            f.write("B,10\n")
            f.write("B,11\n")
            f.write("C,100\n")
            f.write("C,200\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)

            # First filter to groups with count > 2, then derive
            result = (
                t.group_ordered(lambda r: r.id)
                .filter(lambda g: g.count() > 2)
                .derive(lambda g: {"total": g.sum("val"), "avg": g.avg("val")})
            )

            df = result.to_pandas()

            # Only group A has count > 2
            assert len(df) == 3
            assert (df["id"] == "A").all()
            assert (df["total"] == 6).all()
            assert (df["avg"] == 2).all()

        finally:
            os.unlink(fname)

    def test_showcase_complex_predicate(self):
        """Test filter with complex boolean predicate: g.first().val > 5 & g.count() > 1."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,val\n")
            f.write("A,1\n")  # first=1, count=1 (fails count check)
            f.write("B,10\n")  # first=10, count=1 (fails count check)
            f.write("B,11\n")
            f.write("C,3\n")  # first=3, count=1 (fails both)
            f.write("D,6\n")  # first=6, count=2 (passes both)
            f.write("D,7\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)

            result = t.group_ordered(lambda r: r.id).filter(
                lambda g: (g.first().val > 5) & (g.count() > 1)
            )

            df = result.to_pandas()

            # Only group D passes (first=6 > 5 AND count=2 > 1)
            assert len(df) == 2
            assert (df["id"] == "D").all()

        finally:
            os.unlink(fname)

    def test_showcase_with_real_stock_data(self):
        """Test with the actual stock.csv file from the repository."""
        # Use the real stock.csv file
        stock_file = "/Users/ruoshi/code/github/ltseq/stock.csv"

        if not os.path.exists(stock_file):
            pytest.skip("stock.csv not found in repository root")

        t = LTSeq.read_csv(stock_file)

        result = (
            t.sort(lambda r: r.date)
            # Add is_up column (note: stock.csv already has it, but recalculate for completeness)
            .derive(lambda r: {"is_up_calc": r.price > r.price.shift(1)})
            # Group by is_up_calc to find consecutive up/down periods
            .group_ordered(lambda r: r.is_up_calc)
            # Filter for rising groups with more than 2 days
            .filter(lambda g: (g.first().is_up_calc == True) & (g.count() > 2))
            # Extract key info
            .derive(
                lambda g: {
                    "start_date": g.first().date,
                    "end_date": g.last().date,
                    "start_price": g.first().price,
                    "end_price": g.last().price,
                    "gain": (g.last().price - g.first().price) / g.first().price,
                }
            )
        )

        df = result.to_pandas()

        # Verify results have expected columns
        assert "start_date" in df.columns
        assert "end_date" in df.columns
        assert "gain" in df.columns

        # All rows should be from rising periods (is_up_calc=True)
        assert (df["is_up_calc"] == True).all()

        # Each group should have at least 3 rows
        for start_date in df["start_date"].unique():
            group_rows = df[df["start_date"] == start_date]
            assert len(group_rows) > 2
            # All rows in the same group should have same gain
            assert len(group_rows["gain"].unique()) == 1


class TestGroupOrderedEdgeCases:
    """Test edge cases and error conditions."""

    def test_single_row_groups(self):
        """Test filtering/deriving when each group has only 1 row."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,val\n")
            f.write("A,10\n")
            f.write("B,20\n")
            f.write("C,30\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)

            result = t.group_ordered(lambda r: r.id).derive(
                lambda g: {"count": g.count(), "first_val": g.first().val}
            )

            df = result.to_pandas()

            # All groups should have count=1
            assert (df["count"] == 1).all()
            # First and only value should match original
            assert (df["first_val"] == df["val"]).all()

        finally:
            os.unlink(fname)

    def test_all_same_group(self):
        """Test when all rows belong to the same group."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,val\n")
            f.write("A,1\n")
            f.write("A,2\n")
            f.write("A,3\n")
            f.write("A,4\n")
            f.write("A,5\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)

            result = (
                t.group_ordered(lambda r: r.id)
                .filter(lambda g: g.count() > 3)
                .derive(
                    lambda g: {
                        "total": g.sum("val"),
                        "min": g.min("val"),
                        "max": g.max("val"),
                    }
                )
            )

            df = result.to_pandas()

            # All 5 rows should remain
            assert len(df) == 5
            # All should have same totals
            assert (df["total"] == 15).all()
            assert (df["min"] == 1).all()
            assert (df["max"] == 5).all()

        finally:
            os.unlink(fname)

    def test_alternating_groups(self):
        """Test with alternating groups (each group has 1 row)."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,val\n")
            f.write("A,1\n")
            f.write("B,2\n")
            f.write("A,3\n")
            f.write("B,4\n")
            f.write("C,5\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)

            result = t.group_ordered(lambda r: r.id).derive(
                lambda g: {"count": g.count()}
            )

            df = result.to_pandas()

            # All groups should have count=1 (because they're not consecutive)
            assert (df["count"] == 1).all()

        finally:
            os.unlink(fname)
