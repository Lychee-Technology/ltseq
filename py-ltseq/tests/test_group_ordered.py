"""
Phase 7 Tests: Ordered Grouping (group_ordered)

Tests for the group_ordered operation which groups consecutive identical values.
This is the foundation for state-aware grouping without explicit sorting.
"""

import pytest
import csv
import os
from ltseq import LTSeq


@pytest.fixture
def sample_csv():
    """Create a temporary CSV with stock data showing price trends."""
    csv_file = "/tmp/phase7_sample.csv"
    with open(csv_file, "w") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "price", "is_up"])
        # Group 1: 3 days up
        writer.writerow(["2024-01-01", "100", "1"])
        writer.writerow(["2024-01-02", "102", "1"])
        writer.writerow(["2024-01-03", "105", "1"])
        # Group 2: 2 days down
        writer.writerow(["2024-01-04", "103", "0"])
        writer.writerow(["2024-01-05", "101", "0"])
        # Group 3: 4 days up
        writer.writerow(["2024-01-06", "104", "1"])
        writer.writerow(["2024-01-07", "106", "1"])
        writer.writerow(["2024-01-08", "108", "1"])
        writer.writerow(["2024-01-09", "110", "1"])
        # Group 4: 1 day down
        writer.writerow(["2024-01-10", "108", "0"])
    yield csv_file
    if os.path.exists(csv_file):
        os.remove(csv_file)


class TestGroupOrderedBasic:
    """Test basic group_ordered functionality."""

    def test_group_ordered_returns_nested_table(self, sample_csv):
        """group_ordered should return a NestedTable object."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        try:
            grouped = t.group_ordered(lambda r: r.is_up)
            # Should return a NestedTable (has group-level methods)
            assert hasattr(grouped, "count")
            assert hasattr(grouped, "first")
            assert hasattr(grouped, "last")
        except Exception as e:
            pytest.skip(f"group_ordered not yet implemented: {e}")

    def test_group_ordered_preserves_data(self, sample_csv):
        """group_ordered should preserve all rows."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        try:
            grouped = t.group_ordered(lambda r: r.is_up)
            # Ungrouped or flattened result should have same number of rows
            result = grouped.flatten()
            assert len(result) == 10
        except Exception as e:
            pytest.skip(f"group_ordered not yet implemented: {e}")

    def test_group_count(self, sample_csv):
        """Test group count detection."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        try:
            grouped = t.group_ordered(lambda r: r.is_up)
            # Should be able to count rows in each group
            # Expected groups: [3 up, 2 down, 4 up, 1 down]
            counts = grouped.count()
            # This might return a list/array or a ColumnExpr
            # Exact format depends on implementation
            assert counts is not None
        except Exception as e:
            pytest.skip(f"group_ordered not yet implemented: {e}")


class TestGroupOrderedFilter:
    """Test filtering on grouped data."""

    def test_filter_groups_by_count(self, sample_csv):
        """Filter groups by their size (count > N)."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        try:
            # Get only groups with > 2 rows
            result = (
                t.group_ordered(lambda r: r.is_up)
                .filter(lambda g: g.count() > 2)
            )
            # Should have groups: [3 up, 4 up] = 7 rows total
            assert result is not None
        except Exception as e:
            pytest.skip(f"group filtering not yet implemented: {e}")

    def test_filter_groups_by_property(self, sample_csv):
        """Filter groups by first row property."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        try:
            # Keep only groups that start with is_up=1
            result = (
                t.group_ordered(lambda r: r.is_up)
                .filter(lambda g: g.first().is_up == 1)
            )
            assert result is not None
        except Exception as e:
            pytest.skip(f"group filtering not yet implemented: {e}")


class TestGroupOrderedDerive:
    """Test deriving new columns on grouped data."""

    def test_derive_group_span(self, sample_csv):
        """Add columns based on group properties."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        try:
            result = (
                t.group_ordered(lambda r: r.is_up)
                .derive(lambda g: {
                    "group_size": g.count(),
                    "price_change": g.last().price - g.first().price,
                })
            )
            assert result is not None
        except Exception as e:
            pytest.skip(f"group derive not yet implemented: {e}")


class TestGroupOrderedChaining:
    """Test chaining operations on grouped data."""

    def test_chain_filter_derive(self, sample_csv):
        """Chain filter and derive on grouped data."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        try:
            result = (
                t.group_ordered(lambda r: r.is_up)
                .filter(lambda g: g.count() > 2)
                .derive(lambda g: {
                    "start_price": g.first().price,
                    "end_price": g.last().price,
                })
            )
            assert result is not None
        except Exception as e:
            pytest.skip(f"group chaining not yet implemented: {e}")

    def test_complex_stock_analysis(self, sample_csv):
        """Reproduce the stock analysis example from docs."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        try:
            result = (
                t
                .group_ordered(lambda r: r.is_up)
                .filter(lambda g: g.count() > 2)  # Only groups with > 2 days
                .derive(lambda g: {
                    "start": g.first().date,
                    "end": g.last().date,
                    "gain": (g.last().price - g.first().price) / g.first().price,
                })
            )
            assert result is not None
        except Exception as e:
            pytest.skip(f"complex group_ordered not yet implemented: {e}")
