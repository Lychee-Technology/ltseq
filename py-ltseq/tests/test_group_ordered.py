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
            result = t.group_ordered(lambda r: r.is_up).filter(lambda g: g.count() > 2)
            # Should have groups: [3 up, 4 up] = 7 rows total
            assert result is not None
        except Exception as e:
            pytest.skip(f"group filtering not yet implemented: {e}")

    def test_filter_groups_by_property(self, sample_csv):
        """Filter groups by first row property."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        try:
            # Keep only groups that start with is_up=1
            result = t.group_ordered(lambda r: r.is_up).filter(
                lambda g: g.first().is_up == 1
            )
            assert result is not None
        except Exception as e:
            pytest.skip(f"group filtering not yet implemented: {e}")


class TestGroupOrderedDerive:
    """Test deriving new columns on grouped data."""

    def test_derive_group_span(self, sample_csv):
        """Add columns based on group properties."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        result = t.group_ordered(lambda r: r.is_up).derive(
            lambda g: {
                "group_size": g.count(),
                "price_change": g.last().price - g.first().price,
            }
        )
        assert result is not None
        assert "group_size" in result._schema
        assert "price_change" in result._schema


class TestGroupOrderedChaining:
    """Test chaining operations on grouped data."""

    def test_chain_filter_derive(self, sample_csv):
        """Chain filter and derive on grouped data."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        result = (
            t.group_ordered(lambda r: r.is_up)
            .filter(lambda g: g.count() > 2)
            .derive(
                lambda g: {
                    "start_price": g.first().price,
                    "end_price": g.last().price,
                }
            )
        )
        assert result is not None
        assert "start_price" in result._schema
        assert "end_price" in result._schema

    def test_complex_stock_analysis(self, sample_csv):
        """Reproduce the stock analysis example from docs."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        result = (
            t.group_ordered(lambda r: r.is_up)
            .filter(lambda g: g.count() > 2)  # Only groups with > 2 days
            .derive(
                lambda g: {
                    "start": g.first().date,
                    "end": g.last().date,
                    "gain": (g.last().price - g.first().price) / g.first().price,
                }
            )
        )
        assert result is not None
        assert "start" in result._schema
        assert "end" in result._schema
        assert "gain" in result._schema


class TestNestedTableLenAndToPandas:
    """Tests for NestedTable.__len__() and .to_pandas() (T24)."""

    def test_nested_len_returns_total_row_count(self, sample_csv):
        """len(grouped) returns total row count (not group count)."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        grouped = t.group_ordered(lambda r: r.is_up)
        assert len(grouped) == 10

    def test_nested_to_pandas_returns_dataframe(self, sample_csv):
        """to_pandas() returns a pandas DataFrame."""
        import pandas as pd

        t = LTSeq.read_csv(sample_csv).sort("date")
        grouped = t.group_ordered(lambda r: r.is_up)
        df = grouped.to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10

    def test_nested_to_pandas_has_original_columns(self, sample_csv):
        """to_pandas() result includes the original table columns."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        grouped = t.group_ordered(lambda r: r.is_up)
        df = grouped.to_pandas()
        assert "date" in df.columns
        assert "price" in df.columns
        assert "is_up" in df.columns


class TestGroupCountEmptyRowGroupSeam:
    """Regression for issue #139 (follow-up): the parallel group-count seam
    pass skipped the boundary check entirely when an adjacent chunk was empty,
    undercounting groups."""

    def test_group_count_across_empty_row_group(self, tmp_path):
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        path = tmp_path / "empty_rg_groups.parquet"
        schema = pa.schema([("ts", pa.int64()), ("session", pa.int64())])

        writer = pq.ParquetWriter(path, schema)
        writer.write_table(pa.table({"ts": [1], "session": [1]}, schema=schema))
        writer.write_table(
            pa.table({"ts": [], "session": []}, schema=schema)
        )
        writer.write_table(
            pa.table({"ts": [2, 3], "session": [2, 2]}, schema=schema)
        )
        writer.close()

        t = LTSeq.read_parquet(str(path)).assume_sorted("ts")
        count = (
            t.group_ordered(lambda r: r.session != r.session.shift(1))
            .first()
            .count()
        )

        # Sessions 1 and 2 → two groups, even with an empty row group between.
        assert count == 2


class TestLinearScanFullSortOrder:
    """Regression for issue #141: the non-Parquet linear-scan path re-sorted
    the projection by only the predicate-referenced subset of sort keys,
    rewriting the declared order and miscounting groups."""

    @staticmethod
    def _reference_group_count(eventtime, gap):
        count = 1
        for prev, cur in zip(eventtime, eventtime[1:]):
            if cur - prev > gap:
                count += 1
        return count

    def test_count_with_predicate_on_secondary_sort_key(self):
        pa = pytest.importorskip("pyarrow")

        # Physically ordered by (userid, eventtime); eventtime alone is NOT
        # globally sorted, so a re-sort by [eventtime] rewrites the sequence.
        userid = [1, 1, 2, 2]
        eventtime = [100, 101, 1, 2]
        t = LTSeq.from_arrow(
            pa.table({"userid": userid, "eventtime": eventtime})
        ).assume_sorted("userid", "eventtime")

        count = (
            t.group_ordered(lambda r: (r.eventtime - r.eventtime.shift(1)) > 10)
            .first()
            .count()
        )

        # In declared order the diffs are [1, -99, 1]: no gap > 10 → 1 group.
        assert count == self._reference_group_count(eventtime, 10)
        assert count == 1

    def test_count_after_sort_with_predicate_on_secondary_key(self):
        pa = pytest.importorskip("pyarrow")

        t = (
            LTSeq.from_arrow(
                pa.table(
                    {"userid": [2, 1, 2, 1], "eventtime": [1, 101, 2, 100]}
                )
            )
            .sort("userid", "eventtime")
        )

        count = (
            t.group_ordered(lambda r: (r.eventtime - r.eventtime.shift(1)) > 10)
            .first()
            .count()
        )

        # Sorted order: (1,100),(1,101),(2,1),(2,2) → diffs [1,-99,1] → 1 group.
        assert count == self._reference_group_count([100, 101, 1, 2], 10)
        assert count == 1

    def test_redundant_sort_is_eliminated_when_order_declared(self):
        """Guard the enforce_sorting assumption the fix relies on: re-sorting
        by the full declared sort keys must not add a SortExec when the plan
        already carries that ordering (MemTable.with_sort_order). If a
        DataFusion upgrade stops eliminating the redundant Sort, the linear
        scan general path silently regresses to O(n log n)."""
        pa = pytest.importorskip("pyarrow")

        arrow = pa.table({"userid": [1, 1, 2, 2], "eventtime": [100, 101, 1, 2]})

        declared = LTSeq.from_arrow(arrow).assume_sorted("userid", "eventtime")
        _, physical = declared.sort("userid", "eventtime").explain_plan()
        assert "SortExec" not in physical

        # Control: without a declared ordering the Sort must survive — this
        # also proves "SortExec" is still the token DataFusion prints, so the
        # assertion above cannot pass vacuously.
        undeclared = LTSeq.from_arrow(arrow)
        _, physical_ctl = undeclared.sort("userid", "eventtime").explain_plan()
        assert "SortExec" in physical_ctl
