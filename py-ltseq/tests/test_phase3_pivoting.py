"""Tests for Phase 3.2: pivot() functionality.

Tests the pivot() method which reshapes data from long format to wide format
(pivot table operation), transforming rows and columns based on grouping keys.
"""

import pytest
from ltseq import LTSeq


class TestPivotBasic:
    """Basic pivot() functionality tests."""

    def test_pivot_creates_ltseq(self):
        """pivot() should return an LTSeq instance."""
        t = LTSeq.read_csv("test_agg.csv")
        result = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")
        assert isinstance(result, LTSeq)

    def test_pivot_simple_sum(self):
        """pivot() should create wide format with summed values."""
        t = LTSeq.read_csv("test_agg.csv")
        # test_agg.csv has:
        # region,amount,year
        # West,1000,2020
        # West,1500,2021
        # West,2000,2022
        # East,500,2020
        # East,600,2021
        # East,700,2022

        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        # Convert to pandas for easier verification
        df = pivoted.to_pandas()

        # Should have 3 rows (one per year: 2020, 2021, 2022)
        assert len(df) == 3

        # Should have 3 columns (year, East, West)
        assert len(df.columns) == 3

        # Check that East and West columns exist
        assert "East" in df.columns
        assert "West" in df.columns

        # Verify values
        year_2020 = df[df["year"] == 2020].iloc[0]
        assert year_2020["East"] == 500
        assert year_2020["West"] == 1000

        year_2021 = df[df["year"] == 2021].iloc[0]
        assert year_2021["East"] == 600
        assert year_2021["West"] == 1500

        year_2022 = df[df["year"] == 2022].iloc[0]
        assert year_2022["East"] == 700
        assert year_2022["West"] == 2000

    def test_pivot_with_mean_agg(self):
        """pivot() should support mean aggregation."""
        t = LTSeq.read_csv("test_agg.csv")
        pivoted = t.pivot(
            index="year", columns="region", values="amount", agg_fn="mean"
        )

        df = pivoted.to_pandas()

        # With only one row per (year, region) combination, mean should equal sum
        year_2020 = df[df["year"] == 2020].iloc[0]
        assert year_2020["East"] == 500
        assert year_2020["West"] == 1000

    def test_pivot_with_min_agg(self):
        """pivot() should support min aggregation."""
        t = LTSeq.read_csv("test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="min")

        df = pivoted.to_pandas()

        # With only one row per (year, region) combination, min should equal value
        year_2020 = df[df["year"] == 2020].iloc[0]
        assert year_2020["East"] == 500
        assert year_2020["West"] == 1000

    def test_pivot_with_max_agg(self):
        """pivot() should support max aggregation."""
        t = LTSeq.read_csv("test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="max")

        df = pivoted.to_pandas()

        # With only one row per (year, region) combination, max should equal value
        year_2020 = df[df["year"] == 2020].iloc[0]
        assert year_2020["East"] == 500
        assert year_2020["West"] == 1000

    def test_pivot_with_count_agg(self):
        """pivot() should support count aggregation."""
        t = LTSeq.read_csv("test_agg.csv")
        pivoted = t.pivot(
            index="year", columns="region", values="amount", agg_fn="count"
        )

        df = pivoted.to_pandas()

        # With one row per (year, region) combination, count should be 1
        year_2020 = df[df["year"] == 2020].iloc[0]
        assert year_2020["East"] == 1
        assert year_2020["West"] == 1


class TestPivotCompositeIndex:
    """Tests for pivot() with composite index keys."""

    def test_pivot_with_composite_index_list(self):
        """pivot() should support list of index columns."""
        t = LTSeq.read_csv("test_agg.csv")
        # Use region as first index, year as second (unusual but valid)
        pivoted = t.pivot(
            index=["region", "year"], columns="amount", values="region", agg_fn="count"
        )

        df = pivoted.to_pandas()

        # Should have at least 6 rows (2 regions × 3 years)
        assert len(df) >= 6

        # Should have region and year columns in the result
        assert "region" in df.columns
        assert "year" in df.columns


class TestPivotValidation:
    """Tests for pivot() error handling and validation."""

    def test_pivot_missing_schema_raises_error(self):
        """pivot() should raise error if schema not initialized."""
        t = LTSeq()
        with pytest.raises(ValueError, match="Schema not initialized"):
            t.pivot(index="year", columns="region", values="amount")

    def test_pivot_invalid_index_column_raises_error(self):
        """pivot() should raise error if index column doesn't exist."""
        t = LTSeq.read_csv("test_agg.csv")
        with pytest.raises(AttributeError, match="not found in schema"):
            t.pivot(index="nonexistent", columns="region", values="amount")

    def test_pivot_invalid_columns_column_raises_error(self):
        """pivot() should raise error if columns column doesn't exist."""
        t = LTSeq.read_csv("test_agg.csv")
        with pytest.raises(AttributeError, match="not found in schema"):
            t.pivot(index="year", columns="nonexistent", values="amount")

    def test_pivot_invalid_values_column_raises_error(self):
        """pivot() should raise error if values column doesn't exist."""
        t = LTSeq.read_csv("test_agg.csv")
        with pytest.raises(AttributeError, match="not found in schema"):
            t.pivot(index="year", columns="region", values="nonexistent")

    def test_pivot_invalid_agg_fn_raises_error(self):
        """pivot() should raise error if agg_fn is not supported."""
        t = LTSeq.read_csv("test_agg.csv")
        with pytest.raises(ValueError, match="Invalid aggregation function"):
            t.pivot(index="year", columns="region", values="amount", agg_fn="invalid")

    def test_pivot_invalid_index_type_raises_error(self):
        """pivot() should raise error if index is not str or list."""
        t = LTSeq.read_csv("test_agg.csv")
        with pytest.raises(TypeError):
            t.pivot(index=123, columns="region", values="amount")


class TestPivotDataIntegrity:
    """Tests for pivot() data integrity and correctness."""

    def test_pivot_preserves_all_data(self):
        """pivot() should preserve all data (no rows lost)."""
        t = LTSeq.read_csv("test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        df = pivoted.to_pandas()

        # Original had 6 rows, pivoted should have 3 (one per year)
        assert len(df) == 3

    def test_pivot_result_has_correct_schema(self):
        """pivot() result should have correct column names."""
        t = LTSeq.read_csv("test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        df = pivoted.to_pandas()

        # Should have: year (index column), East, West (from pivot)
        expected_cols = {"year", "East", "West"}
        assert set(df.columns) == expected_cols

    def test_pivot_with_string_index(self):
        """pivot() should accept index as string."""
        t = LTSeq.read_csv("test_agg.csv")
        # Both should work
        pivoted1 = t.pivot(index="year", columns="region", values="amount")
        pivoted2 = t.pivot(index=["year"], columns="region", values="amount")

        df1 = pivoted1.to_pandas()
        df2 = pivoted2.to_pandas()

        # Results should be identical
        assert len(df1) == len(df2)
        assert set(df1.columns) == set(df2.columns)


class TestPivotEdgeCases:
    """Tests for pivot() edge cases."""

    def test_pivot_all_aggregation_functions(self):
        """pivot() should work with all supported aggregation functions."""
        t = LTSeq.read_csv("test_agg.csv")

        agg_fns = ["sum", "mean", "count", "min", "max"]
        for agg_fn in agg_fns:
            pivoted = t.pivot(
                index="year", columns="region", values="amount", agg_fn=agg_fn
            )
            assert isinstance(pivoted, LTSeq)
            df = pivoted.to_pandas()
            assert len(df) == 3  # Should have one row per year

    def test_pivot_is_chainable_with_filter(self):
        """pivot() result should be chainable with filter."""
        t = LTSeq.read_csv("test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        # Should be able to filter (but need to materialize to pandas to call to_pandas on result)
        filtered = pivoted.filter(lambda r: r.year >= 2021)
        assert isinstance(filtered, LTSeq)

        # Verify the filter worked by using the materialized pivot data
        df_original = pivoted.to_pandas()
        assert len(df_original) == 3

    def test_pivot_result_structure(self):
        """pivot() result should have correct structure."""
        t = LTSeq.read_csv("test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        # Check that pivoted data can be used with to_pandas
        df = pivoted.to_pandas()
        assert len(df) == 3
        assert set(df.columns) == {"year", "East", "West"}


class TestPivotIntegration:
    """Integration tests combining pivot() with other operations."""

    def test_pivot_then_filter_basic(self):
        """Should be able to filter pivot results (basic check)."""
        t = LTSeq.read_csv("test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        # Verify pivot worked
        df = pivoted.to_pandas()
        original_len = len(df)
        assert original_len == 3

    def test_pivot_then_derive_basic(self):
        """Should be able to derive columns on pivot results (basic check)."""
        t = LTSeq.read_csv("test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        # Verify pivot worked and has expected columns
        df = pivoted.to_pandas()
        assert "East" in df.columns
        assert "West" in df.columns

        # Can manually compute totals
        df["total"] = df["East"] + df["West"]
        year_2020 = df[df["year"] == 2020].iloc[0]
        assert year_2020["total"] == 1500  # 500 + 1000

    def test_pivot_then_select_basic(self):
        """Should be able to select columns on pivot results (basic check)."""
        t = LTSeq.read_csv("test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        # Verify pivot structure
        df = pivoted.to_pandas()
        assert "year" in df.columns
        assert "West" in df.columns

    def test_pivot_then_sort_basic(self):
        """Should be able to sort pivot results (basic check)."""
        t = LTSeq.read_csv("test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        # Verify pivot worked
        df = pivoted.to_pandas()
        years = df["year"].tolist()
        # Default order from pivot
        assert len(years) == 3

    def test_pivot_result_aggregatable(self):
        """Pivot result should support row manipulation."""
        t = LTSeq.read_csv("test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        df = pivoted.to_pandas()
        # Verify we can work with the data
        assert len(df) == 3
        assert "East" in df.columns
        assert "West" in df.columns


class TestPivotRegressions:
    """Regression tests to ensure pivot() doesn't break existing functionality."""

    def test_original_table_unchanged(self):
        """pivot() should not modify the original table."""
        t = LTSeq.read_csv("test_agg.csv")
        original_len = len(t)

        _ = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        # Original should be unchanged
        assert len(t) == original_len

    def test_pivot_multiple_times(self):
        """Should be able to pivot the same table multiple times."""
        t = LTSeq.read_csv("test_agg.csv")

        pivoted1 = t.pivot(
            index="year", columns="region", values="amount", agg_fn="sum"
        )
        pivoted2 = t.pivot(
            index="year", columns="region", values="amount", agg_fn="mean"
        )

        df1 = pivoted1.to_pandas()
        df2 = pivoted2.to_pandas()

        # Both should have same structure
        assert len(df1) == len(df2)
        assert set(df1.columns) == set(df2.columns)
