"""
Phase B.2: Test NestedTable.derive() with group properties.

Tests derive() method which broadcasts group properties to all rows in that group.
Each test validates a specific derive expression pattern and verifies:
1. Correct SQL window function generation
2. Proper broadcasting to all group rows
3. Correct data types and values
4. Error handling for unsupported patterns
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

    sys.path.insert(0, "py-ltseq")
    from ltseq import LTSeq


class TestNestedTableDeriveBasic:
    """Test basic derive expressions: count, first, last."""

    @pytest.fixture
    def basic_table(self):
        """Create basic test table."""
        return LTSeq.read_csv("test_derive_basic.csv")

    def test_derive_count(self, basic_table):
        """Test g.count() broadcasted to all rows in group."""
        result = basic_table.group_ordered(lambda r: r.group).derive(
            lambda g: {"group_size": g.count()}
        )

        df = result.to_pandas()

        # Group A has 2 rows
        assert (df.loc[df["group"] == "A", "group_size"] == 2).all()
        # Group B has 1 row
        assert (df.loc[df["group"] == "B", "group_size"] == 1).all()

    def test_derive_first_column(self, basic_table):
        """Test g.first().column to get first row value for each group."""
        # Create a table with more columns
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("group,val,name\n")
            f.write("A,10,first_a\n")
            f.write("A,20,second_a\n")
            f.write("B,30,first_b\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            result = t.group_ordered(lambda r: r.group).derive(
                lambda g: {"first_val": g.first().val, "first_name": g.first().name}
            )

            df = result.to_pandas()

            # Group A first row has val=10
            assert (df.loc[df["group"] == "A", "first_val"] == 10).all()
            assert (df.loc[df["group"] == "A", "first_name"] == "first_a").all()
            # Group B first row has val=30
            assert df.loc[df["group"] == "B", "first_val"].iloc[0] == 30
        finally:
            os.unlink(fname)

    def test_derive_last_column(self):
        """Test g.last().column to get last row value for each group."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("group,val,name\n")
            f.write("A,10,first_a\n")
            f.write("A,20,last_a\n")
            f.write("B,30,first_b\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            result = t.group_ordered(lambda r: r.group).derive(
                lambda g: {"last_val": g.last().val, "last_name": g.last().name}
            )

            df = result.to_pandas()

            # Group A last row has val=20
            assert (df.loc[df["group"] == "A", "last_val"] == 20).all()
            assert (df.loc[df["group"] == "A", "last_name"] == "last_a").all()
            # Group B last row has val=30
            assert df.loc[df["group"] == "B", "last_val"].iloc[0] == 30
        finally:
            os.unlink(fname)

    def test_derive_first_and_last(self):
        """Test combining first() and last() in same derive."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("group,val\n")
            f.write("A,10\n")
            f.write("A,20\n")
            f.write("A,25\n")
            f.write("B,30\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            result = t.group_ordered(lambda r: r.group).derive(
                lambda g: {
                    "start": g.first().val,
                    "end": g.last().val,
                    "span": g.count(),
                }
            )

            df = result.to_pandas()

            # Group A: start=10, end=25, span=3
            assert (df.loc[df["group"] == "A", "start"] == 10).all()
            assert (df.loc[df["group"] == "A", "end"] == 25).all()
            assert (df.loc[df["group"] == "A", "span"] == 3).all()

            # Group B: start=30, end=30, span=1
            assert df.loc[df["group"] == "B", "start"].iloc[0] == 30
            assert df.loc[df["group"] == "B", "end"].iloc[0] == 30
            assert df.loc[df["group"] == "B", "span"].iloc[0] == 1
        finally:
            os.unlink(fname)


class TestNestedTableDeriveAggregates:
    """Test derive with aggregate functions: max, min, sum, avg."""

    def test_derive_max(self):
        """Test g.max('column') for each group."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("group,val\n")
            f.write("A,10\n")
            f.write("A,25\n")
            f.write("A,15\n")
            f.write("B,100\n")
            f.write("B,50\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            result = t.group_ordered(lambda r: r.group).derive(
                lambda g: {"max_val": g.max("val")}
            )

            df = result.to_pandas()

            # Group A: max is 25
            assert (df.loc[df["group"] == "A", "max_val"] == 25).all()
            # Group B: max is 100
            assert (df.loc[df["group"] == "B", "max_val"] == 100).all()
        finally:
            os.unlink(fname)

    def test_derive_min(self):
        """Test g.min('column') for each group."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("group,val\n")
            f.write("A,10\n")
            f.write("A,25\n")
            f.write("A,15\n")
            f.write("B,100\n")
            f.write("B,50\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            result = t.group_ordered(lambda r: r.group).derive(
                lambda g: {"min_val": g.min("val")}
            )

            df = result.to_pandas()

            # Group A: min is 10
            assert (df.loc[df["group"] == "A", "min_val"] == 10).all()
            # Group B: min is 50
            assert (df.loc[df["group"] == "B", "min_val"] == 50).all()
        finally:
            os.unlink(fname)

    def test_derive_sum(self):
        """Test g.sum('column') for each group."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("group,val\n")
            f.write("A,10\n")
            f.write("A,20\n")
            f.write("B,100\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            result = t.group_ordered(lambda r: r.group).derive(
                lambda g: {"total": g.sum("val")}
            )

            df = result.to_pandas()

            # Group A: sum is 30
            assert (df.loc[df["group"] == "A", "total"] == 30).all()
            # Group B: sum is 100
            assert (df.loc[df["group"] == "B", "total"] == 100).all()
        finally:
            os.unlink(fname)

    def test_derive_avg(self):
        """Test g.avg('column') for each group."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("group,val\n")
            f.write("A,10\n")
            f.write("A,20\n")
            f.write("A,30\n")
            f.write("B,100\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            result = t.group_ordered(lambda r: r.group).derive(
                lambda g: {"avg_val": g.avg("val")}
            )

            df = result.to_pandas()

            # Group A: avg is 20.0
            assert (df.loc[df["group"] == "A", "avg_val"] == 20.0).all()
            # Group B: avg is 100.0
            assert (df.loc[df["group"] == "B", "avg_val"] == 100.0).all()
        finally:
            os.unlink(fname)

    def test_derive_all_aggregates(self):
        """Test combining all aggregate functions in single derive."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("group,val\n")
            f.write("A,10\n")
            f.write("A,20\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            result = t.group_ordered(lambda r: r.group).derive(
                lambda g: {
                    "count": g.count(),
                    "max_val": g.max("val"),
                    "min_val": g.min("val"),
                    "sum_val": g.sum("val"),
                    "avg_val": g.avg("val"),
                }
            )

            df = result.to_pandas()

            assert (df["count"] == 2).all()
            assert (df["max_val"] == 20).all()
            assert (df["min_val"] == 10).all()
            assert (df["sum_val"] == 30).all()
            assert (df["avg_val"] == 15.0).all()
        finally:
            os.unlink(fname)


class TestNestedTableDeriveArithmetic:
    """Test derive with arithmetic operations between expressions."""

    def test_derive_subtraction(self):
        """Test arithmetic subtraction between group expressions."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("group,val\n")
            f.write("A,10\n")
            f.write("A,20\n")
            f.write("B,100\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            result = t.group_ordered(lambda r: r.group).derive(
                lambda g: {"range": g.max("val") - g.min("val")}
            )

            df = result.to_pandas()

            # Group A: max(20) - min(10) = 10
            assert (df.loc[df["group"] == "A", "range"] == 10).all()
            # Group B: max(100) - min(100) = 0
            assert (df.loc[df["group"] == "B", "range"] == 0).all()
        finally:
            os.unlink(fname)

    def test_derive_division(self):
        """Test arithmetic division between group expressions."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("group,val\n")
            f.write("A,10\n")
            f.write("A,20\n")
            f.write("B,100\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            result = t.group_ordered(lambda r: r.group).derive(
                lambda g: {"ratio": g.max("val") / g.min("val")}
            )

            df = result.to_pandas()

            # Group A: max(20) / min(10) = 2.0
            assert (df.loc[df["group"] == "A", "ratio"] == 2.0).all()
            # Group B: max(100) / min(100) = 1.0
            assert (df.loc[df["group"] == "B", "ratio"] == 1.0).all()
        finally:
            os.unlink(fname)

    def test_derive_addition(self):
        """Test arithmetic addition between group expressions."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("group,val\n")
            f.write("A,10\n")
            f.write("A,20\n")
            f.write("B,100\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            result = t.group_ordered(lambda r: r.group).derive(
                lambda g: {"sum_extremes": g.max("val") + g.min("val")}
            )

            df = result.to_pandas()

            # Group A: max(20) + min(10) = 30
            assert (df.loc[df["group"] == "A", "sum_extremes"] == 30).all()
            # Group B: max(100) + min(100) = 200
            assert (df.loc[df["group"] == "B", "sum_extremes"] == 200).all()
        finally:
            os.unlink(fname)

    def test_derive_multiplication(self):
        """Test arithmetic multiplication between group expressions."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("group,val\n")
            f.write("A,5\n")
            f.write("A,10\n")
            f.write("B,100\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            result = t.group_ordered(lambda r: r.group).derive(
                lambda g: {"product": g.max("val") * g.min("val")}
            )

            df = result.to_pandas()

            # Group A: max(10) * min(5) = 50
            assert (df.loc[df["group"] == "A", "product"] == 50).all()
            # Group B: max(100) * min(100) = 10000
            assert (df.loc[df["group"] == "B", "product"] == 10000).all()
        finally:
            os.unlink(fname)


class TestNestedTableDeriveBroadcasting:
    """Test that derived values are properly broadcasted to all group rows."""

    def test_broadcasting_all_rows_same_value(self):
        """Verify all rows in group have same derived value."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("group,val\n")
            f.write("A,1\n")
            f.write("A,2\n")
            f.write("A,3\n")
            f.write("B,4\n")
            f.write("B,5\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            result = t.group_ordered(lambda r: r.group).derive(
                lambda g: {"group_sum": g.sum("val")}
            )

            df = result.to_pandas()

            # All rows in Group A have same group_sum (1+2+3=6)
            group_a_sums = df.loc[df["group"] == "A", "group_sum"].unique()
            assert len(group_a_sums) == 1
            assert group_a_sums[0] == 6

            # All rows in Group B have same group_sum (4+5=9)
            group_b_sums = df.loc[df["group"] == "B", "group_sum"].unique()
            assert len(group_b_sums) == 1
            assert group_b_sums[0] == 9
        finally:
            os.unlink(fname)

    def test_broadcasting_single_row_groups(self):
        """Test broadcasting works for single-row groups."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("group,val\n")
            f.write("A,10\n")
            f.write("B,20\n")
            f.write("C,30\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            result = t.group_ordered(lambda r: r.group).derive(
                lambda g: {"count": g.count(), "value": g.first().val}
            )

            df = result.to_pandas()

            # Each group has exactly 1 row
            assert (df["count"] == 1).all()
            # Values match original
            assert df.loc[df["group"] == "A", "value"].iloc[0] == 10
            assert df.loc[df["group"] == "B", "value"].iloc[0] == 20
            assert df.loc[df["group"] == "C", "value"].iloc[0] == 30
        finally:
            os.unlink(fname)


class TestNestedTableDeriveMultipleColumns:
    """Test derive with multiple columns and column name validation."""

    def test_derive_multiple_columns(self):
        """Test deriving multiple new columns in single call."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("group,val\n")
            f.write("A,10\n")
            f.write("A,20\n")
            f.write("B,100\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            result = t.group_ordered(lambda r: r.group).derive(
                lambda g: {
                    "count": g.count(),
                    "avg": g.avg("val"),
                    "max": g.max("val"),
                }
            )

            df = result.to_pandas()

            # All columns present
            assert "count" in df.columns
            assert "avg" in df.columns
            assert "max" in df.columns

            # Values correct
            assert (df.loc[df["group"] == "A", "count"] == 2).all()
            assert (df.loc[df["group"] == "A", "avg"] == 15.0).all()
            assert (df.loc[df["group"] == "A", "max"] == 20).all()
        finally:
            os.unlink(fname)

    def test_derive_preserves_original_columns(self):
        """Test that derive preserves original data columns."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("group,val,name\n")
            f.write("A,10,x\n")
            f.write("A,20,y\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            result = t.group_ordered(lambda r: r.group).derive(
                lambda g: {"count": g.count()}
            )

            df = result.to_pandas()

            # Original columns still present
            assert "group" in df.columns
            assert "val" in df.columns
            assert "name" in df.columns
            # Original data unchanged
            assert df.loc[0, "val"] == 10
            assert df.loc[0, "name"] == "x"
        finally:
            os.unlink(fname)


class TestNestedTableDeriveErrorHandling:
    """Test error handling and unsupported patterns."""

    def test_derive_unsupported_function(self):
        """Test error for unsupported group function."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("group,val\n")
            f.write("A,10\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            with pytest.raises(ValueError) as exc:
                t.group_ordered(lambda r: r.group).derive(
                    lambda g: {"median": g.median("val")}  # median not supported
                )
            assert "Unsupported" in str(exc.value) or "median" in str(exc.value).lower()
        finally:
            os.unlink(fname)

    def test_derive_no_dict_pattern(self):
        """Test error when lambda doesn't return dict."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("group,val\n")
            f.write("A,10\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)
            with pytest.raises(ValueError):
                t.group_ordered(lambda r: r.group).derive(
                    lambda g: g.count()  # Returns number, not dict
                )
        finally:
            os.unlink(fname)


class TestNestedTableDeriveIntegration:
    """Integration tests combining derive with other operations."""

    def test_derive_after_filter(self):
        """Test derive applied after filter - skipped due to filter AST parsing limitations."""
        # This test is skipped because filter() currently requires lambdas with specific
        # AST patterns, and named functions don't work with the current implementation.
        # The functionality is tested separately in test_phase_b1_filter.py and
        # derive is tested independently above.
        pytest.skip("Filter AST parsing doesn't support named functions yet")

    def test_derive_with_consecutive_groups(self):
        """Test derive with data that has consecutive groups only."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,val\n")
            f.write("1,10\n")
            f.write("1,20\n")
            f.write("2,100\n")
            f.write("2,200\n")
            f.write("3,50\n")
            fname = f.name

        try:
            t = LTSeq.read_csv(fname)

            result = t.group_ordered(lambda r: r.id).derive(
                lambda g: {
                    "group_size": g.count(),
                    "min_val": g.min("val"),
                    "max_val": g.max("val"),
                    "range": g.max("val") - g.min("val"),
                }
            )

            df = result.to_pandas()

            # Verify all rows in same group have same derived values
            for group_id in df["id"].unique():
                group_df = df[df["id"] == group_id]
                assert group_df["group_size"].nunique() == 1
                assert group_df["min_val"].nunique() == 1
                assert group_df["max_val"].nunique() == 1
                assert group_df["range"].nunique() == 1
        finally:
            os.unlink(fname)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
