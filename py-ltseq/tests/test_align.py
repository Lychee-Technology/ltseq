"""Tests for LTSeq.align() method."""

import os
import tempfile
import pytest
from ltseq import LTSeq


@pytest.fixture
def sample_csv():
    """Create a sample CSV with gaps in dates."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("date,value,name\n")
        f.write("2024-01-02,100,Alice\n")
        f.write("2024-01-04,200,Bob\n")
        f.write("2024-01-05,300,Charlie\n")
        path = f.name
    yield path
    os.unlink(path)


@pytest.fixture
def int_key_csv():
    """Create a sample CSV with integer keys."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("id,value\n")
        f.write("2,20\n")
        f.write("4,40\n")
        f.write("5,50\n")
        path = f.name
    yield path
    os.unlink(path)


class TestAlignBasic:
    """Basic functionality tests for align()."""

    def test_align_reorders_rows(self, sample_csv):
        """Test that align reorders rows to match ref_sequence."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-04", "2024-01-02"]  # Reversed order
        result = t.align(ref, key=lambda r: r.date)

        df = result.to_pandas()
        assert len(df) == 2
        assert str(df.iloc[0]["date"]) == "2024-01-04"
        assert str(df.iloc[1]["date"]) == "2024-01-02"

    def test_align_inserts_null_rows(self, sample_csv):
        """Test that align inserts NULL rows for missing keys."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-01", "2024-01-02", "2024-01-03"]
        result = t.align(ref, key=lambda r: r.date)

        df = result.to_pandas()
        assert len(df) == 3
        # First row: 2024-01-01 not in original -> NULL
        assert df.iloc[0]["value"] is None or (
            hasattr(df.iloc[0]["value"], "__float__")
            and str(df.iloc[0]["value"]) == "nan"
        )
        # Second row: 2024-01-02 exists
        assert df.iloc[1]["value"] == 100
        # Third row: 2024-01-03 not in original -> NULL
        assert df.iloc[2]["value"] is None or (
            hasattr(df.iloc[2]["value"], "__float__")
            and str(df.iloc[2]["value"]) == "nan"
        )

    def test_align_excludes_unmatched(self, sample_csv):
        """Test that rows not in ref_sequence are excluded."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02", "2024-01-04"]  # Excludes 2024-01-05
        result = t.align(ref, key=lambda r: r.date)

        df = result.to_pandas()
        assert len(df) == 2
        dates = df["date"].tolist()
        assert "2024-01-05" not in dates

    def test_align_preserves_schema(self, sample_csv):
        """Test that align preserves the original schema."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02"]
        result = t.align(ref, key=lambda r: r.date)

        assert set(result._schema.keys()) == {"date", "value", "name"}

    def test_align_returns_ltseq(self, sample_csv):
        """Test that align returns an LTSeq instance."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02"]
        result = t.align(ref, key=lambda r: r.date)

        assert isinstance(result, LTSeq)


class TestAlignDuplicates:
    """Tests for duplicate key handling."""

    def test_align_duplicate_ref_keys(self, sample_csv):
        """Test that duplicate ref_sequence keys produce duplicate rows."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02", "2024-01-02", "2024-01-04"]
        result = t.align(ref, key=lambda r: r.date)

        df = result.to_pandas()
        assert len(df) == 3
        assert str(df.iloc[0]["date"]) == "2024-01-02"
        assert str(df.iloc[1]["date"]) == "2024-01-02"
        assert str(df.iloc[2]["date"]) == "2024-01-04"


class TestAlignIntKeys:
    """Tests for integer key handling."""

    def test_align_with_int_keys(self, int_key_csv):
        """Test align with integer keys."""
        t = LTSeq.read_csv(int_key_csv)
        ref = [1, 2, 3, 4]
        result = t.align(ref, key=lambda r: r.id)

        df = result.to_pandas()
        assert len(df) == 4
        # id=1 and id=3 should be NULL
        import pandas as pd

        assert pd.isna(df.iloc[0]["value"])
        assert df.iloc[1]["value"] == 20
        assert pd.isna(df.iloc[2]["value"])
        assert df.iloc[3]["value"] == 40


class TestAlignErrors:
    """Error handling tests for align()."""

    def test_align_empty_ref_sequence(self, sample_csv):
        """Test that empty ref_sequence raises ValueError."""
        t = LTSeq.read_csv(sample_csv)
        with pytest.raises(ValueError, match="ref_sequence cannot be empty"):
            t.align([], key=lambda r: r.date)

    def test_align_no_schema(self):
        """Test that align raises if schema not initialized."""
        t = LTSeq()
        with pytest.raises(ValueError, match="Schema not initialized"):
            t.align(["a"], key=lambda r: r.col)

    def test_align_invalid_key_type(self, sample_csv):
        """Test that align raises for non-callable key."""
        t = LTSeq.read_csv(sample_csv)
        with pytest.raises(TypeError, match="key must be callable"):
            t.align(["a"], key="date")  # type: ignore

    def test_align_nonexistent_column(self, sample_csv):
        """Test that align raises for nonexistent key column."""
        t = LTSeq.read_csv(sample_csv)
        with pytest.raises(AttributeError, match="not found in schema"):
            t.align(["a"], key=lambda r: r.nonexistent)

    def test_align_complex_expression(self, sample_csv):
        """Test that align raises for complex key expressions."""
        t = LTSeq.read_csv(sample_csv)
        with pytest.raises(TypeError, match="simple column reference"):
            t.align(["a"], key=lambda r: r.value + 1)


class TestAlignEdgeCases:
    """Edge case tests for align()."""

    def test_align_all_keys_missing(self, sample_csv):
        """Test align when all ref keys are missing from table."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-10", "2024-01-11"]
        result = t.align(ref, key=lambda r: r.date)

        df = result.to_pandas()
        assert len(df) == 2
        import pandas as pd

        assert all(pd.isna(df["value"]))

    def test_align_all_keys_present(self, sample_csv):
        """Test align when all ref keys exist in table."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02", "2024-01-04", "2024-01-05"]
        result = t.align(ref, key=lambda r: r.date)

        df = result.to_pandas()
        assert len(df) == 3
        import pandas as pd

        assert all(pd.notna(df["value"]))

    def test_align_single_row(self, sample_csv):
        """Test align with single ref key."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02"]
        result = t.align(ref, key=lambda r: r.date)

        df = result.to_pandas()
        assert len(df) == 1
        assert df.iloc[0]["value"] == 100

    def test_align_preserves_column_order(self, sample_csv):
        """Test that align preserves the original column order."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02"]
        result = t.align(ref, key=lambda r: r.date)

        df = result.to_pandas()
        columns = list(df.columns)
        # Original schema order should be preserved
        assert columns == ["date", "value", "name"]

    def test_align_clears_sort_keys(self, sample_csv):
        """Test that align clears sort key tracking."""
        t = LTSeq.read_csv(sample_csv)
        t_sorted = t.sort("date")
        assert t_sorted.sort_keys is not None

        ref = ["2024-01-02", "2024-01-04"]
        result = t_sorted.align(ref, key=lambda r: r.date)

        # Sort keys should be cleared after align (order is now from ref_sequence)
        assert result.sort_keys is None


class TestAlignChaining:
    """Tests for chaining align with other operations."""

    def test_align_then_filter(self, sample_csv):
        """Test align followed by filter."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
        result = t.align(ref, key=lambda r: r.date).filter(
            lambda r: r.value.is_not_null()
        )

        df = result.to_pandas()
        assert len(df) == 2  # Only 2024-01-02 and 2024-01-04 have values
        import pandas as pd

        assert all(pd.notna(df["value"]))

    def test_align_then_derive(self, sample_csv):
        """Test align followed by derive."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02", "2024-01-04"]
        result = t.align(ref, key=lambda r: r.date).derive(
            doubled=lambda r: r.value * 2
        )

        df = result.to_pandas()
        assert len(df) == 2
        assert df.iloc[0]["doubled"] == 200
        assert df.iloc[1]["doubled"] == 400
