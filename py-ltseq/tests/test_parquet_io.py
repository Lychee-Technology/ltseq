"""Tests for Parquet I/O functionality."""

import pytest
import os
import tempfile

from ltseq import LTSeq


@pytest.fixture
def sample_parquet(tmp_path):
    """Create a sample Parquet file from CSV data for testing."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Create a simple table
    table = pa.table(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["alice", "bob", "carol", "dave", "eve"],
            "score": [85.5, 92.0, 78.3, 95.1, 88.7],
            "active": [True, False, True, True, False],
        }
    )
    path = str(tmp_path / "test_data.parquet")
    pq.write_table(table, path)
    return path


class TestReadParquet:
    """Test read_parquet functionality."""

    def test_read_parquet_basic(self, sample_parquet):
        """Successfully read a Parquet file."""
        t = LTSeq.read_parquet(sample_parquet)
        assert t is not None
        assert t._schema is not None
        assert len(t._schema) > 0

    def test_read_parquet_schema(self, sample_parquet):
        """Schema is correctly populated from Parquet metadata."""
        t = LTSeq.read_parquet(sample_parquet)
        assert "id" in t._schema
        assert "name" in t._schema
        assert "score" in t._schema
        assert "active" in t._schema

    def test_read_parquet_count(self, sample_parquet):
        """count() returns correct number of rows."""
        t = LTSeq.read_parquet(sample_parquet)
        assert t.count() == 5

    def test_read_parquet_columns(self, sample_parquet):
        """columns property works correctly."""
        t = LTSeq.read_parquet(sample_parquet)
        cols = t.columns
        assert "id" in cols
        assert "name" in cols
        assert "score" in cols

    def test_read_parquet_filter(self, sample_parquet):
        """filter() works on parquet-loaded data."""
        t = LTSeq.read_parquet(sample_parquet)
        result = t.filter(lambda r: r.score > 90)
        assert result.count() == 2  # bob (92.0) and dave (95.1)

    def test_read_parquet_select(self, sample_parquet):
        """select() works on parquet-loaded data."""
        t = LTSeq.read_parquet(sample_parquet)
        result = t.select("id", "name")
        df = result.to_pandas()
        assert list(df.columns) == ["id", "name"]
        assert len(df) == 5

    def test_read_parquet_sort(self, sample_parquet):
        """sort() works on parquet-loaded data."""
        t = LTSeq.read_parquet(sample_parquet)
        result = t.sort("score", desc=True)
        df = result.to_pandas()
        assert df.iloc[0]["name"] == "dave"  # highest score 95.1

    def test_read_parquet_agg(self, sample_parquet):
        """agg() works on parquet-loaded data."""
        t = LTSeq.read_parquet(sample_parquet)
        result = t.agg(total=lambda g: g.score.sum())
        rows = result.collect()
        assert len(rows) == 1
        assert abs(rows[0]["total"] - (85.5 + 92.0 + 78.3 + 95.1 + 88.7)) < 0.01

    def test_read_parquet_to_pandas(self, sample_parquet):
        """to_pandas() works on parquet-loaded data."""
        t = LTSeq.read_parquet(sample_parquet)
        df = t.to_pandas()
        assert len(df) == 5
        assert "id" in df.columns

    def test_read_parquet_collect(self, sample_parquet):
        """collect() works on parquet-loaded data."""
        t = LTSeq.read_parquet(sample_parquet)
        rows = t.collect()
        assert len(rows) == 5
        assert all(isinstance(r, dict) for r in rows)

    def test_read_parquet_name(self, sample_parquet):
        """Table name is derived from filename."""
        t = LTSeq.read_parquet(sample_parquet)
        assert t._name == "test_data"

    def test_read_parquet_nonexistent(self):
        """Reading non-existent file returns empty table (DataFusion glob behavior)."""
        # DataFusion treats file paths as globs; non-matching globs = 0 rows
        t = LTSeq.read_parquet("/nonexistent/path/to/file.parquet")
        assert t._schema == {}
        assert t.count() == 0
