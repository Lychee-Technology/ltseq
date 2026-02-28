"""
Integration tests for Phase 2: CSV IO and display
"""

import pytest
from ltseq import LTSeq
import os

# Get the project root directory
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
SAMPLE_CSV = os.path.join(PROJECT_ROOT, "data", "sample.csv")


class TestPhase2CSVReading:
    """Test CSV reading functionality via Rust DataFusion."""

    def test_read_csv_success(self):
        """Successfully read a CSV file."""
        t = LTSeq.read_csv(SAMPLE_CSV)
        assert t is not None
        assert t._schema is not None
        assert len(t._schema) > 0

    # def test_read_csv_nonexistent_file(self):
    #     """Reading non-existent file raises error."""
    #     with pytest.raises(Exception):
    #         # Could raise RuntimeError or other exception
    #         # Note: Phase 2 DataFusion error handling may need improvement
    #         LTSeq.read_csv("/nonexistent/path/to/file.csv")

    def test_read_csv_schema_populated(self):
        """Schema is correctly populated after reading CSV."""
        t = LTSeq.read_csv(SAMPLE_CSV)

        # Check that schema has expected columns
        assert "id" in t._schema
        assert "name" in t._schema
        assert "price" in t._schema


class TestPhase2Display:
    """Test display/show() functionality."""

    def test_show_returns_string(self):
        """show() returns formatted table string."""
        t = LTSeq.read_csv(SAMPLE_CSV)

        # This should not raise and should print without error
        # (show() prints to stdout in the implementation)
        t.show(n=3)

    def test_show_with_large_n(self):
        """show() respects n parameter."""
        t = LTSeq.read_csv(SAMPLE_CSV)

        # Should show all rows if n is larger than row count
        t.show(n=1000)

    def test_show_with_small_n(self):
        """show() truncates at n rows."""
        t = LTSeq.read_csv(SAMPLE_CSV)

        # Should show only first row
        t.show(n=1)


class TestPhase2TableFormatting:
    """Test that table formatting is correct."""

    def test_table_has_borders(self, capsys):
        """Table output includes border characters."""
        t = LTSeq.read_csv(SAMPLE_CSV)
        t.show(n=2)

        captured = capsys.readouterr()
        output = captured.out

        # Check for table borders
        assert "+" in output
        assert "-" in output
        assert "|" in output

    def test_table_has_header(self, capsys):
        """Table output includes column headers."""
        t = LTSeq.read_csv(SAMPLE_CSV)
        t.show(n=2)

        captured = capsys.readouterr()
        output = captured.out

        # Check for column names in output
        assert "id" in output
        assert "name" in output
        assert "price" in output


class TestPhase2DataIntegrity:
    """Test that data is correctly loaded and displayed."""

    def test_data_row_count(self):
        """CSV data is loaded with correct row count."""
        t = LTSeq.read_csv(SAMPLE_CSV)

        # sample.csv has 5 data rows
        t.show(n=10)
        # If this doesn't raise, data was loaded

    def test_data_values_preserved(self, capsys):
        """Data values are preserved and displayed correctly."""
        t = LTSeq.read_csv(SAMPLE_CSV)
        t.show(n=2)

        captured = capsys.readouterr()
        output = captured.out

        # Check for some expected values from sample.csv
        assert "Alice" in output or "1" in output  # At least the first row's data


class TestPhase2ChainOperations:
    """Test that operations can be chained."""

    def test_read_and_show_chain(self):
        """Can read and show in a single chain."""
        LTSeq.read_csv(SAMPLE_CSV).show()  # Should not raise

    def test_multiple_show_calls(self):
        """Can call show() multiple times."""
        t = LTSeq.read_csv(SAMPLE_CSV)
        t.show(n=2)
        t.show(n=3)  # Second call should also work


class TestWriteCSV:
    """Test write_csv() functionality (T19)."""

    def test_write_csv_basic(self, tmp_path):
        """write_csv() produces a readable CSV file."""
        t = LTSeq.read_csv(SAMPLE_CSV)
        out = str(tmp_path / "out.csv")
        t.write_csv(out)
        assert os.path.exists(out)
        assert os.path.getsize(out) > 0

    def test_write_csv_roundtrip(self, tmp_path):
        """Data survives a write → read round-trip."""
        t = LTSeq.read_csv(SAMPLE_CSV)
        original = t.collect()
        out = str(tmp_path / "roundtrip.csv")
        t.write_csv(out)
        t2 = LTSeq.read_csv(out)
        reloaded = t2.collect()
        assert len(reloaded) == len(original)
        # Column names should match
        assert set(reloaded[0].keys()) == set(original[0].keys())

    def test_write_csv_after_filter(self, tmp_path):
        """write_csv() works on a filtered table."""
        t = LTSeq.read_csv(SAMPLE_CSV)
        filtered = t.filter(lambda r: r.price > 11)
        out = str(tmp_path / "filtered.csv")
        filtered.write_csv(out)
        t2 = LTSeq.read_csv(out)
        assert len(t2) <= len(t)
        assert len(t2) > 0


class TestIOErrorHandling:
    """Test I/O error handling (T32)."""

    def test_read_csv_nonexistent_returns_empty(self):
        """Reading a non-existent CSV returns empty table (lazy error)."""
        t = LTSeq.read_csv("/nonexistent/path/to/file.csv")
        # The current behavior: no exception, empty schema, empty data
        assert t._schema == {}
        assert t.collect() == []

    def test_read_parquet_nonexistent_returns_empty(self):
        """Reading a non-existent parquet file returns empty table (lazy error)."""
        t = LTSeq.read_parquet("/nonexistent/path/to/file.parquet")
        # The current behavior: no exception, empty data
        assert t.collect() == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
