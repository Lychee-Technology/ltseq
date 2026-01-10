"""Tests for read_csv and scan with has_header parameter."""

import os
import tempfile

import pytest

from ltseq import LTSeq


class TestReadCsvHasHeader:
    """Tests for read_csv with has_header parameter."""

    def test_read_csv_with_header_default(self):
        """Test that read_csv defaults to has_header=True."""
        data = """name,age,city
Alice,30,NYC
Bob,25,LA
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write(data)

            t = LTSeq.read_csv(path)
            df = t.to_pandas()

            assert list(df.columns) == ["name", "age", "city"]
            assert len(df) == 2
            assert df["name"].tolist() == ["Alice", "Bob"]

    def test_read_csv_with_header_explicit_true(self):
        """Test read_csv with has_header=True explicitly."""
        data = """name,age,city
Alice,30,NYC
Bob,25,LA
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write(data)

            t = LTSeq.read_csv(path, has_header=True)
            df = t.to_pandas()

            assert list(df.columns) == ["name", "age", "city"]
            assert len(df) == 2

    def test_read_csv_without_header(self):
        """Test read_csv with has_header=False for headerless CSV."""
        data = """Alice,30,NYC
Bob,25,LA
Charlie,35,Chicago
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "no_header.csv")
            with open(path, "w") as f:
                f.write(data)

            t = LTSeq.read_csv(path, has_header=False)
            df = t.to_pandas()

            # Column names should be auto-generated (1-based to match DataFusion)
            assert list(df.columns) == ["column_1", "column_2", "column_3"]
            assert len(df) == 3
            # First row should be data, not header
            assert df["column_1"].tolist() == ["Alice", "Bob", "Charlie"]

    def test_read_csv_without_header_numeric_data(self):
        """Test read_csv with has_header=False for numeric data."""
        data = """1,100,10.5
2,200,20.5
3,300,30.5
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "numeric.csv")
            with open(path, "w") as f:
                f.write(data)

            t = LTSeq.read_csv(path, has_header=False)
            df = t.to_pandas()

            assert list(df.columns) == ["column_1", "column_2", "column_3"]
            assert len(df) == 3
            # Data should be properly parsed as numbers
            assert df["column_1"].tolist() == [1, 2, 3]
            assert df["column_2"].tolist() == [100, 200, 300]

    def test_read_csv_without_header_schema(self):
        """Test that schema is correctly inferred for headerless CSV."""
        data = """Alice,30,NYC
Bob,25,LA
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write(data)

            t = LTSeq.read_csv(path, has_header=False)

            # Check schema has auto-generated column names (1-based)
            assert "column_1" in t._schema
            assert "column_2" in t._schema
            assert "column_3" in t._schema

    def test_read_csv_without_header_operations(self):
        """Test that operations work correctly on headerless CSV."""
        data = """Alice,30
Bob,25
Charlie,35
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write(data)

            t = LTSeq.read_csv(path, has_header=False)

            # Test filter using auto-generated column names (1-based)
            filtered = t.filter(lambda r: r.column_2 > 28)
            df = filtered.to_pandas()

            assert len(df) == 2
            assert set(df["column_1"].tolist()) == {"Alice", "Charlie"}

    def test_read_csv_without_header_derive(self):
        """Test derive operation on headerless CSV."""
        data = """Alice,30,NYC
Bob,25,LA
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write(data)

            t = LTSeq.read_csv(path, has_header=False)

            # Create derived column using auto-generated column names
            result = t.derive(lambda r: {"age_plus_10": r.column_2 + 10})
            df = result.to_pandas()

            assert "age_plus_10" in df.columns
            assert df["age_plus_10"].tolist() == [40, 35]


class TestScanHasHeader:
    """Tests for scan with has_header parameter."""

    def test_scan_with_header_default(self):
        """Test that scan defaults to has_header=True."""
        data = """name,age
Alice,30
Bob,25
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write(data)

            cursor = LTSeq.scan(path)
            df = cursor.to_pandas()

            assert list(df.columns) == ["name", "age"]
            assert len(df) == 2

    def test_scan_without_header(self):
        """Test scan with has_header=False."""
        data = """Alice,30
Bob,25
Charlie,35
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "no_header.csv")
            with open(path, "w") as f:
                f.write(data)

            cursor = LTSeq.scan(path, has_header=False)
            df = cursor.to_pandas()

            # Column names should be auto-generated (1-based)
            assert list(df.columns) == ["column_1", "column_2"]
            assert len(df) == 3
            assert df["column_1"].tolist() == ["Alice", "Bob", "Charlie"]

    def test_scan_without_header_iteration(self):
        """Test batch iteration with has_header=False."""
        data = """Alice,30
Bob,25
Charlie,35
Diana,40
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write(data)

            cursor = LTSeq.scan(path, has_header=False)

            # Iterate through batches
            all_rows = []
            for batch in cursor:
                df = batch.to_pandas()
                assert list(df.columns) == ["column_1", "column_2"]
                all_rows.extend(df["column_1"].tolist())

            assert len(all_rows) == 4
            assert set(all_rows) == {"Alice", "Bob", "Charlie", "Diana"}


class TestEdgeCases:
    """Test edge cases for has_header parameter."""

    def test_single_column_no_header(self):
        """Test single-column CSV without header."""
        data = """Alice
Bob
Charlie
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "single.csv")
            with open(path, "w") as f:
                f.write(data)

            t = LTSeq.read_csv(path, has_header=False)
            df = t.to_pandas()

            assert list(df.columns) == ["column_1"]
            assert len(df) == 3
            assert df["column_1"].tolist() == ["Alice", "Bob", "Charlie"]

    def test_empty_csv_no_header(self):
        """Test empty CSV without header."""
        data = ""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "empty.csv")
            with open(path, "w") as f:
                f.write(data)

            # Should handle empty file gracefully
            t = LTSeq.read_csv(path, has_header=False)
            # Empty file may raise an error or return empty table
            # Just ensure it doesn't crash

    def test_many_columns_no_header(self):
        """Test CSV with many columns without header."""
        # Create CSV with 10 columns
        data = ",".join([str(i) for i in range(10)]) + "\n"
        data += ",".join([str(i + 10) for i in range(10)]) + "\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "many_cols.csv")
            with open(path, "w") as f:
                f.write(data)

            t = LTSeq.read_csv(path, has_header=False)
            df = t.to_pandas()

            # 1-based column names
            expected_cols = [f"column_{i + 1}" for i in range(10)]
            assert list(df.columns) == expected_cols
            assert len(df) == 2
