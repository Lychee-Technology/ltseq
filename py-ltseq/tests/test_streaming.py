"""Tests for streaming cursor functionality.

These tests verify that the LTSeq.scan() API works correctly for
streaming batch-by-batch data processing.
"""

import os
import tempfile
import pytest


class TestStreamingCursor:
    """Tests for LTSeq.scan() streaming functionality."""

    def test_scan_csv_basic(self):
        """Test basic CSV scanning returns a Cursor."""
        from ltseq import LTSeq
        import csv

        # Create test CSV
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'name', 'value'])
            for i in range(100):
                writer.writerow([i, f'name_{i}', i * 10])
            temp_path = f.name

        try:
            cursor = LTSeq.scan(temp_path)
            assert hasattr(cursor, '__iter__')
            assert hasattr(cursor, '__next__')
            assert cursor.columns == ['id', 'name', 'value']
        finally:
            os.unlink(temp_path)

    def test_scan_csv_iteration(self):
        """Test iterating through batches."""
        from ltseq import LTSeq
        import csv

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'value'])
            for i in range(50):
                writer.writerow([i, i * 2])
            temp_path = f.name

        try:
            cursor = LTSeq.scan(temp_path)
            total_rows = 0
            batch_count = 0

            for batch in cursor:
                batch_count += 1
                total_rows += batch.num_rows

            assert total_rows == 50
            assert batch_count >= 1
            assert cursor.exhausted
        finally:
            os.unlink(temp_path)

    def test_scan_csv_to_pandas(self):
        """Test materializing cursor to pandas DataFrame."""
        from ltseq import LTSeq
        import csv
        import pandas as pd

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'name'])
            writer.writerow([1, 'Alice'])
            writer.writerow([2, 'Bob'])
            writer.writerow([3, 'Charlie'])
            temp_path = f.name

        try:
            cursor = LTSeq.scan(temp_path)
            df = cursor.to_pandas()

            assert len(df) == 3
            assert list(df.columns) == ['id', 'name']
            assert df['id'].tolist() == [1, 2, 3]
            assert cursor.exhausted
        finally:
            os.unlink(temp_path)

    def test_scan_csv_to_arrow(self):
        """Test materializing cursor to PyArrow Table."""
        from ltseq import LTSeq
        import csv
        import pyarrow as pa

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['x', 'y'])
            writer.writerow([1, 10])
            writer.writerow([2, 20])
            temp_path = f.name

        try:
            cursor = LTSeq.scan(temp_path)
            table = cursor.to_arrow()

            assert isinstance(table, pa.Table)
            assert table.num_rows == 2
            assert table.column_names == ['x', 'y']
        finally:
            os.unlink(temp_path)

    def test_scan_parquet(self):
        """Test scanning Parquet files."""
        from ltseq import LTSeq
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create test parquet
        table = pa.table({
            'id': [1, 2, 3, 4, 5],
            'name': ['A', 'B', 'C', 'D', 'E'],
            'value': [10.0, 20.0, 30.0, 40.0, 50.0]
        })

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            temp_path = f.name
        pq.write_table(table, temp_path)

        try:
            cursor = LTSeq.scan_parquet(temp_path)
            assert cursor.columns == ['id', 'name', 'value']

            total_rows = 0
            for batch in cursor:
                total_rows += batch.num_rows

            assert total_rows == 5
        finally:
            os.unlink(temp_path)

    def test_cursor_schema(self):
        """Test cursor schema property."""
        from ltseq import LTSeq
        import csv

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['int_col', 'str_col'])
            writer.writerow([123, 'hello'])
            temp_path = f.name

        try:
            cursor = LTSeq.scan(temp_path)
            schema = cursor.schema

            assert 'int_col' in schema
            assert 'str_col' in schema
        finally:
            os.unlink(temp_path)

    def test_cursor_source(self):
        """Test cursor source property."""
        from ltseq import LTSeq
        import csv

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['a'])
            writer.writerow([1])
            temp_path = f.name

        try:
            cursor = LTSeq.scan(temp_path)
            assert cursor.source == temp_path
        finally:
            os.unlink(temp_path)

    def test_cursor_exhausted(self):
        """Test cursor exhausted property."""
        from ltseq import LTSeq
        import csv

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['x'])
            writer.writerow([1])
            temp_path = f.name

        try:
            cursor = LTSeq.scan(temp_path)
            assert not cursor.exhausted

            list(cursor)  # Consume all

            assert cursor.exhausted
        finally:
            os.unlink(temp_path)

    def test_cursor_count(self):
        """Test cursor count method."""
        from ltseq import LTSeq
        import csv

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['id'])
            for i in range(75):
                writer.writerow([i])
            temp_path = f.name

        try:
            cursor = LTSeq.scan(temp_path)
            count = cursor.count()

            assert count == 75
            assert cursor.exhausted  # count consumes the cursor
        finally:
            os.unlink(temp_path)
