"""Sprint 3 tests: IO & Interop — write_parquet(), from_arrow(), from_pandas()."""

import os
import tempfile

import pyarrow as pa
import pytest

from ltseq import LTSeq


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_table():
    """Small table for testing IO operations."""
    return LTSeq._from_rows(
        [
            {"id": 1, "name": "Alice", "score": 85},
            {"id": 2, "name": "Bob", "score": 92},
            {"id": 3, "name": "Charlie", "score": 78},
        ],
        {"id": "int64", "name": "string", "score": "int64"},
    )


@pytest.fixture
def float_table():
    """Table with float values for round-trip precision testing."""
    return LTSeq._from_rows(
        [
            {"x": 1.5, "y": 2.7},
            {"x": 3.14, "y": -1.0},
            {"x": 0.0, "y": 100.99},
        ],
        {"x": "float64", "y": "float64"},
    )


# ============================================================================
# write_parquet
# ============================================================================


class TestWriteParquet:
    """Tests for LTSeq.write_parquet()."""

    def test_write_parquet_creates_file(self, sample_table):
        """write_parquet creates a file on disk."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        try:
            sample_table.write_parquet(path)
            assert os.path.exists(path)
            assert os.path.getsize(path) > 0
        finally:
            os.unlink(path)

    def test_write_parquet_roundtrip(self, sample_table):
        """write_parquet → read_parquet preserves data."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        try:
            sample_table.write_parquet(path)
            loaded = LTSeq.read_parquet(path)
            assert loaded.count() == 3
            assert set(loaded.columns) == {"id", "name", "score"}
            df = loaded.to_pandas()
            assert df["id"].tolist() == [1, 2, 3]
            assert df["name"].tolist() == ["Alice", "Bob", "Charlie"]
            assert df["score"].tolist() == [85, 92, 78]
        finally:
            os.unlink(path)

    def test_write_parquet_with_snappy(self, sample_table):
        """write_parquet with snappy compression."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        try:
            sample_table.write_parquet(path, compression="snappy")
            loaded = LTSeq.read_parquet(path)
            assert loaded.count() == 3
        finally:
            os.unlink(path)

    def test_write_parquet_with_zstd(self, sample_table):
        """write_parquet with zstd compression."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        try:
            sample_table.write_parquet(path, compression="zstd")
            loaded = LTSeq.read_parquet(path)
            assert loaded.count() == 3
        finally:
            os.unlink(path)

    def test_write_parquet_with_gzip(self, sample_table):
        """write_parquet with gzip compression."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        try:
            sample_table.write_parquet(path, compression="gzip")
            loaded = LTSeq.read_parquet(path)
            assert loaded.count() == 3
        finally:
            os.unlink(path)

    def test_write_parquet_with_none_compression(self, sample_table):
        """write_parquet with compression='none' explicitly."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        try:
            sample_table.write_parquet(path, compression="none")
            loaded = LTSeq.read_parquet(path)
            assert loaded.count() == 3
        finally:
            os.unlink(path)

    def test_write_parquet_invalid_compression(self, sample_table):
        """write_parquet with invalid compression raises ValueError."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        try:
            with pytest.raises(Exception):  # ValueError from Rust side
                sample_table.write_parquet(path, compression="invalid")
        finally:
            if os.path.exists(path):
                os.unlink(path)

    def test_write_parquet_floats_roundtrip(self, float_table):
        """write_parquet preserves float precision."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        try:
            float_table.write_parquet(path)
            loaded = LTSeq.read_parquet(path)
            df = loaded.to_pandas()
            assert df["x"].tolist() == pytest.approx([1.5, 3.14, 0.0])
            assert df["y"].tolist() == pytest.approx([2.7, -1.0, 100.99])
        finally:
            os.unlink(path)

    def test_write_parquet_after_filter(self, sample_table):
        """write_parquet works on filtered table."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        try:
            filtered = sample_table.filter(lambda r: r.score > 80)
            filtered.write_parquet(path)
            loaded = LTSeq.read_parquet(path)
            assert loaded.count() == 2
        finally:
            os.unlink(path)


# ============================================================================
# from_arrow
# ============================================================================


class TestFromArrow:
    """Tests for LTSeq.from_arrow()."""

    def test_from_arrow_basic(self):
        """from_arrow creates an LTSeq table from PyArrow Table."""
        arrow_table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        t = LTSeq.from_arrow(arrow_table)
        assert isinstance(t, LTSeq)
        assert t.count() == 3

    def test_from_arrow_schema(self):
        """from_arrow infers schema correctly."""
        arrow_table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        t = LTSeq.from_arrow(arrow_table)
        assert "x" in t.columns
        assert "y" in t.columns
        assert len(t.columns) == 2

    def test_from_arrow_data_preserved(self):
        """from_arrow preserves all data values."""
        arrow_table = pa.table({"id": [10, 20, 30], "name": ["X", "Y", "Z"]})
        t = LTSeq.from_arrow(arrow_table)
        df = t.to_pandas()
        assert df["id"].tolist() == [10, 20, 30]
        assert df["name"].tolist() == ["X", "Y", "Z"]

    def test_from_arrow_float_columns(self):
        """from_arrow handles float columns."""
        arrow_table = pa.table({"val": [1.1, 2.2, 3.3]})
        t = LTSeq.from_arrow(arrow_table)
        df = t.to_pandas()
        assert df["val"].tolist() == pytest.approx([1.1, 2.2, 3.3])

    def test_from_arrow_empty_table(self):
        """from_arrow handles empty tables."""
        arrow_table = pa.table({"x": pa.array([], type=pa.int64())})
        t = LTSeq.from_arrow(arrow_table)
        assert t.count() == 0

    def test_from_arrow_chainable(self):
        """from_arrow result can be chained with filter/derive."""
        arrow_table = pa.table({"x": [1, 2, 3, 4, 5]})
        t = LTSeq.from_arrow(arrow_table)
        result = t.filter(lambda r: r.x > 3)
        assert result.count() == 2

    def test_from_arrow_roundtrip(self):
        """from_arrow → to_arrow roundtrip preserves data."""
        original = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        t = LTSeq.from_arrow(original)
        recovered = t.to_arrow()
        assert recovered.num_rows == 3
        assert recovered.column("a").to_pylist() == [1, 2, 3]
        assert recovered.column("b").to_pylist() == ["x", "y", "z"]

    def test_from_arrow_type_error(self):
        """from_arrow raises TypeError for non-Table input."""
        with pytest.raises(TypeError, match="Expected pyarrow.Table"):
            LTSeq.from_arrow([1, 2, 3])

    def test_from_arrow_bool_column(self):
        """from_arrow handles boolean columns."""
        arrow_table = pa.table({"flag": [True, False, True]})
        t = LTSeq.from_arrow(arrow_table)
        df = t.to_pandas()
        assert df["flag"].tolist() == [True, False, True]

    def test_from_arrow_many_columns(self):
        """from_arrow handles tables with many columns."""
        data = {f"col_{i}": list(range(5)) for i in range(20)}
        arrow_table = pa.table(data)
        t = LTSeq.from_arrow(arrow_table)
        assert t.count() == 5
        assert len(t.columns) == 20


# ============================================================================
# from_pandas
# ============================================================================


class TestFromPandas:
    """Tests for LTSeq.from_pandas()."""

    def test_from_pandas_basic(self):
        """from_pandas creates an LTSeq table from DataFrame."""
        import pandas as pd

        df = pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        t = LTSeq.from_pandas(df)
        assert isinstance(t, LTSeq)
        assert t.count() == 3

    def test_from_pandas_schema(self):
        """from_pandas infers schema correctly."""
        import pandas as pd

        df = pd.DataFrame({"x": [1, 2, 3], "y": [1.5, 2.5, 3.5]})
        t = LTSeq.from_pandas(df)
        assert "x" in t.columns
        assert "y" in t.columns

    def test_from_pandas_data_preserved(self):
        """from_pandas preserves all data values."""
        import pandas as pd

        df = pd.DataFrame({"id": [10, 20, 30], "name": ["X", "Y", "Z"]})
        t = LTSeq.from_pandas(df)
        result = t.to_pandas()
        assert result["id"].tolist() == [10, 20, 30]
        assert result["name"].tolist() == ["X", "Y", "Z"]

    def test_from_pandas_roundtrip(self):
        """from_pandas → to_pandas roundtrip preserves data."""
        import pandas as pd

        original = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        t = LTSeq.from_pandas(original)
        recovered = t.to_pandas()
        assert recovered["a"].tolist() == [1, 2, 3]
        assert recovered["b"].tolist() == pytest.approx([4.0, 5.0, 6.0])

    def test_from_pandas_chainable(self):
        """from_pandas result can be chained with operations."""
        import pandas as pd

        df = pd.DataFrame({"score": [85, 92, 78, 95, 60]})
        t = LTSeq.from_pandas(df)
        result = t.filter(lambda r: r.score >= 90)
        assert result.count() == 2

    def test_from_pandas_type_error(self):
        """from_pandas raises TypeError for non-DataFrame input."""
        with pytest.raises(TypeError, match="Expected pandas.DataFrame"):
            LTSeq.from_pandas({"x": [1, 2, 3]})

    def test_from_pandas_empty_dataframe(self):
        """from_pandas handles empty DataFrames."""
        import pandas as pd

        df = pd.DataFrame({"x": pd.array([], dtype="int64")})
        t = LTSeq.from_pandas(df)
        assert t.count() == 0

    def test_from_pandas_with_nulls(self):
        """from_pandas handles DataFrames with null values."""
        import pandas as pd

        df = pd.DataFrame({"x": [1, None, 3], "y": ["a", "b", None]})
        t = LTSeq.from_pandas(df)
        assert t.count() == 3


# ============================================================================
# Interop combinations
# ============================================================================


class TestInteropCombinations:
    """Tests for combining IO operations."""

    def test_parquet_write_then_from_arrow(self, sample_table):
        """Write parquet, read back, convert to arrow, then from_arrow."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        try:
            sample_table.write_parquet(path)
            loaded = LTSeq.read_parquet(path)
            arrow = loaded.to_arrow()
            t = LTSeq.from_arrow(arrow)
            assert t.count() == 3
            df = t.to_pandas()
            assert df["name"].tolist() == ["Alice", "Bob", "Charlie"]
        finally:
            os.unlink(path)

    def test_from_pandas_then_write_parquet(self):
        """from_pandas → write_parquet works."""
        import pandas as pd

        df = pd.DataFrame({"x": [1, 2, 3]})
        t = LTSeq.from_pandas(df)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        try:
            t.write_parquet(path)
            loaded = LTSeq.read_parquet(path)
            assert loaded.count() == 3
            assert loaded.to_pandas()["x"].tolist() == [1, 2, 3]
        finally:
            os.unlink(path)

    def test_from_arrow_then_derive_then_write(self):
        """from_arrow → derive → write_parquet pipeline."""
        arrow_table = pa.table({"x": [1, 2, 3]})
        t = LTSeq.from_arrow(arrow_table)
        t2 = t.derive(doubled=lambda r: r.x * 2)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        try:
            t2.write_parquet(path)
            loaded = LTSeq.read_parquet(path)
            df = loaded.to_pandas()
            assert df["doubled"].tolist() == [2, 4, 6]
        finally:
            os.unlink(path)


# ============================================================================
# Sprint 1 & 2 regression checks
# ============================================================================


class TestSprint12Regression:
    """Verify earlier sprint features still work."""

    def test_from_arrow_then_rename(self):
        """from_arrow result supports rename()."""
        arrow_table = pa.table({"old_name": [1, 2, 3]})
        t = LTSeq.from_arrow(arrow_table)
        result = t.rename(old_name="new_name")
        assert "new_name" in result.columns
        assert "old_name" not in result.columns

    def test_from_arrow_then_drop(self):
        """from_arrow result supports drop()."""
        arrow_table = pa.table({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        t = LTSeq.from_arrow(arrow_table)
        result = t.drop("b")
        assert "b" not in result.columns
        assert set(result.columns) == {"a", "c"}

    def test_from_pandas_repr(self):
        """from_pandas result has __repr__."""
        import pandas as pd

        df = pd.DataFrame({"x": [1, 2]})
        t = LTSeq.from_pandas(df)
        r = repr(t)
        assert "LTSeq" in r

    def test_from_arrow_pipe(self):
        """from_arrow result supports pipe()."""
        arrow_table = pa.table({"x": [1, 2, 3, 4, 5]})
        t = LTSeq.from_arrow(arrow_table)
        result = t.pipe(lambda tbl: tbl.filter(lambda r: r.x > 2))
        assert result.count() == 3
