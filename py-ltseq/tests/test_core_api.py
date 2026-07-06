"""Tests for core API methods: schema, columns, count, collect, head, tail."""

import pytest
import tempfile
import os
from ltseq import LTSeq


@pytest.fixture
def sample_csv():
    """Create a sample CSV file for testing."""
    content = """id,name,age,score
1,Alice,30,85.5
2,Bob,25,90.0
3,Charlie,35,78.5
4,Diana,28,92.0
5,Eve,32,88.5
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        temp_path = f.name
    yield temp_path
    os.unlink(temp_path)


@pytest.fixture
def small_csv():
    """Create a small CSV file with 3 rows for edge case testing."""
    content = """id,value
1,a
2,b
3,c
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        temp_path = f.name
    yield temp_path
    os.unlink(temp_path)


class TestSchemaProperty:
    """Test schema property."""

    def test_schema_returns_dict(self, sample_csv):
        """schema property returns a dictionary."""
        t = LTSeq.read_csv(sample_csv)
        assert isinstance(t.schema, dict)

    def test_schema_has_columns(self, sample_csv):
        """schema contains all column names."""
        t = LTSeq.read_csv(sample_csv)
        assert "id" in t.schema
        assert "name" in t.schema
        assert "age" in t.schema
        assert "score" in t.schema

    def test_schema_returns_copy(self, sample_csv):
        """schema returns a copy, not the internal dict."""
        t = LTSeq.read_csv(sample_csv)
        schema1 = t.schema
        schema2 = t.schema
        # Modifying returned schema should not affect internal state
        schema1["new_col"] = "int64"
        assert "new_col" not in t.schema
        assert schema1 is not schema2

    def test_schema_empty_before_read(self):
        """schema is empty before data is loaded."""
        t = LTSeq()
        assert t.schema == {}


class TestColumnsProperty:
    """Test columns property."""

    def test_columns_returns_list(self, sample_csv):
        """columns property returns a list."""
        t = LTSeq.read_csv(sample_csv)
        assert isinstance(t.columns, list)

    def test_columns_has_all_names(self, sample_csv):
        """columns contains all column names."""
        t = LTSeq.read_csv(sample_csv)
        cols = t.columns
        assert "id" in cols
        assert "name" in cols
        assert "age" in cols
        assert "score" in cols

    def test_columns_preserves_order(self, sample_csv):
        """columns preserves original column order."""
        t = LTSeq.read_csv(sample_csv)
        cols = t.columns
        # Order should match CSV header order
        assert cols == ["id", "name", "age", "score"]

    def test_columns_empty_before_read(self):
        """columns is empty before data is loaded."""
        t = LTSeq()
        assert t.columns == []


class TestCountMethod:
    """Test count() method."""

    def test_count_returns_int(self, sample_csv):
        """count() returns an integer."""
        t = LTSeq.read_csv(sample_csv)
        assert isinstance(t.count(), int)

    def test_count_matches_len(self, sample_csv):
        """count() matches len()."""
        t = LTSeq.read_csv(sample_csv)
        assert t.count() == len(t)

    def test_count_correct_value(self, sample_csv):
        """count() returns correct number of rows."""
        t = LTSeq.read_csv(sample_csv)
        assert t.count() == 5

    def test_count_after_filter(self, sample_csv):
        """count() returns correct value after filter."""
        t = LTSeq.read_csv(sample_csv)
        filtered = t.filter(lambda r: r.age > 30)
        assert filtered.count() == 2  # Charlie (35) and Eve (32)


class TestToDicts:
    """Test to_dicts() method (row-dict export, old collect() behavior)."""

    def test_to_dicts_returns_list(self, sample_csv):
        """to_dicts() returns a list."""
        t = LTSeq.read_csv(sample_csv)
        result = t.to_dicts()
        assert isinstance(result, list)

    def test_to_dicts_returns_dicts(self, sample_csv):
        """to_dicts() returns list of dictionaries."""
        t = LTSeq.read_csv(sample_csv)
        result = t.to_dicts()
        assert all(isinstance(row, dict) for row in result)

    def test_to_dicts_has_correct_count(self, sample_csv):
        """to_dicts() returns correct number of rows."""
        t = LTSeq.read_csv(sample_csv)
        result = t.to_dicts()
        assert len(result) == 5

    def test_to_dicts_has_all_columns(self, sample_csv):
        """to_dicts() rows have all columns."""
        t = LTSeq.read_csv(sample_csv)
        result = t.to_dicts()
        for row in result:
            assert "id" in row
            assert "name" in row
            assert "age" in row
            assert "score" in row

    def test_to_dicts_values_correct(self, sample_csv):
        """to_dicts() returns correct values."""
        t = LTSeq.read_csv(sample_csv)
        result = t.to_dicts()
        # Find Alice's row
        alice = next(row for row in result if row["name"] == "Alice")
        assert alice["id"] == 1
        assert alice["age"] == 30
        assert alice["score"] == 85.5

    def test_to_dicts_after_filter(self, sample_csv):
        """to_dicts() works after filter."""
        t = LTSeq.read_csv(sample_csv)
        filtered = t.filter(lambda r: r.name == "Bob")
        result = filtered.to_dicts()
        assert len(result) == 1
        assert result[0]["name"] == "Bob"


class TestCollect:
    """Test collect() method (materialize lazy plan, returns LTSeq)."""

    def test_collect_returns_ltseq(self, sample_csv):
        """collect() returns an LTSeq instance."""
        t = LTSeq.read_csv(sample_csv)
        result = t.collect()
        assert isinstance(result, LTSeq)

    def test_collect_preserves_count_and_schema(self, sample_csv):
        """collect() preserves row count and column names."""
        t = LTSeq.read_csv(sample_csv)
        result = t.collect()
        assert result.count() == t.count()
        assert result.columns == t.columns

    def test_collect_preserves_row_order(self, sample_csv):
        """collect() preserves row order of the sorted plan."""
        t = LTSeq.read_csv(sample_csv)
        assert t.sort("age").collect().to_dicts() == t.sort("age").to_dicts()

    def test_collect_is_chainable(self, sample_csv):
        """collect() result supports further operations."""
        t = LTSeq.read_csv(sample_csv)
        result = t.filter(lambda r: r.age > 25).collect().filter(lambda r: r.score > 85)
        rows = result.to_dicts()
        names = {row["name"] for row in rows}
        assert names == {"Alice", "Diana", "Eve"}

    def test_collect_preserves_sort_metadata(self, sample_csv):
        """collect() carries sort keys over to the materialized table."""
        t = LTSeq.read_csv(sample_csv)
        result = t.sort("age").collect()
        assert result._sort_keys == [("age", False)]

    def test_window_after_collect(self, sample_csv):
        """Window functions work on a collected table (sort metadata intact)."""
        t = LTSeq.read_csv(sample_csv)
        result = t.sort("id").collect().derive(prev_age=lambda r: r.age.shift(1))
        rows = result.to_dicts()
        assert rows[1]["prev_age"] == 30
        assert rows[2]["prev_age"] == 25

    def test_collect_empty_result(self, sample_csv):
        """collect() on an empty filter result keeps schema, count is 0."""
        t = LTSeq.read_csv(sample_csv)
        result = t.filter(lambda r: r.age > 1000).collect()
        assert result.count() == 0
        assert result.columns == ["id", "name", "age", "score"]

    def test_collect_fresh_table_roundtrip(self, sample_csv):
        """collect() on a freshly-read table round-trips all rows."""
        t = LTSeq.read_csv(sample_csv)
        assert t.collect().to_dicts() == t.to_dicts()


class TestHeadMethod:
    """Test head() method."""

    def test_head_returns_ltseq(self, sample_csv):
        """head() returns an LTSeq."""
        t = LTSeq.read_csv(sample_csv)
        result = t.head(3)
        assert isinstance(result, LTSeq)

    def test_head_returns_correct_count(self, sample_csv):
        """head() returns requested number of rows."""
        t = LTSeq.read_csv(sample_csv)
        result = t.head(3)
        assert len(result) == 3

    def test_head_default_10(self, sample_csv):
        """head() defaults to 10 rows."""
        t = LTSeq.read_csv(sample_csv)
        result = t.head()
        # Sample has only 5 rows, so should return all 5
        assert len(result) == 5

    def test_head_exceeds_total(self, small_csv):
        """head(n) where n > total rows returns all rows."""
        t = LTSeq.read_csv(small_csv)
        result = t.head(100)
        assert len(result) == 3

    def test_head_zero(self, sample_csv):
        """head(0) returns empty result."""
        t = LTSeq.read_csv(sample_csv)
        result = t.head(0)
        assert len(result) == 0

    def test_head_negative_raises(self, sample_csv):
        """head() with negative n raises ValueError."""
        t = LTSeq.read_csv(sample_csv)
        with pytest.raises(ValueError, match="non-negative"):
            t.head(-1)

    def test_head_preserves_schema(self, sample_csv):
        """head() preserves schema."""
        t = LTSeq.read_csv(sample_csv)
        result = t.head(3)
        assert result.columns == t.columns


class TestTailMethod:
    """Test tail() method."""

    def test_tail_returns_ltseq(self, sample_csv):
        """tail() returns an LTSeq."""
        t = LTSeq.read_csv(sample_csv)
        result = t.tail(3)
        assert isinstance(result, LTSeq)

    def test_tail_returns_correct_count(self, sample_csv):
        """tail() returns requested number of rows."""
        t = LTSeq.read_csv(sample_csv)
        result = t.tail(3)
        assert len(result) == 3

    def test_tail_default_10(self, sample_csv):
        """tail() defaults to 10 rows."""
        t = LTSeq.read_csv(sample_csv)
        result = t.tail()
        # Sample has only 5 rows, so should return all 5
        assert len(result) == 5

    def test_tail_exceeds_total(self, small_csv):
        """tail(n) where n > total rows returns all rows."""
        t = LTSeq.read_csv(small_csv)
        result = t.tail(100)
        assert len(result) == 3

    def test_tail_zero(self, sample_csv):
        """tail(0) returns empty result."""
        t = LTSeq.read_csv(sample_csv)
        result = t.tail(0)
        assert len(result) == 0

    def test_tail_negative_raises(self, sample_csv):
        """tail() with negative n raises ValueError."""
        t = LTSeq.read_csv(sample_csv)
        with pytest.raises(ValueError, match="non-negative"):
            t.tail(-1)

    def test_tail_preserves_schema(self, sample_csv):
        """tail() preserves schema."""
        t = LTSeq.read_csv(sample_csv)
        result = t.tail(3)
        assert result.columns == t.columns

    def test_tail_returns_last_rows(self, small_csv):
        """tail() returns the last rows, not first."""
        t = LTSeq.read_csv(small_csv)
        result = t.tail(2)
        rows = result.to_dicts()
        # Should get rows with id 2 and 3, not 1 and 2
        ids = [row["id"] for row in rows]
        assert 3 in ids
        assert 2 in ids
        assert 1 not in ids


class TestToArrow:
    """Test to_arrow() method (T20)."""

    def test_to_arrow_returns_pa_table(self, sample_csv):
        """to_arrow() returns a pyarrow Table."""
        import pyarrow as pa

        t = LTSeq.read_csv(sample_csv)
        arrow = t.to_arrow()
        assert isinstance(arrow, pa.Table)

    def test_to_arrow_schema_preserved(self, sample_csv):
        """to_arrow() result has the same column names."""
        t = LTSeq.read_csv(sample_csv)
        arrow = t.to_arrow()
        assert set(arrow.column_names) == set(t.columns)

    def test_to_arrow_row_count(self, sample_csv):
        """to_arrow() result has correct number of rows."""
        t = LTSeq.read_csv(sample_csv)
        arrow = t.to_arrow()
        assert arrow.num_rows == 5

    def test_to_arrow_empty_table(self):
        """to_arrow() on empty LTSeq returns empty table."""
        import pyarrow as pa

        t = LTSeq()
        arrow = t.to_arrow()
        assert isinstance(arrow, pa.Table)
        assert arrow.num_rows == 0

    def test_to_arrow_after_filter(self, sample_csv):
        """to_arrow() works on filtered table."""
        t = LTSeq.read_csv(sample_csv)
        filtered = t.filter(lambda r: r.name == "Alice")
        arrow = filtered.to_arrow()
        assert arrow.num_rows == 1


class TestEmptyTableHandling:
    """Test empty table edge cases (T34)."""

    def test_empty_collect(self):
        """collect() on empty LTSeq returns empty list."""
        t = LTSeq()
        result = t.to_dicts()
        assert result == []

    def test_empty_to_pandas(self):
        """to_pandas() on empty LTSeq returns empty DataFrame."""
        import pandas as pd

        t = LTSeq()
        df = t.to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_filter_to_empty(self, sample_csv):
        """Filtering that eliminates all rows still returns valid LTSeq."""
        t = LTSeq.read_csv(sample_csv)
        empty = t.filter(lambda r: r.age > 9999)
        assert len(empty) == 0
        result = empty.to_dicts()
        assert result == []

    def test_empty_count(self):
        """count() on empty table raises RuntimeError (no data loaded)."""
        t = LTSeq()
        with pytest.raises(RuntimeError, match="No data loaded"):
            t.count()

    def test_empty_columns(self):
        """columns on empty table returns empty list."""
        t = LTSeq()
        assert t.columns == []


class TestHeadTailChaining:
    """Test head() and tail() work in chains."""

    def test_head_after_sort(self, sample_csv):
        """head() works after sort()."""
        t = LTSeq.read_csv(sample_csv)
        result = t.sort("score", desc=True).head(3)
        assert len(result) == 3

    def test_tail_after_filter(self, sample_csv):
        """tail() works after filter()."""
        t = LTSeq.read_csv(sample_csv)
        result = t.filter(lambda r: r.age > 25).tail(2)
        assert len(result) == 2

    def test_head_then_collect(self, sample_csv):
        """head() followed by collect() works."""
        t = LTSeq.read_csv(sample_csv)
        result = t.head(2).to_dicts()
        assert len(result) == 2
        assert all(isinstance(row, dict) for row in result)
