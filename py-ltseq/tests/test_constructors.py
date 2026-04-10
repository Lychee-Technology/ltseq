"""Tests for in-memory constructors: from_rows, from_dict, from_pandas, from_arrow."""

import pytest
from ltseq import LTSeq


# ---------------------------------------------------------------------------
# from_rows
# ---------------------------------------------------------------------------

class TestFromRows:
    def test_basic(self):
        rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        t = LTSeq.from_rows(rows)
        assert t.count() == 2
        assert set(t.columns) == {"id", "name"}

    def test_values_accessible(self):
        rows = [{"x": 10, "y": 2}, {"x": 20, "y": 3}]
        t = LTSeq.from_rows(rows)
        assert t.filter(lambda r: r.x > 15).count() == 1
        assert t.filter(lambda r: r.y == 2).count() == 1

    def test_with_explicit_schema(self):
        rows = [{"id": 1}, {"id": 2}]
        t = LTSeq.from_rows(rows, schema={"id": "Int64"})
        assert t.count() == 2

    def test_empty_with_schema(self):
        t = LTSeq.from_rows([], schema={"id": "Int64", "name": "Utf8"})
        assert t.count() == 0
        assert set(t.columns) == {"id", "name"}

    def test_empty_without_schema_raises(self):
        with pytest.raises(ValueError, match="schema"):
            LTSeq.from_rows([])

    def test_bool_inferred(self):
        rows = [{"active": True}, {"active": False}]
        t = LTSeq.from_rows(rows)
        assert t.count() == 2

    def test_chaining_after_from_rows(self):
        rows = [{"val": i} for i in range(10)]
        t = LTSeq.from_rows(rows)
        result = t.filter(lambda r: r.val > 5).count()
        assert result == 4


# ---------------------------------------------------------------------------
# from_dict
# ---------------------------------------------------------------------------

class TestFromDict:
    def test_basic(self):
        t = LTSeq.from_dict({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        assert t.count() == 3
        assert set(t.columns) == {"id", "name"}

    def test_values_accessible(self):
        t = LTSeq.from_dict({"x": [10, 20], "y": [1, 2]})
        assert t.filter(lambda r: r.x > 15).count() == 1
        assert t.filter(lambda r: r.y == 1).count() == 1

    def test_mismatched_lengths_raises(self):
        with pytest.raises(ValueError, match="same length"):
            LTSeq.from_dict({"a": [1, 2, 3], "b": [1, 2]})

    def test_empty_dict_raises(self):
        with pytest.raises(ValueError, match="at least one column"):
            LTSeq.from_dict({})

    def test_non_dict_raises(self):
        with pytest.raises(TypeError, match="dict"):
            LTSeq.from_dict([1, 2, 3])

    def test_chaining(self):
        t = LTSeq.from_dict({"score": [90, 50, 70, 85]})
        result = t.filter(lambda r: r.score >= 80).count()
        assert result == 2


# ---------------------------------------------------------------------------
# from_pandas
# ---------------------------------------------------------------------------

class TestFromPandas:
    def test_basic(self):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        t = LTSeq.from_pandas(df)
        assert t.count() == 3
        assert set(t.columns) == {"id", "name"}

    def test_round_trip(self):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"x": [1, 2], "y": [3.0, 4.0]})
        t = LTSeq.from_pandas(df)
        result_df = t.to_pandas()
        assert list(result_df["x"]) == [1, 2]

    def test_empty_dataframe(self):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"id": pd.Series([], dtype="int64"), "name": pd.Series([], dtype="object")})
        t = LTSeq.from_pandas(df)
        assert t.count() == 0

    def test_non_dataframe_raises(self):
        pytest.importorskip("pandas")
        with pytest.raises(TypeError):
            LTSeq.from_pandas({"not": "a dataframe"})

    def test_chaining(self):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"val": range(20)})
        t = LTSeq.from_pandas(df)
        assert t.filter(lambda r: r.val >= 10).count() == 10


# ---------------------------------------------------------------------------
# from_arrow
# ---------------------------------------------------------------------------

class TestFromArrow:
    def test_basic(self):
        pa = pytest.importorskip("pyarrow")
        table = pa.table({"id": [1, 2, 3], "label": ["a", "b", "c"]})
        t = LTSeq.from_arrow(table)
        assert t.count() == 3
        assert set(t.columns) == {"id", "label"}

    def test_round_trip(self):
        pa = pytest.importorskip("pyarrow")
        table = pa.table({"x": [10, 20, 30]})
        t = LTSeq.from_arrow(table)
        result = t.to_arrow()
        assert result.num_rows == 3

    def test_empty_table(self):
        pa = pytest.importorskip("pyarrow")
        schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
        table = schema.empty_table()
        t = LTSeq.from_arrow(table)
        assert t.count() == 0

    def test_non_table_raises(self):
        pytest.importorskip("pyarrow")
        with pytest.raises(TypeError):
            LTSeq.from_arrow({"not": "a table"})

    def test_chaining(self):
        pa = pytest.importorskip("pyarrow")
        table = pa.table({"score": [80, 60, 90, 40]})
        t = LTSeq.from_arrow(table)
        assert t.filter(lambda r: r.score >= 80).count() == 2
