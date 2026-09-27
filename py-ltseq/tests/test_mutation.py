"""Tests for MutationMixin: insert, delete, update, modify operations."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pd = pytest.importorskip("pandas")

try:
    from ltseq import LTSeq
except ImportError:
    import sys
    sys.path.insert(0, "py-ltseq")
    from ltseq import LTSeq


@pytest.fixture
def sample():
    """Three-row table: id=[1,2,3], name=[alice,bob,carol], score=[10,20,30]."""
    return LTSeq.from_rows([
        {"id": 1, "name": "alice", "score": 10},
        {"id": 2, "name": "bob",   "score": 20},
        {"id": 3, "name": "carol", "score": 30},
    ])


# ─── insert ──────────────────────────────────────────────────────────────────

class TestInsert:
    def test_insert_at_start(self, sample):
        result = sample.insert(0, {"id": 0, "name": "zero", "score": 0})
        rows = result.to_pandas().to_dict("records")
        assert rows[0] == {"id": 0, "name": "zero", "score": 0}
        assert len(rows) == 4

    def test_insert_at_end(self, sample):
        result = sample.insert(3, {"id": 4, "name": "dave", "score": 40})
        rows = result.to_pandas().to_dict("records")
        assert rows[-1]["name"] == "dave"
        assert len(rows) == 4

    def test_insert_in_middle(self, sample):
        result = sample.insert(1, {"id": 99, "name": "mid", "score": 15})
        rows = result.to_pandas().to_dict("records")
        assert rows[1]["name"] == "mid"
        assert rows[2]["name"] == "bob"

    def test_insert_beyond_end_clamps(self, sample):
        result = sample.insert(100, {"id": 9, "name": "end", "score": 90})
        rows = result.to_pandas().to_dict("records")
        assert rows[-1]["name"] == "end"
        assert len(rows) == 4

    def test_insert_negative_pos_clamps_to_zero(self, sample):
        result = sample.insert(-5, {"id": 0, "name": "first", "score": 0})
        rows = result.to_pandas().to_dict("records")
        assert rows[0]["name"] == "first"

    def test_insert_does_not_mutate_original(self, sample):
        _ = sample.insert(0, {"id": 0, "name": "zero", "score": 0})
        assert len(sample.to_pandas()) == 3


# ─── delete ──────────────────────────────────────────────────────────────────

class TestDelete:
    def test_delete_by_predicate(self, sample):
        result = sample.delete(lambda r: r.name == "bob")
        names = result.to_pandas()["name"].tolist()
        assert "bob" not in names
        assert len(names) == 2

    def test_delete_by_index(self, sample):
        result = sample.delete(0)
        rows = result.to_pandas()
        assert rows.iloc[0]["name"] == "bob"
        assert len(rows) == 2

    def test_delete_last_row_by_index(self, sample):
        result = sample.delete(2)
        assert len(result.to_pandas()) == 2
        assert result.to_pandas().iloc[-1]["name"] == "bob"

    def test_delete_out_of_range_index_is_noop(self, sample):
        result = sample.delete(99)
        assert len(result.to_pandas()) == 3

    def test_delete_predicate_no_match_returns_all(self, sample):
        result = sample.delete(lambda r: r.id == 999)
        assert len(result.to_pandas()) == 3

    def test_delete_does_not_mutate_original(self, sample):
        _ = sample.delete(0)
        assert len(sample.to_pandas()) == 3


# ─── update ──────────────────────────────────────────────────────────────────
    @pytest.mark.parametrize("pos", [0, 4, 5, 9, 14])
    def test_delete_by_index_multi_batch_removes_exactly_one_row(self, pos):
        """Regression (#176): only the batch holding `pos` is spliced.

        The per-batch offset math used `saturating_sub`, which yielded 0 for
        every batch after the target and deleted their first rows too. Small
        single-batch tables never showed it.
        """
        import pyarrow as pa
        from ltseq import LTSeq

        batches = [
            pa.record_batch({"id": list(range(start, start + 5))})
            for start in (0, 5, 10)
        ]
        t = LTSeq.from_arrow(pa.Table.from_batches(batches))
        assert t.count() == 15

        result = t.delete(pos)
        ids = [row["id"] for row in result.to_dicts()]
        assert ids == [i for i in range(15) if i != pos]


class TestUpdate:
    def test_update_matching_rows(self, sample):
        result = sample.update(lambda r: r.name == "bob", score=99)
        df = result.to_pandas()
        assert df[df["name"] == "bob"]["score"].iloc[0] == 99
        assert df[df["name"] == "alice"]["score"].iloc[0] == 10

    def test_update_multiple_columns(self, sample):
        result = sample.update(lambda r: r.id == 1, score=100, name="ALICE")
        df = result.to_pandas()
        row = df[df["id"] == 1].iloc[0]
        assert row["score"] == 100
        assert row["name"] == "ALICE"

    def test_update_no_match_returns_unchanged(self, sample):
        result = sample.update(lambda r: r.id == 999, score=0)
        assert result.to_pandas()["score"].tolist() == [10, 20, 30]

    def test_update_no_kwargs_returns_self(self, sample):
        result = sample.update(lambda r: r.id == 1)
        assert len(result.to_pandas()) == 3

    def test_update_does_not_mutate_original(self, sample):
        _ = sample.update(lambda r: r.id == 1, score=999)
        assert sample.to_pandas().iloc[0]["score"] == 10


# ─── modify ──────────────────────────────────────────────────────────────────

class TestModify:
    def test_modify_single_column(self, sample):
        result = sample.modify(1, score=99)
        df = result.to_pandas()
        assert df.iloc[1]["score"] == 99
        assert df.iloc[0]["score"] == 10

    def test_modify_multiple_columns(self, sample):
        result = sample.modify(0, score=0, name="zero")
        row = result.to_pandas().iloc[0]
        assert row["score"] == 0
        assert row["name"] == "zero"

    def test_modify_out_of_range_is_noop(self, sample):
        result = sample.modify(99, score=999)
        assert result.to_pandas()["score"].tolist() == [10, 20, 30]

    def test_modify_does_not_mutate_original(self, sample):
        _ = sample.modify(0, score=999)
        assert sample.to_pandas().iloc[0]["score"] == 10


# ─── string column types ─────────────────────────────────────────────────────

# `read_parquet` yields string_view (DataFusion 55 scans Parquet strings as
# Utf8View); `from_arrow` keeps the caller's type; `from_rows` gives string.
STRING_TYPES = [pa.string(), pa.large_string(), pa.string_view()]


def _people(name_type: pa.DataType) -> pa.Table:
    return pa.table(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "name": pa.array(["alice", "bob", "carol"], name_type),
            "score": pa.array([10, 20, 30], pa.int64()),
        }
    )


def _names(t: LTSeq) -> list:
    # Read through Arrow: to_dicts() goes via pandas and turns nulls into NaN.
    return t.to_arrow().column("name").to_pylist()


class TestStringColumnTypes:
    """Mutations must write a Python str into every Arrow string type (#177)."""

    @pytest.fixture(params=STRING_TYPES, ids=str)
    def typed(self, request) -> LTSeq:
        return LTSeq.from_arrow(_people(request.param))

    def test_insert_str(self, typed):
        result = typed.insert(1, {"id": 9, "name": "zed", "score": 0})
        assert _names(result) == ["alice", "zed", "bob", "carol"]

    def test_insert_null_str(self, typed):
        result = typed.insert(0, {"id": 9, "name": None, "score": 0})
        assert _names(result) == [None, "alice", "bob", "carol"]

    def test_modify_str(self, typed):
        assert _names(typed.modify(1, name="zed")) == ["alice", "zed", "carol"]

    def test_update_str(self, typed):
        result = typed.update(lambda r: r.score >= 20, name="senior")
        assert _names(result) == ["alice", "senior", "senior"]

    def test_parquet_scan_string_column(self, tmp_path):
        """The reported path: a Parquet scan hands the mutations string_view data."""
        path = tmp_path / "people.parquet"
        pq.write_table(_people(pa.string()), path)
        t = LTSeq.read_parquet(str(path))
        assert _names(t.insert(0, {"id": 0, "name": "zero", "score": 0})) == ["zero", "alice", "bob", "carol"]
        assert _names(t.modify(0, name="zed")) == ["zed", "bob", "carol"]
        assert _names(t.update(lambda r: r.id == 3, name="carla")) == ["alice", "bob", "carla"]
