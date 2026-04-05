"""Tests for MutationMixin: insert, delete, update, modify operations."""

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
# update() is currently broken (issue #17): it calls derive() with an
# if_else expression that references the existing column, causing DataFusion
# schema ambiguity. Tests are marked xfail until the bug is fixed.

_update_bug = pytest.mark.xfail(
    reason="update() broken: derive() cannot overwrite existing column (issue #17)",
    strict=True,
)


class TestUpdate:
    @_update_bug
    def test_update_matching_rows(self, sample):
        result = sample.update(lambda r: r.name == "bob", score=99)
        df = result.to_pandas()
        assert df[df["name"] == "bob"]["score"].iloc[0] == 99
        assert df[df["name"] == "alice"]["score"].iloc[0] == 10

    @_update_bug
    def test_update_multiple_columns(self, sample):
        result = sample.update(lambda r: r.id == 1, score=100, name="ALICE")
        df = result.to_pandas()
        row = df[df["id"] == 1].iloc[0]
        assert row["score"] == 100
        assert row["name"] == "ALICE"

    @_update_bug
    def test_update_no_match_returns_unchanged(self, sample):
        result = sample.update(lambda r: r.id == 999, score=0)
        assert result.to_pandas()["score"].tolist() == [10, 20, 30]

    def test_update_no_kwargs_returns_self(self, sample):
        result = sample.update(lambda r: r.id == 1)
        assert len(result.to_pandas()) == 3

    @_update_bug
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
