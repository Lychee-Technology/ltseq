"""Tests for #112: ordered stateful accumulation via fold()."""

import pytest

from ltseq import LTSeq


def make_table(rows, schema):
    return LTSeq._from_rows(rows, schema)


@pytest.fixture
def rates():
    return make_table(
        [
            {"date": "d1", "rate": 0.10},
            {"date": "d2", "rate": 0.05},
            {"date": "d3", "rate": -0.02},
        ],
        {"date": "string", "rate": "float64"},
    )


class TestFoldBasic:
    def test_compounding(self, rates):
        rows = rates.sort("date").fold(
            lambda s, r: s * (1 + r["rate"]), init=1.0, into="cum_return"
        ).to_dicts()
        assert rows[0]["cum_return"] == pytest.approx(1.10)
        assert rows[1]["cum_return"] == pytest.approx(1.155)
        assert rows[2]["cum_return"] == pytest.approx(1.155 * 0.98)

    def test_running_balance(self):
        t = make_table(
            [{"i": 1, "delta": 100}, {"i": 2, "delta": -30}, {"i": 3, "delta": 50}],
            {"i": "int64", "delta": "int64"},
        )
        rows = t.sort("i").fold(lambda s, r: s + r["delta"], init=0, into="balance").to_dicts()
        assert [r["balance"] for r in rows] == [100, 70, 120]

    def test_original_columns_preserved(self, rates):
        rows = rates.sort("date").fold(
            lambda s, r: s + r["rate"], init=0.0, into="acc"
        ).to_dicts()
        assert set(rows[0].keys()) == {"date", "rate", "acc"}
        # row count unchanged
        assert len(rows) == 3

    def test_row_is_readable_dict(self, rates):
        # fn sees the current row's columns
        seen = []
        rates.sort("date").fold(
            lambda s, r: seen.append(r["date"]) or s, init=0, into="x"
        )
        assert seen == ["d1", "d2", "d3"]

    def test_state_machine(self):
        # Reset a counter whenever value drops to 0
        t = make_table(
            [{"i": 1, "v": 1}, {"i": 2, "v": 1}, {"i": 3, "v": 0}, {"i": 4, "v": 1}],
            {"i": "int64", "v": "int64"},
        )
        rows = t.sort("i").fold(
            lambda s, r: (s + 1) if r["v"] else 0, init=0, into="streak"
        ).to_dicts()
        assert [r["streak"] for r in rows] == [1, 2, 0, 1]


class TestFoldPartition:
    def test_partition_reset(self):
        t = make_table(
            [
                {"sym": "A", "i": 1, "v": 1},
                {"sym": "A", "i": 2, "v": 2},
                {"sym": "B", "i": 3, "v": 10},
                {"sym": "B", "i": 4, "v": 20},
            ],
            {"sym": "string", "i": "int64", "v": "int64"},
        )
        rows = t.sort("i").fold(
            lambda s, r: s + r["v"], init=0, into="run", partition_by="sym"
        ).to_dicts()
        assert [r["run"] for r in rows] == [1, 3, 10, 30]

    def test_partition_interleaved(self):
        t = make_table(
            [
                {"sym": "A", "i": 1, "v": 1},
                {"sym": "B", "i": 2, "v": 10},
                {"sym": "A", "i": 3, "v": 1},
                {"sym": "B", "i": 4, "v": 10},
            ],
            {"sym": "string", "i": "int64", "v": "int64"},
        )
        rows = t.sort("i").fold(
            lambda s, r: s + r["v"], init=0, into="run", partition_by="sym"
        ).to_dicts()
        assert [r["run"] for r in rows] == [1, 10, 2, 20]


class TestFoldChaining:
    def test_returns_ltseq(self, rates):
        result = rates.sort("date").fold(lambda s, r: s + 1, init=0, into="n")
        assert isinstance(result, LTSeq)

    def test_sort_metadata_preserved(self, rates):
        result = rates.sort("date").fold(lambda s, r: s + 1, init=0, into="n")
        assert result._sort_keys == [("date", False)]

    def test_window_after_fold(self, rates):
        # sort metadata carries through so window functions still work
        result = rates.sort("date").fold(
            lambda s, r: s + r["rate"], init=0.0, into="acc"
        ).derive(prev=lambda r: r.acc.shift(1)).to_dicts()
        assert result[1]["prev"] == pytest.approx(0.10)
        assert result[2]["prev"] == pytest.approx(0.15)


class TestFoldErrors:
    def test_requires_sort(self, rates):
        with pytest.raises(ValueError, match="sort"):
            rates.fold(lambda s, r: s, init=0, into="x")

    def test_into_must_be_new(self, rates):
        with pytest.raises(ValueError, match="already exists"):
            rates.sort("date").fold(lambda s, r: s, init=0, into="rate")

    def test_into_required(self, rates):
        with pytest.raises((ValueError, TypeError)):
            rates.sort("date").fold(lambda s, r: s, init=0, into="")

    def test_bad_partition_column(self, rates):
        with pytest.raises(ValueError, match="partition_by"):
            rates.sort("date").fold(
                lambda s, r: s, init=0, into="x", partition_by="nope"
            )

    def test_fn_not_callable(self, rates):
        with pytest.raises(TypeError):
            rates.sort("date").fold(42, init=0, into="x")
