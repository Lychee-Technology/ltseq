"""Tests for #107 (PR 5b): asof_join by= grouping and strategy alignment."""

import math

import numpy as np
import pyarrow as pa
import pytest

from ltseq import LTSeq


def make_table(rows, schema):
    return LTSeq._from_rows(rows, schema)


def _is_null(v):
    return v is None or (isinstance(v, float) and math.isnan(v))


def _bids_equal(a, b):
    """Compare two bid lists treating None/NaN as equal nulls."""
    if len(a) != len(b):
        return False
    return all(
        (_is_null(x) and _is_null(y)) or x == y for x, y in zip(a, b)
    )


@pytest.fixture
def trades():
    return make_table(
        [
            {"time": 10, "symbol": "A", "px": 100},
            {"time": 10, "symbol": "B", "px": 200},
            {"time": 20, "symbol": "A", "px": 101},
            {"time": 20, "symbol": "B", "px": 201},
        ],
        {"time": "int64", "symbol": "string", "px": "int64"},
    )


@pytest.fixture
def quotes():
    return make_table(
        [
            {"time": 5, "symbol": "A", "bid": 99},
            {"time": 5, "symbol": "B", "bid": 199},
            {"time": 15, "symbol": "A", "bid": 100},
            {"time": 15, "symbol": "B", "bid": 200},
        ],
        {"time": "int64", "symbol": "string", "bid": "int64"},
    )


class TestAsofBy:
    def test_by_isolates_groups(self, trades, quotes):
        r = trades.asof_join(quotes, on="time", by="symbol", strategy="backward").to_dicts()
        got = {(x["time"], x["symbol"]): x["bid"] for x in r}
        assert got[(10, "A")] == 99
        assert got[(10, "B")] == 199
        assert got[(20, "A")] == 100
        assert got[(20, "B")] == 200

    def test_by_vs_no_by_differ(self, trades, quotes):
        # Without by=, matching crosses symbols; with by= it must not.
        with_by = trades.asof_join(quotes, on="time", by="symbol").to_dicts()
        no_by = trades.asof_join(quotes, on="time").to_dicts()
        by_keyed = {(x["time"], x["symbol"]): x["bid"] for x in with_by}
        # A at time 10 correctly gets A's quote (99), not B's
        assert by_keyed[(10, "A")] == 99
        # Global (no by) picks the last quote at time<=10 regardless of symbol
        assert any(x["symbol"] == "A" and x["bid"] != 99 for x in no_by)

    def test_by_missing_group_yields_null(self):
        left = make_table(
            [{"time": 10, "g": "X"}, {"time": 10, "g": "Y"}],
            {"time": "int64", "g": "string"},
        )
        right = make_table(
            [{"time": 5, "g": "X", "v": 1}],
            {"time": "int64", "g": "string", "v": "int64"},
        )
        r = left.asof_join(right, on="time", by="g").to_dicts()
        got = {x["g"]: x["v"] for x in r}
        assert got["X"] == 1
        assert _is_null(got["Y"])  # no matching group → null

    def test_by_forward(self, trades, quotes):
        r = trades.asof_join(quotes, on="time", by="symbol", strategy="forward").to_dicts()
        got = {(x["time"], x["symbol"]): x["bid"] for x in r}
        # time=10 A forward → quote A at time>=10 → time 15 bid 100
        assert got[(10, "A")] == 100


class TestStrategyAlias:
    def test_direction_alias(self, trades, quotes):
        a = trades.asof_join(quotes, on="time", direction="forward").to_dicts()
        b = trades.asof_join(quotes, on="time", strategy="forward").to_dicts()
        assert _bids_equal([x["bid"] for x in a], [x["bid"] for x in b])

    def test_conflicting_strategy_direction(self, trades, quotes):
        with pytest.raises(ValueError, match="conflicting"):
            trades.asof_join(quotes, on="time", strategy="backward", direction="forward")

    def test_default_backward(self, trades, quotes):
        default = trades.asof_join(quotes, on="time", by="symbol").to_dicts()
        backward = trades.asof_join(quotes, on="time", by="symbol", strategy="backward").to_dicts()
        assert _bids_equal([x["bid"] for x in default], [x["bid"] for x in backward])


class TestLambdaOperatorAuthoritative:
    def test_ge_implies_backward(self, trades, quotes):
        # t.time >= q.time → backward
        r = trades.asof_join(quotes, on=lambda t, q: t.time >= q.time, by="symbol").to_dicts()
        got = {(x["time"], x["symbol"]): x["bid"] for x in r}
        assert got[(10, "A")] == 99  # backward match

    def test_le_implies_forward(self, trades, quotes):
        # t.time <= q.time → forward
        r = trades.asof_join(quotes, on=lambda t, q: t.time <= q.time, by="symbol").to_dicts()
        got = {(x["time"], x["symbol"]): x["bid"] for x in r}
        assert got[(10, "A")] == 100  # forward match (time 15)

    def test_lambda_strategy_conflict(self, trades, quotes):
        with pytest.raises(ValueError, match="operator implies"):
            trades.asof_join(
                quotes, on=lambda t, q: t.time >= q.time, strategy="forward"
            )

    def test_lambda_nearest_conflict(self, trades, quotes):
        # A directional lambda operator contradicts "nearest": a nearest match
        # can land on the opposite side of the predicate, so it must error.
        with pytest.raises(ValueError, match="operator implies"):
            trades.asof_join(
                quotes, on=lambda t, q: t.time >= q.time, strategy="nearest"
            )

    def test_lambda_plus_left_right_on_rejected(self, trades, quotes):
        # A lambda already names both time columns; combining it with
        # left_on/right_on is ambiguous and must be rejected, not ignored.
        with pytest.raises(ValueError, match="left_on"):
            trades.asof_join(
                quotes, on=lambda t, q: t.time >= q.time, left_on="time", right_on="time"
            )


class TestAsofErrors:
    def test_bad_by_column(self, trades, quotes):
        with pytest.raises(ValueError, match="by="):
            trades.asof_join(quotes, on="time", by="nonexistent")

    def test_left_on_right_on(self):
        left = make_table([{"lt": 10, "g": "A"}], {"lt": "int64", "g": "string"})
        right = make_table([{"rt": 5, "g": "A", "v": 1}], {"rt": "int64", "g": "string", "v": "int64"})
        r = left.asof_join(right, left_on="lt", right_on="rt", by="g").to_dicts()
        assert r[0]["v"] == 1


class TestAsofByKeyEquality:
    """``by`` keys follow ``join``'s equality (#370, contract §12.1 and
    §12.3): a NULL key matches nothing, not even NULL, NaN matches NaN and
    ``-0.0`` matches ``0.0``."""

    @pytest.mark.parametrize("strategy", ["backward", "forward", "nearest"])
    def test_the_issue_reproduction(self, strategy):
        left = LTSeq.from_arrow(pa.table({"t": [10, 10], "s": pa.array([None, "A"], pa.string())}))
        right = LTSeq.from_arrow(
            pa.table(
                {
                    "t": [10, 15],
                    "s": pa.array([None, "A"], pa.string()),
                    "v": ["null-key", "A"],
                }
            )
        )
        r = left.asof_join(right, on="t", by="s", strategy=strategy).to_dicts()
        expected_a = {"backward": None, "forward": "A", "nearest": "A"}[strategy]
        assert [(x["s"], x["v"]) for x in r] == [(None, None), ("A", expected_a)]

    @pytest.mark.parametrize("null_in", ["s", "k", "both"])
    def test_a_null_in_any_of_several_by_columns_never_matches(self, null_in):
        """The right table holds a row with the same NULL, and one whose
        other key is equal, so neither half of the key alone decides."""
        s = None if null_in in ("s", "both") else "A"
        k = None if null_in in ("k", "both") else 1
        left = LTSeq.from_arrow(
            pa.table(
                {
                    "id": [0, 1],
                    "t": [10, 10],
                    "s": pa.array([s, "A"], pa.string()),
                    "k": pa.array([k, 1], pa.int64()),
                }
            )
        )
        right = LTSeq.from_arrow(
            pa.table(
                {
                    "t": [1, 2, 3, 4],
                    "s": pa.array([s, None, "A", "A"], pa.string()),
                    "k": pa.array([k, 1, None, 1], pa.int64()),
                    "v": ["same-nulls", "null-s", "null-k", "A1"],
                }
            )
        )
        r = left.asof_join(right, on="t", by=["s", "k"]).to_dicts()
        assert {x["id"]: x["v"] for x in r} == {0: None, 1: "A1"}

    def test_an_all_null_by_column_never_matches(self):
        left = LTSeq.from_arrow(pa.table({"t": [10, 10], "g": pa.nulls(2, pa.string())}))
        right = LTSeq.from_arrow(
            pa.table({"t": [5, 6], "g": pa.nulls(2, pa.string()), "v": [1, 2]})
        )
        r = left.asof_join(right, on="t", by="g").to_dicts()
        assert [x["v"] for x in r] == [None, None]

    @pytest.mark.parametrize("float_type", [np.float32, np.float64])
    def test_negative_zero_matches_zero(self, float_type):
        left = LTSeq.from_arrow(
            pa.table({"id": [0, 1], "t": [10, 10], "g": np.array([0.0, -0.0], float_type)})
        )
        right = LTSeq.from_arrow(
            pa.table({"t": [5, 6], "g": np.array([-0.0, 1.0], float_type), "v": ["zero", "one"]})
        )
        r = left.asof_join(right, on="t", by="g").to_dicts()
        assert {x["id"]: x["v"] for x in r} == {0: "zero", 1: "zero"}

    @pytest.mark.parametrize(
        "float_type, uint_type, nans",
        [
            (np.float32, np.uint32, [0x7FC00000, 0xFFC00000, 0x7FC00001]),
            (
                np.float64,
                np.uint64,
                [0x7FF8000000000000, 0xFFF8000000000000, 0x7FF8000000000001],
            ),
        ],
        ids=["float32", "float64"],
    )
    def test_nan_matches_nan_whatever_its_bits(self, float_type, uint_type, nans):
        """A quiet NaN, a negative one and one with a payload are all NaN."""
        bits = np.array(nans, uint_type).view(float_type)
        left = LTSeq.from_arrow(
            pa.table({"id": [0, 1, 2], "t": [10, 10, 10], "g": pa.array(bits)})
        )
        right = LTSeq.from_arrow(
            pa.table(
                {
                    "t": [5, 6],
                    "g": pa.array(np.array([bits[1], 1.0], float_type)),
                    "v": ["nan", "one"],
                }
            )
        )
        r = left.asof_join(right, on="t", by="g").to_dicts()
        assert {x["id"]: x["v"] for x in r} == {0: "nan", 1: "nan", 2: "nan"}

    def test_a_float_key_still_tells_values_apart(self):
        left = LTSeq.from_arrow(pa.table({"id": [0, 1], "t": [10, 10], "g": [1.5, 2.5]}))
        right = LTSeq.from_arrow(
            pa.table({"t": [5, 6], "g": [2.5, float("nan")], "v": ["2.5", "nan"]})
        )
        r = left.asof_join(right, on="t", by="g").to_dicts()
        assert {x["id"]: x["v"] for x in r} == {0: None, 1: "2.5"}
