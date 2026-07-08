"""Tests for #110: when/then/otherwise chains and cum_max/cum_min expressions."""

import pytest

from ltseq import LTSeq, when
from ltseq.expr import if_else


def make_table(rows, schema):
    return LTSeq._from_rows(rows, schema)


@pytest.fixture
def amounts():
    return make_table(
        [
            {"id": 1, "amount": 150.0},
            {"id": 2, "amount": 75.0},
            {"id": 3, "amount": 20.0},
            {"id": 4, "amount": 55.0},
        ],
        {"id": "int64", "amount": "float64"},
    )


class TestWhenChain:
    def test_two_arg_form(self, amounts):
        rows = amounts.derive(
            tier=lambda r: when(r.amount > 100, "VIP")
            .when(r.amount > 50, "Gold")
            .otherwise("Normal")
        ).to_dicts()
        assert [r["tier"] for r in rows] == ["VIP", "Gold", "Normal", "Gold"]

    def test_then_form(self, amounts):
        rows = amounts.derive(
            tier=lambda r: when(r.amount > 100)
            .then("VIP")
            .when(r.amount > 50)
            .then("Gold")
            .otherwise("Normal")
        ).to_dicts()
        assert [r["tier"] for r in rows] == ["VIP", "Gold", "Normal", "Gold"]

    def test_first_match_wins(self, amounts):
        # A row matching multiple conditions takes the first branch.
        rows = amounts.derive(
            tag=lambda r: when(r.amount > 10, "big").when(r.amount > 100, "huge").otherwise("x")
        ).to_dicts()
        # amount=150 matches both but should be "big" (first branch)
        assert rows[0]["tag"] == "big"

    def test_single_branch(self, amounts):
        rows = amounts.derive(
            flag=lambda r: when(r.amount > 100, 1).otherwise(0)
        ).to_dicts()
        assert [r["flag"] for r in rows] == [1, 0, 0, 0]

    def test_equivalent_to_nested_if_else(self, amounts):
        via_when = amounts.derive(
            t=lambda r: when(r.amount > 100, "a").when(r.amount > 50, "b").otherwise("c")
        ).to_dicts()
        via_nested = amounts.derive(
            t=lambda r: if_else(r.amount > 100, "a", if_else(r.amount > 50, "b", "c"))
        ).to_dicts()
        assert [r["t"] for r in via_when] == [r["t"] for r in via_nested]

    def test_numeric_branches(self, amounts):
        rows = amounts.derive(
            score=lambda r: when(r.amount > 100, r.amount * 2)
            .when(r.amount > 50, r.amount)
            .otherwise(0)
        ).to_dicts()
        assert rows[0]["score"] == 300.0
        assert rows[1]["score"] == 75.0
        assert rows[2]["score"] == 0.0


class TestCumMaxMin:
    @pytest.fixture
    def prices(self):
        return make_table(
            [
                {"i": 1, "px": 10.0},
                {"i": 2, "px": 8.0},
                {"i": 3, "px": 15.0},
                {"i": 4, "px": 12.0},
            ],
            {"i": "int64", "px": "float64"},
        )

    def test_cum_max(self, prices):
        rows = prices.sort("i").derive(peak=lambda r: r.px.cum_max()).to_dicts()
        assert [r["peak"] for r in rows] == [10.0, 10.0, 15.0, 15.0]

    def test_cum_min(self, prices):
        rows = prices.sort("i").derive(trough=lambda r: r.px.cum_min()).to_dicts()
        assert [r["trough"] for r in rows] == [10.0, 8.0, 8.0, 8.0]

    def test_cum_max_partition_by(self):
        t = make_table(
            [
                {"g": "a", "i": 1, "px": 5.0},
                {"g": "a", "i": 2, "px": 9.0},
                {"g": "b", "i": 3, "px": 20.0},
                {"g": "b", "i": 4, "px": 3.0},
            ],
            {"g": "string", "i": "int64", "px": "float64"},
        )
        rows = t.sort("i").derive(peak=lambda r: r.px.cum_max(partition_by="g")).to_dicts()
        by_g = {}
        for r in rows:
            by_g.setdefault(r["g"], []).append(r["peak"])
        assert by_g["a"] == [5.0, 9.0]
        assert by_g["b"] == [20.0, 20.0]

    def test_cum_min_partition_by(self):
        t = make_table(
            [
                {"g": "a", "i": 1, "px": 5.0},
                {"g": "a", "i": 2, "px": 9.0},
                {"g": "b", "i": 3, "px": 20.0},
                {"g": "b", "i": 4, "px": 3.0},
            ],
            {"g": "string", "i": "int64", "px": "float64"},
        )
        rows = t.sort("i").derive(low=lambda r: r.px.cum_min(partition_by="g")).to_dicts()
        by_g = {}
        for r in rows:
            by_g.setdefault(r["g"], []).append(r["low"])
        assert by_g["a"] == [5.0, 5.0]
        assert by_g["b"] == [20.0, 3.0]

    def test_cum_max_alongside_cum_sum(self, prices):
        rows = prices.sort("i").derive(
            running_total=lambda r: r.px.cum_sum(),
            peak=lambda r: r.px.cum_max(),
        ).to_dicts()
        assert [r["running_total"] for r in rows] == [10.0, 18.0, 33.0, 45.0]
        assert [r["peak"] for r in rows] == [10.0, 10.0, 15.0, 15.0]
