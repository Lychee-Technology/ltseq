"""Tests for #108: group-proxy aggregate completion, mean alias, NestedTable.agg()."""

import math

import pytest

from ltseq import LTSeq


def make_table(rows, schema):
    return LTSeq._from_rows(rows, schema)


@pytest.fixture
def runs_table():
    """Three consecutive runs of is_up: [d1,d2] / [d3] / [d4,d5,d6]."""
    return make_table(
        [
            {"date": "d1", "is_up": True, "price": 10.0},
            {"date": "d2", "is_up": True, "price": 12.0},
            {"date": "d3", "is_up": False, "price": 9.0},
            {"date": "d4", "is_up": True, "price": 11.0},
            {"date": "d5", "is_up": True, "price": 15.0},
            {"date": "d6", "is_up": True, "price": 13.0},
        ],
        {"date": "string", "is_up": "bool", "price": "float64"},
    )


class TestGroupProxyNewAggregates:
    """derive/filter proxies gained median/std/var/percentile/mean."""

    def test_derive_median_std_var(self, runs_table):
        rows = (
            runs_table.group_ordered(lambda r: r.is_up)
            .derive(lambda g: {
                "med": g.median("price"),
                "sd": g.std("price"),
                "v": g.var("price"),
            })
            .to_dicts()
        )
        # Third run [11, 15, 13]: median 13, sample std 2, sample var 4
        assert rows[3]["med"] == 13.0
        assert rows[3]["sd"] == pytest.approx(2.0)
        assert rows[3]["v"] == pytest.approx(4.0)
        # Broadcast: same value on every row of the run
        assert rows[4]["med"] == rows[3]["med"]

    def test_derive_percentile(self, runs_table):
        rows = (
            runs_table.group_ordered(lambda r: r.is_up)
            .derive(lambda g: {"p50": g.percentile("price", 0.5)})
            .to_dicts()
        )
        assert rows[3]["p50"] == pytest.approx(13.0)

    def test_derive_mean_equals_avg(self, runs_table):
        rows = (
            runs_table.group_ordered(lambda r: r.is_up)
            .derive(lambda g: {"a": g.avg("price"), "m": g.mean("price")})
            .to_dicts()
        )
        for row in rows:
            assert row["m"] == row["a"]

    def test_filter_mean(self, runs_table):
        kept = (
            runs_table.group_ordered(lambda r: r.is_up)
            .filter(lambda g: g.mean("price") > 11)
            .flatten()
            .to_dicts()
        )
        assert [r["date"] for r in kept] == ["d4", "d5", "d6"]

    def test_filter_median(self, runs_table):
        kept = (
            runs_table.group_ordered(lambda r: r.is_up)
            .filter(lambda g: g.median("price") < 10)
            .flatten()
            .to_dicts()
        )
        assert [r["date"] for r in kept] == ["d3"]


class TestMeanAliasInAggContexts:
    """mean works in agg()/group_by().agg() alongside avg."""

    def test_agg_mean(self, runs_table):
        rows = runs_table.agg(
            by=lambda r: r.is_up,
            a=lambda g: g.price.avg(),
            m=lambda g: g.price.mean(),
        ).to_dicts()
        for row in rows:
            assert row["m"] == row["a"]

    def test_group_by_chain_mean(self, runs_table):
        rows = (
            runs_table.group_by("is_up")
            .agg(m=lambda g: g.price.mean())
            .to_dicts()
        )
        by_key = {row["is_up"]: row["m"] for row in rows}
        assert by_key[False] == pytest.approx(9.0)
        assert by_key[True] == pytest.approx((10 + 12 + 11 + 15 + 13) / 5)


class TestNestedTableAgg:
    """NestedTable.agg(): one row per group, sequence order preserved."""

    def test_collapse_shape_and_order(self, runs_table):
        spans = (
            runs_table.group_ordered(lambda r: r.is_up)
            .agg(lambda g: {
                "start": g.first().date,
                "end": g.last().date,
                "n": g.count(),
            })
            .to_dicts()
        )
        assert spans == [
            {"start": "d1", "end": "d2", "n": 2},
            {"start": "d3", "end": "d3", "n": 1},
            {"start": "d4", "end": "d6", "n": 3},
        ]

    def test_agg_with_aggregates(self, runs_table):
        spans = (
            runs_table.group_ordered(lambda r: r.is_up)
            .agg(lambda g: {"total": g.sum("price"), "peak": g.max("price")})
            .to_dicts()
        )
        assert spans[2]["total"] == pytest.approx(39.0)
        assert spans[2]["peak"] == 15.0

    def test_agg_matches_derive_broadcast(self, runs_table):
        """agg() yields exactly the per-group values derive() broadcasts."""
        nested = runs_table.group_ordered(lambda r: r.is_up)
        via_agg = [r["n"] for r in nested.agg(lambda g: {"n": g.count()}).to_dicts()]
        assert via_agg == [2, 1, 3]
        broadcast = [r["n"] for r in nested.derive(lambda g: {"n": g.count()}).to_dicts()]
        assert broadcast == [2, 2, 1, 3, 3, 3]

    def test_agg_returns_ltseq_chainable(self, runs_table):
        spans = runs_table.group_ordered(lambda r: r.is_up).agg(
            lambda g: {"n": g.count()}
        )
        assert isinstance(spans, LTSeq)
        assert spans.filter(lambda r: r.n > 1).count() == 2

    def test_agg_rejects_non_dict(self, runs_table):
        with pytest.raises(ValueError, match="dict"):
            runs_table.group_ordered(lambda r: r.is_up).agg(lambda g: g.count())

    def test_agg_rejects_arithmetic(self, runs_table):
        with pytest.raises(ValueError, match="arithmetic|Unsupported"):
            runs_table.group_ordered(lambda r: r.is_up).agg(
                lambda g: {"ratio": g.sum("price") / g.count()}
            )

    def test_derive_broadcast_unchanged(self, runs_table):
        """derive keeps broadcast semantics: row count unchanged."""
        derived = runs_table.group_ordered(lambda r: r.is_up).derive(
            lambda g: {"n": g.count()}
        )
        assert derived.count() == 6
