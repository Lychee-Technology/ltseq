"""LinkedTable join-semantics regression tests (issue #125 findings 6/7/10).

Before the collapse, LinkedTable.select() had a source-only fast path that
returned source rows WITHOUT executing the join, so unmatched rows and
one-to-many fan-out silently disappeared; chained link() dropped the first
join from the plan and stitched a preview schema. These tests lock the
corrected behavior: every transform runs on the real joined plan.

Fixtures write CSV files (like the other linking tests) rather than using
LTSeq.from_pandas — an in-memory MemTable source trips a pre-existing
DataFusion ProjectionPushdown bug on join+projection that is orthogonal to
this change (a plain join().select() hits it too).
"""

import pandas as pd
import pytest

from ltseq import LTSeq


@pytest.fixture
def mk(tmp_path):
    counter = {"n": 0}

    def _mk(**cols):
        counter["n"] += 1
        path = tmp_path / f"t{counter['n']}.csv"
        pd.DataFrame(cols).to_csv(path, index=False)
        return LTSeq.read_csv(str(path))

    return _mk


class TestSelectExecutesJoin:
    """select() of source-only columns must reflect the join, not the source.

    Equivalent-to-a-plain-join row counts lock the old fast-path holes.
    """

    def test_inner_join_drops_unmatched_source_row(self, mk):
        orders = mk(id=[1, 2, 3], pid=[10, 20, 99])  # pid 99 has no product
        products = mk(pid=[10, 20, 30], name=["A", "B", "C"])
        linked = orders.link(products, on=lambda o, p: o.pid == p.pid, as_="prod")

        # Source has 3 rows; inner join keeps only the 2 matched.
        result = linked.select("id")
        assert len(result) == 2
        assert sorted(result.to_pandas()["id"].tolist()) == [1, 2]

    def test_left_join_keeps_unmatched_source_row(self, mk):
        orders = mk(id=[1, 2, 3], pid=[10, 20, 99])
        products = mk(pid=[10, 20, 30], name=["A", "B", "C"])
        linked = orders.link(
            products, on=lambda o, p: o.pid == p.pid, as_="prod", join_type="left"
        )
        result = linked.select("id")
        assert len(result) == 3
        assert sorted(result.to_pandas()["id"].tolist()) == [1, 2, 3]

    def test_right_join_keeps_unmatched_target_row(self, mk):
        orders = mk(id=[1, 2], pid=[10, 20])
        products = mk(pid=[10, 20, 30], name=["A", "B", "C"])  # pid 30 unmatched
        linked = orders.link(
            products, on=lambda o, p: o.pid == p.pid, as_="prod", join_type="right"
        )
        # All 3 products kept; the unmatched one has a NULL source id.
        result = linked.select("id")
        assert len(result) == 3
        assert result.to_pandas()["id"].isna().sum() == 1

    def test_full_join_keeps_both_unmatched_rows(self, mk):
        orders = mk(id=[1, 2, 3], pid=[10, 20, 99])  # 99 order-only
        products = mk(pid=[10, 20, 30], name=["A", "B", "C"])  # 30 product-only
        linked = orders.link(
            products, on=lambda o, p: o.pid == p.pid, as_="prod", join_type="full"
        )
        # 2 matched + 1 order-only + 1 product-only = 4.
        assert len(linked.select("id")) == 4

    def test_one_to_many_fan_out(self, mk):
        orders = mk(id=[1, 2], pid=[10, 20])
        products = mk(pid=[10, 10, 20], name=["A", "A2", "B"])  # pid 10 twice
        linked = orders.link(products, on=lambda o, p: o.pid == p.pid, as_="prod")

        # id=1 fans out to two product rows; select("id") must see all 3
        # joined rows, not the 2 source rows.
        result = linked.select("id").to_pandas()
        assert len(result) == 3
        assert sorted(result["id"].tolist()) == [1, 1, 2]


class TestTransformJoinOrder:
    """Transforms apply AFTER the join, over the joined rows."""

    def test_slice_is_join_then_slice(self, mk):
        orders = mk(id=[1, 2, 3], pid=[99, 10, 20])  # id=1 unmatched (pid 99)
        products = mk(pid=[10, 20], name=["A", "B"])
        linked = orders.link(products, on=lambda o, p: o.pid == p.pid, as_="prod")

        # Inner join drops id=1, leaving joined rows for id=2 and id=3.
        # slice(0,1) takes the first JOINED row — which can never be the
        # first SOURCE row (id=1, dropped). The specific survivor depends on
        # join output order, so assert the semantic, not a fixed id.
        first = linked.slice(0, 1).to_pandas()
        assert len(first) == 1
        assert first["id"].iloc[0] != 1
        assert first["id"].iloc[0] in (2, 3)

    def test_filter_on_source_column_after_join(self, mk):
        orders = mk(id=[1, 2, 3], pid=[10, 20, 99])
        products = mk(pid=[10, 20, 30], name=["A", "B", "C"])
        linked = orders.link(products, on=lambda o, p: o.pid == p.pid, as_="prod")
        # Inner join → 2 rows (id 1,2); filter keeps id>1 → 1 row.
        assert len(linked.filter(lambda r: r.id > 1)) == 1


class TestTransformsSeeUnmatchedRows:
    """Every transform runs on the joined plan, so on right/full joins it
    sees the unmatched rows too — not just the matched subset."""

    def _right(self, mk):
        # pid 30 unmatched on the right → source columns NULL for that row.
        orders = mk(id=[1, 2], pid=[10, 20])
        products = mk(pid=[10, 20, 30], name=["A", "B", "C"])
        return orders.link(
            products, on=lambda o, p: o.pid == p.pid, as_="prod", join_type="right"
        )

    def _full(self, mk):
        # pid 10 matched, pid 99 order-only, pid 20 product-only → 3 rows.
        orders = mk(id=[1, 2], pid=[10, 99])
        products = mk(pid=[10, 20], name=["A", "B"])
        return orders.link(
            products, on=lambda o, p: o.pid == p.pid, as_="prod", join_type="full"
        )

    @pytest.mark.parametrize("which", ["right", "full"])
    def test_transforms_preserve_full_joined_count(self, mk, which):
        linked = getattr(self, f"_{which}")(mk)
        n = len(linked.to_ltseq())
        assert n == 3  # right: 3 products; full: matched + 2 unmatched

        # Count-preserving transforms run over the whole joined set.
        assert len(linked.sort("prod_pid")) == n
        assert len(linked.derive(tag=lambda r: r.prod_pid)) == n
        assert len(linked.slice(0, 100)) == n
        assert len(linked.distinct()) == n

    def test_right_join_filter_finds_unmatched_target_row(self, mk):
        # The unmatched right row has a NULL source id; filtering for it
        # proves the filter sees the joined (not source-only) rows.
        linked = self._right(mk)
        assert len(linked.filter(lambda r: r.id.is_null())) == 1

    def test_full_join_filter_finds_both_unmatched_sides(self, mk):
        linked = self._full(mk)
        # order-only row: NULL prod_pid; product-only row: NULL id.
        assert len(linked.filter(lambda r: r.prod_pid.is_null())) == 1
        assert len(linked.filter(lambda r: r.id.is_null())) == 1


class TestToLtseqAndCollect:
    def test_to_ltseq_returns_lazy_joined_table(self, mk):
        orders = mk(id=[1, 2], pid=[10, 20])
        products = mk(pid=[10, 20], name=["A", "B"])
        linked = orders.link(products, on=lambda o, p: o.pid == p.pid, as_="prod")

        lazy = linked.to_ltseq()
        assert isinstance(lazy, LTSeq)
        assert "prod_name" in lazy._schema
        assert "id" in lazy._schema
        assert len(lazy) == 2

    def test_collect_executes_and_returns_ltseq(self, mk):
        orders = mk(id=[1, 2], pid=[10, 20])
        products = mk(pid=[10, 20], name=["A", "B"])
        linked = orders.link(products, on=lambda o, p: o.pid == p.pid, as_="prod")

        collected = linked.collect()
        assert isinstance(collected, LTSeq)
        assert sorted(collected.to_pandas()["prod_name"].tolist()) == ["A", "B"]

    def test_plan_is_cached(self, mk):
        orders = mk(id=[1, 2], pid=[10, 20])
        products = mk(pid=[10, 20], name=["A", "B"])
        linked = orders.link(products, on=lambda o, p: o.pid == p.pid, as_="prod")
        assert linked._plan is None
        first = linked.to_ltseq()
        assert linked._plan is first
        assert linked.to_ltseq() is first

    def test_collect_is_cached(self, mk):
        # issue #125 requires collect() to execute AND cache the result.
        orders = mk(id=[1, 2], pid=[10, 20])
        products = mk(pid=[10, 20], name=["A", "B"])
        linked = orders.link(products, on=lambda o, p: o.pid == p.pid, as_="prod")
        first = linked.collect()
        assert linked.collect() is first


class TestAliasCollision:
    """A `{alias}_col` name that collides with an existing source column
    makes the join ambiguous; reject it at link() with a clear error
    instead of failing deep in DataFusion on the first transform."""

    def test_collision_rejected_at_link(self, mk):
        orders = mk(id=[1, 2], pid=[10, 20], prod_name=["x", "y"])
        products = mk(pid=[10, 20], name=["A", "B"])
        with pytest.raises(ValueError, match="prod_name.*already exist|already exist.*prod_name"):
            orders.link(products, on=lambda o, p: o.pid == p.pid, as_="prod")

    def test_no_collision_still_works(self, mk):
        orders = mk(id=[1, 2], pid=[10, 20])
        products = mk(pid=[10, 20], name=["A", "B"])
        linked = orders.link(products, on=lambda o, p: o.pid == p.pid, as_="prod")
        assert "prod_name" in linked._schema


class TestChainedLink:
    """A second link must build on the first join's real plan (not a stitched
    preview): its condition can reference the first alias's columns and the
    first link's values must survive into the final result."""

    def _chain(self, mk):
        orders = mk(id=[1, 2], pid=[10, 20])
        products = mk(pid=[10, 20], sid=[100, 200], pname=["A", "B"])
        suppliers = mk(sid=[100, 200], sname=["X", "Y"])
        return (
            orders.link(products, on=lambda o, p: o.pid == p.pid, as_="prod")
            .link(suppliers, on=lambda r, s: r.prod_sid == s.sid, as_="sup")
        )

    def test_chain_schema_has_all_three_tables(self, mk):
        chained = self._chain(mk)
        for col in ("id", "prod_pname", "prod_sid", "sup_sname"):
            assert col in chained._schema, col

    def test_chain_values_from_first_link_survive(self, mk):
        df = self._chain(mk).to_ltseq().to_pandas().sort_values("id")
        assert df["prod_pname"].tolist() == ["A", "B"]
        assert df["sup_sname"].tolist() == ["X", "Y"]

    def test_chain_row_count(self, mk):
        assert len(self._chain(mk).to_ltseq()) == 2

    def test_chain_left_join_keeps_unmatched_through_second_link(self, mk):
        orders = mk(id=[1, 2], pid=[10, 99])
        products = mk(pid=[10, 20], sid=[100, 200], pname=["A", "B"])
        suppliers = mk(sid=[100, 200], sname=["X", "Y"])
        chained = orders.link(
            products, on=lambda o, p: o.pid == p.pid, as_="prod", join_type="left"
        ).link(
            suppliers, on=lambda r, s: r.prod_sid == s.sid, as_="sup", join_type="left"
        )
        # id=2 has no product and no supplier, but left joins keep it.
        assert len(chained.to_ltseq()) == 2

    def test_chained_link_accepts_alias_keyword(self, mk):
        # LinkedTable.link() must accept alias= like LTSeq.link() does.
        orders = mk(id=[1, 2], pid=[10, 20])
        products = mk(pid=[10, 20], sid=[100, 200], pname=["A", "B"])
        suppliers = mk(sid=[100, 200], sname=["X", "Y"])
        chained = orders.link(
            products, on=lambda o, p: o.pid == p.pid, alias="prod"
        ).link(suppliers, on=lambda r, s: r.prod_sid == s.sid, alias="sup")
        assert "sup_sname" in chained._schema
        assert len(chained.to_ltseq()) == 2

    def test_chained_link_rejects_both_as_and_alias(self, mk):
        orders = mk(id=[1], pid=[10])
        products = mk(pid=[10], sid=[100])
        suppliers = mk(sid=[100], sname=["X"])
        base = orders.link(products, on=lambda o, p: o.pid == p.pid, as_="prod")
        with pytest.raises(ValueError, match="either as_ or alias"):
            base.link(
                suppliers, on=lambda r, s: r.prod_sid == s.sid, as_="sup", alias="sup"
            )
