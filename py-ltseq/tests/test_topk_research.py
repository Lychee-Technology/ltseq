"""The #386 research build (cargo feature `topk_research`).

A default build carries neither the metrics hook nor the pruning spike. On
a measurement build:

- `_topk_analyze` executes the plan a terminal runs and returns its
  per-operator metrics, which `benchmarks/topk_agg/` attributes to stages;
- `_topk_set_prune(slots)` turns on the exact slot-bound pruner of a
  grouped COUNT Top-K, with a fixed slot count or one sized from the data
  (`rows_per_slot`); `prune=False` is the benchmarks' control, the same
  operator merging every row. These differential tests hold it to the unpruned
  answer, tie-aware, across engine and input layouts, ties, NULL and float
  keys (-0.0, NaN), COUNT(col) over NULLs, empty inputs and K above the
  group count, and check that it is planned only for the eligible shape.
"""

from __future__ import annotations

import contextlib
import random
from collections import Counter

import pyarrow as pa
import pytest

from ltseq import LTSeq, ltseq_core
from ltseq.expr.types import CallExpr

measurement_build = pytest.mark.skipif(
    not hasattr(ltseq_core, "_topk_analyze"),
    reason="needs a measurement build (--features topk_research)",
)


@contextlib.contextmanager
def pruning(slots: int, rows_per_slot: int = 0):
    previous = ltseq_core._topk_set_prune(slots, True, rows_per_slot)
    try:
        yield
    finally:
        ltseq_core._topk_set_prune(*previous)


def top(t: LTSeq, k: int, *, column: str | None = None) -> LTSeq:
    if column is None:
        grouped = t.agg(by=lambda r: r.k, cnt=lambda g: CallExpr("count"))
    else:
        grouped = t.agg(by=lambda r: r.k, cnt=lambda g: getattr(g, column).count())
    return grouped.sort("cnt", desc=True).slice(0, k)


def _norm(key):
    # The contract's key equality: -0.0 is 0.0 and NaN is NaN.
    if isinstance(key, float):
        if key != key:
            return "nan"
        return key + 0.0
    return key


def assert_exact_top_k(result: pa.Table, counts: Counter, k: int) -> None:
    """`result` is a correct top-K of `counts`, whichever tied keys it chose."""
    got = [(_norm(row["k"]), row["cnt"]) for row in result.to_pylist()]
    expected = sorted(counts.values(), reverse=True)[:k]
    assert [c for _, c in got] == expected
    assert len({key for key, _ in got}) == len(got)
    for key, c in got:
        assert counts[key] == c


def counts_of(keys: list, values: list | None = None) -> Counter:
    counts: Counter = Counter()
    for i, key in enumerate(keys):
        counts[_norm(key)] += 0
        if values is None or values[i] is not None:
            counts[_norm(key)] += 1
    return counts


def unpruned_counts(t: LTSeq, column: str | None = None) -> Counter:
    """Every group's count as the unpruned engine computes it."""
    if column is None:
        grouped = t.agg(by=lambda r: r.k, cnt=lambda g: CallExpr("count"))
    else:
        grouped = t.agg(by=lambda r: r.k, cnt=lambda g: getattr(g, column).count())
    return Counter({_norm(row["k"]): row["cnt"] for row in grouped.to_arrow().to_pylist()})


def skewed_keys(rng: random.Random, groups: int, rows: int) -> list[int]:
    # A few heavy keys, many light ones, and plenty of ties.
    return [min(int(rng.paretovariate(0.7)), groups) for _ in range(rows)]


def test_default_build_has_no_research_hooks():
    if hasattr(ltseq_core, "_topk_analyze"):
        pytest.skip("this extension is a measurement build (--features topk_research)")
    assert not hasattr(ltseq_core, "_topk_set_prune")


@measurement_build
def test_hook_reports_the_stages_of_a_grouped_top_k(engine_config):
    engine_config(target_partitions=4)
    t = LTSeq.from_arrow(pa.table({"k": [i % 7 for i in range(1000)], "v": list(range(1000))}))
    plan_ns, exec_ns, rows, text, operators = ltseq_core._topk_analyze(top(t, 3, column="v")._inner)
    assert rows == 3
    assert plan_ns >= 0 and exec_ns > 0
    assert "metrics=[" in text
    assert operators[0][0] == 0
    aggregates = [dict(m) for _, name, _, m in operators if name == "AggregateExec"]
    # The aggregate nearest the root emits one row per group.
    assert aggregates[0]["output_rows"] == 7
    assert all("elapsed_compute" in m for m in aggregates)


@measurement_build
@pytest.mark.parametrize("slots, rows_per_slot", [(1, 0), (4, 0), (64, 0), (0, 1), (0, 16)])
@pytest.mark.parametrize("k", [1, 3, 10, 500])
def test_pruned_top_k_matches_unpruned(engine_layout, slots, rows_per_slot, k):
    rng = random.Random(386 + slots * 1000 + rows_per_slot * 7 + k)
    keys = skewed_keys(rng, 300, 3000)
    t = LTSeq.from_arrow(pa.table({"k": keys}))
    with pruning(slots, rows_per_slot):
        assert_exact_top_k(top(t, k).to_arrow(), counts_of(keys), k)


@measurement_build
def test_control_merges_every_row(engine_config):
    engine_config(target_partitions=4)
    rng = random.Random(3)
    keys = [h for h in range(16) for _ in range(500)] + [rng.randrange(16, 20_000) for _ in range(20_000)]
    t = LTSeq.from_arrow(pa.table({"k": keys}))
    previous = ltseq_core._topk_set_prune(64, False)
    try:
        table = top(t, 1)
        operators = ltseq_core._topk_analyze(table._inner)[4]
        assert_exact_top_k(table.to_arrow(), counts_of(keys), 1)
    finally:
        ltseq_core._topk_set_prune(*previous)
    pruner = [dict(m) for _, name, _, m in operators if name == "SlotPrunedCountExec"]
    assert pruner
    assert sum(m["rows_merged"] for m in pruner) == sum(m["rows_in"] for m in pruner)


@measurement_build
@pytest.mark.parametrize("slots, rows_per_slot", [(16, 0), (0, 4)])
def test_pruned_top_k_in_every_input_layout(data_layout, slots, rows_per_slot):
    rng = random.Random(7)
    keys = skewed_keys(rng, 200, 2000)
    t = data_layout(pa.table({"k": pa.array(keys, pa.int64())}))
    with pruning(slots, rows_per_slot):
        for k in (1, 5, 50):
            assert_exact_top_k(top(t, k).to_arrow(), counts_of(keys), k)


@measurement_build
def test_flat_counts_and_k_at_and_above_the_group_count(engine_config):
    engine_config(target_partitions=4, batch_size=7)
    keys = [i % 40 for i in range(400)] + [0, 1, 2]  # 37 keys tied at 10
    t = LTSeq.from_arrow(pa.table({"k": keys}))
    with pruning(8):
        for k in (1, 3, 4, 39, 40, 41, 1000):
            assert_exact_top_k(top(t, k).to_arrow(), counts_of(keys), k)


@measurement_build
def test_empty_input(engine_config):
    engine_config(target_partitions=4)
    t = LTSeq.from_arrow(pa.table({"k": pa.array([], pa.int64())}))
    with pruning(8):
        assert top(t, 5).to_arrow().num_rows == 0


@measurement_build
def test_null_string_and_float_keys(engine_config):
    engine_config(target_partitions=3, batch_size=5)
    rng = random.Random(11)
    strings = [None if rng.random() < 0.1 else f"u{min(int(rng.paretovariate(0.8)), 50)}" for _ in range(800)]
    floats = [rng.choice([0.0, -0.0, float("nan"), None, 1.5, 2.5, 3.5]) for _ in range(800)]
    for keys in (strings, floats):
        t = LTSeq.from_arrow(pa.table({"k": keys}))
        reference = unpruned_counts(t)
        if keys is strings:
            assert reference == counts_of(keys)
        with pruning(4):
            for k in (1, 2, 4):
                assert_exact_top_k(top(t, k).to_arrow(), reference, k)


@measurement_build
def test_count_of_a_column_with_nulls(engine_config):
    engine_config(target_partitions=4, batch_size=16)
    rng = random.Random(5)
    keys = skewed_keys(rng, 100, 1500)
    values = [None if rng.random() < 0.4 else 1 for _ in keys]
    t = LTSeq.from_arrow(pa.table({"k": keys, "v": pa.array(values, pa.int64())}))
    with pruning(8):
        for k in (1, 5, 20):
            assert_exact_top_k(top(t, k, column="v").to_arrow(), counts_of(keys, values), k)


def _planned(t: LTSeq) -> bool:
    return "SlotPrunedCountExec" in t.explain_plan()[1]


@measurement_build
def test_planned_only_for_the_eligible_shape(engine_config):
    engine_config(target_partitions=4)
    t = LTSeq.from_arrow(pa.table({"k": [i % 9 for i in range(500)], "v": list(range(500))}))
    count = lambda g: CallExpr("count")  # noqa: E731
    with pruning(8):
        assert _planned(top(t, 3))
        # Ascending, no limit, another aggregate, two aggregates, a sort on
        # a computed column, or a computed column between the aggregate and
        # the sort (it would not be evaluated on pruned groups, and `cnt * 2`
        # is checked arithmetic): the existing plan.
        assert not _planned(
            t.agg(by=lambda r: r.k, cnt=count).derive(lambda r: {"x": r.cnt * 2}).sort("cnt", desc=True).slice(0, 3)
        )
        assert not _planned(t.agg(by=lambda r: r.k, cnt=count).sort("cnt").slice(0, 3))
        assert not _planned(t.agg(by=lambda r: r.k, cnt=count).sort("cnt", desc=True))
        assert not _planned(t.agg(by=lambda r: r.k, s=lambda g: g.v.sum()).sort("s", desc=True).slice(0, 3))
        assert not _planned(
            t.agg(by=lambda r: r.k, cnt=count, s=lambda g: g.v.sum()).sort("cnt", desc=True).slice(0, 3)
        )
        assert not _planned(
            t.agg(by=lambda r: r.k, cnt=count).derive(lambda r: {"x": r.cnt * 2}).sort("x", desc=True).slice(0, 3)
        )
    assert not _planned(top(t, 3))  # off unless turned on


@measurement_build
def test_skewed_input_skips_partial_rows(engine_config):
    engine_config(target_partitions=4)
    rng = random.Random(3)
    # Sixteen heavy keys, so every partition finalizes a high K-th count of
    # its own whatever the partitions' timing, and a long tail of light ones.
    keys = [h for h in range(16) for _ in range(500)] + [rng.randrange(16, 20_000) for _ in range(20_000)]
    rng.shuffle(keys)
    t = LTSeq.from_arrow(pa.table({"k": keys}))
    with pruning(64):
        table = top(t, 1)
        operators = ltseq_core._topk_analyze(table._inner)[4]
        assert_exact_top_k(table.to_arrow(), counts_of(keys), 1)
    pruner = [dict(m) for _, name, _, m in operators if name == "SlotPrunedCountExec"]
    assert pruner
    rows_in = sum(m["rows_in"] for m in pruner)
    rows_merged = sum(m["rows_merged"] for m in pruner)
    assert rows_merged < rows_in / 2
    assert sum(m["bailed"] for m in pruner) == 0


def _pruner_metrics(table: LTSeq) -> list[dict]:
    operators = ltseq_core._topk_analyze(table._inner)[4]
    return [dict(m) for _, name, _, m in operators if name == "SlotPrunedCountExec"]


@measurement_build
@pytest.mark.parametrize("k", [1, 10])
def test_unprunable_input_bails_out_to_one_merge(engine_config, k):
    # Every key twice, a few three times: no slot bound falls below the
    # K-th count, so every partition merges its buffered rows as they are.
    engine_config(target_partitions=4, batch_size=64)
    keys = [i for i in range(3000) for _ in range(2)] + list(range(40))
    random.Random(1).shuffle(keys)
    t = LTSeq.from_arrow(pa.table({"k": keys}))
    with pruning(0, 8):
        table = top(t, k)
        pruner = _pruner_metrics(table)
        assert_exact_top_k(table.to_arrow(), counts_of(keys), k)
    # Metrics are summed over the partitions; each holds about 1,300 rows.
    assert [m["bailed"] for m in pruner] == [4]
    assert sum(m["rows_merged"] for m in pruner) == sum(m["rows_in"] for m in pruner)


@measurement_build
def test_slot_count_follows_the_input(engine_config):
    engine_config(target_partitions=2)
    rng = random.Random(9)
    keys = [h for h in range(4) for _ in range(300)] + [rng.randrange(4, 50_000) for _ in range(8000)]
    t = LTSeq.from_arrow(pa.table({"k": keys}))
    with pruning(0, 10):
        table = top(t, 2)
        pruner = _pruner_metrics(table)
        assert_exact_top_k(table.to_arrow(), counts_of(keys), 2)
    # Summed over the two partitions: one slot per ten rows, rounded up in each.
    for m in pruner:
        assert m["rows_in"] / 10 <= m["slots"] <= m["rows_in"] / 10 + 2
    # The heavy keys reach a K-th count no light slot's bound comes near.
    assert sum(m["rows_merged"] for m in pruner) < sum(m["rows_in"] for m in pruner) / 4


@measurement_build
def test_probe_bails_out_before_buffering_everything(engine_config):
    # 400,000 keys twice each: past the probe's 131,072 rows, every slot
    # reaches T0, so each partition streams the rest into one merge.
    engine_config(target_partitions=2)
    keys = list(range(400_000)) * 2 + [7, 7, 7]
    t = LTSeq.from_arrow(pa.table({"k": keys}))
    with pruning(0, 128):
        table = top(t, 3)
        pruner = _pruner_metrics(table)
        result = table.to_arrow()
    assert result.column("cnt").to_pylist() == [5, 2, 2]
    assert result.column("k").to_pylist()[0] == 7
    assert [(m["bailed"], m["bailed_early"]) for m in pruner] == [(2, 2)]
    assert sum(m["rows_merged"] for m in pruner) == sum(m["rows_in"] for m in pruner)


@measurement_build
@pytest.mark.parametrize("slots, rows_per_slot", [(1024, 0), (0, 128)])
def test_error_of_a_derive_on_a_prunable_group_is_kept(engine_config, slots, rows_per_slot):
    # Sixteen heavy keys and 20,000 singletons, so the pruner would skip the
    # singleton slots; the derive divides by zero on singleton 20,000 only.
    # Unpruned, the derive runs on every group and raises; the pruner must
    # not hide that by never emitting the group, so it is not planned.
    engine_config(target_partitions=4, batch_size=64)
    keys = [h for h in range(16) for _ in range(500)] + list(range(16, 20_016))
    random.Random(3).shuffle(keys)
    t = LTSeq.from_arrow(pa.table({"k": keys}))

    def query():
        grouped = t.agg(by=lambda r: r.k, cnt=lambda g: CallExpr("count"))
        return grouped.derive(lambda r: {"x": 1 // (r.k - 20_000)}).sort("cnt", desc=True).slice(0, 1)

    with pytest.raises(Exception, match="(?i)divide by zero"):
        query().to_arrow()
    with pruning(slots, rows_per_slot):
        assert not _planned(query())
        with pytest.raises(Exception, match="(?i)divide by zero"):
            query().to_arrow()
        # The same input without the derive does prune.
        operators = ltseq_core._topk_analyze(top(t, 1)._inner)[4]
    pruner = [dict(m) for _, name, _, m in operators if name == "SlotPrunedCountExec"]
    assert sum(m["rows_merged"] for m in pruner) < sum(m["rows_in"] for m in pruner)
