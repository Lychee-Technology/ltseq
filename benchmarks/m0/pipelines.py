"""The pipelines and terminals the #251 measurements time.

A pipeline maps the table a cell's input builds to the table a terminal
drains. Pipelines use only public LTSeq calls, so the same names can be
timed again once the gated items replace the prototypes (M30 compares
against these baselines).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

from ltseq import LTSeq
from ltseq.expr.base import corr, covar, power

Pipeline = Callable[[LTSeq], LTSeq]

# A ClickBench user with 659 events, which lie in one row group of
# hits_sorted.parquet (m4b, m5).
USER = 1773273755792258443


# m1: checked integer arithmetic (M5). bench_core in memory: `id` is the row
# number and `value` is in [1, 1000], so nothing overflows.
M1: dict[str, Pipeline] = {
    "m1.add": lambda t: t.derive(x=lambda r: r.id + r.value),
    "m1.sub": lambda t: t.derive(x=lambda r: r.id - r.value),
    "m1.mul": lambda t: t.derive(x=lambda r: r.id * r.value),
    "m1.floordiv": lambda t: t.derive(x=lambda r: r.id // r.value),
    "m1.mod": lambda t: t.derive(x=lambda r: r.id % r.value),
    "m1.abs": lambda t: t.derive(x=lambda r: abs(r.id - 500_000)),
    # v0.4 has no integer `**`: `power` computes in float64. The prototype
    # computes the integer power, checked, and casts back to float64.
    "m1.pow": lambda t: t.derive(x=lambda r: power(r.value, 3)),
    "m1.expr": lambda t: t.derive(x=lambda r: r.id * 3 + r.value * r.value - r.id // 7),
    "m1.sum": lambda t: t.agg(s=lambda g: g.value.sum()),
    "m1.sum_by_category": lambda t: t.group_by("category").agg(s=lambda g: g.value.sum()),
    "m1.sum_by_id_mod": lambda t: t.agg(by=lambda r: r.id % 100_000, s=lambda g: g.value.sum()),
    "m1.cum_sum": lambda t: t.derive(c=lambda r: r.value.cum_sum()),
}


# m2: exact float statistics (M13, M4). bench_core in memory: `amount` is
# round(uniform(0, 10000), 2).
STATS = {
    "s": lambda g: g.amount.sum(),
    "m": lambda g: g.amount.mean(),
    "v": lambda g: g.amount.var(),
    "sd": lambda g: g.amount.std(),
}


M2: dict[str, Pipeline] = {
    "m2.sum": lambda t: t.agg(s=lambda g: g.amount.sum()),
    "m2.mean": lambda t: t.agg(m=lambda g: g.amount.mean()),
    "m2.var": lambda t: t.agg(v=lambda g: g.amount.var()),
    "m2.std": lambda t: t.agg(sd=lambda g: g.amount.std()),
    "m2.cov": lambda t: t.agg(c=lambda g: covar(g.amount, g.value)),
    "m2.corr": lambda t: t.agg(c=lambda g: corr(g.amount, g.value)),
    "m2.stats_by_category": lambda t: t.group_by("category").agg(**STATS),
    "m2.stats_by_id_mod": lambda t: t.agg(by=lambda r: r.id % 100_000, **STATS),
    "m2.sum_by_id_mod": lambda t: t.agg(by=lambda r: r.id % 100_000, s=lambda g: g.amount.sum()),
    "m2.rolling_sum": lambda t: t.derive(x=lambda r: r.amount.rolling(20).sum()),
    "m2.rolling_mean": lambda t: t.derive(x=lambda r: r.amount.rolling(20).mean()),
    "m2.rolling_std": lambda t: t.derive(x=lambda r: r.amount.rolling(20).std()),
    "m2.rolling_mean_1000": lambda t: t.derive(x=lambda r: r.amount.rolling(1000).mean()),
    "m2.cum_sum": lambda t: t.derive(x=lambda r: r.amount.cum_sum()),
}


# m3: the order-preserving merge (M1). The R3 input's MUST pipeline
# (py-ltseq/tests/memory/catalog.py), bench_core's chain query from CSV, and
# a projection of ClickBench.
M3: dict[str, Pipeline] = {
    "m3.r3_filter_select_derive": lambda t: (
        t.filter(lambda r: r.id % 4 != 0).select("id", "v").derive(x=lambda r: r.v * 2)
    ),
    "m3.csv_chain": lambda t: (
        t.filter(lambda r: r.value > 100)
        .derive(doubled=lambda r: r.value * 2)
        .select("id", "value", "doubled", "category")
    ),
    "m3.hits_scan": lambda t: t.select(
        "watchid", "eventtime", "userid", "counterid", "regionid", "url", "title"
    ),
    "m3.hits_filter": lambda t: t.filter(lambda r: r.counterid == 62).select(
        "watchid", "eventtime", "userid", "url"
    ),
}


# m4: masked `&` and `|` (M16). `g > 0` is the guard, `a1 * a2 > 0` and
# `b1 * b2 > 0` the fallible operands (inputs.zones). The `|` forms negate
# each comparison, so the fallible operand is demanded on the same rows.
M4: dict[str, Pipeline] = {
    "m4.guard_and_fallible": lambda t: t.filter(lambda r: (r.g > 0) & (r.a1 * r.a2 > 0)),
    "m4.fallible_and_guard": lambda t: t.filter(lambda r: (r.a1 * r.a2 > 0) & (r.g > 0)),
    "m4.fallible_and_fallible": lambda t: t.filter(
        lambda r: (r.a1 * r.a2 > 0) & (r.b1 * r.b2 > 0)
    ),
    "m4.guard_or_fallible": lambda t: t.filter(lambda r: (r.g == 0) | (r.a1 * r.a2 <= 0)),
    "m4.fallible_or_guard": lambda t: t.filter(lambda r: (r.a1 * r.a2 <= 0) | (r.g == 0)),
    "m4.fallible_or_fallible": lambda t: t.filter(
        lambda r: (r.a1 * r.a2 <= 0) | (r.b1 * r.b2 <= 0)
    ),
}


# m4b: a filter above a sort, aggregate or distinct whose key or argument is
# a derived integer (D10a). DataFusion pushes each filter below the operator
# today, and into the scan. ClickBench sample, except M4B_SORTED.
M4B: dict[str, Pipeline] = {
    "m4b.sort_then_filter": lambda t: (
        t.derive(k=lambda r: r.counterid + 1)
        .sort("k", "watchid")
        .filter(lambda r: r.regionid == 229)
        .select("watchid", "k", "regionid", "url")
    ),
    "m4b.group_then_filter": lambda t: (
        t.derive(k=lambda r: r.regionid + 1)
        .group_by("k")
        .agg(n=lambda g: g.watchid.count(), s=lambda g: g.resolutionwidth.sum())
        .filter(lambda r: r.k == 230)
    ),
    "m4b.distinct_then_filter": lambda t: (
        t.derive(k=lambda r: r.counterid + 1)
        .select("k", "regionid", "searchengineid")
        .distinct()
        .filter(lambda r: r.regionid == 229)
    ),
    # hits_sorted: USER's events lie in one of 814 row groups, so
    # today the filter on the group key reaches the scan and statistics skip
    # the rest; the argument `k` is demanded on every row under D10a.
    "m4b.user_totals": lambda t: (
        t.derive(k=lambda r: r.counterid + 1)
        .group_by("userid")
        .agg(n=lambda g: g.watchid.count(), s=lambda g: g.k.sum())
        .filter(lambda r: r.userid == USER)
    ),
}
M4B_SORTED = {"m4b.user_totals"}


# m5: `assume_sorted` validated on every row (M3). hits_sorted.parquet,
# declared sorted by (userid, eventtime, watchid) in the input spec.
M5: dict[str, Pipeline] = {
    # USER's events: pruning reads one or two row groups.
    "m5.user_events": lambda t: t.filter(lambda r: r.userid == USER).select(
        "userid", "eventtime", "watchid", "url"
    ),
    # A predicate statistics cannot prune: every row group is read anyway.
    "m5.counter_events": lambda t: t.filter(lambda r: r.counterid == 62).select(
        "userid", "eventtime", "watchid"
    ),
    "m5.keys": lambda t: t.select("userid", "eventtime", "watchid"),
}


# m6: `string_view` normalized to `string` (M18). ClickBench sample.
# Every string column of the ClickBench files prepare_data.py writes.
HITS_STRINGS = (
    "title", "url", "referer", "flashminor2", "useragentminor", "mobilephonemodel",
    "params", "searchphrase", "pagecharset", "originalurl", "hitcolor", "browserlanguage",
    "browsercountry", "socialnetwork", "socialaction", "socialsourcepage", "paramorderid",
    "paramcurrency", "openstatservicename", "openstatcampaignid", "openstatadid",
    "openstatsourceid", "utmsource", "utmmedium", "utmcampaign", "utmcontent", "utmterm",
    "fromtag",
)

M6: dict[str, Pipeline] = {
    "m6.strings": lambda t: t.select(*HITS_STRINGS),
    "m6.url_title": lambda t: t.select("url", "title"),
    "m6.all_columns": lambda t: t,
    "m6.filter_contains": lambda t: t.filter(lambda r: r.url.s.contains("google")).select(
        "watchid", "url"
    ),
    "m6.group_by_url": lambda t: t.group_by("url").agg(n=lambda g: g.watchid.count()),
}


PIPELINES: dict[str, Pipeline] = {**M1, **M2, **M3, **M4, **M4B, **M5, **M6}


@dataclass
class Outcome:
    rows: int | None
    # perf_counter() when the first row arrived, for streaming terminals.
    first_row: float | None = None


def _stream(table: LTSeq, out_dir: Path) -> Outcome:
    # v0.4 has no `to_batches` (M1); its C stream is what `to_batches` will
    # return (contract §15.1). Each batch is dropped once read.
    rows = 0
    first = None
    reader = pa.RecordBatchReader.from_stream(table)
    for batch in reader:
        if first is None and batch.num_rows:
            first = time.perf_counter()
        rows += batch.num_rows
    return Outcome(rows, first)


def _to_arrow(table: LTSeq, out_dir: Path) -> Outcome:
    return Outcome(table.to_arrow().num_rows)


def _write_parquet(table: LTSeq, out_dir: Path) -> Outcome:
    path = out_dir / "out.parquet"
    table.write_parquet(str(path))
    return Outcome(None)


def _count(table: LTSeq, out_dir: Path) -> Outcome:
    return Outcome(table.count())


TERMINALS = {
    "stream": _stream,
    "to_arrow": _to_arrow,
    "write_parquet": _write_parquet,
    "count": _count,
}


def run_terminal(terminal: str, table: LTSeq, out_dir: Path) -> Outcome:
    return TERMINALS[terminal](table, out_dir)
