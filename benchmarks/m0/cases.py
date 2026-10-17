"""The cells of each #251 measurement.

Every measurement crosses its prototype's modes with the default and three
fixed partition counts (1, 2, 4 and the CPU count), as the issue asks.
Where crossing every size, share and partition count would multiply into
thousands of cells, the full cross runs at the largest input and the other
axes are varied around it; each function says which.
"""

from __future__ import annotations

import os

import inputs
import pipelines

CPUS = os.cpu_count() or 1
PARTITIONS = sorted({1, 2, 4, CPUS})
# m1 and m2 add 10M rows to bench_core's sizes: at 1M rows an in-memory
# kernel takes about a millisecond, and the fixed cost of building and
# planning a query (about 0.3 ms) dilutes the kernel's ratio.
KERNEL_SIZES = (*inputs.SIZES, 10_000_000)

# What each measurement prices and which impact-map items wait on it.
MEASUREMENTS = {
    "m1": {"title": "Checked integer arithmetic", "gates": "M5, #221"},
    "m2": {"title": "Exact float accumulation", "gates": "M13, M4; OD-W (#276)"},
    "m3": {"title": "Order-preserving merge", "gates": "M1, #148"},
    "m4": {"title": "Masked evaluation of & and |", "gates": "M16, #252"},
    "m4b": {"title": "Filter pushdown lost past sort, aggregate and distinct (D10a)", "gates": "M16"},
    "m5": {"title": "assume_sorted validation without pruning (D7)", "gates": "M3"},
    "m6": {"title": "string_view normalized to string", "gates": "M18"},
}


def _windowed(name: str) -> bool:
    return "cum_sum" in name or "rolling" in name


def _memory(rows: int, name: str) -> dict:
    """bench_core in memory; window pipelines get its `id` order declared
    (the CSV is written in `id` order), outside the timed region."""
    return inputs.memory_csv(rows, sorted_by=("id",) if _windowed(name) else ())


def _cell(measurement, pipeline, terminal, source, warmup, partitions, flags, repeat=5, **labels):
    return {
        "measurement": measurement,
        "pipeline": pipeline,
        "terminal": terminal,
        "input": source,
        "warmup": warmup,
        "partitions": partitions,
        "flags": flags,
        "repeat": repeat,
        "labels": labels,
    }


def m1() -> list[dict]:
    """Every pipeline at every size and partition count, three modes."""
    cells = []
    for rows in KERNEL_SIZES:
        for name in pipelines.M1:
            source, warmup = _memory(rows, name), _memory(inputs.WARMUP_ROWS, name)
            for p in PARTITIONS:
                for mode in ("off", "arrow", "fast"):
                    cells.append(
                        _cell("m1", name, "stream", source, warmup, p, {"checked": mode}, rows=rows, mode=mode)
                    )
    return cells


def m2() -> list[dict]:
    """Every pipeline at every size and partition count, three modes."""
    cells = []
    for rows in KERNEL_SIZES:
        for name in pipelines.M2:
            source, warmup = _memory(rows, name), _memory(inputs.WARMUP_ROWS, name)
            for p in PARTITIONS:
                for mode in ("off", "exact", "compensated"):
                    cells.append(
                        _cell("m2", name, "stream", source, warmup, p, {"exact": mode}, rows=rows, mode=mode)
                    )
    return cells


def m3() -> list[dict]:
    """The R3 input at the partition counts of #251's baseline comment
    (1, 2, 4, 12, 24), ClickBench's sample and bench_core's 1M CSV; a slow
    first partition at 4 and 12 partitions for time to first row."""
    cells = []
    r3, r3_warmup = inputs.r3(), inputs.r3_warmup()
    for terminal in ("stream", "count", "write_parquet"):
        for p in sorted({*PARTITIONS, 2 * CPUS}):
            # `count` needs no order, so no merge applies to it, and the
            # row-group split would only change its parallelism; its cells
            # are today's, for #251's baseline comment.
            modes = ("off",) if terminal == "count" else ("off", "concat", "interleaved")
            for mode in modes:
                cells.append(
                    _cell("m3", "m3.r3_filter_select_derive", terminal, r3, r3_warmup, p,
                          {"order": mode}, repeat=3, input_name="r3", mode=mode, slow_ms=0)
                )
    for p in (4, CPUS):
        for mode in ("off", "concat", "interleaved"):
            cells.append(
                _cell("m3", "m3.r3_filter_select_derive", "stream", r3, r3_warmup, p,
                      {"order": mode, "slow_ms": 20}, repeat=3, input_name="r3", mode=mode, slow_ms=20)
            )
    hits, hits_warmup = inputs.clickbench("hits_sample.parquet"), inputs.clickbench_warmup("hits_sample.parquet")
    for name in ("m3.hits_scan", "m3.hits_filter"):
        for p in PARTITIONS:
            for mode in ("off", "concat", "interleaved"):
                cells.append(
                    _cell("m3", name, "stream", hits, hits_warmup, p, {"order": mode},
                          input_name="hits_sample", mode=mode, slow_ms=0)
                )
    # A CSV scan has no row groups to split, so only the contiguous merge.
    csv, csv_warmup = inputs.scan_csv(1_000_000), inputs.scan_csv(inputs.WARMUP_ROWS)
    for p in PARTITIONS:
        for mode in ("off", "concat"):
            cells.append(
                _cell("m3", "m3.csv_chain", "stream", csv, csv_warmup, p, {"order": mode},
                      input_name="bench_core_csv_1m", mode=mode, slow_ms=0)
            )
    return cells


M4_MODES = {
    # Today without checks, today's evaluation of checked operands, and the
    # three prototypes (all with checked arithmetic, so operands can fail).
    "today": {"checked": "off"},
    "today_checked": {"checked": "fast"},
    "select": {"checked": "fast", "masked": "select"},
    "errors": {"checked": "fast", "masked": "errors"},
    "optimistic": {"checked": "fast", "masked": "optimistic"},
}


def m4() -> list[dict]:
    """1M rows at the default partition count for every guard share and
    pipeline, with and without failing rows; partition scaling and the
    smaller sizes at a 50% share without failures."""
    cells = []
    warmup = inputs.zones_spec(inputs.WARMUP_ROWS, 0.5, False)
    for share in (0.01, 0.20, 0.50, 0.99):
        for failing in (False, True):
            source = inputs.zones_spec(1_000_000, share, failing)
            for name in pipelines.M4:
                for mode, flags in M4_MODES.items():
                    cells.append(
                        _cell("m4", name, "stream", source, warmup, CPUS, flags,
                              rows=1_000_000, share=share, failing=failing, mode=mode)
                    )
    for p in PARTITIONS:
        if p == CPUS:
            continue
        source = inputs.zones_spec(1_000_000, 0.5, False)
        for name in ("m4.guard_and_fallible", "m4.fallible_and_guard", "m4.fallible_and_fallible"):
            for mode, flags in M4_MODES.items():
                cells.append(
                    _cell("m4", name, "stream", source, warmup, p, flags,
                          rows=1_000_000, share=0.5, failing=False, mode=mode)
                )
    for rows in inputs.SIZES[:-1]:
        source = inputs.zones_spec(rows, 0.5, False)
        for name in pipelines.M4:
            for mode, flags in M4_MODES.items():
                cells.append(
                    _cell("m4", name, "stream", source, warmup, CPUS, flags,
                          rows=rows, share=0.5, failing=False, mode=mode)
                )
    return cells


M4B_MODES = {"pushdown": "off", "operator": "operator", "keys": "keys"}


def m4b() -> list[dict]:
    """Today's pushdown and the two barrier placements, with checked
    arithmetic on in all three so only the pushdown differs: ClickBench's
    sample, and hits_sorted for a filter that statistics prune."""
    cells = []
    for name in pipelines.M4B:
        file = "hits_sorted.parquet" if name in pipelines.M4B_SORTED else "hits_sample.parquet"
        source, warmup = inputs.clickbench(file), inputs.clickbench_warmup(file)
        for p in PARTITIONS:
            for mode, restrict in M4B_MODES.items():
                cells.append(
                    _cell("m4b", name, "stream", source, warmup, p,
                          {"checked": "fast", "restrict_pushdown": restrict},
                          repeat=2 if file == "hits_sorted.parquet" else 5,
                          input_name=file.removesuffix(".parquet"), mode=mode)
                )
    return cells


def m5() -> list[dict]:
    """hits_sorted.parquet (100M rows) declared sorted by its keys."""
    cells = []
    keys = ("userid", "eventtime", "watchid")
    source = inputs.clickbench("hits_sorted.parquet", sorted_by=keys)
    warmup = inputs.clickbench_warmup("hits_sorted.parquet", sorted_by=keys)
    for name in pipelines.M5:
        for p in PARTITIONS:
            for mode in ("off", "prune_off", "validate"):
                cells.append(
                    _cell("m5", name, "stream", source, warmup, p, {"validate": mode},
                          repeat=2, input_name="hits_sorted", mode=mode)
                )
    return cells


def m6() -> list[dict]:
    """ClickBench's sample: every pipeline, terminal and partition count."""
    cells = []
    hits, warmup = inputs.clickbench("hits_sample.parquet"), inputs.clickbench_warmup("hits_sample.parquet")
    for name in pipelines.M6:
        for terminal in ("stream", "to_arrow"):
            for p in PARTITIONS:
                for mode in ("off", "plain", "cast"):
                    cells.append(
                        _cell("m6", name, terminal, hits, warmup, p, {"views": mode},
                              input_name="hits_sample", mode=mode)
                    )
    return cells


CASES = {"m1": m1, "m2": m2, "m3": m3, "m4": m4, "m4b": m4b, "m5": m5, "m6": m6}
