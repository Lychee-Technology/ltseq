"""Tables of the #386 measurements: ``results/*.json`` -> ``results/cells.md``.

Per cell: the median of the timed runs with their spread, the peak resident
growth, where the CPU went (from DataFusion's operator metrics, the median
over the metric runs), the final aggregate's input, and the most a
slot-bound pruner could save (``ceiling.py``).

Stage CPU, as shares of the process's CPU time over a metrics run:

- ``scan``: the Parquet scan's ``time_elapsed_processing`` (decoding).
- ``partial``: ``AggregateExec`` in mode ``Partial``, ``elapsed_compute``.
- ``repart``: ``RepartitionExec``'s ``repartition_time`` (hashing and
  splitting batches).
- ``final``: ``AggregateExec`` in mode ``FinalPartitioned`` (or ``Single``
  at one partition), ``elapsed_compute``.
- ``topk``: ``SortExec(TopK)`` and the merge above it, ``elapsed_compute``.

The ceiling multiplies the share of the final aggregate and the Top-K sort
by the largest share of the final aggregate's work the pruner could skip
(the larger of the pruned group and row shares, with the partition-local
threshold of ``ceiling.simulate``'s ``dynamic`` and the best of the slot
counts tried). It is a bound on CPU saved, not a speedup: it leaves out
the pruner's own cost, and it assumes the saved CPU was on the critical
path.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import ceiling  # noqa: E402
import fixtures  # noqa: E402

SLOTS = (1, 1024, 8192)


def _q(values: list[float], q: float) -> float:
    ordered = sorted(values)
    i = (len(ordered) - 1) * q
    lo, hi = int(i), min(int(i) + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (i - lo)


def stages(run: dict) -> dict:
    """CPU seconds per stage of one metrics run, and the final aggregate's
    input and output rows."""
    out = {"scan": 0.0, "partial": 0.0, "repart": 0.0, "final": 0.0, "topk": 0.0}
    rows = {"final_in": None, "groups": None, "partial_skipped": 0}
    ops = run["operators"]
    for i, op in enumerate(ops):
        m, line, name = op["metrics"], op["line"], op["name"]
        if name == "DataSourceExec":
            out["scan"] += m.get("time_elapsed_processing", 0) / 1e9
        elif name == "AggregateExec" and "mode=Partial" in line:
            out["partial"] += m.get("elapsed_compute", 0) / 1e9
            rows["partial_skipped"] += m.get("skipped_aggregation_rows", 0)
        elif name == "AggregateExec":
            out["final"] += m.get("elapsed_compute", 0) / 1e9
            rows["groups"] = m.get("output_rows")
            child = ops[i + 1] if i + 1 < len(ops) else None
            if child is not None and child["depth"] == op["depth"] + 1:
                rows["final_in"] = child["metrics"].get("output_rows")
        elif name == "RepartitionExec":
            out["repart"] += m.get("repartition_time", 0) / 1e9
        elif name.startswith("SortExec") or name == "SortPreservingMergeExec":
            out["topk"] += m.get("elapsed_compute", 0) / 1e9
    return {"cpu": out, "rows": rows, "process_cpu": run["process_cpu_seconds"], "exec": run["exec_seconds"]}


class Counts:
    """Exact per-group counts of each query, from DuckDB, computed once."""

    def __init__(self, clickbench: Path | None = None) -> None:
        self.cache: dict = {}
        self.clickbench = clickbench

    def _source(self, source: str) -> str:
        # Results name ClickBench as benchmarks/data/hits.parquet wherever
        # it was read from (`fixtures.portable`).
        if not Path(source).exists() and self.clickbench and Path(source).name == self.clickbench.name:
            return str(self.clickbench)
        return source

    def get(self, spec: dict):
        where = ""
        if spec.get("filter") == "flag_lt_5":
            where = "WHERE flag < 5"
        elif spec.get("filter") == "nonempty_key":
            where = f"WHERE \"{spec['key']}\" <> ''"
        group_key = "key" if spec["key"] == "skey" else spec["key"]  # skey is unique per key
        cache_key = (spec["source"], group_key, where)
        if cache_key not in self.cache:
            self.cache[cache_key] = ceiling.group_counts(self._source(spec["source"]), group_key, where=where)
        return self.cache[cache_key]


def summarize(cell: dict, counts: Counts) -> dict:
    spec = cell["spec"]
    seconds = [r["seconds"] for r in cell.get("runs", [])]
    row = {
        "fixture": Path(spec["source"]).stem,
        "key": spec["key"],
        "shape": spec["shape"] + (f"+{spec['filter']}" if spec.get("filter") else ""),
        "k": spec["k"],
        "p": spec["partitions"],
        "ok": cell.get("check", {}).get("ok"),
        "error": cell.get("error"),
        "other_cores": cell.get("other_cores"),
    }
    if not seconds:
        return row
    median = statistics.median(seconds)
    row.update(
        median=median,
        p25=_q(seconds, 0.25),
        p75=_q(seconds, 0.75),
        min=min(seconds),
        max=max(seconds),
        cv=statistics.pstdev(seconds) / median if median else 0.0,
        peak_mib=cell["runs"][0].get("peak_above", 0) / (1 << 20),
    )
    analyses = [stages(a) for a in cell.get("analyze", [])]
    if analyses:
        shares = {}
        for stage in analyses[0]["cpu"]:
            shares[stage] = statistics.median(a["cpu"][stage] / a["process_cpu"] for a in analyses)
        row["shares"] = shares
        row["utilization"] = statistics.median(a["process_cpu"] / a["exec"] for a in analyses)
        row.update(analyses[0]["rows"])
    if spec["partitions"] > 1 and analyses:
        hashes, group_counts = counts.get(spec)
        best = None
        for slots in SLOTS:
            sim = ceiling.simulate(hashes, group_counts, spec["k"], spec["partitions"], slots)
            prunable = max(sim["dynamic"]["groups"], sim["dynamic"]["rows"])
            if best is None or prunable > best["prunable"]:
                best = {"prunable": prunable, "slots": slots, "sim": sim}
        row["kth_count"] = best["sim"]["kth_count"]
        row["prunable"] = best["prunable"]
        row["prunable_groups"] = best["sim"]["dynamic"]["groups"]
        row["prunable_rows"] = best["sim"]["dynamic"]["rows"]
        row["oracle_groups"] = best["sim"]["oracle"]["groups"]
        row["best_slots"] = best["slots"]
        row["ceiling"] = (row["shares"]["final"] + row["shares"]["topk"]) * best["prunable"]
    return row


def _pct(x) -> str:
    return "" if x is None else f"{100 * x:.1f}%"


def _host(host: dict) -> str:
    return (
        f"Commit `{host.get('commit')}` (dirty: {host.get('worktree_dirty')}), {host.get('cpu')}, "
        f"{host.get('cpu_count')} CPUs, {(host.get('memory_bytes') or 0) >> 30} GiB, kernel {host.get('kernel')}, "
        f"Python {host.get('python')}, pyarrow {host.get('pyarrow')}, {host.get('rustc')}. "
        f"Command: `{host.get('command')}`, started {host.get('started')}."
    )


def markdown(rows: list[dict], hosts: list[dict]) -> str:
    lines = [
        "# #386 grouped Top-K measurements",
        "",
        *[f"- {_host(host)}" for host in hosts],
        "",
        "Times are seconds over the timed runs (median, interquartile range, coefficient of variation); "
        "peak is resident growth over the first timed run. Stage columns are shares of the process's CPU "
        "time (see `report.py`); `util` is CPU seconds per wall second. `final in` is the rows the final "
        "aggregate merged, `groups` the groups it emitted. `prunable` is the largest share of the final "
        "merge a slot-bound pruner could skip with the partition-local threshold, at `slots` slots per "
        "partition; `ceiling` is the CPU share that would save, before the pruner's own cost.",
        "",
        "| fixture | key | shape | K | P | ok | median s | IQR s | cv | peak MiB | util | scan | partial | repart "
        "| final | topk | final in | groups | K-th count | prunable | slots | ceiling | other cores |",
        "|---|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        if "median" not in r:
            lines.append(f"| {r['fixture']} | {r['key']} | {r['shape']} | {r['k']} | {r['p']} | error: {r['error']} |")
            continue
        s = r.get("shares", {})
        lines.append(
            "| {fixture} | {key} | {shape} | {k} | {p} | {ok} | {median:.3f} | {p25:.3f}–{p75:.3f} | {cv:.1%} "
            "| {peak:.0f} | {util} | {scan} | {partial} | {repart} | {final} | {topk} | {fin} | {groups} | {kth} "
            "| {prunable} | {slots} | {ceiling} | {other} |".format(
                fixture=r["fixture"],
                key=r["key"],
                shape=r["shape"],
                k=r["k"],
                p=r["p"],
                ok="yes" if r["ok"] else "**NO**",
                median=r["median"],
                p25=r["p25"],
                p75=r["p75"],
                cv=r["cv"],
                peak=r["peak_mib"],
                util=f"{r['utilization']:.1f}" if "utilization" in r else "",
                scan=_pct(s.get("scan")),
                partial=_pct(s.get("partial")),
                repart=_pct(s.get("repart")),
                final=_pct(s.get("final")),
                topk=_pct(s.get("topk")),
                fin=f"{r['final_in']:,}" if r.get("final_in") is not None else "",
                groups=f"{r['groups']:,}" if r.get("groups") is not None else "",
                kth=f"{r['kth_count']:,}" if r.get("kth_count") is not None else "",
                prunable=_pct(r.get("prunable")),
                slots=r.get("best_slots", ""),
                ceiling=_pct(r.get("ceiling")),
                other=f"{r['other_cores']:.2f}" if r.get("other_cores") is not None else "",
            )
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--cells",
        type=Path,
        nargs="+",
        default=[HERE / "results" / "cells.json", HERE / "results" / "clickbench.json"],
        help="run.py outputs; a missing one is skipped",
    )
    parser.add_argument(
        "--clickbench",
        type=Path,
        default=HERE.parent / "data" / "hits.parquet",
        help="where to read hits.parquet for the simulation, if not in benchmarks/data/",
    )
    parser.add_argument("--out", type=Path, default=HERE / "results" / "cells.md")
    parser.add_argument("--summary", type=Path, default=HERE / "results" / "summary.json")
    parser.add_argument("--plans", type=Path, default=HERE / "results" / "plans")
    args = parser.parse_args()
    runs = [json.loads(path.read_text()) for path in args.cells if path.exists()]
    data = {"hosts": [run["host"] for run in runs], "cells": [c for run in runs for c in run["cells"]]}
    counts = Counts(args.clickbench)
    rows = [summarize(cell, counts) for cell in data["cells"]]
    args.out.write_text(markdown(rows, data["hosts"]))
    args.summary.write_text(fixtures.portable(json.dumps({"hosts": data["hosts"], "rows": rows}, indent=1)))
    args.plans.mkdir(parents=True, exist_ok=True)
    for cell in data["cells"]:
        spec = cell["spec"]
        if not cell.get("analyze") or not fixtures.representative(spec):
            continue
        name = "{}-{}-{}{}-k{}-p{}.txt".format(
            Path(spec["source"]).stem,
            spec["key"],
            spec["shape"],
            f"-{spec['filter']}" if spec.get("filter") else "",
            spec["k"],
            spec["partitions"],
        )
        (args.plans / name).write_text(fixtures.portable(cell["analyze"][0]["plan"]))
    print(f"wrote {args.out}, {args.summary} and {len(list(args.plans.iterdir()))} plans")


if __name__ == "__main__":
    main()
