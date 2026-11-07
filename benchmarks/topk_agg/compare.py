"""Baseline vs the slot-bound pruning spike, cell by cell (#386, Phase 3).

``python benchmarks/topk_agg/compare.py`` needs a ``topk_research`` build
and the fixtures; it writes ``results/compare.json`` and, with
``--report``, ``results/compare.md``.

Per cell, one process times every mode interleaved (``child.py``'s
``compare``): mode 0 is today's plan, N is the spike with N slots per
final partition. Each mode's peak is a cold single run in a process of its
own. A negative mode -N is the control: the spike's operator with N slots
that buffers, slots and merges in waves but prunes nothing, so the gap
between -N and N is what pruning buys and the gap between 0 and -N is what
the restructuring alone does. The table reports, per mode, the median and spread of the timed runs,
the change of the median against mode 0, the peak, the final stage's
share of process CPU (the final ``AggregateExec`` or the
``SlotPrunedCountExec`` that replaces it), and the share of the final
stage's input rows the spike merged.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.append(str(HERE.parent / "m0"))  # after HERE: both have run.py

import fixtures  # noqa: E402
from harness import host_info  # noqa: E402
from run import CLICKBENCH, CONTENDED, run_cell  # noqa: E402

SPIKE = 8192


def cells(clickbench: Path, repeat: int) -> list[dict]:
    out = []

    def cell(source, key, k=10, partitions=8, predicate=None, modes=(0, SPIKE, -SPIKE)):
        out.append(
            {
                "group": "compare",
                "source": str(source),
                "key": key,
                "shape": "count_star",
                "k": k,
                "partitions": partitions,
                "filter": predicate,
                "repeat": repeat,
                "modes": list(modes),
            }
        )

    for distribution in fixtures.DISTRIBUTIONS:
        for cardinality in fixtures.CARDINALITIES:
            for key in ("key", "skey"):
                cell(fixtures.path(distribution, cardinality), key)
    for name in ("zipf_1000000", "uniform_1000000"):
        source = fixtures.DATA / f"{name}.parquet"
        for key in ("key", "skey"):
            cell(source, key, k=1000)
        cell(source, "key", partitions=32)
    if clickbench.exists():
        cpus = os.cpu_count() or 1
        for k in (10, 1000):
            for key, predicate in (("UserID", None), ("URL", None), ("SearchPhrase", "nonempty_key")):
                cell(clickbench, key, k=k, partitions=cpus, predicate=predicate, modes=(0, 1024, SPIKE, -SPIKE))
    return out


def _median_share(analyses: list[dict], names: tuple[str, ...]) -> float | None:
    shares = []
    for analysis in analyses:
        cpu = 0.0
        for op in analysis["operators"]:
            if op["name"] in names and "mode=Partial" not in op["line"]:
                cpu += op["metrics"].get("elapsed_compute", 0) / 1e9
        shares.append(cpu / analysis["process_cpu_seconds"])
    return statistics.median(shares) if shares else None


def _merged(analyses: list[dict]) -> float | None:
    for analysis in analyses:
        ops = [op["metrics"] for op in analysis["operators"] if op["name"] == "SlotPrunedCountExec"]
        if ops:
            rows_in = sum(m.get("rows_in", 0) for m in ops)
            return sum(m.get("rows_merged", 0) for m in ops) / rows_in if rows_in else None
    return None


def summarize(result: dict, peaks: dict) -> list[dict]:
    spec = result["spec"]
    rows = []
    base = None
    for mode, data in result.get("modes", {}).items():
        seconds = [r["seconds"] for r in data["runs"]]
        median = statistics.median(seconds)
        ordered = sorted(seconds)
        if mode == "0":
            base = median
        rows.append(
            {
                "fixture": Path(spec["source"]).stem,
                "key": spec["key"],
                "filter": spec.get("filter"),
                "k": spec["k"],
                "p": spec["partitions"],
                "mode": int(mode),
                "median": median,
                "p25": ordered[len(ordered) // 4],
                "p75": ordered[(3 * len(ordered)) // 4],
                "cv": statistics.pstdev(seconds) / median,
                "delta": None if base is None else median / base - 1,
                "peak_mib": peaks.get(mode),
                "final_share": _median_share(data["analyze"], ("AggregateExec", "SlotPrunedCountExec")),
                "merged": _merged(data["analyze"]),
                "ok": data.get("check", {}).get("ok"),
                "other_cores": result.get("other_cores"),
            }
        )
    return rows


def markdown(rows: list[dict], host: dict) -> str:
    lines = [
        "# #386 baseline vs slot-bound pruning spike",
        "",
        f"Commit `{host.get('commit')}` (dirty: {host.get('worktree_dirty')}), {host.get('cpu')}, "
        f"{host.get('cpu_count')} CPUs, {(host.get('memory_bytes') or 0) >> 30} GiB, {host.get('rustc')}, "
        f"command `{host.get('command')}`, started {host.get('started')}.",
        "",
        "`slots` 0 is today's plan, N the spike with N slots per partition, -N its control (the same "
        "operator merging every slot). Times are seconds over the interleaved timed runs; `Δ` is the change of "
        "the median against `slots` 0 (negative is faster). `peak` is the resident growth of a cold single "
        "run in its own process. `final` is the final stage's share of process CPU, `merged` the share of the "
        "final stage's input rows the spike merged.",
        "",
        "| fixture | key | filter | K | P | slots | ok | median s | IQR s | cv | Δ | peak MiB | final | merged | other cores |",
        "|---|---|---|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        lines.append(
            "| {fixture} | {key} | {filter} | {k} | {p} | {mode} | {ok} | {median:.3f} | {p25:.3f}–{p75:.3f} "
            "| {cv:.1%} | {delta} | {peak} | {final} | {merged} | {other} |".format(
                fixture=r["fixture"],
                key=r["key"],
                filter=r["filter"] or "",
                k=r["k"],
                p=r["p"],
                mode=r["mode"],
                median=r["median"],
                p25=r["p25"],
                p75=r["p75"],
                cv=r["cv"],
                ok="yes" if r["ok"] else "**NO**",
                delta="" if r["mode"] == 0 or r["delta"] is None else f"{r['delta']:+.1%}",
                peak="" if r["peak_mib"] is None else f"{r['peak_mib']:.0f}",
                final="" if r["final_share"] is None else f"{r['final_share']:.1%}",
                merged="" if r["merged"] is None else f"{r['merged']:.1%}",
                other="" if r["other_cores"] is None else f"{r['other_cores']:.2f}",
            )
        )
    return "\n".join(lines) + "\n"


def report(path: Path, out: Path) -> None:
    data = json.loads(path.read_text())
    rows = [row for cell in data["cells"] for row in summarize(cell["timing"], cell["peaks"])]
    out.write_text(markdown(rows, data["host"]))
    (out.with_suffix(".summary.json")).write_text(json.dumps(rows, indent=1))
    plans = out.parent / "plans"
    plans.mkdir(exist_ok=True)
    for cell in data["cells"]:
        spec = cell["timing"]["spec"]
        if not fixtures.representative(spec):
            continue
        for mode, mode_data in cell["timing"].get("modes", {}).items():
            if mode == "0" or not mode_data["analyze"]:
                continue
            name = "{}-{}-{}{}-k{}-p{}.txt".format(
                f"spike{mode}" if int(mode) > 0 else f"control{-int(mode)}",
                Path(spec["source"]).stem,
                spec["key"],
                f"-{spec['filter']}" if spec.get("filter") else "",
                spec["k"],
                spec["partitions"],
            )
            (plans / name).write_text(fixtures.portable(mode_data["analyze"][0]["plan"]))
    print(f"wrote {out}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--clickbench", type=Path, default=CLICKBENCH)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=3600)
    parser.add_argument("--only", help="substring a cell's fixture name must contain")
    parser.add_argument("--out", type=Path, default=HERE / "results" / "compare.json")
    parser.add_argument("--report", action="store_true", help="only write compare.md from --out")
    args = parser.parse_args()
    if args.report:
        return report(args.out, args.out.with_suffix(".md"))

    todo = [c for c in cells(args.clickbench, args.repeat) if not args.only or args.only in c["source"]]
    args.out.parent.mkdir(parents=True, exist_ok=True)
    data = {"host": host_info(), "cells": []}
    for i, spec in enumerate(todo, 1):
        timing = None
        for _ in range(args.retries + 1):
            result = run_cell(spec, args.timeout)
            if timing is None or result.get("other_cores", 0) < timing.get("other_cores", 0):
                timing = result
            if result.get("other_cores", 0) <= CONTENDED or "error" in result:
                break
        peaks = {}
        for mode in spec["modes"]:
            cold = {**spec, "modes": [mode], "repeat": 1, "warmup": False, "analyze": False, "check": False}
            result = run_cell(cold, args.timeout)
            runs = result.get("modes", {}).get(str(mode), {}).get("runs", [])
            if runs and "peak_above" in runs[0]:
                peaks[str(mode)] = runs[0]["peak_above"] / (1 << 20)
        data["cells"].append({"timing": timing, "peaks": peaks})
        parts = []
        for r in summarize(timing, peaks) if "modes" in timing else []:
            delta = "" if r["mode"] == 0 or r["delta"] is None else f" {r['delta']:+.1%}"
            parts.append(f"slots={r['mode']} {r['median']:.3f}s{delta} ok={r['ok']}")
        line = "; ".join(parts)
        print(f"[{i}/{len(todo)}] {Path(spec['source']).stem} {spec['key']} k={spec['k']} p={spec['partitions']}: "
              f"{line or timing.get('error', '')[:300]}", flush=True)
        args.out.write_text(fixtures.slim(json.dumps(data, indent=1)))
    report(args.out, args.out.with_suffix(".md"))


if __name__ == "__main__":
    main()
