"""Run the #386 grouped Top-K matrix, one cell per process.

``python benchmarks/topk_agg/run.py`` writes ``results/cells.json``: the
host, and per cell its spec, engine layout, timed runs, peak memory,
operator metrics (``topk_research`` builds only) and DuckDB check. Build
first with ``maturin develop --release --features topk_research`` and write
the fixtures with ``fixtures.py``.

Cells:

- ``synthetic``: every fixture x {key, skey} x COUNT(*) at partitions
  1, 8 and 32 with K=10, plus K = 1, 100 and 1000 at 8 partitions.
- ``shapes``: COUNT(col), a post-aggregate projection and a filter below
  the aggregate on zipf_1000000 and uniform_1000000, 8 partitions, K=10.
- ``clickbench``: Q16 (UserID), Q34 (URL) and a Q13-like filtered count
  (SearchPhrase <> '') on ``hits.parquet``, at K = 10 and 1000, at the CPU
  count's partitions; skipped when the file is missing.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
sys.path.insert(0, str(HERE))
sys.path.append(str(HERE.parent / "m0"))  # after HERE: both have run.py

import fixtures  # noqa: E402
from harness import _children_cpu, _cpu_seconds, host_info, wait_for_quiet  # noqa: E402

CHILD = HERE / "child.py"
CLICKBENCH = REPO / "benchmarks" / "data" / "hits.parquet"
CONTENDED = 0.5


def cells(groups: set[str], clickbench: Path, repeat: int) -> list[dict]:
    out = []

    def cell(group, source, key, shape="count_star", k=10, partitions=8, predicate=None):
        out.append(
            {
                "group": group,
                "source": str(source),
                "key": key,
                "shape": shape,
                "k": k,
                "partitions": partitions,
                "filter": predicate,
                "repeat": repeat,
            }
        )

    if "synthetic" in groups:
        for distribution in fixtures.DISTRIBUTIONS:
            for cardinality in fixtures.CARDINALITIES:
                source = fixtures.path(distribution, cardinality)
                for key in ("key", "skey"):
                    for partitions in (1, 8, 32):
                        cell("synthetic", source, key, partitions=partitions)
                    for k in (1, 100, 1000):
                        cell("synthetic", source, key, k=k)
    if "shapes" in groups:
        for name in ("zipf_1000000", "uniform_1000000"):
            source = fixtures.DATA / f"{name}.parquet"
            for key in ("key", "skey"):
                cell("shapes", source, key, shape="count_col")
                cell("shapes", source, key, shape="projected")
                cell("shapes", source, key, predicate="flag_lt_5")
    if "clickbench" in groups and clickbench.exists():
        cpus = os.cpu_count() or 1
        for k in (10, 1000):
            cell("clickbench", clickbench, "UserID", k=k, partitions=cpus)
            cell("clickbench", clickbench, "URL", k=k, partitions=cpus)
            cell("clickbench", clickbench, "SearchPhrase", k=k, partitions=cpus, predicate="nonempty_key")
    return out


def run_cell(spec: dict, timeout: float) -> dict:
    quiet = wait_for_quiet()
    cpu_before, child_before = _cpu_seconds(), _children_cpu()
    started = time.perf_counter()
    try:
        done = subprocess.run(
            [sys.executable, str(CHILD), json.dumps(spec)],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=REPO,
        )
    except subprocess.TimeoutExpired:
        return {"spec": spec, "error": f"timed out after {timeout:.0f} s"}
    lines = [line for line in done.stdout.splitlines() if line.startswith("{")]
    if done.returncode != 0 or not lines:
        return {"spec": spec, "error": f"child exited {done.returncode}: {(done.stderr or done.stdout)[-2000:]}"}
    result = json.loads(lines[-1])
    result["wall"] = time.perf_counter() - started
    result.update(quiet)
    cpu_after = _cpu_seconds()
    if cpu_before is not None and cpu_after is not None:
        others = (cpu_after[0] - cpu_before[0]) - (_children_cpu() - child_before)
        result["other_cores"] = max(others, 0.0) / result["wall"]
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--groups", default="synthetic,shapes,clickbench")
    parser.add_argument("--clickbench", type=Path, default=CLICKBENCH)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--retries", type=int, default=2, help="reruns of a contended cell")
    parser.add_argument("--timeout", type=float, default=1800)
    parser.add_argument("--out", type=Path, default=HERE / "results" / "cells.json")
    args = parser.parse_args()

    todo = cells(set(args.groups.split(",")), args.clickbench, args.repeat)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    report = {"host": host_info(), "cells": []}
    for i, spec in enumerate(todo, 1):
        best = None
        for _ in range(args.retries + 1):
            result = run_cell(spec, args.timeout)
            if best is None or result.get("other_cores", 0) < best.get("other_cores", 0):
                best = result
            if result.get("other_cores", 0) <= CONTENDED or "error" in result:
                break
        report["cells"].append(best)
        runs = [r["seconds"] for r in best.get("runs", [])]
        median = sorted(runs)[len(runs) // 2] if runs else float("nan")
        ok = best.get("check", {}).get("ok")
        print(
            f"[{i}/{len(todo)}] {Path(spec['source']).stem} {spec['key']} {spec['shape']} "
            f"k={spec['k']} p={spec['partitions']} filter={spec['filter']}: "
            f"median {median:.3f}s ok={ok} {best.get('error', '')[:200]}",
            flush=True,
        )
        args.out.write_text(fixtures.slim(json.dumps(report, indent=1)))


if __name__ == "__main__":
    main()
