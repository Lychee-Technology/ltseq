"""Run #251's measurements: ``python benchmarks/m0/run.py --only m1,m3``.

Needs the extension built with the prototypes, in release mode::

    maturin develop --release --features m0

and, for m3-m6, ClickBench's files (``benchmarks/prepare_data.py``). Each
measurement's results go to ``benchmarks/m0/results/<name>.json``, written
after every cell, so an interrupted run keeps what it measured;
``report.py`` turns them into tables.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import cases  # noqa: E402
import harness  # noqa: E402
import inputs  # noqa: E402

RESULTS = HERE / "results"


def _relative(value):
    """Input specs with paths relative to the repository, for the record."""
    if isinstance(value, dict):
        return {k: _relative(v) for k, v in value.items()}
    if isinstance(value, str) and value.startswith(str(harness.REPO)):
        return str(Path(value).relative_to(harness.REPO))
    return value


def _summary(result: dict) -> str:
    if result.get("aborted"):
        return f"ABORTED {result.get('error') or ''}"
    runs = result.get("runs") or []
    if result.get("error") and not runs:
        return f"error: {result['error'][:120]}"
    seconds = sorted(r["seconds"] for r in runs)
    text = f"{seconds[len(seconds) // 2]:.4f} s, peak {runs[0]['peak_above'] / 2**20:.1f} MiB"
    if runs[0].get("ttfr") is not None:
        text += f", ttfr {runs[0]['ttfr'] * 1000:.1f} ms"
    if result.get("error"):
        text += f" then error: {result['error'][:80]}"
    return text


def _measure(cell: dict, scratch: Path, retries: int) -> dict:
    """Run a cell; while other processes contend it, run it again, up to
    `retries` more times, and keep the least contended attempt."""
    best = None
    for attempt in range(1, retries + 2):
        if cell["input"].get("warm_columns"):
            inputs.warm_columns(Path(cell["input"]["path"]), cell["input"]["warm_columns"])
        result = harness.run_cell(cell, scratch)
        result["attempt"] = attempt
        others = result.get("other_cores") or 0.0
        if best is None or others < (best.get("other_cores") or 0.0):
            best = result
        if others <= harness.CONTENDED:
            break
    return best


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--only", default=",".join(cases.CASES), help="measurements to run (comma-separated)")
    parser.add_argument("--partitions", help="keep only these partition counts (comma-separated)")
    parser.add_argument("--pipeline", help="keep only pipelines whose name contains this")
    parser.add_argument("--repeat", type=int, help="override each cell's measured runs")
    parser.add_argument("--retries", type=int, default=3,
                        help="runs of a contended cell beyond the first (default 3)")
    parser.add_argument("--allow-debug", action="store_true", help="measure a debug build (for smoke tests)")
    parser.add_argument("--out", type=Path, help="results directory (default benchmarks/m0/results)")
    args = parser.parse_args()

    host = harness.host_info()
    if host["m0_build"].get("debug_assertions", True) and not args.allow_debug:
        print("refusing to measure: the extension is not a release build with the m0 feature;\n"
              "run `maturin develop --release --features m0` (or pass --allow-debug)", file=sys.stderr)
        return 2
    out_dir = args.out or RESULTS
    out_dir.mkdir(parents=True, exist_ok=True)
    scratch = inputs.WORK / "out"
    keep = {int(p) for p in args.partitions.split(",")} if args.partitions else None

    for name in args.only.split(","):
        cells = cases.CASES[name]()
        if keep is not None:
            cells = [c for c in cells if c["partitions"] in keep]
        if args.pipeline:
            cells = [c for c in cells if args.pipeline in c["pipeline"]]
        if args.repeat:
            for c in cells:
                c["repeat"] = args.repeat
        for path in {Path(c["input"]["path"]) for c in cells if c["input"].get("kind") == "parquet"}:
            # hits_sorted.parquet is larger than the page cache can hold;
            # its cells warm the columns they read (`warm_columns`).
            if path.is_dir() or path.stat().st_size < 4 << 30:
                inputs.warm_page_cache(path)
        record = {"host": host, "measurement": name, **cases.MEASUREMENTS[name], "cells": []}
        target = out_dir / f"{name}.json"
        started = time.perf_counter()
        for i, cell in enumerate(cells, 1):
            result = _measure(cell, scratch, args.retries)
            record["cells"].append({**_relative(cell), "result": result})
            target.write_text(json.dumps(record, indent=1))
            labels = " ".join(f"{k}={v}" for k, v in cell["labels"].items())
            print(f"[{name} {i}/{len(cells)}] {cell['pipeline']} {cell['terminal']} p={cell['partitions']} "
                  f"{labels}: {_summary(result)}", flush=True)
        print(f"{name}: {len(cells)} cells in {time.perf_counter() - started:.0f} s -> {target}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
