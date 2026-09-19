#!/usr/bin/env python3
"""
Python <-> Rust Arrow boundary benchmark (issue #143).

Measures wall time and peak RSS growth of the data-transfer paths between
pyarrow and the Rust kernel:

- ``from_arrow``: pyarrow.Table -> LTSeq
- ``to_arrow (memtable)``: LTSeq materialized in memory (``collect()``) ->
  pyarrow.Table (isolates the boundary cost: the collect only clones Arc'd batches)
- ``to_arrow (parquet)``: LTSeq lazily reading Parquet -> pyarrow.Table
- ``pa.table(t)``: the ``__arrow_c_stream__`` protocol (skipped when the
  installed build does not implement it)
- ``cursor``: iterate ``LTSeq.scan_parquet`` batch by batch

Every measurement runs in a fresh subprocess so ``ru_maxrss`` is a per-operation
high-water mark: ``peak_mb`` is the growth of the process's maximum RSS caused
by the operation itself, after the input has been built.

Run with:

    uv run python benchmarks/bench_arrow_boundary.py --target-mb 1024 --label after

Results are printed as a table and, with ``--out``, saved as JSON so two runs
(before/after a change) can be compared with ``--compare before.json``.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "py-ltseq"))

OPS = ["from_arrow", "to_arrow_memtable", "to_arrow_parquet", "pa_table", "cursor"]

# Bytes per row of the generated table (int64 + float64 + float64 + ~12-byte string).
_BYTES_PER_ROW = 8 + 8 + 8 + 12 + 4
_CHUNK_ROWS = 250_000


def rows_for_target(target_mb: int) -> int:
    return max(_CHUNK_ROWS, (target_mb * 1024 * 1024) // _BYTES_PER_ROW)


def build_table(num_rows: int):
    """Build a chunked pyarrow.Table of exactly ``num_rows`` rows.

    Chunks hold 250K rows each; the last chunk is shorter when ``num_rows``
    is not a multiple, so the reported row count matches the input.
    """
    import numpy as np
    import pyarrow as pa

    rng = np.random.default_rng(42)
    n = min(_CHUNK_ROWS, num_rows)
    cats = np.array(["alpha", "beta", "gamma", "delta", "epsilon", "zeta"])
    chunk = pa.table(
        {
            "id": pa.array(np.arange(n, dtype=np.int64)),
            "value": pa.array(rng.random(n)),
            "amount": pa.array(rng.random(n) * 1000.0),
            "category": pa.array(cats[rng.integers(0, len(cats), n)]),
        }
    )
    full, tail = divmod(num_rows, n)
    parts = [chunk] * full
    if tail:
        parts.append(chunk.slice(0, tail))
    return pa.concat_tables(parts)


def _maxrss_mb() -> float:
    # ru_maxrss is KiB on Linux, bytes on macOS.
    raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return raw / 1024.0 if sys.platform != "darwin" else raw / (1024.0 * 1024.0)


def run_worker(op: str, num_rows: int, parquet_path: str) -> dict:
    import pyarrow as pa

    from ltseq import LTSeq

    # --- setup (excluded from the measurement) ---
    result: dict = {"op": op, "rows": num_rows}
    if op == "from_arrow":
        source = build_table(num_rows)
        result["bytes"] = source.nbytes
        result["chunks"] = source["id"].num_chunks

        def action():
            return LTSeq.from_arrow(source)

    elif op == "to_arrow_memtable":
        # Materialize from Parquet rather than via from_arrow, so the setup's
        # high-water mark does not hide the export's own peak.
        t = LTSeq.read_parquet(parquet_path).collect()

        def action():
            return t.to_arrow()

    elif op == "to_arrow_parquet":
        t = LTSeq.read_parquet(parquet_path)

        def action():
            return t.to_arrow()

    elif op == "pa_table":
        t = LTSeq.read_parquet(parquet_path).collect()
        if not hasattr(t, "__arrow_c_stream__"):
            result["skipped"] = "build has no __arrow_c_stream__"
            return result

        def action():
            return pa.table(t)

    elif op == "cursor":

        def action():
            total = 0
            for batch in LTSeq.scan_parquet(parquet_path):
                total += batch.num_rows
            return total

    else:
        raise SystemExit(f"unknown op {op}")

    # --- measurement ---
    rss_before = _maxrss_mb()
    start = time.perf_counter()
    out = action()
    elapsed = time.perf_counter() - start
    rss_after = _maxrss_mb()

    if op == "cursor":
        result["rows_out"] = out
    elif op == "from_arrow":
        result["rows_out"] = out.count()
    else:
        result["rows_out"] = out.num_rows
    result["time_s"] = round(elapsed, 3)
    result["peak_mb"] = round(rss_after - rss_before, 1)
    result["maxrss_mb"] = round(rss_after, 1)
    return result


def run_all(num_rows: int, ops: list[str], label: str) -> list[dict]:
    import pyarrow.parquet as pq

    results = []
    with tempfile.TemporaryDirectory() as tmp:
        parquet_path = os.path.join(tmp, "boundary.parquet")
        if any(op != "from_arrow" for op in ops):
            pq.write_table(build_table(num_rows), parquet_path)
        for op in ops:
            cmd = [
                sys.executable,
                __file__,
                "--worker",
                op,
                "--rows",
                str(num_rows),
                "--parquet",
                parquet_path,
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if proc.returncode != 0:
                failure = {"op": op, "rows": num_rows, "label": label, "error": proc.stderr.strip()[-2000:]}
                results.append(failure)
                print(format_row(failure), flush=True)
                continue
            payload = json.loads(proc.stdout.strip().splitlines()[-1])
            payload["label"] = label
            results.append(payload)
            print(format_row(payload), flush=True)
    return results


def format_row(r: dict) -> str:
    if "error" in r:
        return f"| {r['op']} | {r.get('label', '')} | ERROR | | | {r['error'][-120:]} |"
    if "skipped" in r:
        return f"| {r['op']} | {r.get('label', '')} | skipped | | | {r['skipped']} |"
    return (
        f"| {r['op']} | {r.get('label', '')} | {r['time_s']:.3f} | {r['peak_mb']:.0f} | "
        f"{r['rows']:,} | {_describe_input(r)} |"
    )


def _describe_input(r: dict) -> str:
    if "bytes" in r:
        return f"pyarrow.Table {r['bytes'] / 1e6:.0f} MB, {r.get('chunks', '-')} chunks"
    return "parquet file"


HEADER = "| op | build | time (s) | peak RSS growth (MB) | rows | input |\n|---|---|---|---|---|---|"


def print_comparison(before: list[dict], after: list[dict]) -> None:
    by_op = {r["op"]: r for r in before}
    print("\n| op | before (s) | after (s) | speedup | before peak (MB) | after peak (MB) |")
    print("|---|---|---|---|---|---|")
    for r in after:
        b = by_op.get(r["op"])
        if not b or "time_s" not in b or "time_s" not in r:
            continue
        speed = b["time_s"] / r["time_s"] if r["time_s"] else float("inf")
        print(
            f"| {r['op']} | {b['time_s']:.3f} | {r['time_s']:.3f} | {speed:.1f}x | "
            f"{b['peak_mb']:.0f} | {r['peak_mb']:.0f} |"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--target-mb", type=int, default=1024, help="approximate input size in MB")
    parser.add_argument("--rows", type=int, default=None, help="explicit row count (overrides --target-mb)")
    parser.add_argument("--ops", nargs="*", default=OPS, choices=OPS)
    parser.add_argument("--label", default="current", help="build label recorded in the results")
    parser.add_argument("--out", type=Path, default=None, help="write results JSON here")
    parser.add_argument("--compare", type=Path, default=None, help="previous results JSON to compare against")
    parser.add_argument("--worker", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--parquet", default=None, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.worker:
        print(json.dumps(run_worker(args.worker, args.rows, args.parquet)))
        return

    num_rows = args.rows or rows_for_target(args.target_mb)
    print(f"# Arrow boundary benchmark: {num_rows:,} rows (~{num_rows * _BYTES_PER_ROW / 1e6:.0f} MB), build={args.label}\n")
    print(HEADER)
    results = run_all(num_rows, args.ops, args.label)

    if args.out:
        args.out.write_text(json.dumps(results, indent=2))
        print(f"\nSaved to {args.out}")
    if args.compare:
        print_comparison(json.loads(args.compare.read_text()), results)


if __name__ == "__main__":
    main()
