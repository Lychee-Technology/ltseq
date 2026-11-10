"""What reading ``to_batches()`` through a Rust iterator costs (#256, step 3).

The §15.1 candidate is a ``pyarrow.RecordBatchReader`` over a Python
iterator implemented in Rust, so every batch is a call into Python under the
GIL. Today's ``__arrow_c_stream__`` never calls into Python. This times the
two, and the spike's own C stream (``Stream``, the §16.4 candidate), over
#251's m3 input and pipeline: the R3 harness's 8 Parquet files (640 MiB
decoded), ``filter(id % 4 != 0).select(id, v).derive(x = v * 2)``.

Terminals:

- ``py_today``: a Python loop over ``RecordBatchReader.from_stream(t)``,
  today's C stream.
- ``py_spike_stream``: the same loop over the spike's C stream.
- ``py_to_batches``: a Python loop over the spike's ``to_batches()``, whose
  iterator releases the GIL for each pull (ADR 0016); ``_attached`` keeps
  it (a lower bound: no hand-off of the iterator's own), ``_poll`` takes a
  ready batch without releasing it.
- ``py_to_batches_reimported``: a Python loop over
  ``RecordBatchReader.from_stream(to_batches())``: the reader imported
  through the C stream, as a consumer that takes any PyCapsule object does.
- ``duckdb_today``: ``select count(*), sum(x)`` in DuckDB over ``t``
  itself; DuckDB's threads pull today's C stream without the GIL.
- ``duckdb_to_batches``: the same over ``to_batches()``; DuckDB's threads
  pull through pyarrow's reader, which takes the GIL for every batch.
  ``_attached`` and ``_poll`` as above.

Each cell runs in a fresh process: build the input table, run the terminal
once over a small warm-up input, then ``--repeat`` timed runs (plan and
drain). With ``busy``, a Python thread spins for the whole cell, so every
GIL acquisition competes with it, as in a program with other Python work.

    python benchmarks/stream_errors/bench.py            # results/bench.json, .md
    python benchmarks/stream_errors/bench.py --quick    # fewer cells, to try it
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
WORK = REPO / "target" / "stream_errors"
RESULTS = HERE / "results"

TERMINALS = [
    "py_today",
    "py_spike_stream",
    "py_to_batches",
    "py_to_batches_attached",
    "py_to_batches_poll",
    "py_to_batches_reimported",
    "duckdb_today",
    "duckdb_to_batches",
    "duckdb_to_batches_attached",
    "duckdb_to_batches_poll",
]


def _gil(terminal: str) -> str:
    """The iterator's GIL mode a ``*_to_batches*`` terminal uses."""
    for mode in ("attached", "poll"):
        if terminal.endswith(mode):
            return mode
    return "detach"


# --- child -----------------------------------------------------------------


def _input(warmup: bool) -> Path:
    sys.path.insert(0, str(REPO / "py-ltseq"))
    from tests.memory import harness

    directory = WORK / ("r3_warmup" if warmup else "r3")
    rows = 8 * 1024 if warmup else harness.rows_for(harness.BUDGET, files=8)
    if not (directory / "part-00007.parquet").exists():
        harness.write_input(directory, rows)
    return directory


def _pipeline(t: Any) -> Any:
    return t.filter(lambda r: r.id % 4 != 0).select("id", "v").derive(x=lambda r: r.v * 2)


def _run(terminal: str, t: Any) -> int:
    """Drain ``t`` through ``terminal``; return the batches read (0 for DuckDB)."""
    import pyarrow as pa

    from ltseq import _stream_spike as spike

    if terminal.startswith("py_"):
        if terminal == "py_today":
            reader = pa.RecordBatchReader.from_stream(t)
        elif terminal == "py_spike_stream":
            reader = pa.RecordBatchReader.from_stream(spike.Stream(t))
        elif terminal == "py_to_batches_reimported":
            reader = pa.RecordBatchReader.from_stream(spike.to_batches(t))
        else:
            reader = spike.to_batches(t, gil=_gil(terminal))
        n = 0
        for _ in reader:
            n += 1
        return n
    import duckdb

    src = t if terminal == "duckdb_today" else spike.to_batches(t, gil=_gil(terminal))  # noqa: F841
    duckdb.sql("select count(*), sum(x) from src").fetchall()
    return 0


def child(cell: dict[str, Any]) -> dict[str, Any]:
    from ltseq import LTSeq
    from ltseq._testing import pinned_engine

    stop = threading.Event()
    spins = [0]
    if cell["busy"]:

        def spin() -> None:
            while not stop.is_set():
                spins[0] += 1

        threading.Thread(target=spin, daemon=True).start()

    source = _input(False)
    if cell["one_file"]:
        source = source / "part-00000.parquet"
    with pinned_engine(target_partitions=cell["partitions"], batch_size=cell["batch_size"]):
        warm = _pipeline(LTSeq.read_parquet(str(_input(True))))
        table = _pipeline(LTSeq.read_parquet(str(source)))
    _run(cell["terminal"], warm)
    times = []
    batches = 0
    for _ in range(cell["repeat"]):
        start = time.perf_counter()
        batches = _run(cell["terminal"], table)
        times.append(time.perf_counter() - start)
    stop.set()
    return {**cell, "times": times, "batches": batches, "spins": spins[0]}


# --- parent ----------------------------------------------------------------


def _cells(args: argparse.Namespace) -> list[dict[str, Any]]:
    """Idle cells read all 8 files; busy cells read the first file, an
    eighth of the input, because each GIL hand-off then waits for the
    spinning thread (up to the switch interval) and the full input would
    take minutes per run."""
    idle = [
        {
            "terminal": terminal,
            "partitions": partitions,
            "batch_size": batch_size,
            "busy": False,
            "one_file": False,
            "repeat": args.repeat,
        }
        for batch_size in args.batch_sizes
        for partitions in args.partitions
        for terminal in TERMINALS
    ]
    busy = [
        {
            "terminal": terminal,
            "partitions": partitions,
            "batch_size": batch_size,
            "busy": True,
            "one_file": True,
            "repeat": args.busy_repeat,
        }
        for batch_size in args.busy_batch_sizes
        for partitions in args.partitions
        for terminal in TERMINALS
    ]
    return idle + busy


def _build_info() -> dict[str, str]:
    import pyarrow as pa

    info = {
        "python": platform.python_version(),
        "pyarrow": pa.__version__,
        "platform": platform.platform(),
        "cpus": str(os.cpu_count()),
        "switch_interval_s": str(sys.getswitchinterval()),
    }
    try:
        import duckdb

        info["duckdb"] = duckdb.__version__
    except ImportError:
        pass
    commit = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"], cwd=REPO, capture_output=True, text=True
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=no"],
        cwd=REPO,
        capture_output=True,
        text=True,
    ).stdout.strip()
    info["commit"] = commit + ("-dirty" if dirty else "")
    return info


def render(result: dict[str, Any]) -> str:
    info = result["build"]
    lines = [
        "# to_batches() cost (#256)",
        "",
        "Generated by `bench.py`; "
        + ", ".join(f"{k} {v}" for k, v in info.items())
        + f". Median of {result['repeat']} runs per idle cell and "
        f"{result['busy_repeat']} per busy cell, each cell in a fresh process.",
        "",
        "Each cell: median seconds, then the ratio to `py_today` (Python loops) or "
        "`duckdb_today` (DuckDB) at the same partitions, batch size and load. "
        "`busy`: a Python thread spins during the cell.",
    ]
    cells = result["cells"]

    def find(**kw: Any) -> dict[str, Any] | None:
        for c in cells:
            if all(c[k] == v for k, v in kw.items()):
                return c
        return None

    for busy in (False, True):
        sizes = result["busy_batch_sizes"] if busy else result["batch_sizes"]
        for batch_size in sizes:
            what = "busy, first file only" if busy else "idle, all 8 files"
            lines += [
                "",
                f"## Batch size {batch_size}, {what}",
                "",
                "| Terminal | " + " | ".join(f"{p} partitions" for p in result["partitions"]) + " |",
                "|---|" + "---|" * len(result["partitions"]),
            ]
            batches = []
            for terminal in TERMINALS:
                row = [f"`{terminal}`"]
                base = "duckdb_today" if terminal.startswith("duckdb") else "py_today"
                for p in result["partitions"]:
                    c = find(terminal=terminal, partitions=p, batch_size=batch_size, busy=busy)
                    b = find(terminal=base, partitions=p, batch_size=batch_size, busy=busy)
                    if c is None or b is None:
                        row.append("")
                        continue
                    m = statistics.median(c["times"])
                    r = m / statistics.median(b["times"])
                    extra = ""
                    if busy and c["batches"]:
                        per = 1000 * m / c["batches"]
                        extra = f", {per:.1f} ms/batch"
                    elif busy and terminal.startswith("duckdb"):
                        n = find(terminal="py_today", partitions=p, batch_size=batch_size, busy=busy)
                        if n and n["batches"]:
                            extra = f", {1000 * m / n['batches']:.1f} ms/batch"
                    row.append(f"{m:.3f} s, {r:.2f}{extra}")
                    if terminal == "py_to_batches":
                        batches.append(c["batches"])
                lines.append("| " + " | ".join(row) + " |")
            if batches:
                lines.append("")
                lines.append(
                    "Batches per run: " + ", ".join(str(b) for b in batches) + " at "
                    + ", ".join(str(p) for p in result["partitions"]) + " partitions."
                )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", help=argparse.SUPPRESS)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--partitions", type=int, nargs="+", default=[1, 4, 8])
    parser.add_argument("--batch-sizes", type=int, nargs="+", default=[8192, 1024])
    parser.add_argument("--busy-batch-sizes", type=int, nargs="+", default=[8192])
    parser.add_argument("--busy-repeat", type=int, default=3)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--out", default=str(RESULTS / "bench"))
    args = parser.parse_args()

    if args.child:
        print(json.dumps(child(json.loads(args.child))))
        return 0

    from ltseq import _stream_spike as spike

    if not spike.available():
        print("needs a spike build: maturin develop --release --features stream_spike")
        return 2
    if args.quick:
        args.repeat, args.busy_repeat, args.partitions = 2, 2, [4]
        args.batch_sizes = args.busy_batch_sizes = [8192]

    _input(True)
    _input(False)
    cells = []
    plan = _cells(args)
    for i, cell in enumerate(plan, 1):
        out = subprocess.run(
            [sys.executable, __file__, "--child", json.dumps(cell)],
            capture_output=True,
            text=True,
            check=True,
        )
        done = json.loads(out.stdout.strip().splitlines()[-1])
        cells.append(done)
        print(
            f"[{i}/{len(plan)}] {cell['terminal']} p={cell['partitions']} "
            f"b={cell['batch_size']} busy={cell['busy']}: "
            f"{statistics.median(done['times']):.3f} s",
            flush=True,
        )
    result = {
        "build": _build_info(),
        "repeat": args.repeat,
        "busy_repeat": args.busy_repeat,
        "partitions": args.partitions,
        "batch_sizes": args.batch_sizes,
        "busy_batch_sizes": args.busy_batch_sizes,
        "cells": cells,
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.with_suffix(".json").write_text(json.dumps(result, indent=1) + "\n")
    out.with_suffix(".md").write_text(render(result))
    print(render(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
