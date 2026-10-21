#!/usr/bin/env python3
"""What the writers' flush before the rename costs (#259).

``write_csv`` and ``write_parquet`` write a temporary file, flush it with
``File::sync_all`` (``fsync`` on Linux, ``F_FULLFSYNC`` on macOS) and rename
it over the destination. This times both writers at three output sizes,
replacing an existing file in ``--dir`` each time, as an overwrite does.

The flush is measured two ways:

- **Replayed** (every OS): after each write, the same bytes are written to a
  new file in the same directory and the flush the writer uses is timed on
  it. This estimates the writer's flush without touching the extension.
- **In the writer** (Linux, ``--shim``): the run is repeated in a child
  process with an ``LD_PRELOAD`` library that wraps ``fsync``. In ``time``
  mode it times the writer's own call; in ``skip`` mode ``fsync`` returns
  at once, which gives the write without the flush, everything else equal.
  The library is compiled with ``cc`` from the source below.

Build the extension in release mode first, and put ``--dir`` on the
filesystem to measure (not tmpfs, where a flush does nothing):

    maturin develop --release
    uv run --no-sync python benchmarks/bench_writer_fsync.py --dir ~/bench-out --shim

Results are printed as a table and, with ``--out``, saved as JSON.
"""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "py-ltseq"))

WRITERS = ["write_csv", "write_parquet"]
# Target output sizes; the row count for each is calibrated per writer.
SIZES = {"small": 50 * 1024, "medium": 5 * 1024**2, "large": 150 * 1024**2}
REPS = {"small": 41, "medium": 21, "large": 9}

SHIM_SOURCE = r"""
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdlib.h>
#include <time.h>

static long long total_ns, calls;

int fsync(int fd) {
    static int (*real)(int);
    static int skip = -1;
    if (!real) real = (int (*)(int))dlsym(RTLD_NEXT, "fsync");
    if (skip < 0) {
        const char *mode = getenv("FSYNC_SHIM");
        skip = mode && mode[0] == 's';
    }
    struct timespec a, b;
    clock_gettime(CLOCK_MONOTONIC, &a);
    int rc = skip ? 0 : real(fd);
    clock_gettime(CLOCK_MONOTONIC, &b);
    __atomic_add_fetch(&total_ns, (b.tv_sec - a.tv_sec) * 1000000000LL + (b.tv_nsec - a.tv_nsec), __ATOMIC_RELAXED);
    __atomic_add_fetch(&calls, 1, __ATOMIC_RELAXED);
    return rc;
}

long long fsync_shim_ns(void) { return __atomic_load_n(&total_ns, __ATOMIC_RELAXED); }
long long fsync_shim_calls(void) { return __atomic_load_n(&calls, __ATOMIC_RELAXED); }
"""


def build_table(rows: int):
    """Deterministic rows: an id, a float and one of 16 short strings."""
    import numpy as np
    import pyarrow as pa

    from ltseq import LTSeq

    rng = np.random.default_rng(259)
    names = pa.array([f"category_{i:02d}" for i in range(16)])
    return LTSeq.from_arrow(
        pa.table(
            {
                "id": pa.array(np.arange(rows, dtype=np.int64)),
                "value": pa.array(rng.standard_normal(rows)),
                "category": names.take(pa.array(rng.integers(0, 16, rows))),
            }
        )
    )


def rows_for(writer: str, target: int, scratch: Path) -> int:
    """Rows that make `writer` write about `target` bytes."""
    probe = 100_000
    path = scratch / f"calibrate-{writer}"
    getattr(build_table(probe), writer)(str(path))
    per_row = path.stat().st_size / probe
    path.unlink()
    return max(100, round(target / per_row))


def flush(fd: int) -> None:
    """The flush `File::sync_all` performs."""
    if sys.platform == "darwin":
        import fcntl

        fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
    else:
        os.fsync(fd)


def replay(data: bytes, directory: Path) -> float:
    """Seconds to flush a new file of `data` in `directory`."""
    path = directory / ".replay"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, data)
        start = time.perf_counter()
        flush(fd)
        return time.perf_counter() - start
    finally:
        os.close(fd)
        path.unlink()


def child(args) -> None:
    """Run every case in this process and print one JSON document."""
    shim = None
    if os.environ.get("FSYNC_SHIM"):
        shim = ctypes.CDLL(None)
        shim.fsync_shim_ns.restype = ctypes.c_longlong
        shim.fsync_shim_calls.restype = ctypes.c_longlong
    directory = Path(args.dir)
    results = []
    for size in args.sizes:
        for writer in WRITERS:
            rows = rows_for(writer, SIZES[size], directory)
            t = build_table(rows)
            dest = directory / f"out-{writer}-{size}"
            getattr(t, writer)(str(dest))  # warm-up; later writes replace it
            totals, in_writer, replayed = [], [], []
            for _ in range(args.reps or REPS[size]):
                before = (shim.fsync_shim_ns(), shim.fsync_shim_calls()) if shim else None
                start = time.perf_counter()
                getattr(t, writer)(str(dest))
                totals.append(time.perf_counter() - start)
                if shim:
                    ns, calls = shim.fsync_shim_ns(), shim.fsync_shim_calls()
                    assert calls - before[1] == 1, "expected one fsync per write"
                    in_writer.append((ns - before[0]) / 1e9)
                if os.environ.get("FSYNC_SHIM") != "skip":
                    replayed.append(replay(dest.read_bytes(), directory))
            results.append(
                {
                    "writer": writer,
                    "size": size,
                    "rows": rows,
                    "bytes": dest.stat().st_size,
                    "total": totals,
                    "fsync_in_writer": in_writer,
                    "fsync_replayed": replayed,
                }
            )
            dest.unlink()
    json.dump(results, sys.stdout)


def run_child(args, env_extra: dict[str, str]) -> list[dict]:
    cmd = [sys.executable, __file__, "--child", "--dir", args.dir, "--sizes", *args.sizes]
    if args.reps:
        cmd += ["--reps", str(args.reps)]
    out = subprocess.run(
        cmd, env={**os.environ, **env_extra}, check=True, capture_output=True, text=True
    )
    return json.loads(out.stdout)


def build_shim(scratch: Path) -> Path:
    src, lib = scratch / "fsync_shim.c", scratch / "fsync_shim.so"
    src.write_text(SHIM_SOURCE)
    subprocess.run(["cc", "-O2", "-shared", "-fPIC", "-o", lib, src, "-ldl"], check=True)
    return lib


def quantile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(q * (len(ordered) - 1)))]


def ms(seconds: float) -> str:
    return f"{seconds * 1000:.2f}"


def environment(directory: str) -> dict:
    import pyarrow

    fs = subprocess.run(
        ["df", "-T", directory] if sys.platform.startswith("linux") else ["df", directory],
        capture_output=True,
        text=True,
    ).stdout.strip().splitlines()[-1]
    return {
        "os": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "pyarrow": pyarrow.__version__,
        "filesystem": fs,
        "cpus": os.cpu_count(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dir", required=True, help="directory on the filesystem to measure")
    parser.add_argument("--sizes", nargs="+", choices=list(SIZES), default=list(SIZES))
    parser.add_argument("--reps", type=int, help="repetitions per case (default: by size)")
    parser.add_argument("--shim", action="store_true", help="Linux: also time fsync in the writer")
    parser.add_argument("--out", help="save the results as JSON")
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    os.makedirs(args.dir, exist_ok=True)
    if args.child:
        child(args)
        return

    with tempfile.TemporaryDirectory() as scratch:
        if args.shim:
            lib = str(build_shim(Path(scratch)))
            synced = run_child(args, {"LD_PRELOAD": lib, "FSYNC_SHIM": "time"})
            skipped = run_child(args, {"LD_PRELOAD": lib, "FSYNC_SHIM": "skip"})
        else:
            synced, skipped = run_child(args, {}), None

    report = {"environment": environment(args.dir), "synced": synced, "unsynced": skipped}
    print(json.dumps(report["environment"], indent=2))
    header = "| Writer | Output | Total p50 | Total p95 | fsync p50 | fsync p95 | fsync share | Replayed fsync p50 |"
    if skipped:
        header += " No-fsync p50 |"
    print(header)
    print("|" + "---|" * (header.count("|") - 1))
    for i, case in enumerate(synced):
        total = case["total"]
        flushes = case["fsync_in_writer"] or case["fsync_replayed"]
        row = [
            case["writer"],
            f"{case['bytes'] / 1024**2:.2f} MiB ({case['size']}, {case['rows']} rows, n={len(total)})",
            ms(quantile(total, 0.5)),
            ms(quantile(total, 0.95)),
            ms(quantile(flushes, 0.5)),
            ms(quantile(flushes, 0.95)),
            f"{statistics.median(flushes) / statistics.median(total):.1%}",
            ms(quantile(case["fsync_replayed"], 0.5)),
        ]
        if skipped:
            row.append(ms(quantile(skipped[i]["total"], 0.5)))
        print("| " + " | ".join(row) + " |")
    if not synced[0]["fsync_in_writer"]:
        print("fsync columns are the replayed flush (no --shim).")
    if args.out:
        Path(args.out).write_text(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
