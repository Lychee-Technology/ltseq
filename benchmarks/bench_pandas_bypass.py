#!/usr/bin/env python3
"""
LTSeq Pandas Bypass Benchmark
================================

Benchmarks the main hotspots from GitHub issue #69 that previously materialized
data through pandas/pyarrow, bypassing the Rust execution engine. After the
Phase 1 optimization, these paths now use Arrow directly (avoiding pandas
round-trips) or delegate to Rust-native operations.

Hotspots benchmarked:
  1. NestedTable.__len__ — was len(to_pandas()), now len(LTSeq) -> Rust count()
  2. SQLPartitionedTable._compute_keys — was to_pandas()+iterrows(), now to_arrow()+to_pylist()
  3. PartitionedTable._materialize_partitions — was to_pandas()+_from_rows(), now to_arrow()+take()
  4. contain() — was to_pandas(), now to_arrow()+to_pylist()

Each benchmark is measured at 3 scales (10K, 100K, 1M rows) with warmup +
multiple iterations, matching the style of bench_core.py.

Usage:
    # Full benchmark (1M rows)
    uv run python benchmarks/bench_pandas_bypass.py

    # Quick run (10K and 100K only)
    uv run python benchmarks/bench_pandas_bypass.py --quick

    # Scale overrides
    uv run python benchmarks/bench_pandas_bypass.py --scales 10000 100000 1000000
"""

import argparse
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field

sys.path.insert(0, str(os.path.join(os.path.dirname(__file__), "..", "py-ltseq")))

from ltseq import LTSeq  # type: ignore


# ---------------------------------------------------------------------------
# Infra
# ---------------------------------------------------------------------------


@dataclass
class BenchResult:
    name: str
    scale: int
    median_ms: float
    times_ms: list[float] = field(default_factory=list)
    mem_delta_mb: float = 0.0


def benchmark_fn(name, scale, fn, warmup=1, iterations=5):
    """Run fn with warmup, return median time + memory delta."""
    import gc

    import psutil

    for _ in range(warmup):
        fn()

    gc.collect()
    proc = psutil.Process(os.getpid())
    times = []
    peak_mem = 0

    for _ in range(iterations):
        gc.collect()
        mem_before = proc.memory_info().rss / (1024**2)
        t0 = time.perf_counter()
        fn()
        t1 = time.perf_counter()
        mem_after = proc.memory_info().rss / (1024**2)
        elapsed_ms = (t1 - t0) * 1000
        times.append(elapsed_ms)
        peak_mem = max(peak_mem, mem_after - mem_before)

    median = sorted(times)[len(times) // 2]
    return BenchResult(
        name=name,
        scale=scale,
        median_ms=round(median, 2),
        times_ms=[round(t, 2) for t in times],
        mem_delta_mb=round(peak_mem, 1),
    )


def generate_csv(path, num_rows, num_categories=20):
    """Generate a CSV with id, category, value, amount columns."""
    import random

    random.seed(42)
    categories = [f"cat_{i}" for i in range(num_categories)]

    with open(path, "w") as f:
        f.write("id,category,value,amount\n")
        for i in range(num_rows):
            cat = random.choice(categories)
            val = random.randint(1, 1000)
            amt = round(random.uniform(10.0, 5000.0), 2)
            f.write(f"{i},{cat},{val},{amt}\n")


# ---------------------------------------------------------------------------
# Benchmark 1: NestedTable.__len__
# Previously: len(self._ltseq.to_pandas()) — materializes entire DF just to count
# Now: len(self._ltseq) — delegates to Rust count()
# ---------------------------------------------------------------------------


def bench_nested_len(csv_path):
    t = LTSeq.read_csv(csv_path)
    grouped = t.group_ordered(lambda r: r.category)
    return len(grouped)


# ---------------------------------------------------------------------------
# Benchmark 2: SQLPartitionedTable.keys() / _compute_keys()
# Previously: distinct.to_pandas() then iterrows()
# Now: distinct.to_arrow() then column.to_pylist()
# ---------------------------------------------------------------------------


def bench_partition_keys(csv_path):
    t = LTSeq.read_csv(csv_path)
    partitions = t.partition("category")
    return len(partitions.keys())


# ---------------------------------------------------------------------------
# Benchmark 3: PartitionedTable._materialize_partitions()
# Previously: to_pandas() -> dict of rows -> _from_rows()
# Now: to_arrow() -> group by indices -> take() -> from_arrow()
# ---------------------------------------------------------------------------


def bench_partition_materialize(csv_path):
    t = LTSeq.read_csv(csv_path)
    partitions = t.partition(by=lambda r: r.category)
    keys = partitions.keys()
    return len(keys)


# ---------------------------------------------------------------------------
# Benchmark 4: contain()
# Previously: to_pandas() then set(df[col].tolist())
# Now: to_arrow() then set(pa_table.column(col).to_pylist())
# ---------------------------------------------------------------------------


def bench_contain(csv_path):
    t = LTSeq.read_csv(csv_path)
    cat_values = ["cat_0", "cat_5", "cat_10", "cat_15", "cat_19"]
    return t.contain("category", *cat_values)


BENCHMARKS = [
    ("nested_len", bench_nested_len),
    ("partition_keys_sql", bench_partition_keys),
    ("partition_materialize", bench_partition_materialize),
    ("contain", bench_contain),
]


def print_results(results: list[BenchResult]):
    print("\n" + "=" * 90)
    print("PANDAS BYPASS BENCHMARK RESULTS")
    print("=" * 90)
    header = (
        f"{'Benchmark':<40} {'Scale':>8} {'Median(ms)':>12} "
        f"{'Min(ms)':>10} {'Max(ms)':>10} {'Mem Δ(MB)':>12}"
    )
    print(header)
    print("-" * len(header))

    for r in results:
        print(
            f"{r.name:<40} {r.scale:>8,} {r.median_ms:>12.2f} "
            f"{min(r.times_ms):>10.2f} {max(r.times_ms):>10.2f} "
            f"{r.mem_delta_mb:>12.1f}"
        )

    print("=" * 90)


def collect_host_info():
    info = {
        "os": f"{platform.system()} {platform.release()}",
        "python": platform.python_version(),
        "processor": platform.processor() or "unknown",
        "cpu_count": os.cpu_count(),
    }
    try:
        import psutil

        info["ram_gb"] = round(psutil.virtual_memory().total / (1024**3), 1)
    except ImportError:
        pass
    try:
        info["git_commit"] = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], text=True, timeout=5
        ).strip()
    except Exception:
        pass
    try:
        rustc = subprocess.check_output(
            ["rustc", "--version"], text=True, timeout=5
        ).strip()
        info["rustc"] = rustc.split()[1] if len(rustc.split()) > 1 else rustc
    except Exception:
        pass
    return info


def main():
    parser = argparse.ArgumentParser(description="LTSeq Pandas Bypass Benchmark")
    parser.add_argument(
        "--quick", action="store_true", help="Only run 10K and 100K scales"
    )
    parser.add_argument(
        "--scales",
        nargs="+",
        type=int,
        default=[10_000, 100_000, 1_000_000],
        help="Row counts to benchmark (default: 10K 100K 1M)",
    )
    parser.add_argument(
        "--warmup", type=int, default=1, help="Warmup iterations (default: 1)"
    )
    parser.add_argument(
        "--iterations", type=int, default=5, help="Timed iterations (default: 5)"
    )
    args = parser.parse_args()

    if args.quick:
        args.scales = [10_000, 100_000]

    print("LTSeq Pandas Bypass Benchmark")
    print("=" * 90)
    print(f"Scales: {args.scales}")
    print(f"Warmup: {args.warmup}, Iterations: {args.iterations}")
    print(f"Host: {platform.processor()} | {os.cpu_count()} cores")
    try:
        import psutil

        print(f"RAM: {psutil.virtual_memory().total / (1024**3):.0f}GB")
    except ImportError:
        pass
    print()

    results: list[BenchResult] = []

    with tempfile.TemporaryDirectory() as tmpdir:
        for scale in args.scales:
            csv_path = os.path.join(tmpdir, f"bench_{scale}.csv")
            print(f"Generating {scale:,} rows... ", end="", flush=True)
            generate_csv(csv_path, scale)
            print("done.")

            for bench_name, bench_fn in BENCHMARKS:
                label = f"{bench_name}_{scale // 1000}k" if scale < 1_000_000 else f"{bench_name}_{scale // 1000}k"
                print(f"  {label:<40}", end="", flush=True)
                r = benchmark_fn(
                    label, scale, lambda p=csv_path: bench_fn(p),
                    warmup=args.warmup, iterations=args.iterations,
                )
                print(f"  {r.median_ms:>8.2f} ms")
                results.append(r)

    print_results(results)

    output = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "host": collect_host_info(),
        "scales": args.scales,
        "warmup": args.warmup,
        "iterations": args.iterations,
        "results": [
            {
                "name": r.name,
                "scale": r.scale,
                "median_ms": r.median_ms,
                "times_ms": r.times_ms,
                "mem_delta_mb": r.mem_delta_mb,
            }
            for r in results
        ],
    }
    results_path = os.path.join(os.path.dirname(__file__), "pandas_bypass_results.json")
    with open(results_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved to {results_path}")


if __name__ == "__main__":
    main()
