#!/usr/bin/env python3
"""
LTSeq Ordered-Data GPU Operator Benchmarks (Phase 1.6)
======================================================

Measures the performance of Phase 1 ordered-data operators that leverage
sort metadata for algorithmic advantages (consecutive grouping, segmented
aggregation, merge join, binary search).

Benchmark scenarios:
  1. group_ordered: Consecutive grouping on sorted data (boundary detection)
  2. Sorted GROUP BY: Segmented aggregate vs hash-based aggregate
  3. Merge join: Sorted merge join vs hash join
  4. search_first: Binary search on sorted data vs linear scan

Each benchmark compares:
  - LTSeq CPU path (current)
  - DuckDB equivalent (when available)

The GPU column is left as N/A until CUDA hardware is available. The
benchmark structure is ready for GPU timing to be added.

Usage:
    # Full benchmark (generates synthetic data, ~1 minute)
    uv run python benchmarks/bench_ordered_gpu.py

    # Quick validation with smaller data
    uv run python benchmarks/bench_ordered_gpu.py --sample

    # Specific test only
    uv run python benchmarks/bench_ordered_gpu.py --test group
    uv run python benchmarks/bench_ordered_gpu.py --test agg
    uv run python benchmarks/bench_ordered_gpu.py --test join
    uv run python benchmarks/bench_ordered_gpu.py --test search

    # Control iterations
    uv run python benchmarks/bench_ordered_gpu.py --iterations 5 --warmup 2

Requirements:
    maturin develop (or maturin develop --release for production numbers)
    pip install duckdb  (optional, for comparison)
"""

import argparse
import gc
import json
import os
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent / "py-ltseq"))

from ltseq import LTSeq  # type: ignore

try:
    import duckdb

    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BENCHMARKS_DIR = os.path.dirname(os.path.abspath(__file__))

WARMUP = 1
ITERATIONS = 3

# Scale configurations
SAMPLE_SIZES = {
    "group": [10_000, 100_000],
    "agg": [10_000, 100_000],
    "join": [(10_000, 10_000), (100_000, 100_000)],
    "search": [10_000, 100_000],
}

FULL_SIZES = {
    "group": [100_000, 1_000_000, 10_000_000],
    "agg": [100_000, 1_000_000, 10_000_000],
    "join": [(100_000, 100_000), (1_000_000, 1_000_000), (10_000_000, 10_000_000)],
    "search": [100_000, 1_000_000, 10_000_000],
}


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


@contextmanager
def timer():
    """Context manager that yields elapsed time in milliseconds."""
    gc.collect()
    start = time.perf_counter()
    result: dict[str, float] = {"elapsed_ms": 0.0}
    yield result
    result["elapsed_ms"] = (time.perf_counter() - start) * 1000


@dataclass
class BenchmarkResult:
    name: str
    rows: int
    ltseq_ms: float
    duckdb_ms: Optional[float] = None
    gpu_ms: Optional[float] = None
    speedup_vs_duckdb: Optional[float] = None

    def to_dict(self):
        d = {
            "name": self.name,
            "rows": self.rows,
            "ltseq_ms": round(self.ltseq_ms, 2),
        }
        if self.duckdb_ms is not None:
            d["duckdb_ms"] = round(self.duckdb_ms, 2)
            d["speedup_vs_duckdb"] = (
                round(self.duckdb_ms / self.ltseq_ms, 2)
                if self.ltseq_ms > 0
                else None
            )
        if self.gpu_ms is not None:
            d["gpu_ms"] = round(self.gpu_ms, 2)
        return d


def run_timed(fn, warmup=WARMUP, iterations=ITERATIONS) -> float:
    """Run fn with warmup and return average time in ms."""
    for _ in range(warmup):
        fn()

    times = []
    for _ in range(iterations):
        with timer() as t:
            fn()
        times.append(t["elapsed_ms"])

    return sum(times) / len(times)


# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------


def generate_sorted_csv(path: str, num_rows: int, num_groups: int = 100) -> None:
    """Generate a sorted CSV with consecutive group structure.

    Columns: id (sorted), group_key (consecutive runs), value (int), amount (float)
    The group_key changes every (num_rows / num_groups) rows, creating
    consecutive groups suitable for group_ordered().
    """
    import random

    random.seed(42)
    rows_per_group = max(1, num_rows // num_groups)

    with open(path, "w") as f:
        f.write("id,group_key,value,amount\n")
        for i in range(num_rows):
            group = i // rows_per_group
            value = random.randint(1, 1000)
            amount = round(random.uniform(0, 10000), 2)
            f.write(f"{i},{group},{value},{amount}\n")


def generate_sorted_parquet(path: str, num_rows: int, num_groups: int = 100) -> None:
    """Generate a sorted Parquet file for benchmarks (faster I/O)."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    import random

    random.seed(42)
    rows_per_group = max(1, num_rows // num_groups)

    ids = list(range(num_rows))
    group_keys = [i // rows_per_group for i in range(num_rows)]
    values = [random.randint(1, 1000) for _ in range(num_rows)]
    amounts = [round(random.uniform(0, 10000), 2) for _ in range(num_rows)]

    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "group_key": pa.array(group_keys, type=pa.int64()),
            "value": pa.array(values, type=pa.int64()),
            "amount": pa.array(amounts, type=pa.float64()),
        }
    )
    pq.write_table(table, path)


def generate_join_parquet(
    left_path: str,
    right_path: str,
    left_rows: int,
    right_rows: int,
) -> None:
    """Generate two sorted Parquet files for join benchmarks.

    Both are sorted by join_key. Left has unique keys, right has ~3 matches per key.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    import random

    random.seed(42)

    # Left table: sorted unique join keys
    left_keys = sorted(random.sample(range(left_rows * 2), left_rows))
    left_values = [random.randint(1, 1000) for _ in range(left_rows)]

    left_table = pa.table(
        {
            "join_key": pa.array(left_keys, type=pa.int64()),
            "left_value": pa.array(left_values, type=pa.int64()),
        }
    )
    pq.write_table(left_table, left_path)

    # Right table: sorted keys with duplicates (some matching left, some not)
    right_keys = []
    right_values = []
    # Pick ~60% of left keys and create 1-5 rows per key
    matching_keys = random.sample(left_keys, int(len(left_keys) * 0.6))
    for key in matching_keys:
        n_dups = random.randint(1, 5)
        for _ in range(n_dups):
            right_keys.append(key)
            right_values.append(random.randint(1, 1000))

    # Fill remaining with non-matching keys
    max_key = max(left_keys) + 1
    while len(right_keys) < right_rows:
        right_keys.append(max_key + len(right_keys))
        right_values.append(random.randint(1, 1000))

    # Sort by key
    sorted_pairs = sorted(zip(right_keys, right_values), key=lambda x: x[0])
    right_keys = [p[0] for p in sorted_pairs]
    right_values = [p[1] for p in sorted_pairs]

    right_table = pa.table(
        {
            "join_key": pa.array(right_keys[:right_rows], type=pa.int64()),
            "right_value": pa.array(right_values[:right_rows], type=pa.int64()),
        }
    )
    pq.write_table(right_table, right_path)


# ---------------------------------------------------------------------------
# Benchmark 1: group_ordered (consecutive grouping)
# ---------------------------------------------------------------------------


def bench_group_ordered(
    num_rows: int,
    temp_dir: str,
    warmup: int = WARMUP,
    iterations: int = ITERATIONS,
) -> BenchmarkResult:
    """Benchmark group_ordered: consecutive grouping + filter + derive."""
    parquet_path = os.path.join(temp_dir, f"group_{num_rows}.parquet")
    num_groups = max(10, num_rows // 1000)
    generate_sorted_parquet(parquet_path, num_rows, num_groups)

    # LTSeq: group_ordered with filter + derive
    def run_ltseq():
        t = LTSeq.read_parquet(parquet_path).sort("id")
        result = (
            t.group_ordered(lambda r: r.group_key)
            .filter(lambda g: g.count() > 5)
            .derive(
                lambda g: {
                    "group_size": g.count(),
                    "group_sum": g.sum("value"),
                }
            )
        )
        _ = len(result)

    ltseq_ms = run_timed(run_ltseq, warmup, iterations)

    # DuckDB comparison: window-based consecutive grouping
    duckdb_ms = None
    if HAS_DUCKDB:

        def run_duckdb():
            con = duckdb.connect()
            con.execute(
                f"""
                WITH lagged AS (
                    SELECT *,
                        CASE WHEN group_key != LAG(group_key) OVER (ORDER BY id)
                            THEN 1 ELSE 0 END AS boundary
                    FROM read_parquet('{parquet_path}')
                ),
                grouped AS (
                    SELECT *,
                        SUM(boundary) OVER (ORDER BY id) AS grp_id
                    FROM lagged
                ),
                group_stats AS (
                    SELECT grp_id,
                        COUNT(*) as group_size,
                        SUM(value) as group_sum
                    FROM grouped
                    GROUP BY grp_id
                    HAVING COUNT(*) > 5
                )
                SELECT g.*, gs.group_size, gs.group_sum
                FROM grouped g
                JOIN group_stats gs ON g.grp_id = gs.grp_id
                """
            )
            _ = con.fetchall()
            con.close()

        duckdb_ms = run_timed(run_duckdb, warmup, iterations)

    return BenchmarkResult(
        name=f"group_ordered_{num_rows // 1000}K",
        rows=num_rows,
        ltseq_ms=ltseq_ms,
        duckdb_ms=duckdb_ms,
    )


# ---------------------------------------------------------------------------
# Benchmark 2: Sorted GROUP BY (segmented aggregate)
# ---------------------------------------------------------------------------


def bench_sorted_agg(
    num_rows: int,
    temp_dir: str,
    warmup: int = WARMUP,
    iterations: int = ITERATIONS,
) -> BenchmarkResult:
    """Benchmark sorted GROUP BY: agg() on pre-sorted data."""
    parquet_path = os.path.join(temp_dir, f"agg_{num_rows}.parquet")
    num_groups = max(10, num_rows // 1000)
    generate_sorted_parquet(parquet_path, num_rows, num_groups)

    # LTSeq: agg on sorted data (DataFusion may use segmented path)
    def run_ltseq():
        t = LTSeq.read_parquet(parquet_path).sort("group_key")
        result = t.agg(
            by=lambda r: r.group_key,
            total_value=lambda g: g.value.sum(),
            avg_amount=lambda g: g.amount.avg(),
            cnt=lambda g: g.value.count(),
        )
        _ = len(result)

    ltseq_ms = run_timed(run_ltseq, warmup, iterations)

    # DuckDB comparison
    duckdb_ms = None
    if HAS_DUCKDB:

        def run_duckdb():
            con = duckdb.connect()
            con.execute(
                f"""
                SELECT group_key,
                    SUM(value) as total_value,
                    AVG(amount) as avg_amount,
                    COUNT(value) as cnt
                FROM read_parquet('{parquet_path}')
                GROUP BY group_key
                """
            )
            _ = con.fetchall()
            con.close()

        duckdb_ms = run_timed(run_duckdb, warmup, iterations)

    return BenchmarkResult(
        name=f"sorted_agg_{num_rows // 1000}K",
        rows=num_rows,
        ltseq_ms=ltseq_ms,
        duckdb_ms=duckdb_ms,
    )


# ---------------------------------------------------------------------------
# Benchmark 3: Merge join (sorted tables)
# ---------------------------------------------------------------------------


def bench_merge_join(
    left_rows: int,
    right_rows: int,
    temp_dir: str,
    warmup: int = WARMUP,
    iterations: int = ITERATIONS,
) -> BenchmarkResult:
    """Benchmark merge join: join_sorted on pre-sorted tables."""
    left_path = os.path.join(temp_dir, f"join_left_{left_rows}.parquet")
    right_path = os.path.join(temp_dir, f"join_right_{right_rows}.parquet")
    generate_join_parquet(left_path, right_path, left_rows, right_rows)

    total_rows = left_rows + right_rows

    # LTSeq: merge join on sorted data
    def run_ltseq():
        left = LTSeq.read_parquet(left_path).sort("join_key")
        right = LTSeq.read_parquet(right_path).sort("join_key")
        result = left.join(
            right, on=lambda l, r: l.join_key == r.join_key, how="inner",
            strategy="merge",
        )
        _ = len(result)

    ltseq_ms = run_timed(run_ltseq, warmup, iterations)

    # DuckDB comparison: hash join (DuckDB default)
    duckdb_ms = None
    if HAS_DUCKDB:

        def run_duckdb():
            con = duckdb.connect()
            con.execute(
                f"""
                SELECT l.join_key, l.left_value, r.right_value
                FROM read_parquet('{left_path}') l
                JOIN read_parquet('{right_path}') r
                ON l.join_key = r.join_key
                """
            )
            _ = con.fetchall()
            con.close()

        duckdb_ms = run_timed(run_duckdb, warmup, iterations)

    return BenchmarkResult(
        name=f"merge_join_{left_rows // 1000}Kx{right_rows // 1000}K",
        rows=total_rows,
        ltseq_ms=ltseq_ms,
        duckdb_ms=duckdb_ms,
    )


# ---------------------------------------------------------------------------
# Benchmark 4: search_first (binary search on sorted data)
# ---------------------------------------------------------------------------


def bench_search_first(
    num_rows: int,
    temp_dir: str,
    warmup: int = WARMUP,
    iterations: int = ITERATIONS,
) -> BenchmarkResult:
    """Benchmark search_first: single binary search on sorted data."""
    parquet_path = os.path.join(temp_dir, f"search_{num_rows}.parquet")
    generate_sorted_parquet(parquet_path, num_rows, num_groups=1)

    # The data has 'value' column with random ints 1-1000, sorted by 'id'.
    # For binary search to kick in, we search on the sorted column 'id'.
    # search_first with sorted data uses O(log N) binary search.

    # LTSeq: search_first on sorted column (binary search fast path)
    def run_ltseq():
        t = LTSeq.read_parquet(parquet_path).sort("id")
        # Search for row where id > 75% of the way through
        target = int(num_rows * 0.75)
        result = t.search_first(lambda r: r.id > target)
        _ = len(result)

    ltseq_ms = run_timed(run_ltseq, warmup, iterations)

    # DuckDB comparison: filter + limit
    duckdb_ms = None
    if HAS_DUCKDB:

        def run_duckdb():
            con = duckdb.connect()
            target = int(num_rows * 0.75)
            con.execute(
                f"""
                SELECT * FROM read_parquet('{parquet_path}')
                WHERE id > {target}
                ORDER BY id
                LIMIT 1
                """
            )
            _ = con.fetchall()
            con.close()

        duckdb_ms = run_timed(run_duckdb, warmup, iterations)

    return BenchmarkResult(
        name=f"search_first_{num_rows // 1000}K",
        rows=num_rows,
        ltseq_ms=ltseq_ms,
        duckdb_ms=duckdb_ms,
    )


def bench_search_first_batch(
    num_rows: int,
    num_searches: int,
    temp_dir: str,
    warmup: int = WARMUP,
    iterations: int = ITERATIONS,
) -> BenchmarkResult:
    """Benchmark batch search_first: multiple searches on same sorted table."""
    parquet_path = os.path.join(temp_dir, f"search_batch_{num_rows}.parquet")
    generate_sorted_parquet(parquet_path, num_rows, num_groups=1)

    import random

    random.seed(99)
    targets = [random.randint(0, num_rows - 1) for _ in range(num_searches)]

    # LTSeq: multiple search_first calls
    def run_ltseq():
        t = LTSeq.read_parquet(parquet_path).sort("id")
        for target in targets:
            result = t.search_first(lambda r: r.id > target)
            _ = len(result)

    ltseq_ms = run_timed(run_ltseq, warmup, iterations)

    # DuckDB comparison: multiple filter+limit queries
    duckdb_ms = None
    if HAS_DUCKDB:

        def run_duckdb():
            con = duckdb.connect()
            for target in targets:
                con.execute(
                    f"""
                    SELECT * FROM read_parquet('{parquet_path}')
                    WHERE id > {target}
                    ORDER BY id
                    LIMIT 1
                    """
                )
                _ = con.fetchall()
            con.close()

        duckdb_ms = run_timed(run_duckdb, warmup, iterations)

    return BenchmarkResult(
        name=f"search_batch_{num_searches}x{num_rows // 1000}K",
        rows=num_rows,
        ltseq_ms=ltseq_ms,
        duckdb_ms=duckdb_ms,
    )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def print_results(results: list[BenchmarkResult]) -> None:
    """Print results as a formatted table."""
    has_duckdb = any(r.duckdb_ms is not None for r in results)
    has_gpu = any(r.gpu_ms is not None for r in results)

    # Header
    print("\n" + "=" * 90)
    print("ORDERED-DATA GPU OPERATOR BENCHMARKS (Phase 1.6)")
    print("=" * 90)

    header = f"{'Benchmark':<35} {'Rows':>10} {'LTSeq (ms)':>12}"
    if has_duckdb:
        header += f" {'DuckDB (ms)':>12} {'Speedup':>8}"
    if has_gpu:
        header += f" {'GPU (ms)':>10} {'GPU Spdup':>10}"
    print(header)
    print("-" * 90)

    prev_category = None
    for r in results:
        # Category separator
        category = r.name.split("_")[0]
        if prev_category and category != prev_category:
            print("-" * 90)
        prev_category = category

        line = f"{r.name:<35} {r.rows:>10,} {r.ltseq_ms:>12.2f}"
        if has_duckdb:
            if r.duckdb_ms is not None:
                speedup = r.duckdb_ms / r.ltseq_ms if r.ltseq_ms > 0 else float("inf")
                line += f" {r.duckdb_ms:>12.2f} {speedup:>7.2f}x"
            else:
                line += f" {'N/A':>12} {'N/A':>8}"
        if has_gpu:
            if r.gpu_ms is not None:
                gpu_speedup = (
                    r.ltseq_ms / r.gpu_ms if r.gpu_ms > 0 else float("inf")
                )
                line += f" {r.gpu_ms:>10.2f} {gpu_speedup:>9.2f}x"
            else:
                line += f" {'N/A':>10} {'N/A':>10}"
        print(line)

    print("=" * 90)
    if not has_gpu:
        print("GPU column: N/A (no CUDA hardware detected)")
    if not has_duckdb:
        print("DuckDB column: N/A (pip install duckdb for comparison)")
    print()


def save_results(results: list[BenchmarkResult], path: str) -> None:
    """Save results to JSON file."""
    data = {
        "benchmark": "ordered_gpu_phase1",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "gpu_available": False,  # Update when GPU hardware available
        "duckdb_available": HAS_DUCKDB,
        "results": [r.to_dict() for r in results],
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Results saved to {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="LTSeq Ordered-Data GPU Operator Benchmarks"
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Use smaller data sizes for quick validation",
    )
    parser.add_argument(
        "--test",
        choices=["group", "agg", "join", "search"],
        help="Run only a specific benchmark",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=ITERATIONS,
        help=f"Number of timed iterations (default: {ITERATIONS})",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=WARMUP,
        help=f"Number of warmup iterations (default: {WARMUP})",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="Save results to JSON",
    )
    args = parser.parse_args()

    sizes = SAMPLE_SIZES if args.sample else FULL_SIZES
    warmup = args.warmup
    iterations = args.iterations

    print("LTSeq Ordered-Data GPU Operator Benchmarks (Phase 1.6)")
    print(f"Mode: {'sample' if args.sample else 'full'}")
    print(f"DuckDB: {'available' if HAS_DUCKDB else 'not installed'}")
    print(f"Warmup: {warmup}, Iterations: {iterations}")
    print("=" * 90)

    results: list[BenchmarkResult] = []

    with tempfile.TemporaryDirectory() as temp_dir:
        # Benchmark 1: group_ordered
        if args.test is None or args.test == "group":
            print("\n[1/4] Consecutive grouping (group_ordered)...")
            for n in sizes["group"]:
                print(f"  {n:>12,} rows...", end=" ", flush=True)
                r = bench_group_ordered(n, temp_dir, warmup, iterations)
                results.append(r)
                print(f"{r.ltseq_ms:.2f} ms")

        # Benchmark 2: Sorted GROUP BY
        if args.test is None or args.test == "agg":
            print("\n[2/4] Sorted GROUP BY (segmented aggregate)...")
            for n in sizes["agg"]:
                print(f"  {n:>12,} rows...", end=" ", flush=True)
                r = bench_sorted_agg(n, temp_dir, warmup, iterations)
                results.append(r)
                print(f"{r.ltseq_ms:.2f} ms")

        # Benchmark 3: Merge join
        if args.test is None or args.test == "join":
            print("\n[3/4] Merge join (join_sorted)...")
            for left_n, right_n in sizes["join"]:
                print(
                    f"  {left_n:>10,} x {right_n:>10,} rows...",
                    end=" ",
                    flush=True,
                )
                r = bench_merge_join(left_n, right_n, temp_dir, warmup, iterations)
                results.append(r)
                print(f"{r.ltseq_ms:.2f} ms")

        # Benchmark 4: search_first
        if args.test is None or args.test == "search":
            print("\n[4/4] Binary search (search_first)...")
            for n in sizes["search"]:
                print(f"  {n:>12,} rows (single)...", end=" ", flush=True)
                r = bench_search_first(n, temp_dir, warmup, iterations)
                results.append(r)
                print(f"{r.ltseq_ms:.2f} ms")

            # Batch search: 100 searches on largest table
            largest = sizes["search"][-1]
            print(f"  100 searches on {largest:,} rows...", end=" ", flush=True)
            r = bench_search_first_batch(largest, 100, temp_dir, warmup, iterations)
            results.append(r)
            print(f"{r.ltseq_ms:.2f} ms")

    # Output
    print_results(results)

    if args.save:
        results_path = os.path.join(BENCHMARKS_DIR, "ordered_gpu_results.json")
        save_results(results, results_path)


if __name__ == "__main__":
    main()
