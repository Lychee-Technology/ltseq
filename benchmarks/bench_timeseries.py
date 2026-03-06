#!/usr/bin/env python3
"""
LTSeq Time-Series Operator Benchmarks (Phase 3.6c)
===================================================

Measures the performance of time-series operators that are GPU-acceleratable
in Phase 3: ASOF join, rolling window aggregates, cumulative sum, and
shift/lag/lead.

Benchmark scenarios:
  1. asof:     ASOF join (backward/forward/nearest) at varying scales
  2. rolling:  Rolling window sum/avg at varying window sizes and scales
  3. cumsum:   Cumulative sum (prefix sum) at varying scales
  4. shift:    Lag/lead/shift at varying scales

Each benchmark compares:
  - LTSeq CPU path
  - DuckDB equivalent (when available)

Usage:
    # Full benchmark (generates synthetic data)
    uv run python benchmarks/bench_timeseries.py

    # Quick validation with smaller data
    uv run python benchmarks/bench_timeseries.py --sample

    # Specific test only
    uv run python benchmarks/bench_timeseries.py --test asof
    uv run python benchmarks/bench_timeseries.py --test rolling
    uv run python benchmarks/bench_timeseries.py --test cumsum
    uv run python benchmarks/bench_timeseries.py --test shift

    # Control iterations
    uv run python benchmarks/bench_timeseries.py --iterations 5 --warmup 2

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
    "asof": [(10_000, 10_000), (50_000, 50_000)],
    "rolling": [10_000, 50_000],
    "cumsum": [10_000, 50_000],
    "shift": [10_000, 50_000],
}

FULL_SIZES = {
    "asof": [(100_000, 100_000), (1_000_000, 1_000_000), (5_000_000, 5_000_000)],
    "rolling": [100_000, 1_000_000, 10_000_000],
    "cumsum": [100_000, 1_000_000, 10_000_000],
    "shift": [100_000, 1_000_000, 10_000_000],
}

# Rolling window sizes to test
WINDOW_SIZES = [10, 100, 1000]


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
    extra: dict = field(default_factory=dict)

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
        if self.extra:
            d.update(self.extra)
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


def generate_timeseries_parquet(path: str, num_rows: int) -> None:
    """Generate a sorted time-series Parquet file.

    Columns: timestamp (sorted int64 epoch ms), symbol (string, 10 symbols),
             price (float64), volume (int64)
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    import random

    random.seed(42)

    symbols = [f"SYM_{i:02d}" for i in range(10)]
    base_ts = 1_700_000_000_000  # ~Nov 2023 in epoch ms

    # Generate interleaved time-series for multiple symbols
    timestamps = []
    syms = []
    prices = []
    volumes = []

    for i in range(num_rows):
        timestamps.append(base_ts + i * 100)  # 100ms intervals
        syms.append(symbols[i % len(symbols)])
        prices.append(round(100.0 + random.gauss(0, 5), 2))
        volumes.append(random.randint(100, 10000))

    table = pa.table(
        {
            "timestamp": pa.array(timestamps, type=pa.int64()),
            "symbol": pa.array(syms, type=pa.utf8()),
            "price": pa.array(prices, type=pa.float64()),
            "volume": pa.array(volumes, type=pa.int64()),
        }
    )
    pq.write_table(table, path)


def generate_asof_data(
    left_path: str, right_path: str, left_rows: int, right_rows: int
) -> None:
    """Generate two sorted Parquet files for ASOF join benchmarks.

    Left: trade events (timestamp, symbol, trade_price)
    Right: quote updates (timestamp, symbol, bid, ask) — more frequent
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    import random

    random.seed(42)

    base_ts = 1_700_000_000_000

    # Left (trades): sparser timestamps
    left_ts = []
    left_prices = []
    trade_step = max(1, (right_rows * 100) // left_rows)
    for i in range(left_rows):
        left_ts.append(base_ts + i * trade_step)
        left_prices.append(round(100.0 + random.gauss(0, 2), 2))

    left_table = pa.table(
        {
            "timestamp": pa.array(left_ts, type=pa.int64()),
            "trade_price": pa.array(left_prices, type=pa.float64()),
        }
    )
    pq.write_table(left_table, left_path)

    # Right (quotes): denser timestamps
    right_ts = []
    right_bids = []
    right_asks = []
    for i in range(right_rows):
        right_ts.append(base_ts + i * 100)
        bid = round(99.5 + random.gauss(0, 1), 2)
        right_bids.append(bid)
        right_asks.append(round(bid + random.uniform(0.01, 0.5), 2))

    right_table = pa.table(
        {
            "timestamp": pa.array(right_ts, type=pa.int64()),
            "bid": pa.array(right_bids, type=pa.float64()),
            "ask": pa.array(right_asks, type=pa.float64()),
        }
    )
    pq.write_table(right_table, right_path)


# ---------------------------------------------------------------------------
# Benchmark 1: ASOF join
# ---------------------------------------------------------------------------


def bench_asof_join(
    left_rows: int,
    right_rows: int,
    temp_dir: str,
    warmup: int = WARMUP,
    iterations: int = ITERATIONS,
) -> list[BenchmarkResult]:
    """Benchmark ASOF join with backward, forward, nearest directions."""
    left_path = os.path.join(temp_dir, f"asof_left_{left_rows}.parquet")
    right_path = os.path.join(temp_dir, f"asof_right_{right_rows}.parquet")
    generate_asof_data(left_path, right_path, left_rows, right_rows)

    results = []

    for direction in ["backward", "forward", "nearest"]:

        def run_ltseq(dir_=direction):
            left = LTSeq.read_parquet(left_path).sort("timestamp")
            right = LTSeq.read_parquet(right_path).sort("timestamp")
            result = left.asof_join(
                right,
                on=lambda a, b: a.timestamp >= b.timestamp,
                direction=dir_,
            )
            _ = len(result)

        ltseq_ms = run_timed(run_ltseq, warmup, iterations)

        duckdb_ms = None
        if HAS_DUCKDB:

            def run_duckdb(dir_=direction):
                con = duckdb.connect()
                # DuckDB ASOF join syntax
                con.execute(
                    f"""
                    SELECT l.*, r.bid, r.ask
                    FROM read_parquet('{left_path}') l
                    ASOF JOIN read_parquet('{right_path}') r
                    ON l.timestamp >= r.timestamp
                    """
                )
                _ = con.fetchall()

            # DuckDB only supports backward ASOF (>=), skip forward/nearest comparison
            if direction == "backward":
                duckdb_ms = run_timed(run_duckdb, warmup, iterations)

        result = BenchmarkResult(
            name=f"asof_{direction}",
            rows=left_rows,
            ltseq_ms=ltseq_ms,
            duckdb_ms=duckdb_ms,
            extra={"right_rows": right_rows, "direction": direction},
        )
        results.append(result)

    return results


# ---------------------------------------------------------------------------
# Benchmark 2: Rolling window aggregates
# ---------------------------------------------------------------------------


def bench_rolling(
    num_rows: int,
    temp_dir: str,
    warmup: int = WARMUP,
    iterations: int = ITERATIONS,
) -> list[BenchmarkResult]:
    """Benchmark rolling window sum and avg at various window sizes."""
    parquet_path = os.path.join(temp_dir, f"ts_{num_rows}.parquet")
    if not os.path.exists(parquet_path):
        generate_timeseries_parquet(parquet_path, num_rows)

    results = []

    for window_size in WINDOW_SIZES:
        if window_size >= num_rows:
            continue  # skip meaningless window sizes

        # Rolling sum
        def run_ltseq_sum(ws=window_size):
            t = LTSeq.read_parquet(parquet_path).sort("timestamp")
            result = t.derive(
                rolling_sum=lambda r, w=ws: r.price.rolling(w).sum()
            )
            _ = len(result)

        ltseq_sum_ms = run_timed(run_ltseq_sum, warmup, iterations)

        duckdb_sum_ms = None
        if HAS_DUCKDB:

            def run_duckdb_sum(ws=window_size):
                con = duckdb.connect()
                con.execute(
                    f"""
                    SELECT *,
                      SUM(price) OVER (
                        ORDER BY timestamp
                        ROWS BETWEEN {ws - 1} PRECEDING AND CURRENT ROW
                      ) as rolling_sum
                    FROM read_parquet('{parquet_path}')
                    """
                )
                _ = con.fetchall()

            duckdb_sum_ms = run_timed(run_duckdb_sum, warmup, iterations)

        results.append(
            BenchmarkResult(
                name=f"rolling_sum_w{window_size}",
                rows=num_rows,
                ltseq_ms=ltseq_sum_ms,
                duckdb_ms=duckdb_sum_ms,
                extra={"window_size": window_size, "agg": "sum"},
            )
        )

        # Rolling avg (mean)
        def run_ltseq_avg(ws=window_size):
            t = LTSeq.read_parquet(parquet_path).sort("timestamp")
            result = t.derive(
                rolling_avg=lambda r, w=ws: r.price.rolling(w).mean()
            )
            _ = len(result)

        ltseq_avg_ms = run_timed(run_ltseq_avg, warmup, iterations)

        duckdb_avg_ms = None
        if HAS_DUCKDB:

            def run_duckdb_avg(ws=window_size):
                con = duckdb.connect()
                con.execute(
                    f"""
                    SELECT *,
                      AVG(price) OVER (
                        ORDER BY timestamp
                        ROWS BETWEEN {ws - 1} PRECEDING AND CURRENT ROW
                      ) as rolling_avg
                    FROM read_parquet('{parquet_path}')
                    """
                )
                _ = con.fetchall()

            duckdb_avg_ms = run_timed(run_duckdb_avg, warmup, iterations)

        results.append(
            BenchmarkResult(
                name=f"rolling_avg_w{window_size}",
                rows=num_rows,
                ltseq_ms=ltseq_avg_ms,
                duckdb_ms=duckdb_avg_ms,
                extra={"window_size": window_size, "agg": "avg"},
            )
        )

    return results


# ---------------------------------------------------------------------------
# Benchmark 3: Cumulative sum (prefix sum)
# ---------------------------------------------------------------------------


def bench_cumsum(
    num_rows: int,
    temp_dir: str,
    warmup: int = WARMUP,
    iterations: int = ITERATIONS,
) -> list[BenchmarkResult]:
    """Benchmark cumulative sum at varying scales."""
    parquet_path = os.path.join(temp_dir, f"ts_{num_rows}.parquet")
    if not os.path.exists(parquet_path):
        generate_timeseries_parquet(parquet_path, num_rows)

    # LTSeq cumulative sum
    def run_ltseq():
        t = LTSeq.read_parquet(parquet_path).sort("timestamp")
        result = t.cum_sum("volume")
        _ = len(result)

    ltseq_ms = run_timed(run_ltseq, warmup, iterations)

    duckdb_ms = None
    if HAS_DUCKDB:

        def run_duckdb():
            con = duckdb.connect()
            con.execute(
                f"""
                SELECT *,
                  SUM(volume) OVER (
                    ORDER BY timestamp
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                  ) as volume_cumsum
                FROM read_parquet('{parquet_path}')
                """
            )
            _ = con.fetchall()

        duckdb_ms = run_timed(run_duckdb, warmup, iterations)

    return [
        BenchmarkResult(
            name="cumsum",
            rows=num_rows,
            ltseq_ms=ltseq_ms,
            duckdb_ms=duckdb_ms,
        )
    ]


# ---------------------------------------------------------------------------
# Benchmark 4: Shift (lag/lead)
# ---------------------------------------------------------------------------


def bench_shift(
    num_rows: int,
    temp_dir: str,
    warmup: int = WARMUP,
    iterations: int = ITERATIONS,
) -> list[BenchmarkResult]:
    """Benchmark shift/lag/lead at varying scales."""
    parquet_path = os.path.join(temp_dir, f"ts_{num_rows}.parquet")
    if not os.path.exists(parquet_path):
        generate_timeseries_parquet(parquet_path, num_rows)

    results = []

    # Lag (shift forward by 1)
    def run_ltseq_lag():
        t = LTSeq.read_parquet(parquet_path).sort("timestamp")
        result = t.derive(prev_price=lambda r: r.price.shift(1))
        _ = len(result)

    ltseq_lag_ms = run_timed(run_ltseq_lag, warmup, iterations)

    duckdb_lag_ms = None
    if HAS_DUCKDB:

        def run_duckdb_lag():
            con = duckdb.connect()
            con.execute(
                f"""
                SELECT *,
                  LAG(price, 1) OVER (ORDER BY timestamp) as prev_price
                FROM read_parquet('{parquet_path}')
                """
            )
            _ = con.fetchall()

        duckdb_lag_ms = run_timed(run_duckdb_lag, warmup, iterations)

    results.append(
        BenchmarkResult(
            name="lag_1",
            rows=num_rows,
            ltseq_ms=ltseq_lag_ms,
            duckdb_ms=duckdb_lag_ms,
            extra={"shift": 1},
        )
    )

    # Lead (shift backward by 1)
    def run_ltseq_lead():
        t = LTSeq.read_parquet(parquet_path).sort("timestamp")
        result = t.derive(next_price=lambda r: r.price.shift(-1))
        _ = len(result)

    ltseq_lead_ms = run_timed(run_ltseq_lead, warmup, iterations)

    duckdb_lead_ms = None
    if HAS_DUCKDB:

        def run_duckdb_lead():
            con = duckdb.connect()
            con.execute(
                f"""
                SELECT *,
                  LEAD(price, 1) OVER (ORDER BY timestamp) as next_price
                FROM read_parquet('{parquet_path}')
                """
            )
            _ = con.fetchall()

        duckdb_lead_ms = run_timed(run_duckdb_lead, warmup, iterations)

    results.append(
        BenchmarkResult(
            name="lead_1",
            rows=num_rows,
            ltseq_ms=ltseq_lead_ms,
            duckdb_ms=duckdb_lead_ms,
            extra={"shift": -1},
        )
    )

    # Diff (price - previous price)
    def run_ltseq_diff():
        t = LTSeq.read_parquet(parquet_path).sort("timestamp")
        result = t.derive(price_diff=lambda r: r.price.diff(1))
        _ = len(result)

    ltseq_diff_ms = run_timed(run_ltseq_diff, warmup, iterations)

    duckdb_diff_ms = None
    if HAS_DUCKDB:

        def run_duckdb_diff():
            con = duckdb.connect()
            con.execute(
                f"""
                SELECT *,
                  price - LAG(price, 1) OVER (ORDER BY timestamp) as price_diff
                FROM read_parquet('{parquet_path}')
                """
            )
            _ = con.fetchall()

        duckdb_diff_ms = run_timed(run_duckdb_diff, warmup, iterations)

    results.append(
        BenchmarkResult(
            name="diff_1",
            rows=num_rows,
            ltseq_ms=ltseq_diff_ms,
            duckdb_ms=duckdb_diff_ms,
            extra={"shift": 1},
        )
    )

    return results


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------


def print_results_table(title: str, results: list[BenchmarkResult]) -> None:
    """Print a formatted table of benchmark results."""
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}")

    # Header
    header = f"{'Name':<30} {'Rows':>10} {'LTSeq(ms)':>12}"
    if any(r.duckdb_ms is not None for r in results):
        header += f" {'DuckDB(ms)':>12} {'Speedup':>8}"
    if any(r.gpu_ms is not None for r in results):
        header += f" {'GPU(ms)':>12}"
    print(header)
    print("-" * len(header))

    for r in results:
        line = f"{r.name:<30} {r.rows:>10,} {r.ltseq_ms:>12.1f}"
        if r.duckdb_ms is not None:
            speedup = r.duckdb_ms / r.ltseq_ms if r.ltseq_ms > 0 else float("inf")
            line += f" {r.duckdb_ms:>12.1f} {speedup:>7.2f}x"
        elif any(r2.duckdb_ms is not None for r2 in results):
            line += f" {'N/A':>12} {'N/A':>8}"
        if r.gpu_ms is not None:
            line += f" {r.gpu_ms:>12.1f}"
        elif any(r2.gpu_ms is not None for r2 in results):
            line += f" {'N/A':>12}"
        print(line)

    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="LTSeq time-series benchmarks")
    parser.add_argument(
        "--sample", action="store_true", help="Use smaller data for quick validation"
    )
    parser.add_argument(
        "--test",
        choices=["asof", "rolling", "cumsum", "shift"],
        help="Run only a specific test",
    )
    parser.add_argument("--warmup", type=int, default=WARMUP, help="Warmup iterations")
    parser.add_argument(
        "--iterations", type=int, default=ITERATIONS, help="Timed iterations"
    )
    args = parser.parse_args()

    sizes = SAMPLE_SIZES if args.sample else FULL_SIZES
    tests_to_run = [args.test] if args.test else ["asof", "rolling", "cumsum", "shift"]

    all_results = {}

    print(f"\nLTSeq Time-Series Benchmarks")
    print(f"Mode: {'sample' if args.sample else 'full'}")
    print(f"DuckDB available: {HAS_DUCKDB}")
    print(f"Warmup: {args.warmup}, Iterations: {args.iterations}")

    with tempfile.TemporaryDirectory() as temp_dir:

        # -- ASOF join --
        if "asof" in tests_to_run:
            print(f"\n--- ASOF Join ---")
            asof_results = []
            for left_rows, right_rows in sizes["asof"]:
                print(
                    f"  Generating {left_rows:,} x {right_rows:,} row ASOF data..."
                )
                batch = bench_asof_join(
                    left_rows, right_rows, temp_dir, args.warmup, args.iterations
                )
                asof_results.extend(batch)
                for r in batch:
                    ddb = f"  duckdb={r.duckdb_ms:.1f}ms" if r.duckdb_ms else ""
                    print(
                        f"    {r.name}: ltseq={r.ltseq_ms:.1f}ms{ddb}"
                    )
            print_results_table("ASOF Join", asof_results)
            all_results["asof"] = [r.to_dict() for r in asof_results]

        # -- Rolling window --
        if "rolling" in tests_to_run:
            print(f"\n--- Rolling Window ---")
            rolling_results = []
            for num_rows in sizes["rolling"]:
                print(f"  Benchmarking {num_rows:,} rows...")
                batch = bench_rolling(
                    num_rows, temp_dir, args.warmup, args.iterations
                )
                rolling_results.extend(batch)
                for r in batch:
                    ddb = f"  duckdb={r.duckdb_ms:.1f}ms" if r.duckdb_ms else ""
                    print(
                        f"    {r.name}: ltseq={r.ltseq_ms:.1f}ms{ddb}"
                    )
            print_results_table("Rolling Window", rolling_results)
            all_results["rolling"] = [r.to_dict() for r in rolling_results]

        # -- Cumulative sum --
        if "cumsum" in tests_to_run:
            print(f"\n--- Cumulative Sum ---")
            cumsum_results = []
            for num_rows in sizes["cumsum"]:
                print(f"  Benchmarking {num_rows:,} rows...")
                batch = bench_cumsum(
                    num_rows, temp_dir, args.warmup, args.iterations
                )
                cumsum_results.extend(batch)
                for r in batch:
                    ddb = f"  duckdb={r.duckdb_ms:.1f}ms" if r.duckdb_ms else ""
                    print(
                        f"    {r.name}: ltseq={r.ltseq_ms:.1f}ms{ddb}"
                    )
            print_results_table("Cumulative Sum", cumsum_results)
            all_results["cumsum"] = [r.to_dict() for r in cumsum_results]

        # -- Shift/Lag/Lead --
        if "shift" in tests_to_run:
            print(f"\n--- Shift / Lag / Lead ---")
            shift_results = []
            for num_rows in sizes["shift"]:
                print(f"  Benchmarking {num_rows:,} rows...")
                batch = bench_shift(
                    num_rows, temp_dir, args.warmup, args.iterations
                )
                shift_results.extend(batch)
                for r in batch:
                    ddb = f"  duckdb={r.duckdb_ms:.1f}ms" if r.duckdb_ms else ""
                    print(
                        f"    {r.name}: ltseq={r.ltseq_ms:.1f}ms{ddb}"
                    )
            print_results_table("Shift / Lag / Lead", shift_results)
            all_results["shift"] = [r.to_dict() for r in shift_results]

    # Save results to JSON
    results_path = os.path.join(BENCHMARKS_DIR, "timeseries_results.json")
    with open(results_path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nResults saved to {results_path}")


if __name__ == "__main__":
    main()
