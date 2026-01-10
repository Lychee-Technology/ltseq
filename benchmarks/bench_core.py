#!/usr/bin/env python3
"""
LTSeq Core Benchmarks

Measures performance of key operations at different scales.
Run with: uv run python benchmarks/bench_core.py

Results are printed as a table and optionally saved to benchmarks/results.json
"""

import json
import os
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

# Add py-ltseq to path
sys.path.insert(0, str(Path(__file__).parent.parent / "py-ltseq"))

from ltseq import LTSeq  # type: ignore


@dataclass
class BenchmarkResult:
    name: str
    rows: int
    time_ms: float
    rows_per_sec: float

    def to_dict(self):
        return {
            "name": self.name,
            "rows": self.rows,
            "time_ms": round(self.time_ms, 2),
            "rows_per_sec": round(self.rows_per_sec, 0),
        }


@contextmanager
def timer():
    """Context manager that yields elapsed time in milliseconds."""
    start = time.perf_counter()
    result: dict[str, float] = {"elapsed_ms": 0.0}
    yield result
    result["elapsed_ms"] = (time.perf_counter() - start) * 1000


def generate_test_csv(path: str, num_rows: int, num_cols: int = 5) -> None:
    """Generate a test CSV file with numeric and string columns."""
    import random

    random.seed(42)

    with open(path, "w") as f:
        # Header
        cols = ["id", "value", "category", "amount", "name"][:num_cols]
        f.write(",".join(cols) + "\n")

        # Data
        categories = ["A", "B", "C", "D", "E"]
        names = ["Alice", "Bob", "Charlie", "Diana", "Eve"]

        for i in range(num_rows):
            row = [
                str(i),  # id
                str(random.randint(1, 1000)),  # value
                random.choice(categories),  # category
                f"{random.uniform(0, 10000):.2f}",  # amount
                random.choice(names),  # name
            ][:num_cols]
            f.write(",".join(row) + "\n")


def generate_join_csv(path: str, num_rows: int, key_range: int) -> None:
    """Generate a CSV for join benchmarks with foreign key."""
    import random

    random.seed(43)

    with open(path, "w") as f:
        f.write("id,foreign_key,data\n")
        for i in range(num_rows):
            fk = random.randint(0, key_range - 1)
            f.write(f"{i},{fk},{random.randint(1, 100)}\n")


class Benchmarks:
    """Collection of benchmark functions."""

    def __init__(self, temp_dir: str):
        self.temp_dir = temp_dir
        self.results: list[BenchmarkResult] = []

    def run_benchmark(
        self,
        name: str,
        rows: int,
        fn: Callable[[], None],
        warmup: int = 1,
        iterations: int = 3,
    ) -> BenchmarkResult:
        """Run a benchmark with warmup and multiple iterations."""
        # Warmup
        for _ in range(warmup):
            fn()

        # Timed runs
        times = []
        for _ in range(iterations):
            with timer() as t:
                fn()
            times.append(t["elapsed_ms"])

        avg_time = sum(times) / len(times)
        rows_per_sec = (rows / avg_time) * 1000 if avg_time > 0 else 0

        result = BenchmarkResult(
            name=name,
            rows=rows,
            time_ms=avg_time,
            rows_per_sec=rows_per_sec,
        )
        self.results.append(result)
        return result

    # =========================================================================
    # Benchmark: Filter
    # =========================================================================

    def bench_filter(self, num_rows: int) -> BenchmarkResult:
        """Benchmark filter operation."""
        csv_path = os.path.join(self.temp_dir, f"filter_{num_rows}.csv")
        generate_test_csv(csv_path, num_rows)

        def run():
            t = LTSeq.read_csv(csv_path)
            # Filter ~50% of rows
            result = t.filter(lambda r: r.value > 500)
            _ = len(result)  # Force materialization

        return self.run_benchmark(f"filter_{num_rows}", num_rows, run)

    def bench_filter_complex(self, num_rows: int) -> BenchmarkResult:
        """Benchmark filter with complex predicate."""
        csv_path = os.path.join(self.temp_dir, f"filter_complex_{num_rows}.csv")
        generate_test_csv(csv_path, num_rows)

        def run():
            t = LTSeq.read_csv(csv_path)
            result = t.filter(
                lambda r: (r.value > 200) & (r.value < 800) & (r.category == "A")
            )
            _ = len(result)

        return self.run_benchmark(f"filter_complex_{num_rows}", num_rows, run)

    # =========================================================================
    # Benchmark: Derive
    # =========================================================================

    def bench_derive(self, num_rows: int) -> BenchmarkResult:
        """Benchmark derive (add computed column)."""
        csv_path = os.path.join(self.temp_dir, f"derive_{num_rows}.csv")
        generate_test_csv(csv_path, num_rows)

        def run():
            t = LTSeq.read_csv(csv_path)
            result = t.derive(doubled=lambda r: r.value * 2)
            _ = len(result)

        return self.run_benchmark(f"derive_{num_rows}", num_rows, run)

    def bench_derive_multi(self, num_rows: int) -> BenchmarkResult:
        """Benchmark derive with multiple columns."""
        csv_path = os.path.join(self.temp_dir, f"derive_multi_{num_rows}.csv")
        generate_test_csv(csv_path, num_rows)

        def run():
            t = LTSeq.read_csv(csv_path)
            result = t.derive(
                doubled=lambda r: r.value * 2,
                tripled=lambda r: r.value * 3,
                ratio=lambda r: r.amount / r.value,
            )
            _ = len(result)

        return self.run_benchmark(f"derive_multi_{num_rows}", num_rows, run)

    # =========================================================================
    # Benchmark: Join
    # =========================================================================

    def bench_join(self, left_rows: int, right_rows: int) -> BenchmarkResult:
        """Benchmark join operation."""
        left_path = os.path.join(self.temp_dir, f"join_left_{left_rows}.csv")
        right_path = os.path.join(self.temp_dir, f"join_right_{right_rows}.csv")

        generate_test_csv(left_path, left_rows)
        generate_join_csv(right_path, right_rows, key_range=left_rows)

        def run():
            left = LTSeq.read_csv(left_path)
            right = LTSeq.read_csv(right_path)
            result = left.join(
                right,
                on=lambda l, r: l.id == r.foreign_key,
            )
            _ = len(result)

        total_rows = left_rows + right_rows
        return self.run_benchmark(f"join_{left_rows}x{right_rows}", total_rows, run)

    # =========================================================================
    # Benchmark: Window Functions
    # =========================================================================

    def bench_window_lag(self, num_rows: int) -> BenchmarkResult:
        """Benchmark LAG window function."""
        csv_path = os.path.join(self.temp_dir, f"window_{num_rows}.csv")
        generate_test_csv(csv_path, num_rows)

        def run():
            t = LTSeq.read_csv(csv_path)
            result = t.sort(lambda r: r.id).derive(
                prev_value=lambda r: r.value.shift(1)
            )
            _ = len(result)

        return self.run_benchmark(f"window_lag_{num_rows}", num_rows, run)

    def bench_window_cumsum(self, num_rows: int) -> BenchmarkResult:
        """Benchmark cumulative sum window function."""
        csv_path = os.path.join(self.temp_dir, f"cumsum_{num_rows}.csv")
        generate_test_csv(csv_path, num_rows)

        def run():
            t = LTSeq.read_csv(csv_path)
            result = t.sort(lambda r: r.id).derive(
                running_total=lambda r: r.value.cum_sum()
            )
            _ = len(result)

        return self.run_benchmark(f"window_cumsum_{num_rows}", num_rows, run)

    # =========================================================================
    # Benchmark: Group Operations
    # =========================================================================

    def bench_group_agg(self, num_rows: int) -> BenchmarkResult:
        """Benchmark agg (group + aggregate)."""
        csv_path = os.path.join(self.temp_dir, f"group_{num_rows}.csv")
        generate_test_csv(csv_path, num_rows)

        def run():
            t = LTSeq.read_csv(csv_path)
            result = t.agg(
                by=lambda r: r.category,
                total=lambda g: g.value.sum(),
                avg=lambda g: g.amount.avg(),
                cnt=lambda g: g.id.count(),
            )
            _ = len(result)

        return self.run_benchmark(f"group_agg_{num_rows}", num_rows, run)

    # =========================================================================
    # Benchmark: Chained Operations
    # =========================================================================

    def bench_chain(self, num_rows: int) -> BenchmarkResult:
        """Benchmark typical chained workflow."""
        csv_path = os.path.join(self.temp_dir, f"chain_{num_rows}.csv")
        generate_test_csv(csv_path, num_rows)

        def run():
            t = LTSeq.read_csv(csv_path)
            result = (
                t.filter(lambda r: r.value > 100)
                .derive(doubled=lambda r: r.value * 2)
                .select("id", "value", "doubled", "category")
                .filter(lambda r: r.category == "A")
            )
            _ = len(result)

        return self.run_benchmark(f"chain_{num_rows}", num_rows, run)

    # =========================================================================
    # Benchmark: I/O
    # =========================================================================

    def bench_read_csv(self, num_rows: int) -> BenchmarkResult:
        """Benchmark CSV reading."""
        csv_path = os.path.join(self.temp_dir, f"read_{num_rows}.csv")
        generate_test_csv(csv_path, num_rows)

        def run():
            t = LTSeq.read_csv(csv_path)
            _ = len(t)  # Force full read

        return self.run_benchmark(f"read_csv_{num_rows}", num_rows, run)


def print_results(results: list[BenchmarkResult]) -> None:
    """Print results as a formatted table."""
    print("\n" + "=" * 70)
    print("BENCHMARK RESULTS")
    print("=" * 70)
    print(f"{'Benchmark':<30} {'Rows':>10} {'Time (ms)':>12} {'Rows/sec':>15}")
    print("-" * 70)

    for r in results:
        rows_sec_str = f"{r.rows_per_sec:,.0f}"
        print(f"{r.name:<30} {r.rows:>10,} {r.time_ms:>12.2f} {rows_sec_str:>15}")

    print("=" * 70)


def save_results(results: list[BenchmarkResult], path: str) -> None:
    """Save results to JSON file."""
    data = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "results": [r.to_dict() for r in results],
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\nResults saved to {path}")


def main():
    """Run all benchmarks."""
    print("LTSeq Performance Benchmarks")
    print("=" * 70)

    # Sizes to test
    small = 10_000
    medium = 100_000
    large = 1_000_000

    with tempfile.TemporaryDirectory() as temp_dir:
        bench = Benchmarks(temp_dir)

        # Filter benchmarks
        print("\n[1/7] Running filter benchmarks...")
        bench.bench_filter(small)
        bench.bench_filter(medium)
        bench.bench_filter(large)
        bench.bench_filter_complex(medium)

        # Derive benchmarks
        print("[2/7] Running derive benchmarks...")
        bench.bench_derive(small)
        bench.bench_derive(medium)
        bench.bench_derive(large)
        bench.bench_derive_multi(medium)

        # Join benchmarks
        print("[3/7] Running join benchmarks...")
        bench.bench_join(small, small)
        bench.bench_join(medium, small)
        bench.bench_join(medium, medium)

        # Window benchmarks
        print("[4/7] Running window benchmarks...")
        bench.bench_window_lag(small)
        bench.bench_window_lag(medium)
        bench.bench_window_cumsum(small)
        bench.bench_window_cumsum(medium)

        # Group benchmarks
        print("[5/7] Running group benchmarks...")
        bench.bench_group_agg(small)
        bench.bench_group_agg(medium)

        # Chain benchmarks
        print("[6/7] Running chain benchmarks...")
        bench.bench_chain(small)
        bench.bench_chain(medium)
        bench.bench_chain(large)

        # I/O benchmarks
        print("[7/7] Running I/O benchmarks...")
        bench.bench_read_csv(small)
        bench.bench_read_csv(medium)
        bench.bench_read_csv(large)

        # Print results
        print_results(bench.results)

        # Save results
        results_path = Path(__file__).parent / "results.json"
        save_results(bench.results, str(results_path))


if __name__ == "__main__":
    main()
