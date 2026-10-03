#!/usr/bin/env python3
"""
LTSeq Core Benchmarks

Measures performance of key operations at different scales.
Run with: uv run python benchmarks/bench_core.py

Every benchmark belongs to one group, and the group fixes what its timed
region contains:

- ``operator``: the input CSV is parsed and held in memory before timing; the
  timed region builds the operator's plan and executes it.
- ``io``: the timed region is the read (every column decoded) or the write
  (from an in-memory input).
- ``end_to_end``: the timed region is a whole pipeline from the CSV file,
  scan and parse included. These benchmarks are named ``e2e_csv_*``.

Each timed call returns its result to the harness, which executes it with
``collect()`` (see ``consume``).

Results are printed as one table per group and saved to benchmarks/results.json
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

OPERATOR = "operator"
IO = "io"
END_TO_END = "end_to_end"

# Print order and table headings.
GROUP_DESCRIPTIONS = {
    OPERATOR: "Operators on in-memory input (CSV parsed before timing)",
    IO: "I/O (reads decode every column; writes start from memory)",
    END_TO_END: "End to end from CSV (scan + parse + operator)",
}


@dataclass
class BenchmarkResult:
    name: str
    group: str
    rows: int
    time_ms: float
    rows_per_sec: float

    def to_dict(self):
        return {
            "name": self.name,
            "group": self.group,
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


def consume(result: object) -> None:
    """Execute what a timed call returned, so the timed region includes the work.

    A table-returning LTSeq call only builds a plan; ``collect()`` executes it
    and produces every output column. ``len()`` is not enough: its
    ``count(*)`` lets DataFusion drop derived columns and sorts that cannot
    change the row count, and answer a Parquet row count from file metadata.

    ``None`` and integers come from terminal calls (writes, counts) that have
    already executed. Anything else, such as a NestedTable, is a lazy wrapper
    this harness cannot execute, and is rejected instead of being timed as a
    no-op.
    """
    if isinstance(result, LTSeq):
        result.collect()
    elif result is not None and not isinstance(result, int):
        raise TypeError(
            f"benchmark op returned {type(result).__name__}; return an LTSeq "
            "(the harness executes it) or the result of a terminal call"
        )


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


# Query shapes shared by an operator benchmark and its end-to-end twin, so the
# two differ only in where the input comes from.


def filter_query(t: LTSeq) -> LTSeq:
    """Keep ~50% of rows."""
    return t.filter(lambda r: r.value > 500)


def chain_query(t: LTSeq) -> LTSeq:
    """A typical filter -> derive -> select -> filter workflow."""
    return (
        t.filter(lambda r: r.value > 100)
        .derive(doubled=lambda r: r.value * 2)
        .select("id", "value", "doubled", "category")
        .filter(lambda r: r.category == "A")
    )


class Benchmarks:
    """Collection of benchmark functions."""

    def __init__(self, temp_dir: str):
        self.temp_dir = temp_dir
        self.results: list[BenchmarkResult] = []
        self._tables: dict[int, LTSeq] = {}

    def run_benchmark(
        self,
        name: str,
        rows: int,
        op: Callable[[], object],
        *,
        group: str,
        warmup: int = 1,
        iterations: int = 3,
    ) -> BenchmarkResult:
        """Time ``op()`` plus the execution of what it returns.

        ``op`` holds only the measured work. The caller prepares the inputs it
        closes over before calling this; operator benchmarks take theirs from
        ``table()``, already in memory.
        """
        # Warmup
        for _ in range(warmup):
            consume(op())

        # Timed runs
        times = []
        for _ in range(iterations):
            with timer() as t:
                consume(op())
            times.append(t["elapsed_ms"])

        avg_time = sum(times) / len(times)
        rows_per_sec = (rows / avg_time) * 1000 if avg_time > 0 else 0

        result = BenchmarkResult(
            name=name,
            group=group,
            rows=rows,
            time_ms=avg_time,
            rows_per_sec=rows_per_sec,
        )
        self.results.append(result)
        return result

    # =========================================================================
    # Inputs
    # =========================================================================

    def csv_path(self, num_rows: int) -> str:
        """Path of the synthetic CSV with ``num_rows`` rows, generated once."""
        path = os.path.join(self.temp_dir, f"data_{num_rows}.csv")
        if not os.path.exists(path):
            generate_test_csv(path, num_rows)
        return path

    def table(self, num_rows: int) -> LTSeq:
        """The synthetic table, parsed and held in memory.

        ``read_csv`` is a lazy scan: a table that is not collected parses the
        whole file again every time a plan built on it executes, which would
        put CSV parsing inside the operator's timed region.
        """
        if num_rows not in self._tables:
            self._tables[num_rows] = LTSeq.read_csv(self.csv_path(num_rows)).collect()
        return self._tables[num_rows]

    def sorted_table(self, num_rows: int, *keys: str) -> LTSeq:
        """``table()`` physically sorted by ``keys``, with that order declared.

        ``collect()`` alone keeps the sort keys but does not tell DataFusion
        the rows are ordered, so every window over them would sort again
        inside the timed region. ``assume_sorted()`` passes the order on, which
        removes that sort at the sizes these benchmarks use; at 1M rows
        DataFusion splits the in-memory table and sorts anyway (#212).
        """
        return self.table(num_rows).sort(*keys).collect().assume_sorted(*keys)

    # =========================================================================
    # Benchmark: Filter
    # =========================================================================

    def bench_filter(self, num_rows: int) -> BenchmarkResult:
        """Benchmark filter operation."""
        t = self.table(num_rows)
        return self.run_benchmark(
            f"filter_{num_rows}", num_rows, lambda: filter_query(t), group=OPERATOR
        )

    def bench_filter_complex(self, num_rows: int) -> BenchmarkResult:
        """Benchmark filter with complex predicate."""
        t = self.table(num_rows)

        def run():
            return t.filter(
                lambda r: (r.value > 200) & (r.value < 800) & (r.category == "A")
            )

        return self.run_benchmark(
            f"filter_complex_{num_rows}", num_rows, run, group=OPERATOR
        )

    # =========================================================================
    # Benchmark: Derive
    # =========================================================================

    def bench_derive(self, num_rows: int) -> BenchmarkResult:
        """Benchmark derive (add computed column)."""
        t = self.table(num_rows)

        def run():
            return t.derive(doubled=lambda r: r.value * 2)

        return self.run_benchmark(f"derive_{num_rows}", num_rows, run, group=OPERATOR)

    def bench_derive_multi(self, num_rows: int) -> BenchmarkResult:
        """Benchmark derive with multiple columns."""
        t = self.table(num_rows)

        def run():
            return t.derive(
                doubled=lambda r: r.value * 2,
                tripled=lambda r: r.value * 3,
                ratio=lambda r: r.amount / r.value,
            )

        return self.run_benchmark(
            f"derive_multi_{num_rows}", num_rows, run, group=OPERATOR
        )

    # =========================================================================
    # Benchmark: Join
    # =========================================================================

    def bench_join(self, left_rows: int, right_rows: int) -> BenchmarkResult:
        """Benchmark join operation."""
        right_path = os.path.join(
            self.temp_dir, f"join_right_{right_rows}_{left_rows}.csv"
        )
        generate_join_csv(right_path, right_rows, key_range=left_rows)

        left = self.table(left_rows)
        right = LTSeq.read_csv(right_path).collect()

        def run():
            return left.join(right, on=lambda l, r: l.id == r.foreign_key)

        total_rows = left_rows + right_rows
        return self.run_benchmark(
            f"join_{left_rows}x{right_rows}", total_rows, run, group=OPERATOR
        )

    # =========================================================================
    # Benchmark: Window Functions
    # =========================================================================

    def bench_window_lag(self, num_rows: int) -> BenchmarkResult:
        """Benchmark LAG window function."""
        t = self.sorted_table(num_rows, "id")

        def run():
            return t.derive(prev_value=lambda r: r.value.shift(1))

        return self.run_benchmark(
            f"window_lag_{num_rows}", num_rows, run, group=OPERATOR
        )

    def bench_window_cumsum(self, num_rows: int) -> BenchmarkResult:
        """Benchmark cumulative sum window function."""
        t = self.sorted_table(num_rows, "id")

        def run():
            return t.derive(running_total=lambda r: r.value.cum_sum())

        return self.run_benchmark(
            f"window_cumsum_{num_rows}", num_rows, run, group=OPERATOR
        )

    # =========================================================================
    # Benchmark: Group Operations
    # =========================================================================

    def bench_group_agg(self, num_rows: int) -> BenchmarkResult:
        """Benchmark agg (group + aggregate)."""
        t = self.table(num_rows)

        def run():
            return t.agg(
                by=lambda r: r.category,
                total=lambda g: g.value.sum(),
                avg=lambda g: g.amount.avg(),
                cnt=lambda g: g.id.count(),
            )

        return self.run_benchmark(
            f"group_agg_{num_rows}", num_rows, run, group=OPERATOR
        )

    # =========================================================================
    # Benchmark: Chained Operations
    # =========================================================================

    def bench_chain(self, num_rows: int) -> BenchmarkResult:
        """Benchmark typical chained workflow."""
        t = self.table(num_rows)
        return self.run_benchmark(
            f"chain_{num_rows}", num_rows, lambda: chain_query(t), group=OPERATOR
        )

    # =========================================================================
    # Benchmark: Sort
    # =========================================================================

    def bench_sort(self, num_rows: int) -> BenchmarkResult:
        """Benchmark sort operation."""
        t = self.table(num_rows)
        return self.run_benchmark(
            f"sort_{num_rows}", num_rows, lambda: t.sort("value"), group=OPERATOR
        )

    def bench_sort_multi(self, num_rows: int) -> BenchmarkResult:
        """Benchmark sort by multiple keys."""
        t = self.table(num_rows)
        return self.run_benchmark(
            f"sort_multi_{num_rows}",
            num_rows,
            lambda: t.sort("category", "value"),
            group=OPERATOR,
        )

    # =========================================================================
    # Benchmark: Group Ordered (sequential grouping)
    # =========================================================================

    def bench_group_ordered(self, num_rows: int) -> BenchmarkResult:
        """Benchmark group_ordered (sequential run-length grouping)."""
        t = self.sorted_table(num_rows, "category")

        def run():
            # Derive a group-level count (broadcast to all rows in each run)
            return t.group_ordered(lambda r: r.category).derive(
                lambda g: {"group_size": g.count()}
            )

        return self.run_benchmark(
            f"group_ordered_{num_rows}", num_rows, run, group=OPERATOR
        )

    # =========================================================================
    # Benchmark: Search First
    # =========================================================================

    def bench_search_first(self, num_rows: int) -> BenchmarkResult:
        """Benchmark search_first (find first matching row).

        search_first returns a lazy filter + limit(1) plan; the harness
        executes it. The only match is the last id, so the scan cannot stop
        early.
        """
        t = self.table(num_rows)
        last_id = num_rows - 1

        def run():
            return t.search_first(lambda r: r.id == last_id)

        return self.run_benchmark(
            f"search_first_{num_rows}", num_rows, run, group=OPERATOR
        )

    # =========================================================================
    # Benchmark: Mutation (insert, delete, update)
    # =========================================================================

    def bench_mutation_insert(self, num_rows: int) -> BenchmarkResult:
        """Benchmark insert (copy-on-write row insert)."""
        t = self.table(num_rows)
        row = {"id": -1, "value": 0, "category": "X", "amount": 0.0, "name": "New"}

        def run():
            return t.insert(0, row)

        return self.run_benchmark(
            f"mutation_insert_{num_rows}", num_rows, run, group=OPERATOR
        )

    def bench_mutation_delete(self, num_rows: int) -> BenchmarkResult:
        """Benchmark delete by predicate (copy-on-write row delete)."""
        t = self.table(num_rows)

        def run():
            # Delete ~20% of rows
            return t.delete(lambda r: r.value < 200)

        return self.run_benchmark(
            f"mutation_delete_{num_rows}", num_rows, run, group=OPERATOR
        )

    def bench_mutation_update(self, num_rows: int) -> BenchmarkResult:
        """Benchmark update by predicate (copy-on-write conditional update)."""
        t = self.table(num_rows)

        def run():
            # Update ~50% of rows
            return t.update(lambda r: r.value > 500, category="Z")

        return self.run_benchmark(
            f"mutation_update_{num_rows}", num_rows, run, group=OPERATOR
        )

    # =========================================================================
    # Benchmark: I/O
    # =========================================================================

    def bench_read_csv(self, num_rows: int) -> BenchmarkResult:
        """Benchmark CSV reading (all columns parsed)."""
        path = self.csv_path(num_rows)
        return self.run_benchmark(
            f"read_csv_{num_rows}", num_rows, lambda: LTSeq.read_csv(path), group=IO
        )

    def bench_write_parquet(self, num_rows: int) -> BenchmarkResult:
        """Benchmark Parquet write from an in-memory table."""
        t = self.table(num_rows)
        pq_path = os.path.join(self.temp_dir, f"pq_out_{num_rows}.parquet")
        return self.run_benchmark(
            f"write_parquet_{num_rows}",
            num_rows,
            lambda: t.write_parquet(pq_path),
            group=IO,
        )

    def bench_read_parquet(self, num_rows: int) -> BenchmarkResult:
        """Benchmark Parquet reading (all columns decoded)."""
        pq_path = os.path.join(self.temp_dir, f"pq_in_{num_rows}.parquet")
        self.table(num_rows).write_parquet(pq_path)
        return self.run_benchmark(
            f"read_parquet_{num_rows}",
            num_rows,
            lambda: LTSeq.read_parquet(pq_path),
            group=IO,
        )

    # =========================================================================
    # Benchmark: End to end (CSV scan + operator)
    # =========================================================================

    def bench_e2e_csv_filter(self, num_rows: int) -> BenchmarkResult:
        """Benchmark read_csv + filter, parsing included."""
        path = self.csv_path(num_rows)
        return self.run_benchmark(
            f"e2e_csv_filter_{num_rows}",
            num_rows,
            lambda: filter_query(LTSeq.read_csv(path)),
            group=END_TO_END,
        )

    def bench_e2e_csv_chain(self, num_rows: int) -> BenchmarkResult:
        """Benchmark read_csv + chained workflow, parsing included."""
        path = self.csv_path(num_rows)
        return self.run_benchmark(
            f"e2e_csv_chain_{num_rows}",
            num_rows,
            lambda: chain_query(LTSeq.read_csv(path)),
            group=END_TO_END,
        )


def run_suite(bench: Benchmarks, small: int, medium: int, large: int) -> None:
    """Run every benchmark at the given sizes."""
    # Filter benchmarks
    print("\n[1/12] Running filter benchmarks...")
    bench.bench_filter(small)
    bench.bench_filter(medium)
    bench.bench_filter(large)
    bench.bench_filter_complex(medium)

    # Derive benchmarks
    print("[2/12] Running derive benchmarks...")
    bench.bench_derive(small)
    bench.bench_derive(medium)
    bench.bench_derive(large)
    bench.bench_derive_multi(medium)

    # Join benchmarks
    print("[3/12] Running join benchmarks...")
    bench.bench_join(small, small)
    bench.bench_join(medium, small)
    bench.bench_join(medium, medium)

    # Window benchmarks
    print("[4/12] Running window benchmarks...")
    bench.bench_window_lag(small)
    bench.bench_window_lag(medium)
    bench.bench_window_cumsum(small)
    bench.bench_window_cumsum(medium)

    # Group benchmarks
    print("[5/12] Running group benchmarks...")
    bench.bench_group_agg(small)
    bench.bench_group_agg(medium)

    # Chain benchmarks
    print("[6/12] Running chain benchmarks...")
    bench.bench_chain(small)
    bench.bench_chain(medium)
    bench.bench_chain(large)

    # Sort benchmarks
    print("[7/12] Running sort benchmarks...")
    bench.bench_sort(small)
    bench.bench_sort(medium)
    bench.bench_sort(large)
    bench.bench_sort_multi(medium)

    # Group ordered benchmarks
    print("[8/12] Running group_ordered benchmarks...")
    bench.bench_group_ordered(small)
    bench.bench_group_ordered(medium)

    # Search first benchmarks
    print("[9/12] Running search_first benchmarks...")
    bench.bench_search_first(small)
    bench.bench_search_first(medium)

    # Mutation benchmarks
    print("[10/12] Running mutation benchmarks...")
    bench.bench_mutation_insert(small)
    bench.bench_mutation_insert(medium)
    bench.bench_mutation_delete(small)
    bench.bench_mutation_delete(medium)
    bench.bench_mutation_update(small)
    bench.bench_mutation_update(medium)

    # I/O benchmarks
    print("[11/12] Running CSV and Parquet I/O benchmarks...")
    bench.bench_read_csv(small)
    bench.bench_read_csv(medium)
    bench.bench_read_csv(large)
    bench.bench_write_parquet(small)
    bench.bench_write_parquet(medium)
    bench.bench_read_parquet(small)
    bench.bench_read_parquet(medium)

    # End-to-end benchmarks
    print("[12/12] Running end-to-end CSV benchmarks...")
    bench.bench_e2e_csv_filter(small)
    bench.bench_e2e_csv_filter(medium)
    bench.bench_e2e_csv_filter(large)
    bench.bench_e2e_csv_chain(small)
    bench.bench_e2e_csv_chain(medium)
    bench.bench_e2e_csv_chain(large)


def print_results(results: list[BenchmarkResult]) -> None:
    """Print results as one table per group."""
    print("\n" + "=" * 70)
    print("BENCHMARK RESULTS")

    for group, description in GROUP_DESCRIPTIONS.items():
        group_results = [r for r in results if r.group == group]
        if not group_results:
            continue

        print("=" * 70)
        print(description)
        print("-" * 70)
        print(f"{'Benchmark':<30} {'Rows':>10} {'Time (ms)':>12} {'Rows/sec':>15}")
        print("-" * 70)
        for r in group_results:
            rows_sec_str = f"{r.rows_per_sec:,.0f}"
            print(f"{r.name:<30} {r.rows:>10,} {r.time_ms:>12.2f} {rows_sec_str:>15}")

    print("=" * 70)


def collect_host_info() -> dict:
    """Collect hardware and software environment info."""
    import platform
    import subprocess

    info: dict = {
        "os": f"{platform.system()} {platform.release()}",
        "python": platform.python_version(),
    }

    # macOS: chip + memory via system_profiler
    if platform.system() == "Darwin":
        try:
            sp = subprocess.check_output(
                ["system_profiler", "SPHardwareDataType"], text=True, timeout=10
            )
            for line in sp.splitlines():
                if "Chip:" in line:
                    info["cpu"] = line.split(":", 1)[1].strip()
                elif "Total Number of Cores:" in line:
                    info["cores"] = line.split(":", 1)[1].strip()
                elif "Memory:" in line:
                    info["memory"] = line.split(":", 1)[1].strip()
        except Exception:
            pass
    else:
        info["cpu"] = platform.processor() or "unknown"

    # Git commit
    try:
        info["git_commit"] = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], text=True, timeout=5
        ).strip()
    except Exception:
        pass

    # Rust/cargo version
    try:
        rustc = subprocess.check_output(["rustc", "--version"], text=True, timeout=5).strip()
        info["rustc"] = rustc.split()[1] if len(rustc.split()) > 1 else rustc
    except Exception:
        pass

    return info


def save_results(results: list[BenchmarkResult], path: str) -> None:
    """Save results to JSON file."""
    data = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "host": collect_host_info(),
        "results": [r.to_dict() for r in results],
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\nResults saved to {path}")


def main():
    """Run all benchmarks."""
    print("LTSeq Performance Benchmarks")
    print("=" * 70)

    with tempfile.TemporaryDirectory() as temp_dir:
        bench = Benchmarks(temp_dir)
        run_suite(bench, small=10_000, medium=100_000, large=1_000_000)

        # Print results
        print_results(bench.results)

        # Save results
        results_path = Path(__file__).parent / "results.json"
        save_results(bench.results, str(results_path))


if __name__ == "__main__":
    main()
