#!/usr/bin/env python3
"""
LTSeq GPU vs CPU Filter Benchmark
==================================

Measures the performance of CUDA-accelerated filter operations against
CPU-only execution on the ClickBench hits_sorted.parquet dataset (~100M rows).

Dimensions tested:
  1. Scale:       Filter throughput at 100K, 1M, 10M, 100M rows
  2. Selectivity: ~1%, 10%, 50%, 90% pass rates on full dataset
  3. Data type:   int32 (counterid) vs int64 (eventtime) at 50% selectivity

The GPU path is toggled via the LTSEQ_DISABLE_GPU environment variable
(checked at session creation time). Each mode loads the data from scratch
so that the session is created with the correct optimizer rules.

Usage:
    # Full benchmark (requires hits_sorted.parquet)
    uv run python benchmarks/bench_gpu.py

    # Quick validation with sample data
    uv run python benchmarks/bench_gpu.py --sample

    # Specific test only
    uv run python benchmarks/bench_gpu.py --test scale

    # Fewer iterations (faster)
    uv run python benchmarks/bench_gpu.py --iterations 1 --warmup 0

Requirements:
    maturin develop --release --features gpu
    pip install duckdb psutil
"""

import argparse
import gc
import json
import os
import platform
import subprocess
import sys
import time
from datetime import datetime

import duckdb
import psutil

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BENCHMARKS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BENCHMARKS_DIR, "data")
HITS_SORTED = os.path.join(DATA_DIR, "hits_sorted.parquet")
HITS_SAMPLE = os.path.join(DATA_DIR, "hits_sample.parquet")

WARMUP = 1
ITERATIONS = 3

# Scale test row counts
SCALE_SIZES = [100_000, 1_000_000, 10_000_000, 100_000_000]

# GPU-eligible ClickBench columns for benchmarking
#   int32: counterid, regionid, sendtiming, dnstiming, connecttiming
#   int64: eventtime, userid, watchid, paramprice
INT32_COL = "counterid"
INT64_COL = "eventtime"


# ---------------------------------------------------------------------------
# Measurement infrastructure (matches bench_vs.py conventions)
# ---------------------------------------------------------------------------


def benchmark(name, func, warmup=WARMUP, iterations=ITERATIONS):
    """Run function with warmup, return timing stats."""
    gc.collect()

    # Warmup
    for _ in range(warmup):
        func()

    gc.collect()
    process = psutil.Process(os.getpid())
    times = []
    peak_mem_delta = 0
    result = None

    for _ in range(iterations):
        gc.collect()
        mem_before = process.memory_info().rss / (1024 * 1024)
        t0 = time.perf_counter()
        result = func()
        t1 = time.perf_counter()
        mem_after = process.memory_info().rss / (1024 * 1024)
        times.append(t1 - t0)
        peak_mem_delta = max(peak_mem_delta, mem_after - mem_before)

    median_time = sorted(times)[len(times) // 2]
    print(
        f"  [{name}] median={median_time:.3f}s "
        f"times=[{', '.join(f'{t:.3f}' for t in times)}] "
        f"mem_delta={peak_mem_delta:.0f}MB"
    )
    return {
        "name": name,
        "median_s": round(median_time, 4),
        "times": [round(t, 4) for t in times],
        "mem_delta_mb": round(peak_mem_delta, 1),
        "result": str(result)[:200],
    }


# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------


def load_ltseq(data_file, gpu_enabled=True):
    """Load data into LTSeq with GPU enabled or disabled.

    The LTSEQ_DISABLE_GPU env var is checked at session creation time,
    so we must set it before constructing the LTSeq object.
    """
    if gpu_enabled:
        os.environ.pop("LTSEQ_DISABLE_GPU", None)
    else:
        os.environ["LTSEQ_DISABLE_GPU"] = "1"

    from ltseq import LTSeq

    t = LTSeq.read_parquet(data_file)
    return t


def compute_percentile(data_file, column, pct):
    """Use DuckDB to compute a percentile threshold for a column."""
    result = duckdb.sql(
        f"SELECT approx_quantile({column}, {pct}) FROM '{data_file}'"
    ).fetchone()[0]
    return result


def compute_row_count(data_file):
    """Get total row count via DuckDB."""
    return duckdb.sql(f"SELECT count(*) FROM '{data_file}'").fetchone()[0]


# ---------------------------------------------------------------------------
# GPU info collection
# ---------------------------------------------------------------------------


def get_gpu_info():
    """Collect GPU information via nvidia-smi."""
    info = {"available": False}
    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=name,memory.total,driver_version",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            parts = result.stdout.strip().split(", ")
            if len(parts) >= 3:
                info["available"] = True
                info["model"] = parts[0]
                info["vram_mb"] = int(parts[1])
                info["driver_version"] = parts[2]

        # Get CUDA version
        result = subprocess.run(
            ["nvidia-smi"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            for line in result.stdout.split("\n"):
                if "CUDA Version" in line:
                    cuda_ver = line.split("CUDA Version:")[1].strip().split()[0]
                    info["cuda_version"] = cuda_ver
                    break
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return info


def get_system_info():
    """Collect system + GPU information."""
    from ltseq import gpu_available

    info = {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "ram_gb": round(psutil.virtual_memory().total / (1024**3), 1),
        "python_version": platform.python_version(),
        "gpu": get_gpu_info(),
        "ltseq_gpu_available": gpu_available(),
    }
    return info


# ---------------------------------------------------------------------------
# Test 1: Scale — filter throughput at different row counts
# ---------------------------------------------------------------------------


def run_scale_test(data_file, warmup, iterations, total_rows):
    """Filter counterid > threshold (~50% selectivity) at different scales."""
    print("\n" + "=" * 70)
    print("TEST 1: SCALE — Filter throughput at different row counts")
    print("=" * 70)

    # Compute 50th percentile threshold for counterid
    threshold = compute_percentile(data_file, INT32_COL, 0.5)
    print(f"  Column: {INT32_COL} (int32), threshold: {threshold} (~50% selectivity)")

    results = []

    for n_rows in SCALE_SIZES:
        # Allow sizes within 10% of total (e.g. 100M request vs 99.9M actual)
        if n_rows > total_rows * 1.1:
            print(f"\n  Skipping {n_rows:,} rows (dataset has {total_rows:,})")
            continue

        # Use full dataset for the largest size that fits
        use_full = n_rows >= total_rows * 0.9  # e.g. 100M request vs 99.9M actual
        effective_rows = total_rows if use_full else n_rows

        print(f"\n  --- {effective_rows:,} rows{' (full dataset)' if use_full else ''} ---")

        def bench(name, func):
            return benchmark(name, func, warmup=warmup, iterations=iterations)

        # GPU run
        t_gpu = load_ltseq(data_file, gpu_enabled=True)
        t_gpu_sliced = t_gpu if use_full else t_gpu.slice(0, n_rows)
        gpu_result = bench(
            f"GPU {effective_rows:>11,}",
            lambda: t_gpu_sliced.filter(lambda r: r.counterid > threshold).count(),
        )

        # CPU run
        t_cpu = load_ltseq(data_file, gpu_enabled=False)
        t_cpu_sliced = t_cpu if use_full else t_cpu.slice(0, n_rows)
        cpu_result = bench(
            f"CPU {effective_rows:>11,}",
            lambda: t_cpu_sliced.filter(lambda r: r.counterid > threshold).count(),
        )

        # Validate correctness (only on full dataset — sliced subsets may
        # differ due to non-deterministic multi-partition ordering)
        if use_full:
            gpu_count = t_gpu_sliced.filter(lambda r: r.counterid > threshold).count()
            cpu_count = t_cpu_sliced.filter(lambda r: r.counterid > threshold).count()
            match = gpu_count == cpu_count
            if match:
                print(f"  Validation: PASS (both = {gpu_count:,})")
            else:
                print(f"  Validation: FAIL (GPU={gpu_count:,}, CPU={cpu_count:,})")
        else:
            match = None  # Cannot validate on sliced subsets
            print("  Validation: SKIP (sliced subset, partition order may differ)")

        speedup = cpu_result["median_s"] / gpu_result["median_s"] if gpu_result["median_s"] > 0 else 0
        print(f"  Speedup: {speedup:.2f}x")

        results.append({
            "n_rows": effective_rows,
            "gpu": gpu_result,
            "cpu": cpu_result,
            "speedup": round(speedup, 3),
            "correct": match,
        })

    return results


# ---------------------------------------------------------------------------
# Test 2: Selectivity — impact of pass rate on GPU performance
# ---------------------------------------------------------------------------


def run_selectivity_test(data_file, warmup, iterations, total_rows):
    """Filter eventtime > threshold at different selectivities on full dataset."""
    print("\n" + "=" * 70)
    print("TEST 2: SELECTIVITY — Impact of pass rate on GPU performance")
    print("=" * 70)

    # Selectivity targets: ~1%, 10%, 50%, 90%
    # Higher percentile threshold = fewer rows pass (lower selectivity)
    targets = [
        (0.99, "~1%"),   # 99th percentile → ~1% pass
        (0.90, "~10%"),  # 90th percentile → ~10% pass
        (0.50, "~50%"),  # 50th percentile → ~50% pass
        (0.10, "~90%"),  # 10th percentile → ~90% pass
    ]

    print(f"  Column: {INT64_COL} (int64), rows: {total_rows:,}")
    results = []

    for pct, label in targets:
        threshold = compute_percentile(data_file, INT64_COL, pct)
        print(f"\n  --- Selectivity {label} (threshold={threshold}) ---")

        def bench(name, func):
            return benchmark(name, func, warmup=warmup, iterations=iterations)

        # GPU run
        t_gpu = load_ltseq(data_file, gpu_enabled=True)
        # Need to capture threshold in lambda to avoid late binding
        thresh = int(threshold)
        gpu_result = bench(
            f"GPU sel={label}",
            lambda t=t_gpu, th=thresh: t.filter(lambda r: r.eventtime > th).count(),
        )

        # CPU run
        t_cpu = load_ltseq(data_file, gpu_enabled=False)
        cpu_result = bench(
            f"CPU sel={label}",
            lambda t=t_cpu, th=thresh: t.filter(lambda r: r.eventtime > th).count(),
        )

        # Validate
        gpu_count = t_gpu.filter(lambda r: r.eventtime > thresh).count()
        cpu_count = t_cpu.filter(lambda r: r.eventtime > thresh).count()
        match = gpu_count == cpu_count
        if match:
            print(f"  Validation: PASS (both = {gpu_count:,})")
        else:
            print(f"  Validation: FAIL (GPU={gpu_count:,}, CPU={cpu_count:,})")

        speedup = cpu_result["median_s"] / gpu_result["median_s"] if gpu_result["median_s"] > 0 else 0
        print(f"  Speedup: {speedup:.2f}x")

        results.append({
            "selectivity": label,
            "percentile": pct,
            "threshold": thresh,
            "gpu": gpu_result,
            "cpu": cpu_result,
            "speedup": round(speedup, 3),
            "correct": match,
        })

    return results


# ---------------------------------------------------------------------------
# Test 3: Data type — int32 vs int64 at 50% selectivity
# ---------------------------------------------------------------------------


def run_dtype_test(data_file, warmup, iterations, total_rows):
    """Compare int32 and int64 filter performance at 50% selectivity."""
    print("\n" + "=" * 70)
    print("TEST 3: DATA TYPE — int32 vs int64 at 50% selectivity")
    print("=" * 70)

    configs = [
        (INT32_COL, "int32"),
        (INT64_COL, "int64"),
    ]

    print(f"  Rows: {total_rows:,}")
    results = []

    for col, dtype in configs:
        threshold = compute_percentile(data_file, col, 0.5)
        threshold = int(threshold)
        print(f"\n  --- {col} ({dtype}), threshold={threshold} ---")

        def bench(name, func):
            return benchmark(name, func, warmup=warmup, iterations=iterations)

        # GPU run
        t_gpu = load_ltseq(data_file, gpu_enabled=True)
        gpu_result = bench(
            f"GPU {dtype}",
            lambda t=t_gpu, c=col, th=threshold: t.filter(
                lambda r, _c=c, _th=th: getattr(r, _c) > _th
            ).count(),
        )

        # CPU run
        t_cpu = load_ltseq(data_file, gpu_enabled=False)
        cpu_result = bench(
            f"CPU {dtype}",
            lambda t=t_cpu, c=col, th=threshold: t.filter(
                lambda r, _c=c, _th=th: getattr(r, _c) > _th
            ).count(),
        )

        # Validate
        gpu_count = t_gpu.filter(
            lambda r, _c=col, _th=threshold: getattr(r, _c) > _th
        ).count()
        cpu_count = t_cpu.filter(
            lambda r, _c=col, _th=threshold: getattr(r, _c) > _th
        ).count()
        match = gpu_count == cpu_count
        if match:
            print(f"  Validation: PASS (both = {gpu_count:,})")
        else:
            print(f"  Validation: FAIL (GPU={gpu_count:,}, CPU={cpu_count:,})")

        speedup = cpu_result["median_s"] / gpu_result["median_s"] if gpu_result["median_s"] > 0 else 0
        print(f"  Speedup: {speedup:.2f}x")

        results.append({
            "column": col,
            "dtype": dtype,
            "threshold": threshold,
            "gpu": gpu_result,
            "cpu": cpu_result,
            "speedup": round(speedup, 3),
            "correct": match,
        })

    return results


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def print_summary(scale_results, selectivity_results, dtype_results):
    """Print a summary table of all results."""
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    if scale_results:
        print("\nScale Test (counterid > threshold, ~50% selectivity):")
        header = f"  {'Rows':>15} | {'GPU (s)':>10} | {'CPU (s)':>10} | {'Speedup':>8} | {'Valid':>5}"
        print(header)
        print("  " + "-" * (len(header) - 2))
        for r in scale_results:
            gpu_t = r["gpu"]["median_s"]
            cpu_t = r["cpu"]["median_s"]
            sp = r["speedup"]
            sp_str = f"{sp:.2f}x" if sp >= 1 else f"{1/sp:.2f}x CPU"
            valid = "OK" if r["correct"] is True else ("FAIL" if r["correct"] is False else "N/A")
            print(f"  {r['n_rows']:>15,} | {gpu_t:>9.3f}s | {cpu_t:>9.3f}s | {sp_str:>8} | {valid:>5}")

    if selectivity_results:
        print("\nSelectivity Test (eventtime > threshold, full dataset):")
        header = f"  {'Selectivity':>12} | {'GPU (s)':>10} | {'CPU (s)':>10} | {'Speedup':>8} | {'Valid':>5}"
        print(header)
        print("  " + "-" * (len(header) - 2))
        for r in selectivity_results:
            gpu_t = r["gpu"]["median_s"]
            cpu_t = r["cpu"]["median_s"]
            sp = r["speedup"]
            sp_str = f"{sp:.2f}x" if sp >= 1 else f"{1/sp:.2f}x CPU"
            valid = "OK" if r["correct"] else "FAIL"
            print(f"  {r['selectivity']:>12} | {gpu_t:>9.3f}s | {cpu_t:>9.3f}s | {sp_str:>8} | {valid:>5}")

    if dtype_results:
        print("\nData Type Test (50% selectivity, full dataset):")
        header = f"  {'Type':>12} | {'GPU (s)':>10} | {'CPU (s)':>10} | {'Speedup':>8} | {'Valid':>5}"
        print(header)
        print("  " + "-" * (len(header) - 2))
        for r in dtype_results:
            gpu_t = r["gpu"]["median_s"]
            cpu_t = r["cpu"]["median_s"]
            sp = r["speedup"]
            sp_str = f"{sp:.2f}x" if sp >= 1 else f"{1/sp:.2f}x CPU"
            valid = "OK" if r["correct"] else "FAIL"
            print(f"  {r['dtype']:>12} | {gpu_t:>9.3f}s | {cpu_t:>9.3f}s | {sp_str:>8} | {valid:>5}")

    print()


def save_results(all_results, data_file, warmup, iterations):
    """Save results to JSON."""
    output_path = os.path.join(BENCHMARKS_DIR, "gpu_results.json")
    output = {
        "timestamp": datetime.now().isoformat(),
        "system": get_system_info(),
        "data_file": data_file,
        "warmup": warmup,
        "iterations": iterations,
        "results": all_results,
    }
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"Results saved to {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="LTSeq GPU vs CPU Filter Benchmark")
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Use 1M-row sample instead of full dataset",
    )
    parser.add_argument(
        "--test",
        type=str,
        choices=["scale", "selectivity", "dtype"],
        help="Run specific test only",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=ITERATIONS,
        help="Number of timed iterations (default: 3)",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=WARMUP,
        help="Number of warmup iterations (default: 1)",
    )
    parser.add_argument(
        "--data",
        type=str,
        default=None,
        help="Path to parquet file (overrides --sample)",
    )
    args = parser.parse_args()

    warmup = args.warmup
    iterations = args.iterations

    # Determine data file
    if args.data:
        data_file = args.data
    elif args.sample:
        data_file = HITS_SAMPLE
    else:
        data_file = HITS_SORTED

    if not os.path.exists(data_file):
        print(f"Error: Data file not found: {data_file}")
        print(
            "Run 'python benchmarks/prepare_data.py' first to download and prepare data."
        )
        return 1

    # Check GPU availability
    from ltseq import gpu_available

    gpu_avail = gpu_available()
    if not gpu_avail:
        print("WARNING: GPU acceleration not available.")
        print("  Build with: maturin develop --release --features gpu")
        print("  Benchmark will run CPU-only (no comparison possible).")
        print()

    # System info
    total_rows = compute_row_count(data_file)
    print(f"Data file: {data_file}")
    print(f"Row count: {total_rows:,}")
    print(
        f"System: {platform.processor()} | {os.cpu_count()} cores | "
        f"{psutil.virtual_memory().total / (1024**3):.0f}GB RAM"
    )
    gpu_info = get_gpu_info()
    if gpu_info["available"]:
        print(
            f"GPU: {gpu_info['model']} | {gpu_info.get('vram_mb', '?')}MB VRAM | "
            f"CUDA {gpu_info.get('cuda_version', '?')} | Driver {gpu_info.get('driver_version', '?')}"
        )
    print(f"LTSeq GPU available: {gpu_avail}")
    print(f"Config: warmup={warmup}, iterations={iterations}")

    # Run tests
    all_results = {}

    if args.test is None or args.test == "scale":
        all_results["scale"] = run_scale_test(data_file, warmup, iterations, total_rows)

    if args.test is None or args.test == "selectivity":
        all_results["selectivity"] = run_selectivity_test(
            data_file, warmup, iterations, total_rows
        )

    if args.test is None or args.test == "dtype":
        all_results["dtype"] = run_dtype_test(data_file, warmup, iterations, total_rows)

    # Print summary and save
    print_summary(
        all_results.get("scale", []),
        all_results.get("selectivity", []),
        all_results.get("dtype", []),
    )
    save_results(all_results, data_file, warmup, iterations)

    return 0


if __name__ == "__main__":
    exit(main())
