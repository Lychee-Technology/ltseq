#!/usr/bin/env python3
"""
LTSeq GPU vs CPU Filter Benchmark (wgpu)
=========================================

Measures the performance of wgpu-accelerated filter operations against
CPU-only execution on the ClickBench hits_sorted.parquet dataset (~100M rows).

Dimensions tested:
  1. Scale:       Filter throughput at 100K, 1M, 10M, 100M rows
  2. Selectivity: ~1%, 10%, 50%, 90% pass rates on full dataset
  3. Data type:   int32 (counterid) vs int64 (eventtime) at 50% selectivity
  4. Compound:    AND/OR compound predicates (wgpu multi-pass bitmask)
  5. DuckDB:      Head-to-head vs DuckDB on equivalent filter queries

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

    # List available GPU adapters and exit
    uv run python benchmarks/bench_gpu.py --list-gpus

    # Select a specific GPU by name substring (case-insensitive)
    uv run python benchmarks/bench_gpu.py --gpu 3070 --sample

    # Select a specific GPU by index (see --list-gpus for indices)
    uv run python benchmarks/bench_gpu.py --gpu 0 --sample

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
INT32_COL2 = "regionid"  # second int32 for compound predicates


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
    row = duckdb.sql(
        f"SELECT approx_quantile({column}, {pct}) FROM '{data_file}'"
    ).fetchone()
    assert row is not None, f"No data for percentile query on {column}"
    return row[0]


def compute_row_count(data_file):
    """Get total row count via DuckDB."""
    row = duckdb.sql(f"SELECT count(*) FROM '{data_file}'").fetchone()
    assert row is not None, "No data for count query"
    return row[0]


# ---------------------------------------------------------------------------
# GPU info collection (wgpu — no nvidia-smi dependency)
# ---------------------------------------------------------------------------


def get_gpu_info():
    """Collect GPU information from wgpu adapter (cross-platform)."""
    info = {"available": False, "backend": "wgpu"}
    try:
        import ltseq

        info["available"] = ltseq.gpu_available()

        # Use the richer gpu_info() API when the selected adapter is known
        adapter = ltseq.gpu_info()
        if adapter is not None:
            info["name"] = adapter["name"]
            info["backend"] = adapter["backend"]
            info["device_type"] = adapter["device_type"]
            info["is_uma"] = adapter["is_uma"]
    except ImportError:
        pass

    # Try nvidia-smi if available (supplementary VRAM / driver info)
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
                info["nvidia_gpu"] = parts[0]
                info["vram_mb"] = int(parts[1])
                info["driver_version"] = parts[2]
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    # Try lspci for GPU model on Linux (works for any GPU)
    if "nvidia_gpu" not in info and "name" not in info:
        try:
            result = subprocess.run(
                ["lspci"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                for line in result.stdout.split("\n"):
                    if "VGA" in line or "3D" in line or "Display" in line:
                        info["gpu_device"] = line.strip()
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
        use_full = n_rows >= total_rows * 0.9
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
            match = None
            print("  Validation: SKIP (sliced subset, partition order may differ)")

        speedup = (
            cpu_result["median_s"] / gpu_result["median_s"]
            if gpu_result["median_s"] > 0
            else 0
        )
        print(f"  Speedup: {speedup:.2f}x")

        results.append(
            {
                "n_rows": effective_rows,
                "gpu": gpu_result,
                "cpu": cpu_result,
                "speedup": round(speedup, 3),
                "correct": match,
            }
        )

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
        (0.99, "~1%"),  # 99th percentile -> ~1% pass
        (0.90, "~10%"),  # 90th percentile -> ~10% pass
        (0.50, "~50%"),  # 50th percentile -> ~50% pass
        (0.10, "~90%"),  # 10th percentile -> ~90% pass
    ]

    print(f"  Column: {INT64_COL} (int64), rows: {total_rows:,}")
    results = []

    for pct, label in targets:
        threshold = compute_percentile(data_file, INT64_COL, pct)
        thresh = int(threshold)
        print(f"\n  --- Selectivity {label} (threshold={thresh}) ---")

        def bench(name, func):
            return benchmark(name, func, warmup=warmup, iterations=iterations)

        # GPU run
        t_gpu = load_ltseq(data_file, gpu_enabled=True)
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

        speedup = (
            cpu_result["median_s"] / gpu_result["median_s"]
            if gpu_result["median_s"] > 0
            else 0
        )
        print(f"  Speedup: {speedup:.2f}x")

        results.append(
            {
                "selectivity": label,
                "percentile": pct,
                "threshold": thresh,
                "gpu": gpu_result,
                "cpu": cpu_result,
                "speedup": round(speedup, 3),
                "correct": match,
            }
        )

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

        speedup = (
            cpu_result["median_s"] / gpu_result["median_s"]
            if gpu_result["median_s"] > 0
            else 0
        )
        print(f"  Speedup: {speedup:.2f}x")

        results.append(
            {
                "column": col,
                "dtype": dtype,
                "threshold": threshold,
                "gpu": gpu_result,
                "cpu": cpu_result,
                "speedup": round(speedup, 3),
                "correct": match,
            }
        )

    return results


# ---------------------------------------------------------------------------
# Test 4: Compound predicates — AND/OR (wgpu multi-pass bitmask)
# ---------------------------------------------------------------------------


def run_compound_test(data_file, warmup, iterations, total_rows):
    """Test compound predicate performance (AND/OR) — a wgpu differentiator."""
    print("\n" + "=" * 70)
    print("TEST 4: COMPOUND — AND/OR compound predicates (wgpu multi-pass)")
    print("=" * 70)

    # Compute thresholds for ~50% selectivity on each column
    counter_thresh = int(compute_percentile(data_file, INT32_COL, 0.5))
    region_thresh = int(compute_percentile(data_file, INT32_COL2, 0.5))
    event_thresh = int(compute_percentile(data_file, INT64_COL, 0.5))

    print(f"  Rows: {total_rows:,}")
    print(f"  {INT32_COL} threshold: {counter_thresh}")
    print(f"  {INT32_COL2} threshold: {region_thresh}")
    print(f"  {INT64_COL} threshold: {event_thresh}")

    results = []

    # --- Simple predicate (baseline) ---
    print(f"\n  --- Simple: {INT32_COL} > {counter_thresh} ---")

    def bench(name, func):
        return benchmark(name, func, warmup=warmup, iterations=iterations)

    t_gpu = load_ltseq(data_file, gpu_enabled=True)
    t_cpu = load_ltseq(data_file, gpu_enabled=False)

    simple_gpu = bench(
        "GPU simple",
        lambda: t_gpu.filter(lambda r: r.counterid > counter_thresh).count(),
    )
    simple_cpu = bench(
        "CPU simple",
        lambda: t_cpu.filter(lambda r: r.counterid > counter_thresh).count(),
    )
    simple_speedup = (
        simple_cpu["median_s"] / simple_gpu["median_s"]
        if simple_gpu["median_s"] > 0
        else 0
    )
    print(f"  Speedup: {simple_speedup:.2f}x")
    results.append(
        {
            "predicate": "simple",
            "description": f"{INT32_COL} > threshold",
            "gpu": simple_gpu,
            "cpu": simple_cpu,
            "speedup": round(simple_speedup, 3),
        }
    )

    # --- AND predicate (two int32 columns) ---
    print(f"\n  --- AND: {INT32_COL} > {counter_thresh} AND {INT32_COL2} > {region_thresh} ---")

    t_gpu = load_ltseq(data_file, gpu_enabled=True)
    t_cpu = load_ltseq(data_file, gpu_enabled=False)

    and_gpu = bench(
        "GPU AND(i32,i32)",
        lambda: t_gpu.filter(
            lambda r: (r.counterid > counter_thresh) & (r.regionid > region_thresh)
        ).count(),
    )
    and_cpu = bench(
        "CPU AND(i32,i32)",
        lambda: t_cpu.filter(
            lambda r: (r.counterid > counter_thresh) & (r.regionid > region_thresh)
        ).count(),
    )

    # Validate
    gpu_count = t_gpu.filter(
        lambda r: (r.counterid > counter_thresh) & (r.regionid > region_thresh)
    ).count()
    cpu_count = t_cpu.filter(
        lambda r: (r.counterid > counter_thresh) & (r.regionid > region_thresh)
    ).count()
    and_match = gpu_count == cpu_count
    if and_match:
        print(f"  Validation: PASS (both = {gpu_count:,})")
    else:
        print(f"  Validation: FAIL (GPU={gpu_count:,}, CPU={cpu_count:,})")

    and_speedup = (
        and_cpu["median_s"] / and_gpu["median_s"] if and_gpu["median_s"] > 0 else 0
    )
    print(f"  Speedup: {and_speedup:.2f}x")
    results.append(
        {
            "predicate": "AND",
            "description": f"{INT32_COL} > t1 AND {INT32_COL2} > t2",
            "gpu": and_gpu,
            "cpu": and_cpu,
            "speedup": round(and_speedup, 3),
            "correct": and_match,
        }
    )

    # --- OR predicate (int32 OR int64) ---
    print(f"\n  --- OR: {INT32_COL} > {counter_thresh} OR {INT64_COL} > {event_thresh} ---")

    t_gpu = load_ltseq(data_file, gpu_enabled=True)
    t_cpu = load_ltseq(data_file, gpu_enabled=False)

    or_gpu = bench(
        "GPU OR(i32,i64)",
        lambda: t_gpu.filter(
            lambda r: (r.counterid > counter_thresh) | (r.eventtime > event_thresh)
        ).count(),
    )
    or_cpu = bench(
        "CPU OR(i32,i64)",
        lambda: t_cpu.filter(
            lambda r: (r.counterid > counter_thresh) | (r.eventtime > event_thresh)
        ).count(),
    )

    # Validate
    gpu_count = t_gpu.filter(
        lambda r: (r.counterid > counter_thresh) | (r.eventtime > event_thresh)
    ).count()
    cpu_count = t_cpu.filter(
        lambda r: (r.counterid > counter_thresh) | (r.eventtime > event_thresh)
    ).count()
    or_match = gpu_count == cpu_count
    if or_match:
        print(f"  Validation: PASS (both = {gpu_count:,})")
    else:
        print(f"  Validation: FAIL (GPU={gpu_count:,}, CPU={cpu_count:,})")

    or_speedup = (
        or_cpu["median_s"] / or_gpu["median_s"] if or_gpu["median_s"] > 0 else 0
    )
    print(f"  Speedup: {or_speedup:.2f}x")
    results.append(
        {
            "predicate": "OR",
            "description": f"{INT32_COL} > t1 OR {INT64_COL} > t2",
            "gpu": or_gpu,
            "cpu": or_cpu,
            "speedup": round(or_speedup, 3),
            "correct": or_match,
        }
    )

    # --- Triple AND (int32 AND int32 AND int64) ---
    print(
        f"\n  --- AND3: {INT32_COL} > {counter_thresh} AND "
        f"{INT32_COL2} > {region_thresh} AND {INT64_COL} > {event_thresh} ---"
    )

    t_gpu = load_ltseq(data_file, gpu_enabled=True)
    t_cpu = load_ltseq(data_file, gpu_enabled=False)

    and3_gpu = bench(
        "GPU AND3(i32,i32,i64)",
        lambda: t_gpu.filter(
            lambda r: (r.counterid > counter_thresh)
            & (r.regionid > region_thresh)
            & (r.eventtime > event_thresh)
        ).count(),
    )
    and3_cpu = bench(
        "CPU AND3(i32,i32,i64)",
        lambda: t_cpu.filter(
            lambda r: (r.counterid > counter_thresh)
            & (r.regionid > region_thresh)
            & (r.eventtime > event_thresh)
        ).count(),
    )

    # Validate
    gpu_count = t_gpu.filter(
        lambda r: (r.counterid > counter_thresh)
        & (r.regionid > region_thresh)
        & (r.eventtime > event_thresh)
    ).count()
    cpu_count = t_cpu.filter(
        lambda r: (r.counterid > counter_thresh)
        & (r.regionid > region_thresh)
        & (r.eventtime > event_thresh)
    ).count()
    and3_match = gpu_count == cpu_count
    if and3_match:
        print(f"  Validation: PASS (both = {gpu_count:,})")
    else:
        print(f"  Validation: FAIL (GPU={gpu_count:,}, CPU={cpu_count:,})")

    and3_speedup = (
        and3_cpu["median_s"] / and3_gpu["median_s"]
        if and3_gpu["median_s"] > 0
        else 0
    )
    print(f"  Speedup: {and3_speedup:.2f}x")
    results.append(
        {
            "predicate": "AND3",
            "description": f"{INT32_COL} > t1 AND {INT32_COL2} > t2 AND {INT64_COL} > t3",
            "gpu": and3_gpu,
            "cpu": and3_cpu,
            "speedup": round(and3_speedup, 3),
            "correct": and3_match,
        }
    )

    return results


# ---------------------------------------------------------------------------
# Test 5: DuckDB comparison — filter performance head-to-head
# ---------------------------------------------------------------------------


def run_duckdb_test(data_file, warmup, iterations, total_rows):
    """Compare LTSeq GPU/CPU filter against DuckDB SQL equivalents."""
    print("\n" + "=" * 70)
    print("TEST 5: DUCKDB — Head-to-head filter comparison")
    print("=" * 70)

    counter_thresh = int(compute_percentile(data_file, INT32_COL, 0.5))
    event_thresh = int(compute_percentile(data_file, INT64_COL, 0.5))
    region_thresh = int(compute_percentile(data_file, INT32_COL2, 0.5))

    print(f"  Rows: {total_rows:,}")

    results = []

    queries = [
        {
            "name": "simple_i32",
            "description": f"{INT32_COL} > {counter_thresh}",
            "ltseq_filter": lambda r, th=counter_thresh: r.counterid > th,
            "duckdb_sql": f"SELECT count(*) FROM '{data_file}' WHERE {INT32_COL} > {counter_thresh}",
        },
        {
            "name": "simple_i64",
            "description": f"{INT64_COL} > {event_thresh}",
            "ltseq_filter": lambda r, th=event_thresh: r.eventtime > th,
            "duckdb_sql": f"SELECT count(*) FROM '{data_file}' WHERE {INT64_COL} > {event_thresh}",
        },
        {
            "name": "and_i32_i32",
            "description": f"{INT32_COL} > {counter_thresh} AND {INT32_COL2} > {region_thresh}",
            "ltseq_filter": lambda r, th1=counter_thresh, th2=region_thresh: (
                r.counterid > th1
            )
            & (r.regionid > th2),
            "duckdb_sql": (
                f"SELECT count(*) FROM '{data_file}' "
                f"WHERE {INT32_COL} > {counter_thresh} AND {INT32_COL2} > {region_thresh}"
            ),
        },
        {
            "name": "or_i32_i64",
            "description": f"{INT32_COL} > {counter_thresh} OR {INT64_COL} > {event_thresh}",
            "ltseq_filter": lambda r, th1=counter_thresh, th2=event_thresh: (
                r.counterid > th1
            )
            | (r.eventtime > th2),
            "duckdb_sql": (
                f"SELECT count(*) FROM '{data_file}' "
                f"WHERE {INT32_COL} > {counter_thresh} OR {INT64_COL} > {event_thresh}"
            ),
        },
    ]

    def bench(name, func):
        return benchmark(name, func, warmup=warmup, iterations=iterations)

    for q in queries:
        print(f"\n  --- {q['name']}: {q['description']} ---")

        # LTSeq GPU
        t_gpu = load_ltseq(data_file, gpu_enabled=True)
        filt = q["ltseq_filter"]
        gpu_result = bench(
            f"LTSeq GPU {q['name']}",
            lambda t=t_gpu, f=filt: t.filter(f).count(),
        )

        # LTSeq CPU
        t_cpu = load_ltseq(data_file, gpu_enabled=False)
        cpu_result = bench(
            f"LTSeq CPU {q['name']}",
            lambda t=t_cpu, f=filt: t.filter(f).count(),
        )

        # DuckDB
        sql = q["duckdb_sql"]

        def _duck_query(s=sql):
            row = duckdb.sql(s).fetchone()
            assert row is not None
            return row[0]

        duck_result = bench(f"DuckDB     {q['name']}", _duck_query)

        # Validate: all three should produce the same count
        gpu_count = t_gpu.filter(filt).count()
        cpu_count = t_cpu.filter(filt).count()
        duck_row = duckdb.sql(sql).fetchone()
        assert duck_row is not None
        duck_count = duck_row[0]
        all_match = gpu_count == cpu_count == duck_count
        if all_match:
            print(f"  Validation: PASS (all = {gpu_count:,})")
        else:
            print(
                f"  Validation: FAIL (GPU={gpu_count:,}, CPU={cpu_count:,}, DuckDB={duck_count:,})"
            )

        gpu_vs_duck = (
            duck_result["median_s"] / gpu_result["median_s"]
            if gpu_result["median_s"] > 0
            else 0
        )
        cpu_vs_duck = (
            duck_result["median_s"] / cpu_result["median_s"]
            if cpu_result["median_s"] > 0
            else 0
        )
        gpu_vs_cpu = (
            cpu_result["median_s"] / gpu_result["median_s"]
            if gpu_result["median_s"] > 0
            else 0
        )
        print(f"  GPU vs CPU: {gpu_vs_cpu:.2f}x | GPU vs DuckDB: {gpu_vs_duck:.2f}x | CPU vs DuckDB: {cpu_vs_duck:.2f}x")

        results.append(
            {
                "query": q["name"],
                "description": q["description"],
                "gpu": gpu_result,
                "cpu": cpu_result,
                "duckdb": duck_result,
                "gpu_vs_cpu": round(gpu_vs_cpu, 3),
                "gpu_vs_duckdb": round(gpu_vs_duck, 3),
                "cpu_vs_duckdb": round(cpu_vs_duck, 3),
                "correct": all_match,
            }
        )

    return results


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def print_summary(scale, selectivity, dtype, compound, duckdb_results):
    """Print a summary table of all results."""
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    if scale:
        print("\nScale Test (counterid > threshold, ~50% selectivity):")
        header = f"  {'Rows':>15} | {'GPU (s)':>10} | {'CPU (s)':>10} | {'Speedup':>10} | {'Valid':>5}"
        print(header)
        print("  " + "-" * (len(header) - 2))
        for r in scale:
            gpu_t = r["gpu"]["median_s"]
            cpu_t = r["cpu"]["median_s"]
            sp = r["speedup"]
            sp_str = f"{sp:.2f}x" if sp >= 1 else f"1/{1 / sp:.2f}x"
            valid = (
                "OK"
                if r["correct"] is True
                else ("FAIL" if r["correct"] is False else "N/A")
            )
            print(
                f"  {r['n_rows']:>15,} | {gpu_t:>9.3f}s | {cpu_t:>9.3f}s | {sp_str:>10} | {valid:>5}"
            )

    if selectivity:
        print("\nSelectivity Test (eventtime > threshold, full dataset):")
        header = f"  {'Selectivity':>12} | {'GPU (s)':>10} | {'CPU (s)':>10} | {'Speedup':>10} | {'Valid':>5}"
        print(header)
        print("  " + "-" * (len(header) - 2))
        for r in selectivity:
            gpu_t = r["gpu"]["median_s"]
            cpu_t = r["cpu"]["median_s"]
            sp = r["speedup"]
            sp_str = f"{sp:.2f}x" if sp >= 1 else f"1/{1 / sp:.2f}x"
            valid = "OK" if r["correct"] else "FAIL"
            print(
                f"  {r['selectivity']:>12} | {gpu_t:>9.3f}s | {cpu_t:>9.3f}s | {sp_str:>10} | {valid:>5}"
            )

    if dtype:
        print("\nData Type Test (50% selectivity, full dataset):")
        header = f"  {'Type':>12} | {'GPU (s)':>10} | {'CPU (s)':>10} | {'Speedup':>10} | {'Valid':>5}"
        print(header)
        print("  " + "-" * (len(header) - 2))
        for r in dtype:
            gpu_t = r["gpu"]["median_s"]
            cpu_t = r["cpu"]["median_s"]
            sp = r["speedup"]
            sp_str = f"{sp:.2f}x" if sp >= 1 else f"1/{1 / sp:.2f}x"
            valid = "OK" if r["correct"] else "FAIL"
            print(
                f"  {r['dtype']:>12} | {gpu_t:>9.3f}s | {cpu_t:>9.3f}s | {sp_str:>10} | {valid:>5}"
            )

    if compound:
        print("\nCompound Predicate Test (full dataset):")
        header = f"  {'Predicate':>12} | {'GPU (s)':>10} | {'CPU (s)':>10} | {'Speedup':>10} | {'Valid':>5}"
        print(header)
        print("  " + "-" * (len(header) - 2))
        for r in compound:
            gpu_t = r["gpu"]["median_s"]
            cpu_t = r["cpu"]["median_s"]
            sp = r["speedup"]
            sp_str = f"{sp:.2f}x" if sp >= 1 else f"1/{1 / sp:.2f}x"
            valid = r.get("correct")
            valid_str = (
                "OK"
                if valid is True
                else ("FAIL" if valid is False else "N/A")
            )
            print(
                f"  {r['predicate']:>12} | {gpu_t:>9.3f}s | {cpu_t:>9.3f}s | {sp_str:>10} | {valid_str:>5}"
            )

    if duckdb_results:
        print("\nDuckDB Comparison (full dataset):")
        header = (
            f"  {'Query':>15} | {'GPU (s)':>10} | {'CPU (s)':>10} | {'DuckDB':>10} "
            f"| {'GPU/CPU':>8} | {'GPU/Duck':>9} | {'Valid':>5}"
        )
        print(header)
        print("  " + "-" * (len(header) - 2))
        for r in duckdb_results:
            gpu_t = r["gpu"]["median_s"]
            cpu_t = r["cpu"]["median_s"]
            duck_t = r["duckdb"]["median_s"]
            gvc = r["gpu_vs_cpu"]
            gvd = r["gpu_vs_duckdb"]
            gvc_str = f"{gvc:.2f}x" if gvc >= 1 else f"1/{1 / gvc:.2f}x"
            gvd_str = f"{gvd:.2f}x" if gvd >= 1 else f"1/{1 / gvd:.2f}x"
            valid = "OK" if r["correct"] else "FAIL"
            print(
                f"  {r['query']:>15} | {gpu_t:>9.3f}s | {cpu_t:>9.3f}s | {duck_t:>9.3f}s "
                f"| {gvc_str:>8} | {gvd_str:>9} | {valid:>5}"
            )

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
    parser = argparse.ArgumentParser(
        description="LTSeq GPU vs CPU Filter Benchmark (wgpu)"
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Use sample dataset instead of full hits_sorted.parquet",
    )
    parser.add_argument(
        "--test",
        type=str,
        choices=["scale", "selectivity", "dtype", "compound", "duckdb"],
        help="Run specific test only",
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
        "--data",
        type=str,
        default=None,
        help="Path to parquet file (overrides --sample)",
    )
    parser.add_argument(
        "--gpu",
        type=str,
        default=None,
        metavar="NAME_OR_INDEX",
        help=(
            "GPU adapter to use. Accepts a name substring (case-insensitive, "
            "e.g. '3070') or a zero-based integer index (e.g. '0'). "
            "Sets LTSEQ_GPU_DEVICE before ltseq is imported. "
            "Use --list-gpus to see available adapters."
        ),
    )
    parser.add_argument(
        "--list-gpus",
        action="store_true",
        help="List all available GPU adapters and exit (does not run benchmarks)",
    )
    args = parser.parse_args()

    # Apply GPU device selection before ltseq is imported anywhere.
    # LTSEQ_GPU_DEVICE must be set before the LazyLock singleton fires.
    if args.gpu is not None:
        os.environ["LTSEQ_GPU_DEVICE"] = args.gpu

    # --list-gpus: enumerate adapters and exit (imports ltseq here, which is fine
    # because we only need gpu_list(), not the full GPU context singleton)
    if args.list_gpus:
        import ltseq

        adapters = ltseq.gpu_list()
        if not adapters:
            print("No GPU adapters found (or ltseq compiled without --features gpu).")
            return 0
        print(f"{'Index':>5}  {'Name':<45}  {'Backend':<10}  Device Type")
        print(f"{'-----':>5}  {'-'*45}  {'-'*10}  -----------")
        for a in adapters:
            print(
                f"{a['index']:>5}  {a['name']:<45}  {a['backend']:<10}  {a['device_type']}"
            )
        print()
        print(
            "Use --gpu <name_substring> or --gpu <index> to select a specific adapter."
        )
        return 0

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
    import ltseq

    gpu_avail = ltseq.gpu_available()
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
        f"System: {platform.processor() or platform.machine()} | "
        f"{os.cpu_count()} cores | "
        f"{psutil.virtual_memory().total / (1024**3):.0f}GB RAM"
    )
    gpu_sys_info = get_gpu_info()
    # Prefer the wgpu adapter name (most accurate); fall back to nvidia-smi / lspci
    if gpu_sys_info.get("name"):
        uma_tag = " | UMA" if gpu_sys_info.get("is_uma") else ""
        print(
            f"GPU (wgpu): {gpu_sys_info['name']} | "
            f"{gpu_sys_info.get('backend', '?')} | "
            f"{gpu_sys_info.get('device_type', '?')}{uma_tag}"
        )
        if gpu_sys_info.get("nvidia_gpu"):
            print(
                f"  nvidia-smi: {gpu_sys_info['nvidia_gpu']} | "
                f"{gpu_sys_info.get('vram_mb', '?')}MB VRAM | "
                f"Driver {gpu_sys_info.get('driver_version', '?')}"
            )
    elif gpu_sys_info.get("nvidia_gpu"):
        print(
            f"GPU: {gpu_sys_info['nvidia_gpu']} | {gpu_sys_info.get('vram_mb', '?')}MB VRAM | "
            f"Driver {gpu_sys_info.get('driver_version', '?')}"
        )
    elif gpu_sys_info.get("gpu_device"):
        print(f"GPU: {gpu_sys_info['gpu_device']}")
    print(f"LTSeq GPU available (wgpu): {gpu_avail}")
    if args.gpu is not None:
        print(f"GPU selector: {args.gpu!r}  (LTSEQ_GPU_DEVICE={os.environ.get('LTSEQ_GPU_DEVICE', '(unset)')})")
    print(f"Config: warmup={warmup}, iterations={iterations}")

    # Run tests
    all_results = {}

    if args.test is None or args.test == "scale":
        all_results["scale"] = run_scale_test(
            data_file, warmup, iterations, total_rows
        )

    if args.test is None or args.test == "selectivity":
        all_results["selectivity"] = run_selectivity_test(
            data_file, warmup, iterations, total_rows
        )

    if args.test is None or args.test == "dtype":
        all_results["dtype"] = run_dtype_test(
            data_file, warmup, iterations, total_rows
        )

    if args.test is None or args.test == "compound":
        all_results["compound"] = run_compound_test(
            data_file, warmup, iterations, total_rows
        )

    if args.test is None or args.test == "duckdb":
        all_results["duckdb"] = run_duckdb_test(
            data_file, warmup, iterations, total_rows
        )

    # Print summary and save
    print_summary(
        all_results.get("scale", []),
        all_results.get("selectivity", []),
        all_results.get("dtype", []),
        all_results.get("compound", []),
        all_results.get("duckdb", []),
    )
    save_results(all_results, data_file, warmup, iterations)

    return 0


if __name__ == "__main__":
    exit(main())
