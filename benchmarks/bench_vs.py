#!/usr/bin/env python3
"""
LTSeq vs DuckDB: ClickBench Benchmark
======================================

Compares LTSeq and DuckDB on three categories of queries using
the ClickBench hits.parquet dataset (~100M rows):

  Round 1: Basic aggregation (Top URLs by count)
  Round 2: User sessionization (30-min gap detection)
  Round 3: Sequential pattern matching (URL funnel)

Usage:
    # Full benchmark (requires hits_sorted.parquet)
    uv run python benchmarks/bench_vs.py

    # Quick validation with sample data
    uv run python benchmarks/bench_vs.py --sample

    # Specific round only
    uv run python benchmarks/bench_vs.py --round 2

Requirements:
    pip install duckdb psutil
    maturin develop --release   # Build LTSeq with optimizations
"""

import argparse
import gc
import json
import os
import platform
import time
from datetime import datetime

import duckdb
import psutil

from ltseq import LTSeq

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BENCHMARKS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BENCHMARKS_DIR, "data")
HITS_SORTED = os.path.join(DATA_DIR, "hits_sorted.parquet")
HITS_SAMPLE = os.path.join(DATA_DIR, "hits_sample.parquet")

WARMUP = 1
ITERATIONS = 3

# ---------------------------------------------------------------------------
# URL patterns for funnel (Round 3)
# These should be updated after running prepare_data.py --investigate
# to match actual URL patterns in the ClickBench dataset.
#
# Placeholder patterns based on typical Yandex Metrica URLs:
# ---------------------------------------------------------------------------

# The ClickBench dataset contains Yandex Metrica data with real Russian website URLs.
# These patterns were discovered via prepare_data.py --investigate on the 1M-row sample.
# The liver.ru site has the strongest sequential funnel: users browse city listings
# in a consistent pattern (Saint Petersburg -> craft listings -> Belgorod pages).
# ~1,600 matches per 1M rows = ~160K+ matches on the full 100M dataset.
FUNNEL_PATTERN_1 = "http://liver.ru/saint-peterburg"  # Step 1: Saint Petersburg page
FUNNEL_PATTERN_2 = "http://liver.ru/place_rukodel"  # Step 2: craft/handmade listings
FUNNEL_PATTERN_3 = "http://liver.ru/belgorod"  # Step 3: Belgorod page


# ---------------------------------------------------------------------------
# Measurement infrastructure
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
# Round 1: Basic Aggregation — Top 10 URLs by count
# ---------------------------------------------------------------------------


def duckdb_top_url(data_file):
    """DuckDB: GROUP BY url, ORDER BY count DESC, LIMIT 10."""
    return duckdb.sql(f"""
        SELECT url, count(*) as cnt
        FROM '{data_file}'
        GROUP BY url
        ORDER BY cnt DESC
        LIMIT 10
    """).fetchall()


def ltseq_top_url(t):
    """LTSeq: agg by url, sort descending, slice top 10."""
    return (
        t.agg(by=lambda r: r.url, cnt=lambda g: g.url.count())
        .sort("cnt", desc=True)
        .slice(0, 10)
        .collect()
    )


# ---------------------------------------------------------------------------
# Round 2: User Sessionization — 30-min gap = new session
# ---------------------------------------------------------------------------


def duckdb_session(data_file):
    """DuckDB: LAG window function + SUM to count session boundaries."""
    return duckdb.sql(f"""
        WITH Diff AS (
            SELECT userid,
                CASE WHEN eventtime - LAG(eventtime)
                    OVER (PARTITION BY userid ORDER BY eventtime, watchid) > 1800
                THEN 1 ELSE 0 END AS is_new
            FROM '{data_file}'
        )
        SELECT sum(is_new) + count(DISTINCT userid) as total_sessions
        FROM Diff
    """).fetchone()[0]


def ltseq_session(t_sorted):
    """LTSeq: group_ordered on user/time boundary, count groups via first()."""
    cond = lambda r: (
        (r.userid != r.userid.shift(1)) | (r.eventtime - r.eventtime.shift(1) > 1800)
    )
    grouped = t_sorted.group_ordered(cond)
    # Each group = one session. first() collapses to 1 row per group, count() gives total.
    return grouped.first().count()


def ltseq_session_v2(t_sorted):
    """LTSeq: derive session boundary flag, then count.

    Uses partition_by="userid" on shift so DataFusion can process each user
    independently. The first row per user gets NULL from shift, and we treat
    NULL as a new session start using is_null OR time gap > 1800.
    This avoids a separate distinct-user count pass.
    """
    t = t_sorted.derive(
        lambda r: {
            "prev_time": r.eventtime.shift(1, partition_by="userid"),
        }
    )
    # First row per user: prev_time is NULL → is_null catches it as new session.
    # Subsequent rows: check if gap > 1800 seconds.
    return t.filter(
        lambda r: (r.prev_time.is_null()) | (r.eventtime - r.prev_time > 1800)
    ).count()


# ---------------------------------------------------------------------------
# Round 3: Sequential Funnel — 3-step URL pattern match
# ---------------------------------------------------------------------------


def duckdb_funnel(data_file, p1, p2, p3):
    """DuckDB: LEAD window function for consecutive URL matching."""
    return duckdb.sql(f"""
        SELECT count(*)
        FROM (
            SELECT
                url,
                LEAD(url) OVER (PARTITION BY userid ORDER BY eventtime, watchid) as next1,
                LEAD(url, 2) OVER (PARTITION BY userid ORDER BY eventtime, watchid) as next2
            FROM '{data_file}'
        )
        WHERE starts_with(url, '{p1}')
          AND starts_with(next1, '{p2}')
          AND starts_with(next2, '{p3}')
    """).fetchone()[0]


def ltseq_funnel(t_sorted, p1, p2, p3):
    """LTSeq: streaming pattern matcher for consecutive URL matching.

    Uses search_pattern_count for single-pass evaluation — only collects
    the columns needed for predicate evaluation, never computes LEAD/shift.
    """
    return t_sorted.search_pattern_count(
        lambda r: r.url.s.starts_with(p1),
        lambda r: r.url.s.starts_with(p2),
        lambda r: r.url.s.starts_with(p3),
        partition_by="userid",
    )


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def print_results_table(results):
    """Print formatted comparison table."""
    print("\n" + "=" * 80)
    print("RESULTS")
    print("=" * 80)

    header = f"{'Round':<25} | {'DuckDB (s)':>11} | {'LTSeq (s)':>11} | {'Speedup':>8} | {'LOC SQL':>7} | {'LOC Py':>6}"
    print(header)
    print("-" * len(header))

    loc_data = {
        "R1: Top URLs": (6, 5),
        "R2: Sessionization": (10, 4),
        "R3: Funnel": (10, 5),
    }

    for r in results:
        name = r["round_name"]
        duck_t = r["duckdb"]["median_s"]
        ltseq_t = r["ltseq"]["median_s"]
        if ltseq_t > 0 and duck_t > 0:
            speedup = duck_t / ltseq_t
            speedup_str = f"{speedup:.1f}x"
            if speedup > 1:
                speedup_str += " LT"
            else:
                speedup_str = f"{1 / speedup:.1f}x DK"
        else:
            speedup_str = "N/A"

        loc_sql, loc_py = loc_data.get(name, (0, 0))
        print(
            f"{name:<25} | {duck_t:>10.3f}s | {ltseq_t:>10.3f}s | {speedup_str:>8} | {loc_sql:>7} | {loc_py:>6}"
        )

    print()


def get_system_info():
    """Collect system information for the report."""
    return {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "ram_gb": round(psutil.virtual_memory().total / (1024**3), 1),
        "python_version": platform.python_version(),
    }


def save_results(results, data_file, warmup, iterations):
    """Save results to JSON."""
    output = {
        "timestamp": datetime.now().isoformat(),
        "system": get_system_info(),
        "data_file": data_file,
        "warmup": warmup,
        "iterations": iterations,
        "rounds": results,
    }
    output_path = os.path.join(BENCHMARKS_DIR, "clickbench_results.json")
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"Results saved to {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="LTSeq vs DuckDB ClickBench Benchmark")
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Use 1M-row sample instead of full dataset",
    )
    parser.add_argument(
        "--round", type=int, choices=[1, 2, 3], help="Run specific round only"
    )
    parser.add_argument(
        "--iterations", type=int, default=ITERATIONS, help="Number of timed iterations"
    )
    parser.add_argument(
        "--warmup", type=int, default=WARMUP, help="Number of warmup iterations"
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

    def bench(name, func):
        return benchmark(name, func, warmup=warmup, iterations=iterations)

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

    print(f"Data file: {data_file}")
    row_count = duckdb.sql(f"SELECT count(*) FROM '{data_file}'").fetchone()[0]
    print(f"Row count: {row_count:,}")
    print(
        f"System: {platform.processor()} | {os.cpu_count()} cores | "
        f"{psutil.virtual_memory().total / (1024**3):.0f}GB RAM"
    )
    print(f"Config: warmup={warmup}, iterations={iterations}")

    # Pre-load data for LTSeq
    print("\nLoading data for LTSeq...")
    t0 = time.perf_counter()
    t_ltseq = LTSeq.read_parquet(data_file)
    load_time = time.perf_counter() - t0
    print(f"  LTSeq load time: {load_time:.3f}s")

    print("Sorting for LTSeq (declaring sort order)...")
    t0 = time.perf_counter()
    t_ltseq_sorted = t_ltseq.assume_sorted("userid", "eventtime", "watchid")
    sort_time = time.perf_counter() - t0
    print(f"  LTSeq assume_sorted time: {sort_time:.3f}s")

    results = []

    # -----------------------------------------------------------------------
    # Round 1: Basic Aggregation
    # -----------------------------------------------------------------------
    if args.round is None or args.round == 1:
        print("\n--- Round 1: Basic Aggregation (Top 10 URLs) ---")
        duck_r1 = bench("DuckDB", lambda: duckdb_top_url(data_file))
        ltseq_r1 = bench("LTSeq", lambda: ltseq_top_url(t_ltseq))

        # Validate results match
        duck_urls = [row[0] for row in duckdb_top_url(data_file)]
        ltseq_urls = [row["url"] for row in ltseq_top_url(t_ltseq)]
        if set(duck_urls) == set(ltseq_urls):
            if duck_urls == ltseq_urls:
                print("  Validation: PASS (same top 10 URLs, same order)")
            else:
                print(
                    "  Validation: PASS (same top 10 URLs, different tie-breaking order)"
                )
        else:
            print("  Validation: WARN (different URL sets)")
            print(f"    DuckDB:  {duck_urls[:3]}...")
            print(f"    LTSeq:   {ltseq_urls[:3]}...")

        results.append(
            {"round_name": "R1: Top URLs", "duckdb": duck_r1, "ltseq": ltseq_r1}
        )

    # -----------------------------------------------------------------------
    # Round 2: User Sessionization
    # -----------------------------------------------------------------------
    if args.round is None or args.round == 2:
        print("\n--- Round 2: User Sessionization (30-min gap) ---")
        duck_r2 = bench("DuckDB", lambda: duckdb_session(data_file))

        # Try primary LTSeq approach; fall back to v2 if needed
        try:
            ltseq_r2 = bench("LTSeq", lambda: ltseq_session(t_ltseq_sorted))
        except Exception as e:
            print(f"  LTSeq primary approach failed: {e}")
            print("  Falling back to v2 (derive + filter)...")
            ltseq_r2 = bench("LTSeq-v2", lambda: ltseq_session_v2(t_ltseq_sorted))

        # Validate
        duck_count = duckdb_session(data_file)
        try:
            ltseq_count = ltseq_session(t_ltseq_sorted)
        except Exception:
            ltseq_count = ltseq_session_v2(t_ltseq_sorted)

        if duck_count == ltseq_count:
            print(f"  Validation: PASS (both = {duck_count:,} sessions)")
        else:
            print(f"  Validation: WARN (DuckDB={duck_count:,}, LTSeq={ltseq_count:,})")

        results.append(
            {"round_name": "R2: Sessionization", "duckdb": duck_r2, "ltseq": ltseq_r2}
        )

    # -----------------------------------------------------------------------
    # Round 3: Sequential Funnel
    # -----------------------------------------------------------------------
    if args.round is None or args.round == 3:
        print("\n--- Round 3: Sequential Pattern Matching (Funnel) ---")
        print(
            f"  Patterns: '{FUNNEL_PATTERN_1}' -> '{FUNNEL_PATTERN_2}' -> '{FUNNEL_PATTERN_3}'"
        )

        duck_r3 = bench(
            "DuckDB",
            lambda: duckdb_funnel(
                data_file, FUNNEL_PATTERN_1, FUNNEL_PATTERN_2, FUNNEL_PATTERN_3
            ),
        )
        ltseq_r3 = bench(
            "LTSeq",
            lambda: ltseq_funnel(
                t_ltseq_sorted, FUNNEL_PATTERN_1, FUNNEL_PATTERN_2, FUNNEL_PATTERN_3
            ),
        )

        # Validate
        duck_funnel = duckdb_funnel(
            data_file, FUNNEL_PATTERN_1, FUNNEL_PATTERN_2, FUNNEL_PATTERN_3
        )
        ltseq_funnel_count = ltseq_funnel(
            t_ltseq_sorted, FUNNEL_PATTERN_1, FUNNEL_PATTERN_2, FUNNEL_PATTERN_3
        )
        if duck_funnel == ltseq_funnel_count:
            print(f"  Validation: PASS (both = {duck_funnel:,} matches)")
        else:
            print(
                f"  Validation: WARN (DuckDB={duck_funnel:,}, LTSeq={ltseq_funnel_count:,})"
            )

        results.append(
            {"round_name": "R3: Funnel", "duckdb": duck_r3, "ltseq": ltseq_r3}
        )

    # -----------------------------------------------------------------------
    # Output
    # -----------------------------------------------------------------------
    if results:
        print_results_table(results)
        save_results(results, data_file, warmup, iterations)

    return 0


if __name__ == "__main__":
    exit(main())
