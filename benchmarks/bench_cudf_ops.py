#!/usr/bin/env python3
"""
GPU acceleration for R1/R2/R3 on the 20M-row ClickBench subset.

Tests three query types with GPU paths where feasible:
  R1: Top 10 URLs by count (GROUP BY string)  → requires cuDF
  R2: User sessionization (numeric shift)      → cupy-only (no cuDF needed)
  R3: Sequential funnel (string LEAD match)    → requires cuDF

Usage:
    python benchmarks/bench_cudf_ops.py
    python benchmarks/bench_cudf_ops.py --data benchmarks/data/hits_20m.parquet
    python benchmarks/bench_cudf_ops.py --round 2   # R2 only (cupy, always runs)
"""

import argparse
import gc
import os
import time

import duckdb
import pyarrow.parquet as pq

BENCHMARKS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_20M = os.path.join(BENCHMARKS_DIR, "data", "hits_20m.parquet")

FUNNEL_P1 = "http://liver.ru/saint-peterburg"
FUNNEL_P2 = "http://liver.ru/place_rukodel"
FUNNEL_P3 = "http://liver.ru/belgorod"

WARMUP = 1
ITERATIONS = 3


def bench(name, func, warmup=WARMUP, iterations=ITERATIONS):
    gc.collect()
    for _ in range(warmup):
        func()
    gc.collect()
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        result = func()
        times.append(time.perf_counter() - t0)
    median = sorted(times)[len(times) // 2]
    print(f"  [{name}] median={median:.3f}s  times=[{', '.join(f'{t:.3f}' for t in times)}]")
    return median, result


# ---------------------------------------------------------------------------
# R1: Top 10 URLs — GPU path requires cuDF
# ---------------------------------------------------------------------------

def r1_cpu_ltseq(data_file):
    from ltseq import LTSeq
    t = LTSeq.read_parquet(data_file)
    return (
        t.agg(by=lambda r: r.url, cnt=lambda g: g.url.count())
        .sort("cnt", desc=True)
        .slice(0, 10)
        .collect()
    )

def r1_cpu_duckdb(data_file):
    return duckdb.sql(f"""
        SELECT url, count(*) as cnt FROM '{data_file}'
        GROUP BY url ORDER BY cnt DESC LIMIT 10
    """).fetchall()

def r1_gpu_cudf(data_file, columns=None):
    """GPU groupby on URL column using cuDF."""
    import cudf
    gdf = cudf.read_parquet(data_file, columns=["url"])
    result = (
        gdf.groupby("url", sort=False)
        .size()
        .reset_index()
        .rename(columns={0: "cnt"})
        .sort_values("cnt", ascending=False)
        .head(10)
    )
    return result.to_pandas()


# ---------------------------------------------------------------------------
# R2: Sessionization — GPU path uses cupy (no cuDF needed)
# ---------------------------------------------------------------------------

def r2_cpu_ltseq(data_file):
    from ltseq import LTSeq
    t = LTSeq.read_parquet(data_file).assume_sorted("userid", "eventtime", "watchid")
    cond = lambda r: (
        (r.userid != r.userid.shift(1)) | (r.eventtime - r.eventtime.shift(1) > 1800)
    )
    return t.group_ordered(cond).first().count()

def r2_cpu_duckdb(data_file):
    return duckdb.sql(f"""
        WITH Diff AS (
            SELECT userid,
                CASE WHEN eventtime - LAG(eventtime)
                    OVER (PARTITION BY userid ORDER BY eventtime, watchid) > 1800
                THEN 1 ELSE 0 END AS is_new
            FROM '{data_file}'
        )
        SELECT sum(is_new) + count(DISTINCT userid) FROM Diff
    """).fetchone()[0]

def r2_gpu_cupy(data_file):
    """
    GPU sessionization using cupy — no cuDF required.

    Data path:
      parquet (col-pruned: userid+eventtime) → PyArrow → NumPy → cupy H2D
      → element-wise shift + compare (all GPU)
      → boundary.sum() → D2H (1 int)

    Works because data is sorted by (userid, eventtime, watchid):
      - global shift(1) is equivalent to per-user LAG
      - first row per user: userid differs from prev → always a boundary
    """
    import cupy as cp

    # Column-pruned parquet read (userid + eventtime only)
    # to_numpy() is zero-copy for fixed-width numeric arrays — no Python objects
    table = pq.read_table(data_file, columns=["userid", "eventtime"])
    userid_np    = table.column("userid").to_numpy()
    eventtime_np = table.column("eventtime").to_numpy()

    # H2D transfer (single memcpy per column via pinned staging)
    userid    = cp.asarray(userid_np)
    eventtime = cp.asarray(eventtime_np)

    # GPU: global shift(1) — valid because data is sorted by (userid, eventtime, watchid)
    # sentinel -1 ensures the very first row always counts as a boundary
    prev_user = cp.empty_like(userid)
    prev_time = cp.empty_like(eventtime)
    prev_user[0]  = -1              # sentinel: first row is always a new session
    prev_time[0]  = eventtime[0]
    prev_user[1:]  = userid[:-1]
    prev_time[1:]  = eventtime[:-1]

    # Boundary: new user OR gap > 30 min
    boundary = (userid != prev_user) | (eventtime - prev_time > 1800)
    return int(boundary.sum())      # D2H: single integer


# ---------------------------------------------------------------------------
# R3: Sequential funnel — GPU path requires cuDF
# ---------------------------------------------------------------------------

def r3_cpu_ltseq(data_file, p1, p2, p3):
    from ltseq import LTSeq
    t = LTSeq.read_parquet(data_file).assume_sorted("userid", "eventtime", "watchid")
    return t.search_pattern_count(
        lambda r: r.url.s.starts_with(p1),
        lambda r: r.url.s.starts_with(p2),
        lambda r: r.url.s.starts_with(p3),
        partition_by="userid",
    )

def r3_cpu_duckdb(data_file, p1, p2, p3):
    return duckdb.sql(f"""
        SELECT count(*) FROM (
            SELECT url,
                LEAD(url) OVER (PARTITION BY userid ORDER BY eventtime, watchid) as next1,
                LEAD(url, 2) OVER (PARTITION BY userid ORDER BY eventtime, watchid) as next2
            FROM '{data_file}'
        )
        WHERE starts_with(url, '{p1}')
          AND starts_with(next1, '{p2}')
          AND starts_with(next2, '{p3}')
    """).fetchone()[0]

def r3_gpu_cudf(data_file, p1, p2, p3):
    """GPU funnel using cuDF shift + str.startswith per user partition."""
    import cudf
    gdf = cudf.read_parquet(data_file, columns=["userid", "eventtime", "url"])
    # Data already sorted by (userid, eventtime, watchid) — use global shift
    # (equivalent to LEAD with PARTITION BY userid ORDER BY eventtime, watchid)
    gdf["next1"] = gdf.groupby("userid")["url"].shift(-1)
    gdf["next2"] = gdf.groupby("userid")["url"].shift(-2)
    mask = (
        gdf["url"].str.startswith(p1)
        & gdf["next1"].str.startswith(p2)
        & gdf["next2"].str.startswith(p3)
    )
    return int(mask.sum())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default=DATA_20M)
    parser.add_argument("--round", type=int, choices=[1, 2, 3])
    parser.add_argument("--warmup", type=int, default=WARMUP)
    parser.add_argument("--iterations", type=int, default=ITERATIONS)
    args = parser.parse_args()

    data_file = args.data
    W, I = args.warmup, args.iterations

    if not os.path.exists(data_file):
        print(f"File not found: {data_file}")
        return 1

    n_rows = duckdb.sql(f"SELECT count(*) FROM '{data_file}'").fetchone()[0]
    print(f"Data: {data_file}  ({n_rows:,} rows)")

    try:
        import cupy as cp
        cupy_ok = True
        print(f"cupy {cp.__version__} ✓")
    except ImportError:
        cupy_ok = False
        print("cupy not available")

    try:
        import cudf
        cudf_ok = True
        print(f"cudf {cudf.__version__} ✓")
    except ImportError:
        cudf_ok = False
        print("cudf not available (R1/R3 GPU skipped)")

    print()

    run_r1 = args.round is None or args.round == 1
    run_r2 = args.round is None or args.round == 2
    run_r3 = args.round is None or args.round == 3

    results = {}

    # ── R1 ──────────────────────────────────────────────────────────────────
    if run_r1:
        print("=" * 60)
        print("R1: Top 10 URLs (GROUP BY string)")
        print("=" * 60)
        duck_t, duck_r = bench("DuckDB CPU", lambda: r1_cpu_duckdb(data_file), W, I)
        ltseq_t, ltseq_r = bench("LTSeq  CPU", lambda: r1_cpu_ltseq(data_file), W, I)
        if cudf_ok:
            gpu_t, gpu_r = bench("cuDF   GPU", lambda: r1_gpu_cudf(data_file), W, I)
            # validate
            duck_urls = [r[0] for r in duck_r]
            gpu_urls  = list(gpu_r["url"])
            ok = set(duck_urls) == set(gpu_urls)
            print(f"  Validation GPU vs DuckDB: {'PASS' if ok else 'WARN'}")
            speedup_vs_duck  = duck_t / gpu_t
            speedup_vs_ltseq = ltseq_t / gpu_t
            print(f"  GPU vs DuckDB: {speedup_vs_duck:.2f}x   GPU vs LTSeq: {speedup_vs_ltseq:.2f}x")
            results["R1"] = {"duckdb": duck_t, "ltseq": ltseq_t, "gpu": gpu_t}
        else:
            print("  GPU: SKIP (requires cuDF)")
            results["R1"] = {"duckdb": duck_t, "ltseq": ltseq_t, "gpu": None}
        print()

    # ── R2 ──────────────────────────────────────────────────────────────────
    if run_r2:
        print("=" * 60)
        print("R2: User Sessionization (numeric shift — cupy sufficient)")
        print("=" * 60)
        duck_t,  duck_r  = bench("DuckDB CPU", lambda: r2_cpu_duckdb(data_file), W, I)
        ltseq_t, ltseq_r = bench("LTSeq  CPU", lambda: r2_cpu_ltseq(data_file), W, I)
        if cupy_ok:
            gpu_t, gpu_r = bench("cupy   GPU", lambda: r2_gpu_cupy(data_file), W, I)
            ok = gpu_r == ltseq_r
            print(f"  Validation GPU={gpu_r:,} vs LTSeq={ltseq_r:,}: {'PASS' if ok else 'FAIL'}")
            speedup_vs_duck  = duck_t / gpu_t
            speedup_vs_ltseq = ltseq_t / gpu_t
            print(f"  GPU vs DuckDB: {speedup_vs_duck:.2f}x   GPU vs LTSeq: {speedup_vs_ltseq:.2f}x")
            results["R2"] = {"duckdb": duck_t, "ltseq": ltseq_t, "gpu": gpu_t}
        else:
            print("  GPU: SKIP (cupy not available)")
            results["R2"] = {"duckdb": duck_t, "ltseq": ltseq_t, "gpu": None}
        print()

    # ── R3 ──────────────────────────────────────────────────────────────────
    if run_r3:
        print("=" * 60)
        print("R3: Sequential Funnel (string LEAD — requires cuDF)")
        print("=" * 60)
        duck_t,  duck_r  = bench("DuckDB CPU", lambda: r3_cpu_duckdb(data_file, FUNNEL_P1, FUNNEL_P2, FUNNEL_P3), W, I)
        ltseq_t, ltseq_r = bench("LTSeq  CPU", lambda: r3_cpu_ltseq(data_file, FUNNEL_P1, FUNNEL_P2, FUNNEL_P3), W, I)
        if cudf_ok:
            gpu_t, gpu_r = bench("cuDF   GPU", lambda: r3_gpu_cudf(data_file, FUNNEL_P1, FUNNEL_P2, FUNNEL_P3), W, I)
            ok = gpu_r == ltseq_r
            print(f"  Validation GPU={gpu_r:,} vs LTSeq={ltseq_r:,}: {'PASS' if ok else 'WARN'}")
            speedup_vs_duck  = duck_t / gpu_t
            speedup_vs_ltseq = ltseq_t / gpu_t
            print(f"  GPU vs DuckDB: {speedup_vs_duck:.2f}x   GPU vs LTSeq: {speedup_vs_ltseq:.2f}x")
            results["R3"] = {"duckdb": duck_t, "ltseq": ltseq_t, "gpu": gpu_t}
        else:
            print("  GPU: SKIP (requires cuDF)")
            results["R3"] = {"duckdb": duck_t, "ltseq": ltseq_t, "gpu": None}
        print()

    # ── Summary ─────────────────────────────────────────────────────────────
    if results:
        print("=" * 60)
        print("SUMMARY")
        print("=" * 60)
        hdr = f"  {'Round':<6} | {'DuckDB':>9} | {'LTSeq CPU':>9} | {'GPU':>9} | {'GPU/DK':>7} | {'GPU/LT':>7}"
        print(hdr)
        print("  " + "-" * (len(hdr) - 2))
        for r_name, r in results.items():
            dk = r["duckdb"]
            lt = r["ltseq"]
            gp = r["gpu"]
            if gp is not None:
                gp_dk = f"{dk/gp:.1f}x"
                gp_lt = f"{lt/gp:.2f}x" if lt > gp else f"{gp/lt:.2f}x slower"
                gp_str = f"{gp:.3f}s"
            else:
                gp_str = "N/A"
                gp_dk  = "—"
                gp_lt  = "—"
            print(f"  {r_name:<6} | {dk:>8.3f}s | {lt:>8.3f}s | {gp_str:>9} | {gp_dk:>7} | {gp_lt:>7}")

    return 0


if __name__ == "__main__":
    exit(main())
