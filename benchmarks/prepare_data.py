#!/usr/bin/env python3
"""
Data preparation script for the ClickBench benchmark.

Downloads the ClickBench hits.parquet dataset and creates a pre-sorted version
optimized for LTSeq's ordered operations.

Usage:
    uv run python benchmarks/prepare_data.py [--skip-download] [--sample-only]

Requirements:
    pip install duckdb
"""

import os
import sys
import time
import argparse

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
HITS_URL = "https://datasets.clickhouse.com/hits_compatible/hits.parquet"
HITS_RAW = os.path.join(DATA_DIR, "hits.parquet")
HITS_SORTED = os.path.join(DATA_DIR, "hits_sorted.parquet")
HITS_SAMPLE = os.path.join(DATA_DIR, "hits_sample.parquet")


def download_data():
    """Download the ClickBench hits.parquet (~14GB)."""
    if os.path.exists(HITS_RAW):
        size_gb = os.path.getsize(HITS_RAW) / (1024**3)
        if size_gb > 13:
            print(
                f"  hits.parquet already exists ({size_gb:.1f} GB), skipping download"
            )
            return
        else:
            print(
                f"  hits.parquet exists but is only {size_gb:.1f} GB (expected ~14 GB)"
            )
            print("  Resuming download...")

    print(f"  Downloading hits.parquet from {HITS_URL}")
    print("  This is ~14GB and may take a while...")
    import subprocess

    os.makedirs(DATA_DIR, exist_ok=True)
    # Use curl with retries for large download (CDN doesn't support resume)
    subprocess.run(
        [
            "curl",
            "-L",
            "--retry",
            "10",
            "--retry-delay",
            "5",
            "--retry-max-time",
            "7200",
            "-o",
            HITS_RAW,
            HITS_URL,
        ],
        check=True,
    )
    size_gb = os.path.getsize(HITS_RAW) / (1024**3)
    print(f"  Downloaded: {size_gb:.1f} GB")


def sort_data():
    """Pre-sort the dataset by UserID, EventTime using DuckDB."""
    if os.path.exists(HITS_SORTED):
        size_gb = os.path.getsize(HITS_SORTED) / (1024**3)
        print(f"  hits_sorted.parquet already exists ({size_gb:.1f} GB), skipping sort")
        return

    import duckdb

    print("  Sorting hits.parquet by (UserID, EventTime) with lowercase columns...")
    t0 = time.perf_counter()
    con = duckdb.connect()
    # Get all column names and build rename expressions
    cols = con.execute(f"DESCRIBE SELECT * FROM '{HITS_RAW}'").fetchall()
    select_parts = [f'"{col[0]}" as {col[0].lower()}' for col in cols]
    select_str = ", ".join(select_parts)
    con.execute(f"""
        COPY (
            SELECT {select_str}
            FROM '{HITS_RAW}'
            ORDER BY userid, eventtime
        ) TO '{HITS_SORTED}' (FORMAT 'parquet')
    """)
    elapsed = time.perf_counter() - t0
    size_gb = os.path.getsize(HITS_SORTED) / (1024**3)
    print(f"  Sorted in {elapsed:.1f}s ({size_gb:.1f} GB)")


def create_sample():
    """Create a 1M-row sample for quick validation."""
    if os.path.exists(HITS_SAMPLE):
        print("  hits_sample.parquet already exists, skipping")
        return

    import duckdb

    source = HITS_SORTED if os.path.exists(HITS_SORTED) else HITS_RAW
    print(f"  Creating 1M-row sample from {os.path.basename(source)}...")
    con = duckdb.connect()
    con.execute(f"""
        COPY (
            SELECT * FROM '{source}' LIMIT 1000000
        ) TO '{HITS_SAMPLE}' (FORMAT 'parquet')
    """)
    size_mb = os.path.getsize(HITS_SAMPLE) / (1024**2)
    print(f"  Sample created: {size_mb:.1f} MB")


def investigate_urls():
    """Investigate actual URL patterns in the dataset for funnel design."""
    import duckdb

    source = HITS_SAMPLE if os.path.exists(HITS_SAMPLE) else HITS_RAW
    con = duckdb.connect()

    print("\n=== URL Pattern Investigation ===\n")

    print("Top 20 URLs by frequency:")
    result = con.execute(f"""
        SELECT URL, count(*) as cnt
        FROM '{source}'
        GROUP BY URL
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()
    for url, cnt in result:
        print(f"  {cnt:>10,}  {url[:120]}")

    print("\nTop 20 URL prefixes (first path segment):")
    result = con.execute(f"""
        SELECT
            CASE
                WHEN strpos(substring(URL, 9), '/') > 0
                THEN substring(URL, 1, 8 + strpos(substring(URL, 9), '/'))
                ELSE URL
            END as prefix,
            count(*) as cnt
        FROM '{source}'
        WHERE URL != ''
        GROUP BY prefix
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()
    for prefix, cnt in result:
        print(f"  {cnt:>10,}  {prefix[:120]}")

    print("\nUsers with most distinct URLs (potential funnel candidates):")
    result = con.execute(f"""
        SELECT UserID, count(DISTINCT URL) as n_urls, count(*) as n_events
        FROM '{source}'
        GROUP BY UserID
        ORDER BY n_urls DESC
        LIMIT 10
    """).fetchall()
    for uid, n_urls, n_events in result:
        print(f"  UserID={uid}  distinct_urls={n_urls}  events={n_events}")

    print("\nCommon 2-step URL transitions (same user, consecutive events):")
    result = con.execute(f"""
        WITH seq AS (
            SELECT
                URL as url1,
                LEAD(URL) OVER (PARTITION BY UserID ORDER BY EventTime) as url2,
                UserID
            FROM '{source}'
        )
        SELECT url1, url2, count(*) as cnt
        FROM seq
        WHERE url2 IS NOT NULL AND url1 != url2
        GROUP BY url1, url2
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()
    for u1, u2, cnt in result:
        print(f"  {cnt:>8,}  {u1[:60]} -> {u2[:60]}")


def main():
    parser = argparse.ArgumentParser(
        description="Prepare ClickBench data for benchmarking"
    )
    parser.add_argument(
        "--skip-download", action="store_true", help="Skip download step"
    )
    parser.add_argument(
        "--sample-only", action="store_true", help="Only create sample (skip sort)"
    )
    parser.add_argument(
        "--investigate", action="store_true", help="Investigate URL patterns"
    )
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)

    if not args.skip_download:
        print("[1/3] Downloading dataset...")
        download_data()
    else:
        print("[1/3] Skipping download")

    if not args.sample_only:
        print("[2/3] Pre-sorting dataset...")
        sort_data()
    else:
        print("[2/3] Skipping sort")

    print("[3/3] Creating sample dataset...")
    create_sample()

    if args.investigate:
        investigate_urls()

    print("\nDone! Data files:")
    for f in [HITS_RAW, HITS_SORTED, HITS_SAMPLE]:
        if os.path.exists(f):
            size = os.path.getsize(f) / (1024**2)
            print(f"  {f} ({size:.1f} MB)")
        else:
            print(f"  {f} (not found)")


if __name__ == "__main__":
    main()
