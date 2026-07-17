#!/usr/bin/env python3
"""
Data preparation script for the ClickBench benchmark.

Downloads the ClickBench hits.parquet dataset and creates a pre-sorted version
optimized for LTSeq's ordered operations.

Usage:
    uv run --group bench python benchmarks/prepare_data.py \
        [--skip-download] [--sample-only] [--mem-limit SIZE]

    ``--mem-limit`` (e.g. ``8GB``) overrides the auto-detected DuckDB
    ``memory_limit``; by default the sort is capped at 60% of the detected
    cgroup/host memory ceiling and spills to ``benchmarks/data/.duckdb_spill``
    so it does not get OOM-killed on the ~14GB dataset.

Requirements:
    duckdb (in the optional ``bench`` dependency group; activate it with
    ``uv run --group bench`` so it is synced into the venv).
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


def _memory_ceiling_bytes():
    """Best-effort detection of the memory ceiling this process runs under.

    Inside a container DuckDB's default ``memory_limit`` is 80% of the *host*
    RAM, which ignores the cgroup limit and gets the process OOM-killed mid-sort.
    Prefer the cgroup limit (v2 then v1) and fall back to physical RAM.
    """
    candidates = []
    # cgroup v2
    try:
        with open("/sys/fs/cgroup/memory.max") as f:
            v = f.read().strip()
        if v != "max":
            candidates.append(int(v))
    except (OSError, ValueError):
        pass
    # cgroup v1
    try:
        with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as f:
            v = int(f.read().strip())
        if v < (1 << 62):  # v1 uses a ~2^63 sentinel when unlimited
            candidates.append(v)
    except (OSError, ValueError):
        pass
    # physical RAM
    try:
        candidates.append(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"))
    except (OSError, ValueError, AttributeError):
        pass
    return min(candidates) if candidates else None


def _auto_memory_limit_mb(ceiling_bytes):
    """Pick a DuckDB ``memory_limit`` (MiB) that stays safely below *ceiling*.

    We target 60% of the detected ceiling so DuckDB spills to disk with headroom
    for its own bookkeeping plus the Python process. A small floor keeps DuckDB
    usable on mid-size boxes, but the floor is only applied when it is itself
    below the ceiling — otherwise a tiny cgroup (e.g. 256 MiB) would be handed a
    ``memory_limit`` at or above its own budget, reintroducing the very OOM this
    cap exists to prevent (review finding P2).
    """
    ceiling_mb = ceiling_bytes // (1024**2)
    limit_mb = max(1, int(ceiling_bytes * 0.6 / (1024**2)))
    floor_mb = 256
    if floor_mb < ceiling_mb:
        limit_mb = max(limit_mb, floor_mb)
    return limit_mb


def _connect_duckdb(mem_limit=None):
    """Open a DuckDB connection tuned for out-of-core work on this box.

    A bare ``duckdb.connect()`` is an in-memory database with no spill directory,
    so a large ``ORDER BY`` tries to sort 14GB entirely in RAM and gets
    OOM-killed. We (1) point ``temp_directory`` at the big data volume so the
    sort can spill, (2) cap ``memory_limit`` below the cgroup/host ceiling so
    DuckDB starts spilling instead of exceeding the container limit, and
    (3) drop ``preserve_insertion_order`` (irrelevant once we impose ORDER BY)
    to cut peak memory.

    ``mem_limit`` (e.g. ``"8GB"``, from ``--mem-limit``) overrides the
    auto-detected cap when the cgroup ceiling can't be read or needs tuning.
    """
    import duckdb

    spill_dir = os.path.join(DATA_DIR, ".duckdb_spill")
    os.makedirs(spill_dir, exist_ok=True)
    config = {
        "preserve_insertion_order": "false",
        "temp_directory": spill_dir,
    }
    if mem_limit:
        config["memory_limit"] = mem_limit
        print(f"  DuckDB memory_limit={mem_limit} (override), spilling to {spill_dir}")
    else:
        ceiling = _memory_ceiling_bytes()
        if ceiling:
            limit_mb = _auto_memory_limit_mb(ceiling)
            config["memory_limit"] = f"{limit_mb}MB"
            print(f"  DuckDB memory_limit={limit_mb}MB, spilling to {spill_dir}")
    return duckdb.connect(config=config)


def _is_complete_parquet(path):
    """Return True if *path* looks like a fully-written parquet file.

    A parquet file both begins and ends with the 4-byte magic ``PAR1``; the
    trailing magic is written last, so its presence means the writer ran to
    completion. An interrupted DuckDB ``COPY ... TO`` (Ctrl-C, OOM, disk full)
    leaves a truncated or empty file at the destination path. Callers use this
    to regenerate such a file instead of skipping past it and failing later
    with "too small to be a Parquet file".
    """
    try:
        size = os.path.getsize(path)
    except OSError:
        return False
    if size < 8:
        return False
    with open(path, "rb") as f:
        if f.read(4) != b"PAR1":
            return False
        f.seek(-4, os.SEEK_END)
        return f.read(4) == b"PAR1"


def _atomic_copy_parquet(con, select_sql, dest, what):
    """Run ``COPY (select_sql) TO dest`` with crash-safe temp handling.

    Writes to ``dest + '.tmp'`` and atomically renames on success. Two failure
    modes are handled explicitly (review findings P1/P2):

    * A prior run killed by SIGKILL (OOM) cannot run its own validation, so it
      leaves a stale ``.tmp``. We *report that file's size before discarding it*
      rather than silently deleting the primary evidence of the interrupted run.
    * If the writer is killed mid-``COPY`` in this process DuckDB may return
      without raising; we detect a 0-byte/incomplete ``.tmp``, capture its size
      into the error message *before* removing it, and never rename an empty
      file into place. Any failure (including from validation or ``os.replace``)
      removes the ``.tmp`` so it cannot poison a later run.
    """
    tmp = dest + ".tmp"
    if os.path.exists(tmp):
        stale = os.path.getsize(tmp)
        print(
            f"  found leftover {os.path.basename(tmp)} ({stale} bytes) from a "
            "prior interrupted/OOM-killed run; discarding and regenerating"
        )
        try:
            os.remove(tmp)
        except OSError:
            pass
    try:
        con.execute(f"COPY ({select_sql}) TO '{tmp}' (FORMAT 'parquet')")
        if not _is_complete_parquet(tmp):
            size = os.path.getsize(tmp) if os.path.exists(tmp) else 0
            raise RuntimeError(
                f"{what} produced an incomplete parquet at {tmp} ({size} bytes). "
                "The DuckDB process was likely OOM-killed. Check "
                "`dmesg | grep -i 'killed process'` and lower --mem-limit or free "
                "disk on the spill volume."
            )
        os.replace(tmp, dest)
    except BaseException:
        # Includes KeyboardInterrupt: never leave a half-written tmp behind.
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
        raise


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


def sort_data(mem_limit=None):
    """Pre-sort the dataset by (userid, eventtime, watchid) using DuckDB.

    Sort key is (userid, eventtime, watchid).  watchid is used as a tiebreaker
    for the ~5M rows that share the same (userid, eventtime).  This makes the
    sort order fully deterministic and matches the physical row order that
    LTSeq's parallel pattern matcher relies on (via assume_sorted).

    The DuckDB funnel query uses ORDER BY eventtime, watchid to match this
    ordering, ensuring both engines produce identical results.
    """
    if os.path.exists(HITS_SORTED):
        if _is_complete_parquet(HITS_SORTED):
            size_gb = os.path.getsize(HITS_SORTED) / (1024**3)
            print(
                f"  hits_sorted.parquet already exists ({size_gb:.1f} GB), skipping sort"
            )
            return
        print(
            "  hits_sorted.parquet exists but is incomplete/corrupt "
            "(likely an interrupted sort); regenerating..."
        )
        os.remove(HITS_SORTED)

    print(
        "  Sorting hits.parquet by (userid, eventtime, watchid) with lowercase columns..."
    )
    t0 = time.perf_counter()
    con = _connect_duckdb(mem_limit)
    # Get all column names and build rename expressions
    cols = con.execute(f"DESCRIBE SELECT * FROM '{HITS_RAW}'").fetchall()
    select_parts = [f'"{col[0]}" as {col[0].lower()}' for col in cols]
    select_str = ", ".join(select_parts)
    select_sql = (
        f"SELECT {select_str} FROM '{HITS_RAW}' "
        "ORDER BY userid, eventtime, watchid"
    )
    _atomic_copy_parquet(con, select_sql, HITS_SORTED, "Sort")
    elapsed = time.perf_counter() - t0
    size_gb = os.path.getsize(HITS_SORTED) / (1024**3)
    print(f"  Sorted in {elapsed:.1f}s ({size_gb:.1f} GB)")


def create_sample(mem_limit=None):
    """Create a 1M-row sample for quick validation."""
    if os.path.exists(HITS_SAMPLE):
        if _is_complete_parquet(HITS_SAMPLE):
            print("  hits_sample.parquet already exists, skipping")
            return
        print(
            "  hits_sample.parquet exists but is incomplete/corrupt; regenerating..."
        )
        os.remove(HITS_SAMPLE)

    source = HITS_SORTED if _is_complete_parquet(HITS_SORTED) else HITS_RAW
    print(f"  Creating 1M-row sample from {os.path.basename(source)}...")
    con = _connect_duckdb(mem_limit)
    _atomic_copy_parquet(
        con, f"SELECT * FROM '{source}' LIMIT 1000000", HITS_SAMPLE, "Sample"
    )
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
    parser.add_argument(
        "--mem-limit",
        default=None,
        metavar="SIZE",
        help=(
            "Override DuckDB memory_limit (e.g. '8GB', '4000MB'). Defaults to "
            "60%% of the detected cgroup/host memory ceiling so the sort spills "
            "to disk instead of getting OOM-killed."
        ),
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
        sort_data(args.mem_limit)
    else:
        print("[2/3] Skipping sort")

    print("[3/3] Creating sample dataset...")
    create_sample(args.mem_limit)

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
