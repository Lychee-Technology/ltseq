#!/usr/bin/env python3
"""
verify_parquet_order.py — Verify that a Parquet file is sorted by specified columns.

Supports compound sort keys (multiple columns), ASC/DESC per column, and
produces a clear PASS/FAIL report with violation samples.

Algorithm
---------
Two-phase vectorized scan using PyArrow:

  Phase 1 — Intra-RG:  For each row group, compare col[1:] vs col[:-1]
             using Arrow compute kernels (no Python loop over rows).
             Row groups are processed in parallel (ThreadPoolExecutor).

  Phase 2 — Cross-RG seam:  Compare the last row of RG[i] against the
             first row of RG[i+1] for all adjacent pairs, sequentially
             (only N-1 comparisons for N row groups).

Compound lexicographic violation condition for keys (k0, k1, ..., kn):

    violation = (k0[i] ⊳ k0[i-1])
              | (k0[i] == k0[i-1]  &  k1[i] ⊳ k1[i-1])
              | (k0[i] == k0[i-1]  &  k1[i] == k1[i-1]  &  k2[i] ⊳ k2[i-1])
              | ...

where ⊳ means < for ASC columns and > for DESC columns.

NULL handling: NULLs propagate through Arrow comparisons as NULL (not True),
so they are treated as non-violations.  This matches the common convention
that NULL is considered "not out of order" — flag them separately with
--report-nulls if needed.

Usage
-----
    # One or more columns, all ASC (default)
    python benchmarks/verify_parquet_order.py data/hits_sorted.parquet userid eventtime

    # Mixed ASC/DESC
    python benchmarks/verify_parquet_order.py data/hits.parquet userid:asc score:desc

    # Multiple files (glob-friendly)
    python benchmarks/verify_parquet_order.py data/*.parquet userid eventtime watchid

    # Limit parallel workers
    python benchmarks/verify_parquet_order.py --workers 4 data/file.parquet col1 col2

    # Quick metadata-only pre-check (single primary key, no data read)
    python benchmarks/verify_parquet_order.py --metadata-only data/file.parquet userid

    # Show up to 20 violation samples
    python benchmarks/verify_parquet_order.py --samples 20 data/file.parquet userid

Requirements
------------
    pip install pyarrow
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import NamedTuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class SortKey:
    name: str
    asc: bool = True  # True = ASC, False = DESC

    @classmethod
    def parse(cls, spec: str) -> "SortKey":
        """Parse 'col', 'col:asc', or 'col:desc'."""
        if ":" in spec:
            name, direction = spec.rsplit(":", 1)
            direction = direction.strip().lower()
            if direction not in ("asc", "desc"):
                raise ValueError(
                    f"Unknown sort direction '{direction}' in '{spec}' — use :asc or :desc"
                )
            return cls(name=name.strip(), asc=(direction == "asc"))
        return cls(name=spec.strip())

    def __str__(self) -> str:
        return f"{self.name} {'ASC' if self.asc else 'DESC'}"


class Violation(NamedTuple):
    row_group: int  # Which RG (or -1 for cross-RG seam)
    row_offset: int  # Row index within the combined batch where violation occurs
    prev_values: dict  # Column values of the previous row
    curr_values: dict  # Column values of the violating row


@dataclass
class RgResult:
    rg_idx: int
    violation_count: int
    samples: list[Violation] = field(default_factory=list)
    last_row: pa.RecordBatch | None = None  # 1-row batch — last row of this RG
    first_row: pa.RecordBatch | None = None  # 1-row batch — first row of this RG
    error: str | None = None


# ---------------------------------------------------------------------------
# Core: violation detection on an Arrow RecordBatch
# ---------------------------------------------------------------------------


def _build_violation_mask(
    batch: pa.RecordBatch,
    keys: list[SortKey],
) -> pa.ChunkedArray | pa.Array:
    """
    Return a BooleanArray of length (n-1) where True means row[i+1] violates
    the sort order relative to row[i].

    'batch' must have at least 2 rows and must contain all key columns.
    """
    n = batch.num_rows
    assert n >= 2

    # For each key, build cur = col[1:] and prev = col[:-1]
    cols: dict[str, tuple[pa.Array, pa.Array]] = {}
    for key in keys:
        arr = batch.column(key.name)
        cols[key.name] = (arr.slice(1), arr.slice(0, n - 1))  # (cur, prev)

    # Build violation mask lexicographically
    # violation  = key0_bad
    # eq_prefix  = key0_eq
    # Then for each subsequent key k:
    #   violation |= (eq_prefix & key_k_bad)
    #   eq_prefix &= key_k_eq

    def _less(cur: pa.Array, prev: pa.Array) -> pa.Array:
        return pc.less(cur, prev)

    def _greater(cur: pa.Array, prev: pa.Array) -> pa.Array:
        return pc.greater(cur, prev)

    key0 = keys[0]
    cur0, prev0 = cols[key0.name]
    bad_fn = _less if key0.asc else _greater
    violation: pa.Array = bad_fn(cur0, prev0)
    eq_prefix: pa.Array = pc.equal(cur0, prev0)

    for key in keys[1:]:
        cur_k, prev_k = cols[key.name]
        bad_k = (_less if key.asc else _greater)(cur_k, prev_k)
        eq_k = pc.equal(cur_k, prev_k)
        violation = pc.or_(violation, pc.and_(eq_prefix, bad_k))
        eq_prefix = pc.and_(eq_prefix, eq_k)

    return violation


def _collect_samples(
    violation_mask: pa.Array,
    batch: pa.RecordBatch,
    keys: list[SortKey],
    rg_idx: int,
    max_samples: int,
) -> list[Violation]:
    """Extract up to max_samples violation rows as Violation objects."""
    samples: list[Violation] = []
    n = len(violation_mask)
    col_names = [k.name for k in keys]

    for i in range(n):
        if len(samples) >= max_samples:
            break
        val = violation_mask[i]
        if val.is_valid and val.as_py():
            # row i+1 in the batch is the violating row; row i is the predecessor
            prev_row = {c: batch.column(c)[i].as_py() for c in col_names}
            curr_row = {c: batch.column(c)[i + 1].as_py() for c in col_names}
            samples.append(
                Violation(
                    row_group=rg_idx,
                    row_offset=i + 1,
                    prev_values=prev_row,
                    curr_values=curr_row,
                )
            )
    return samples


# ---------------------------------------------------------------------------
# Phase 1: Intra-RG check (parallel)
# ---------------------------------------------------------------------------


def _check_row_group(
    parquet_file: pq.ParquetFile,
    rg_idx: int,
    keys: list[SortKey],
    max_samples: int,
) -> RgResult:
    col_names = [k.name for k in keys]
    try:
        batch = parquet_file.read_row_group(rg_idx, columns=col_names)
    except Exception as e:
        return RgResult(rg_idx=rg_idx, violation_count=0, error=str(e))

    n = batch.num_rows
    first_row = batch.slice(0, 1) if n > 0 else None
    last_row = batch.slice(n - 1, 1) if n > 0 else None

    if n < 2:
        return RgResult(
            rg_idx=rg_idx,
            violation_count=0,
            last_row=last_row,
            first_row=first_row,
        )

    violation_mask = _build_violation_mask(batch, keys)
    count = pc.sum(violation_mask).as_py() or 0
    samples = (
        _collect_samples(violation_mask, batch, keys, rg_idx, max_samples)
        if count > 0
        else []
    )

    return RgResult(
        rg_idx=rg_idx,
        violation_count=count,
        samples=samples,
        last_row=last_row,
        first_row=first_row,
    )


def check_intra_rg(
    parquet_file: pq.ParquetFile,
    keys: list[SortKey],
    workers: int,
    max_samples: int,
) -> list[RgResult]:
    """Check all row groups in parallel. Returns results sorted by rg_idx."""
    n_rg = parquet_file.metadata.num_row_groups

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_check_row_group, parquet_file, i, keys, max_samples): i
            for i in range(n_rg)
        }
        results: list[RgResult] = []
        for future in as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda r: r.rg_idx)
    return results


# ---------------------------------------------------------------------------
# Phase 2: Cross-RG seam check (sequential, O(N) tiny comparisons)
# ---------------------------------------------------------------------------


def check_cross_rg_seams(
    rg_results: list[RgResult],
    keys: list[SortKey],
    max_samples: int,
) -> list[Violation]:
    """
    For each adjacent pair (RG[i], RG[i+1]), check whether the last row of
    RG[i] is ordered before the first row of RG[i+1].
    """
    violations: list[Violation] = []
    col_names = [k.name for k in keys]

    for i in range(len(rg_results) - 1):
        last_row = rg_results[i].last_row
        first_row = rg_results[i + 1].first_row
        if last_row is None or first_row is None:
            continue

        # Combine into a 2-row batch and check
        try:
            combined = pa.concat_tables(
                [
                    pa.table({c: last_row.column(c) for c in col_names}),
                    pa.table({c: first_row.column(c) for c in col_names}),
                ]
            ).to_batches()[0]
        except pa.lib.ArrowInvalid:
            # Schema mismatch between RGs — cast to common type
            combined = _unify_and_concat(last_row, first_row, col_names)
            if combined is None:
                continue

        if combined.num_rows < 2:
            continue

        violation_mask = _build_violation_mask(combined, keys)
        is_violation = violation_mask[0]
        if is_violation.is_valid and is_violation.as_py():
            if len(violations) < max_samples:
                prev_vals = {c: combined.column(c)[0].as_py() for c in col_names}
                curr_vals = {c: combined.column(c)[1].as_py() for c in col_names}
                violations.append(
                    Violation(
                        row_group=-1,  # cross-RG seam
                        row_offset=i,  # boundary index between RG[i] and RG[i+1]
                        prev_values=prev_vals,
                        curr_values=curr_vals,
                    )
                )

    return violations


def _unify_and_concat(
    a: pa.RecordBatch,
    b: pa.RecordBatch,
    col_names: list[str],
) -> pa.RecordBatch | None:
    """Cast b's columns to a's schema and concatenate."""
    try:
        cast_cols = {}
        for c in col_names:
            target_type = a.schema.field(c).type
            col_b = b.column(c)
            if col_b.type != target_type:
                col_b = col_b.cast(target_type)
            cast_cols[c] = pa.concat_arrays([a.column(c), col_b])
        return pa.record_batch(cast_cols)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Metadata-only fast pre-check (single column, uses Parquet column statistics)
# ---------------------------------------------------------------------------


def check_metadata_only(
    parquet_file: pq.ParquetFile,
    key: SortKey,
) -> tuple[int, list[dict]]:
    """
    Use per-RG column min/max statistics to detect global order violations
    for a single sort key without reading any data.

    Returns (violation_count, samples).

    Limitation: statistics only give per-RG min/max, so this can only detect
    cases where RG[i+1].min < RG[i].max (for ASC). It cannot detect
    intra-RG violations or violations involving equal RG-boundary values.
    Use this as a fast sanity check before the full data scan.
    """
    meta = parquet_file.metadata
    n_rg = meta.num_row_groups
    violations = 0
    samples: list[dict] = []

    # Find column index by name
    col_idx: int | None = None
    rg0 = meta.row_group(0)
    for j in range(rg0.num_columns):
        if rg0.column(j).path_in_schema == key.name:
            col_idx = j
            break

    if col_idx is None:
        raise ValueError(f"Column '{key.name}' not found in Parquet schema")

    prev_max = None
    prev_min = None
    for i in range(n_rg):
        rg = meta.row_group(i)
        stats = rg.column(col_idx).statistics
        if stats is None or not stats.has_min_max:
            continue
        rg_min, rg_max = stats.min, stats.max

        if prev_max is not None:
            if key.asc and rg_min < prev_max:
                violations += 1
                if len(samples) < 5:
                    samples.append(
                        {
                            "rg_prev": i - 1,
                            "rg_curr": i,
                            "prev_max": prev_max,
                            "curr_min": rg_min,
                            "prev_min": prev_min,
                            "curr_max": rg_max,
                        }
                    )
            elif not key.asc and rg_max > prev_min:
                violations += 1
                if len(samples) < 5:
                    samples.append(
                        {
                            "rg_prev": i - 1,
                            "rg_curr": i,
                            "prev_min": prev_min,
                            "curr_max": rg_max,
                        }
                    )

        prev_max = rg_max
        prev_min = rg_min

    return violations, samples


# ---------------------------------------------------------------------------
# Reporting helpers
# ---------------------------------------------------------------------------


def _fmt_values(vals: dict) -> str:
    parts = [f"{k}={repr(v)}" for k, v in vals.items()]
    return "  ".join(parts)


def _print_samples(samples: list[Violation], label: str) -> None:
    print(f"  Sample violations ({label}):")
    for v in samples:
        loc = (
            f"RG {v.row_group} row {v.row_offset}"
            if v.row_group >= 0
            else f"seam after RG {v.row_offset}"
        )
        print(f"    [{loc}]")
        print(f"      prev: {_fmt_values(v.prev_values)}")
        print(f"      curr: {_fmt_values(v.curr_values)}")


# ---------------------------------------------------------------------------
# Main verification entry point
# ---------------------------------------------------------------------------


def verify_file(
    path: str,
    keys: list[SortKey],
    workers: int,
    max_samples: int,
    metadata_only: bool,
) -> bool:
    """
    Verify one Parquet file. Returns True if PASS, False if FAIL.
    Prints a human-readable report to stdout.
    """
    if not os.path.exists(path):
        print(f"ERROR: file not found: {path}")
        return False

    key_str = ", ".join(str(k) for k in keys)
    filename = os.path.basename(path)
    print(f"\nVerifying order [{key_str}] in {filename}")

    pf = pq.ParquetFile(path)
    meta = pf.metadata
    n_rg = meta.num_row_groups
    n_rows = meta.num_rows
    print(f"  Row groups: {n_rg:,} | Rows: {n_rows:,}")

    # ------------------------------------------------------------------
    # Metadata-only path (single key, no data read)
    # ------------------------------------------------------------------
    if metadata_only:
        if len(keys) > 1:
            print(
                "  WARNING: --metadata-only supports only one key; ignoring extra keys"
            )
        t0 = time.perf_counter()
        count, samples = check_metadata_only(pf, keys[0])
        elapsed = time.perf_counter() - t0
        print(
            f"  Metadata-only check (no data read)...  {count} RG-level violations  [{elapsed:.2f}s]"
        )
        if count > 0:
            print(
                f"  FAIL — {count} row group(s) have min/max overlap violating {keys[0]} order"
            )
            for s in samples:
                print(
                    f"    RG {s['rg_prev']} max={s.get('prev_max', s.get('prev_min'))}  "
                    f"RG {s['rg_curr']} min={s.get('curr_min', s.get('curr_max'))}"
                )
            return False
        else:
            print(
                f"  PASS (metadata only — no RG-level violations; run without --metadata-only for full check)"
            )
            return True

    # ------------------------------------------------------------------
    # Full data scan — Phase 1: intra-RG (parallel)
    # ------------------------------------------------------------------
    t0 = time.perf_counter()
    rg_results = check_intra_rg(pf, keys, workers, max_samples)
    t1 = time.perf_counter()

    intra_errors = [r for r in rg_results if r.error]
    intra_violations = sum(r.violation_count for r in rg_results)
    intra_samples: list[Violation] = []
    for r in rg_results:
        intra_samples.extend(r.samples)
        if len(intra_samples) >= max_samples:
            intra_samples = intra_samples[:max_samples]
            break

    status_intra = "FAIL" if (intra_violations > 0 or intra_errors) else "OK"
    print(
        f"  Intra-RG check ({workers} workers)...    "
        f"{intra_violations:,} violations  [{t1 - t0:.2f}s]  {status_intra}"
    )
    if intra_errors:
        print(f"  WARNING: {len(intra_errors)} row group(s) failed to read:")
        for r in intra_errors[:3]:
            print(f"    RG {r.rg_idx}: {r.error}")
    if intra_violations > 0 and intra_samples:
        _print_samples(intra_samples[:max_samples], "intra-RG")

    # ------------------------------------------------------------------
    # Phase 2: cross-RG seams (sequential, fast)
    # ------------------------------------------------------------------
    t0 = time.perf_counter()
    cross_violations = check_cross_rg_seams(rg_results, keys, max_samples)
    t1 = time.perf_counter()

    status_cross = "FAIL" if cross_violations else "OK"
    print(
        f"  Cross-RG seam check...           "
        f"{len(cross_violations):,} violations  [{t1 - t0:.2f}s]  {status_cross}"
    )
    if cross_violations:
        _print_samples(cross_violations[:max_samples], "cross-RG seam")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    total = intra_violations + len(cross_violations)
    passed = total == 0 and not intra_errors
    if passed:
        print(f"  PASS — file is sorted by [{key_str}]")
    else:
        print(f"  FAIL — {total:,} total violation(s) found")
    return passed


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify that a Parquet file is sorted by the specified columns.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "files",
        nargs="+",
        metavar="FILE_OR_COL",
        help=(
            "Parquet file(s) followed by column specs.  "
            "Columns are specified as 'col' (ASC) or 'col:asc'/'col:desc'.  "
            "All non-.parquet arguments after the last .parquet file are treated as column specs.  "
            "Example:  data/hits.parquet userid eventtime:asc score:desc"
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=min(8, (os.cpu_count() or 4)),
        metavar="N",
        help="Number of parallel workers for row-group scanning (default: min(8, cpu_count))",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=5,
        metavar="N",
        help="Maximum number of violation examples to display per file (default: 5)",
    )
    parser.add_argument(
        "--metadata-only",
        action="store_true",
        help=(
            "Fast pre-check using Parquet column statistics (no data read).  "
            "Only supports a single sort key.  Detects RG-level min/max overlaps only."
        ),
    )
    return parser.parse_args()


def split_files_and_keys(args_list: list[str]) -> tuple[list[str], list[SortKey]]:
    """
    Split the positional args into Parquet file paths and column specs.

    Rules:
    - A token ending in '.parquet' (case-insensitive) or that is an existing
      file is treated as a file path.
    - Everything else is treated as a column spec (col or col:asc/col:desc).
    - Files must come before column specs (standard CLI convention).
    """
    files: list[str] = []
    col_specs: list[str] = []
    switched = False  # Once we see the first non-file token, all remaining are cols

    for token in args_list:
        if not switched and (
            token.lower().endswith(".parquet") or os.path.isfile(token)
        ):
            files.append(token)
        else:
            switched = True
            col_specs.append(token)

    if not files:
        raise ValueError("No Parquet file paths provided.")
    if not col_specs:
        raise ValueError(
            "No column specs provided.  Specify at least one column, e.g.: userid  or  userid:asc eventtime:desc"
        )

    keys = [SortKey.parse(s) for s in col_specs]
    return files, keys


def main() -> int:
    args = parse_args()

    try:
        files, keys = split_files_and_keys(args.files)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    all_passed = True
    for path in files:
        passed = verify_file(
            path=path,
            keys=keys,
            workers=args.workers,
            max_samples=args.samples,
            metadata_only=args.metadata_only,
        )
        if not passed:
            all_passed = False

    if len(files) > 1:
        print(f"\nOverall: {'PASS' if all_passed else 'FAIL'} ({len(files)} files)")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
