"""Inputs of the #251 measurements, and how a cell's process builds them.

The issue asks for the existing benchmark data:

- ``bench_core``: ``bench_core.py``'s synthetic table (``id``, ``value``,
  ``category``, ``amount``, ``name``) at 10K, 100K and 1M rows. Operator
  measurements hold it in memory as ``bench_core.py`` does
  (``read_csv(...).collect()``); the ordered-scan measurement reads the CSV.
- ClickBench: ``hits_sample.parquet`` (1M rows), ``hits_sorted.parquet`` and
  ``hits.parquet`` (about 100M rows each), from ``prepare_data.py``.
- ``r3``: the peak-memory harness's input (``py-ltseq/tests/memory``), whose
  unordered-scan peaks #251's comments report, so the ordered merge can be
  compared at the same partition counts.
- ``zones`` (m4 only): ``bench_core``'s columns cannot make an operand fail
  exactly where the other is FALSE, so m4 generates columns that do, at
  ``bench_core``'s sizes.

A spec is a JSON-able dict; ``build`` turns it into an ``LTSeq`` inside the
cell's process, under the cell's layout and prototypes.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[2]
CLICKBENCH = REPO / "benchmarks" / "data"
WORK = REPO / "target" / "m0"

sys.path.insert(0, str(REPO / "py-ltseq"))
sys.path.insert(0, str(REPO / "benchmarks"))

from ltseq import LTSeq  # noqa: E402

SIZES = (10_000, 100_000, 1_000_000)
WARMUP_ROWS = 10_000


def build(spec: dict) -> LTSeq:
    kind = spec["kind"]
    if kind == "memory_csv":
        table = LTSeq.read_csv(spec["path"]).collect()
    elif kind == "csv":
        table = LTSeq.read_csv(spec["path"])
    elif kind == "parquet":
        table = LTSeq.read_parquet(spec["path"])
    elif kind == "zones":
        table = LTSeq.from_arrow(zones(**spec["params"]))
    else:
        raise ValueError(f"unknown input kind {kind!r}")
    if spec.get("sorted_by"):
        table = table.assume_sorted(*spec["sorted_by"])
    return table


# ---------------------------------------------------------------------------
# Preparation (parent process): files are generated once under target/m0.


def bench_core_csv(rows: int) -> Path:
    """``bench_core.py``'s synthetic CSV with ``rows`` rows (same generator, seed)."""
    from bench_core import generate_test_csv

    path = WORK / "bench_core" / f"data_{rows}.csv"
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        generate_test_csv(str(tmp), rows)
        tmp.rename(path)
    return path


def memory_csv(rows: int, sorted_by: tuple[str, ...] = ()) -> dict:
    spec = {"kind": "memory_csv", "path": str(bench_core_csv(rows)), "rows": rows}
    if sorted_by:
        spec["sorted_by"] = list(sorted_by)
    return spec


def scan_csv(rows: int) -> dict:
    return {"kind": "csv", "path": str(bench_core_csv(rows)), "rows": rows}


def clickbench(name: str, sorted_by: tuple[str, ...] = ()) -> dict:
    path = CLICKBENCH / name
    if not path.exists():
        raise FileNotFoundError(
            f"{path} is missing; run `uv run --group bench python benchmarks/prepare_data.py`"
        )
    spec = {"kind": "parquet", "path": str(path), "rows": pq.read_metadata(path).num_rows}
    if sorted_by:
        spec["sorted_by"] = list(sorted_by)
    return spec


def clickbench_warmup(name: str, sorted_by: tuple[str, ...] = ()) -> dict:
    """The first ``WARMUP_ROWS`` rows of a ClickBench file, same schema."""
    path = WORK / "warmup" / name
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        source = pq.ParquetFile(CLICKBENCH / name)
        batch = next(source.iter_batches(batch_size=WARMUP_ROWS))
        pq.write_table(pa.Table.from_batches([batch]), path)
    spec = {"kind": "parquet", "path": str(path), "rows": WARMUP_ROWS}
    if sorted_by:
        spec["sorted_by"] = list(sorted_by)
    return spec


def r3() -> dict:
    """The memory harness's R3 input: 8 Parquet files of 128 Ki-row groups."""
    from tests.memory import harness

    directory = WORK / "r3"
    rows = harness.rows_for(harness.BUDGET, files=8)
    if not (directory / "part-00007.parquet").exists():
        harness.write_input(directory, rows)
    return {"kind": "parquet", "path": str(directory), "rows": rows}


def r3_warmup() -> dict:
    from tests.memory import harness

    directory = WORK / "r3_warmup"
    if not (directory / "part-00007.parquet").exists():
        harness.write_input(directory, 8 * 1024)
    return {"kind": "parquet", "path": str(directory), "rows": 8 * 1024}


def zones_spec(rows: int, demanded: float, failing: bool) -> dict:
    return {
        "kind": "zones",
        "params": {"rows": rows, "demanded": demanded, "failing": failing},
        "rows": rows,
    }


def warm_page_cache(path: Path) -> None:
    """Read a file (or a directory's files) once, so every cell reads it hot."""
    paths = sorted(path.iterdir()) if path.is_dir() else [path]
    for p in paths:
        with open(p, "rb") as f:
            while f.read(64 << 20):
                pass


# ---------------------------------------------------------------------------
# m4's generated columns.

BIG = 2**62


def zones(rows: int, demanded: float, failing: bool, seed: int = 251) -> pa.Table:
    """Columns for m4's guard and fallible operands.

    Each row lies in one of three zones. Zone 0 holds a ``demanded`` share
    of the rows, zones 1 and 2 split the rest:

    ========  =========  ====================  ====================
    Zone      ``g > 0``  ``a1 * a2 > 0``       ``b1 * b2 > 0``
    ========  =========  ====================  ====================
    0         TRUE       TRUE                  TRUE
    1         FALSE      FALSE                 overflows (else FALSE)
    2         FALSE      overflows (else FALSE)  FALSE
    ========  =========  ====================  ====================

    So ``g > 0`` is a guard TRUE on ``demanded`` of the rows, and with
    ``failing`` each product overflows only where the other operand (and
    the guard) is FALSE: the case where D10 decides whether the query
    raises. Without ``failing`` nothing overflows, which prices the
    evaluation alone.
    """
    rng = np.random.default_rng(seed)
    u = rng.random(rows)
    zone = np.where(u < demanded, 0, np.where(u < demanded + (1 - demanded) / 2, 1, 2))
    g = (zone == 0).astype(np.int64)
    a1 = np.select([zone == 0, zone == 1], [3, -3], BIG if failing else -2).astype(np.int64)
    a2 = np.select([zone == 0, zone == 1], [5, 5], 4 if failing else 5).astype(np.int64)
    b1 = np.select([zone == 0, zone == 2], [7, -7], BIG if failing else -7).astype(np.int64)
    b2 = np.select([zone == 0, zone == 2], [11, 11], 4 if failing else 3).astype(np.int64)
    return pa.table(
        {
            "id": np.arange(rows, dtype=np.int64),
            "g": g,
            "a1": a1,
            "a2": a2,
            "b1": b1,
            "b2": b2,
            "x": rng.integers(0, 1000, rows, dtype=np.int64),
        }
    )
