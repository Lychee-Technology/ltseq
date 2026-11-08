"""What each consumer raises when an LTSeq stream fails (#256, steps 1 and 2).

Runs every error source against every way of reading a stream and records
the exception's class, whether it is an ``OSError`` or a ``ValueError``,
the batches read before it, and its message. Also records whether
``close()`` or dropping the reader stops the execution (OD-X6), for
pyarrow's readers and for the native subclass of option (e)
(``closing_reader.py``).

Needs a spike build (``maturin develop --release --features stream_spike``)
and, for their columns, DuckDB, Polars, and Cython with a C++20 compiler.
Writes ``results/probe.json`` and ``results/probe.md``.

    python benchmarks/stream_errors/probe.py
"""

from __future__ import annotations

import gc
import json
import os
import platform
import re
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ltseq import LTSeq
from ltseq import _stream_spike as spike
from ltseq._testing import pinned_engine

HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results"

ROWS = 40
BATCH = 4  # ten batches
AFTER = 3  # injected faults fire after three batches
INT64_MIN = -(2**63)

try:
    import duckdb
except ImportError:  # pragma: no cover - optional
    duckdb = None
try:
    import polars as pl
except ImportError:  # pragma: no cover - optional
    pl = None

# OD-X6's option (e): a Cython subclass of pyarrow's reader whose close()
# reaches the iterator. It needs Cython and a C++ compiler; without them its
# columns are left out.
try:
    import closing_reader

    closing_reader.load()
except Exception as e:  # noqa: BLE001 - optional, like DuckDB and Polars
    print(f"leaving out the native subclass: {type(e).__name__}: {e}", file=sys.stderr)
    closing_reader = None


# --- error sources ---------------------------------------------------------
#
# Each source returns (table, fault). An injected fault stands in for a class
# today's engine never raises during execution; the real ones are today's
# execution errors, classified by `stream_spike::classify`.


def _base() -> LTSeq:
    n = ROWS
    return LTSeq.from_arrow(
        pa.table(
            {
                "x": list(range(n)),
                # z is 0 and s is not a number from row 30 on: batch 8 of 10.
                "z": [1] * 30 + [0] * (n - 30),
                "s": ["1"] * 30 + ["a"] * (n - 30),
            }
        )
    )


def _overflow() -> LTSeq:
    """Integer ``//`` that overflows: ``INT64_MIN // -1`` from row 30 on."""
    n = ROWS
    return LTSeq.from_arrow(
        pa.table(
            {
                "x": list(range(30)) + [INT64_MIN] * (n - 30),
                "y": [1] * 30 + [-1] * (n - 30),
            }
        )
    )


def _missing_file() -> LTSeq:
    d = tempfile.mkdtemp(prefix="ltseq-256-")
    p = os.path.join(d, "gone.parquet")
    pq.write_table(pa.table({"x": list(range(ROWS))}), p)
    t = LTSeq.read_parquet(p)
    os.remove(p)
    return t


def _injected(cls: str) -> Callable[[], tuple[LTSeq, Any]]:
    return lambda: (_base(), (AFTER, cls, f"injected after {AFTER} batches"))


SOURCES: dict[str, Callable[[], tuple[LTSeq, Any]]] = {
    "ArithmeticOverflowError (injected)": _injected("ArithmeticOverflowError"),
    "CastError (injected)": _injected("CastError"),
    "OrderViolationError (injected)": _injected("OrderViolationError"),
    "SourceNotFoundError (injected)": _injected("SourceNotFoundError"),
    "LTSeqIOError (injected)": _injected("LTSeqIOError"),
    "ExecutionError (injected)": _injected("ExecutionError"),
    "integer // 0 (real)": lambda: (_base().derive(y=lambda r: r.x // r.z), None),
    "INT64_MIN // -1 (real)": lambda: (_overflow().derive(z=lambda r: r.x // r.y), None),
    "cast of 'a' to int64 (real)": lambda: (_base().derive(y=lambda r: r.s.cast("int64")), None),
    "file deleted after read_parquet (real, raises at the call)": lambda: (_missing_file(), None),
}


# --- read paths ------------------------------------------------------------
#
# Each takes (table, fault) and reads to the end, returning the batches read.


def _drain(reader: pa.RecordBatchReader, seen: list[int]) -> int:
    """Read batch by batch, counting in ``seen`` the batches read so far."""
    while True:
        try:
            reader.read_next_batch()
        except StopIteration:
            return seen[0]
        seen[0] += 1


def _read_next_batch(t: LTSeq, fault: Any, seen: list[int]) -> int:
    return _drain(spike.to_batches(t, fault), seen)


def _iterate(t: LTSeq, fault: Any, seen: list[int]) -> int:
    for _ in spike.to_batches(t, fault):
        seen[0] += 1
    return seen[0]


def _read_all(t: LTSeq, fault: Any, seen: list[int]) -> int:
    return spike.to_batches(t, fault).read_all().num_rows // BATCH


def _pa_table_stream(t: LTSeq, fault: Any, seen: list[int]) -> int:
    return pa.table(spike.Stream(t, fault)).num_rows // BATCH


def _from_stream_stream(t: LTSeq, fault: Any, seen: list[int]) -> int:
    return _drain(pa.RecordBatchReader.from_stream(spike.Stream(t, fault)), seen)


def _pa_table_today(t: LTSeq, fault: Any, seen: list[int]) -> int:
    if fault is not None:
        raise _NotApplicable
    return pa.table(t).num_rows // BATCH


def _from_stream_today(t: LTSeq, fault: Any, seen: list[int]) -> int:
    if fault is not None:
        raise _NotApplicable
    return _drain(pa.RecordBatchReader.from_stream(t), seen)


def _from_stream_reader_batches(t: LTSeq, fault: Any, seen: list[int]) -> int:
    return _drain(pa.RecordBatchReader.from_stream(spike.to_batches(t, fault)), seen)


def _closing_read_next_batch(t: LTSeq, fault: Any, seen: list[int]) -> int:
    return _drain(closing_reader.to_batches(t, fault), seen)


def _closing_iterate(t: LTSeq, fault: Any, seen: list[int]) -> int:
    for _ in closing_reader.to_batches(t, fault):
        seen[0] += 1
    return seen[0]


def _closing_read_all(t: LTSeq, fault: Any, seen: list[int]) -> int:
    return closing_reader.to_batches(t, fault).read_all().num_rows // BATCH


def _closing_pa_table(t: LTSeq, fault: Any, seen: list[int]) -> int:
    return pa.table(closing_reader.to_batches(t, fault)).num_rows // BATCH


def _closing_duckdb(t: LTSeq, fault: Any, seen: list[int]) -> int:
    reader = closing_reader.to_batches(t, fault)  # noqa: F841 - named for DuckDB's scan
    return duckdb.sql("select count(*) from reader").fetchall()[0][0] // BATCH


def _closing_polars(t: LTSeq, fault: Any, seen: list[int]) -> int:
    return pl.from_arrow(closing_reader.to_batches(t, fault)).height // BATCH


def _duckdb_reader(t: LTSeq, fault: Any, seen: list[int]) -> int:
    reader = spike.to_batches(t, fault)  # noqa: F841 - named for DuckDB's scan
    return duckdb.sql("select count(*) from reader").fetchall()[0][0] // BATCH


def _duckdb_stream(t: LTSeq, fault: Any, seen: list[int]) -> int:
    stream = spike.Stream(t, fault)  # noqa: F841 - named for DuckDB's scan
    return duckdb.sql("select count(*) from stream").fetchall()[0][0] // BATCH


def _polars_reader(t: LTSeq, fault: Any, seen: list[int]) -> int:
    return pl.from_arrow(spike.to_batches(t, fault)).height // BATCH


def _polars_stream(t: LTSeq, fault: Any, seen: list[int]) -> int:
    return pl.DataFrame(spike.Stream(t, fault)).height // BATCH


def _polars_from_dataframe(t: LTSeq, fault: Any, seen: list[int]) -> int:
    return pl.from_dataframe(spike.Stream(t, fault)).height // BATCH


class _NotApplicable(Exception):
    pass


PATHS: dict[str, tuple[str, Callable[[LTSeq, Any, list[int]], int]]] = {
    # name: (what it is, reader)
    "read_next_batch": ("to_batches(), Python read", _read_next_batch),
    "iteration": ("to_batches(), Python read", _iterate),
    "read_all": ("to_batches(), Python read", _read_all),
    "from_stream(reader)": ("to_batches() imported as a C stream", _from_stream_reader_batches),
    "pa.table(s)": ("spike C stream", _pa_table_stream),
    "from_stream(s)": ("spike C stream", _from_stream_stream),
    "pa.table(t) today": ("today's C stream", _pa_table_today),
    "from_stream(t) today": ("today's C stream", _from_stream_today),
}
if duckdb is not None:
    PATHS["duckdb(reader)"] = ("to_batches() imported as a C stream", _duckdb_reader)
    PATHS["duckdb(s)"] = ("spike C stream", _duckdb_stream)
if pl is not None:
    PATHS["pl.from_arrow(reader)"] = ("to_batches(), Python read", _polars_reader)
    PATHS["pl.DataFrame(s)"] = ("spike C stream", _polars_stream)
    PATHS["pl.from_dataframe(s)"] = ("spike C stream", _polars_from_dataframe)
if closing_reader is not None:
    PATHS["closing: read_next_batch"] = ("native subclass, Python read", _closing_read_next_batch)
    PATHS["closing: iteration"] = ("native subclass, Python read", _closing_iterate)
    PATHS["closing: read_all"] = ("native subclass, Python read", _closing_read_all)
    PATHS["pa.table(closing)"] = ("native subclass imported as a C stream", _closing_pa_table)
    if duckdb is not None:
        PATHS["duckdb(closing)"] = ("native subclass imported as a C stream", _closing_duckdb)
    if pl is not None:
        PATHS["pl.from_arrow(closing)"] = ("native subclass, Python read", _closing_polars)


def _scrub(message: str) -> str:
    """Drop the temporary directory from a message, so reruns compare."""
    return re.sub(r"/[^ ]*/ltseq-256-[^/ ]*/", "<tmp>/", message)


def probe_cell(source: str, path: str) -> dict[str, Any]:
    t, fault = SOURCES[source]()
    read = PATHS[path][1]
    # Batches read before the raise, for the paths that read one at a time.
    seen = [0]
    counts = read in (_read_next_batch, _iterate, _from_stream_stream,
                      _from_stream_today, _from_stream_reader_batches,
                      _closing_read_next_batch, _closing_iterate)
    try:
        n = read(t, fault, seen)
    except _NotApplicable:
        return {"applicable": False}
    except BaseException as e:  # noqa: BLE001 - recording what is raised
        cls = type(e)
        return {
            "applicable": True,
            "raised": f"{cls.__module__}.{cls.__qualname__}",
            "os_error": isinstance(e, OSError),
            "value_error": isinstance(e, ValueError),
            "ltseq_class": cls.__module__ == spike.__name__,
            "batches_before": seen[0] if counts else None,
            "message": _scrub(str(e)),
        }
    finally:
        gc.collect()
    return {"applicable": True, "raised": None, "batches": n}


# --- close and drop (OD-X6) ------------------------------------------------


def probe_close() -> list[dict[str, Any]]:
    readers: dict[str, Callable[[LTSeq], pa.RecordBatchReader]] = {
        "to_batches() (from_batches over the Rust iterator)": lambda t: spike.to_batches(t),
        "from_stream(spike stream)": lambda t: pa.RecordBatchReader.from_stream(spike.Stream(t)),
        "from_stream(to_batches())": lambda t: pa.RecordBatchReader.from_stream(
            spike.to_batches(t)
        ),
    }
    if closing_reader is not None:
        readers["closing: the native subclass over the Rust iterator"] = closing_reader.to_batches
    rows = []
    for name, make in readers.items():
        t = _base()
        r = make(t)
        r.read_next_batch()
        open_ = spike.live()
        r.close()
        closed = spike.live()
        try:
            r.read_next_batch()
            after = "returns the next batch"
        except StopIteration:
            after = "StopIteration (end of stream)"
        except Exception as e:  # noqa: BLE001
            after = f"{type(e).__name__}: {e}"
        del r
        gc.collect()
        rows.append(
            {
                "reader": name,
                "live_while_open": open_,
                "live_after_close": closed,
                "read_after_close": after,
                "live_after_drop": spike.live(),
            }
        )
    in_with: dict[str, Callable[[LTSeq], pa.RecordBatchReader]] = {
        "to_batches() in a with block": lambda t: spike.to_batches(t),
    }
    if closing_reader is not None:
        in_with["closing: the native subclass in a with block"] = closing_reader.to_batches
    for name, make in in_with.items():
        t = _base()
        with make(t) as r:
            r.read_next_batch()
        after_with = spike.live()
        del r
        gc.collect()
        rows.append(
            {
                "reader": name,
                "live_after_with": after_with,
                "live_after_drop": spike.live(),
            }
        )
    return rows


def versions() -> dict[str, str]:
    out = {
        "python": platform.python_version(),
        "pyarrow": pa.__version__,
        "platform": platform.platform(),
    }
    if duckdb is not None:
        out["duckdb"] = duckdb.__version__
    if pl is not None:
        out["polars"] = pl.__version__
    if closing_reader is not None:
        import Cython

        out["cython"] = Cython.__version__
    return out


def render(result: dict[str, Any]) -> str:
    lines = ["# Stream error probe (#256)", ""]
    lines.append(
        "Generated by `probe.py`; "
        + ", ".join(f"{k} {v}" for k, v in result["versions"].items())
        + f". {ROWS} rows in batches of {BATCH} (ten batches) at "
        f"{result['partitions']} partitions; injected faults fire after {AFTER} batches. Temporary directories read `<tmp>`."
    )
    lines.append("")
    lines.append(
        "Each cell: the exception raised (`LTSeq` = the stand-in class itself), "
        "`OSError`/`ValueError` when it is one, the batches read first where the "
        "path counts them, and the message. `n/a`: the path does not take an "
        "injected fault. `closing:` paths read the native subclass of "
        "`pyarrow.RecordBatchReader` (`_closing_reader.pyx`, OD-X6's option (e))."
    )
    for path, (kind, _) in PATHS.items():
        lines += ["", f"## `{path}` ({kind})", "", "| Source | Raised | Message |", "|---|---|---|"]
        for source in SOURCES:
            cell = result["cells"][source][path]
            if not cell["applicable"]:
                lines.append(f"| {source} | n/a | |")
                continue
            if cell["raised"] is None:
                lines.append(f"| {source} | nothing ({cell['batches']} batches) | |")
                continue
            name = cell["raised"].rsplit(".", 1)[-1]
            tags = []
            if cell["ltseq_class"]:
                name = f"LTSeq `{name}`"
            else:
                name = f"`{cell['raised']}`"
            if cell["os_error"]:
                tags.append("OSError")
            if cell["value_error"]:
                tags.append("ValueError")
            raised = name + (f" ({', '.join(tags)})" if tags else "")
            if cell["batches_before"] is not None:
                raised += f" after {cell['batches_before']} batches"
            message = cell["message"].replace("\n", "\\n").replace("|", "\\|")
            lines.append(f"| {source} | {raised} | `{message}` |")
    lines += ["", "## Closing and dropping a reader", ""]
    lines.append("Executions held (`_spike_live()`) at each step, after one batch was read.")
    lines.append("")
    lines.append("| Reader | Open | After `close()` | Read after `close()` | After drop |")
    lines.append("|---|---|---|---|---|")
    for row in result["close"]:
        if "live_after_with" in row:
            lines.append(
                f"| {row['reader']} | | {row['live_after_with']} (after the block) | "
                f"| {row['live_after_drop']} |"
            )
        else:
            lines.append(
                f"| {row['reader']} | {row['live_while_open']} | {row['live_after_close']} "
                f"| {row['read_after_close']} | {row['live_after_drop']} |"
            )
    return "\n".join(lines) + "\n"


def main() -> int:
    if not spike.available():
        print("needs a spike build: maturin develop --release --features stream_spike")
        return 2
    partitions = 4
    with pinned_engine(target_partitions=partitions, batch_size=BATCH):
        cells = {s: {p: probe_cell(s, p) for p in PATHS} for s in SOURCES}
        close = probe_close()
    result = {
        "versions": versions(),
        "partitions": partitions,
        "cells": cells,
        "close": close,
        "live_at_end": spike.live(),
    }
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / "probe.json").write_text(json.dumps(result, indent=1) + "\n")
    (RESULTS / "probe.md").write_text(render(result))
    print(render(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
