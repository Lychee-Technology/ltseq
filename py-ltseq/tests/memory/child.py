"""One measured run of the memory harness, in a process of its own.

``python -m tests.memory.child SPEC`` (``SPEC`` is the JSON ``harness.measure``
writes) pins the engine layout, runs the pipeline and terminal once over a
small warm-up input, then measures a run over the real input and prints one
JSON line. The warm-up pages in the code and starts the runtime, which are
one-time costs of the process rather than of the run.

The figure is the peak resident set size from just before the reader's call
through the end of the terminal, minus the resident size just before the
reader's call. Linux resets the peak on a write of ``5`` to
``/proc/self/clear_refs`` and reports it as ``VmHWM``. A watchdog thread
ends the process once the resident size passes ``abort_above``, so a
materializing run reports "aborted" instead of exhausting the machine.
"""

from __future__ import annotations

import gc
import json
import os
import sys
import threading
import time
from pathlib import Path

import pyarrow.parquet as pq

from ltseq import LTSeq
from ltseq._testing import pinned_engine

from .catalog import PIPELINES, TERMINALS

READERS = {"parquet": LTSeq.read_parquet, "csv": LTSeq.read_csv}


def _status_bytes(field: str) -> int:
    with open("/proc/self/status") as status:
        for line in status:
            if line.startswith(field + ":"):
                return int(line.split()[1]) * 1024
    raise LookupError(field)


def _reset_peak() -> None:
    # A kernel without the reset (before Linux 4.0) refuses the write.
    with open("/proc/self/clear_refs", "w") as clear_refs:
        clear_refs.write("5")


def _emit(result: dict) -> None:
    sys.stdout.write(json.dumps(result) + "\n")
    sys.stdout.flush()


def _watch(baseline: int, abort_above: int, done: threading.Event) -> None:
    page = os.sysconf("SC_PAGE_SIZE")
    while not done.wait(0.005):
        with open("/proc/self/statm") as statm:
            resident = int(statm.read().split()[1]) * page
        if resident - baseline > abort_above:
            _emit({"aborted": True, "peak_above": resident - baseline, "drained": None})
            os._exit(0)


def _rows_written(path: Path) -> int:
    if path.suffix == ".parquet":
        return pq.read_metadata(path).num_rows
    with open(path, "rb") as text:
        return sum(chunk.count(b"\n") for chunk in iter(lambda: text.read(1 << 20), b"")) - 1


def _run(source: dict, pipeline: str, terminal: str, out_dir: Path) -> int | Path:
    path = source["path"]
    table = READERS[source["format"]](path)
    result = PIPELINES[pipeline](table, lambda: READERS[source["format"]](path))
    return TERMINALS[terminal](result, out_dir)


def main(spec: dict) -> None:
    out_dir = Path(spec["out_dir"])
    (out_dir / "warmup").mkdir(parents=True, exist_ok=True)
    with pinned_engine(
        target_partitions=spec["target_partitions"], batch_size=spec["batch_size"]
    ):
        _run(spec["warmup"], spec["pipeline"], spec["terminal"], out_dir / "warmup")
        gc.collect()

        baseline = _status_bytes("VmRSS")
        _reset_peak()
        done = threading.Event()
        threading.Thread(
            target=_watch, args=(baseline, spec["abort_above"], done), daemon=True
        ).start()
        started = time.perf_counter()
        drained = _run(spec["source"], spec["pipeline"], spec["terminal"], out_dir)
        seconds = time.perf_counter() - started
        peak = _status_bytes("VmHWM")
        done.set()

    if isinstance(drained, Path):
        drained = _rows_written(drained)
    _emit(
        {
            "aborted": False,
            "peak_above": peak - baseline,
            "baseline": baseline,
            "drained": drained,
            "seconds": round(seconds, 3),
        }
    )


if __name__ == "__main__":
    main(json.loads(sys.argv[1]))
