"""One measured cell of the #251 harness, in a process of its own.

``python benchmarks/m0/child.py SPEC`` (``SPEC`` is the JSON ``harness.run_cell``
writes) pins the engine layout, turns on the cell's prototypes, builds the
input, runs the pipeline and terminal once over the small warm-up input,
then times ``repeat`` runs over the real input and prints one JSON line.

Peak memory follows ``py-ltseq/tests/memory/child.py``: the peak resident
size during a run minus the resident size just before it, with the kernel's
peak counter reset by writing ``5`` to ``/proc/self/clear_refs``. Only the
first measured run's peak is reported as the cell's peak: freed memory the
allocator keeps would hide part of a later run's growth. The warm-up runs
on a small input for the same reason. A watchdog ends the process once the
resident growth passes ``abort_above``.
"""

from __future__ import annotations

import gc
import json
import os
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "py-ltseq"))
sys.path.insert(0, str(HERE))

from ltseq._testing import pinned_engine, prototypes  # noqa: E402

import inputs  # noqa: E402
import pipelines  # noqa: E402


def _status_bytes(field: str) -> int:
    with open("/proc/self/status") as status:
        for line in status:
            if line.startswith(field + ":"):
                return int(line.split()[1]) * 1024
    raise LookupError(field)


def _reset_peak() -> None:
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
            _emit({"aborted": True, "peak_above": resident - baseline})
            os._exit(0)


def _measure_once(build, terminal: str, out_dir: Path, abort_above: int) -> dict:
    gc.collect()
    baseline = _status_bytes("VmRSS")
    _reset_peak()
    done = threading.Event()
    threading.Thread(target=_watch, args=(baseline, abort_above, done), daemon=True).start()
    try:
        started = time.perf_counter()
        table = build()
        outcome = pipelines.run_terminal(terminal, table, out_dir)
        seconds = time.perf_counter() - started
    finally:
        done.set()
    peak = _status_bytes("VmHWM")
    return {
        "seconds": seconds,
        "ttfr": None if outcome.first_row is None else outcome.first_row - started,
        "rows_out": outcome.rows,
        "peak_above": peak - baseline,
    }


def main(spec: dict) -> None:
    out_dir = Path(spec["out_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    pipeline = pipelines.PIPELINES[spec["pipeline"]]
    with (
        pinned_engine(target_partitions=spec["target_partitions"], batch_size=spec["batch_size"]),
        prototypes(**spec["flags"]),
    ):
        warmup = inputs.build(spec["warmup"])
        source = inputs.build(spec["input"])

    def build_warmup():
        return pipeline(warmup)

    def build_source():
        return pipeline(source)

    runs = []
    try:
        pipelines.run_terminal(spec["terminal"], build_warmup(), out_dir)
        for _ in range(spec["repeat"]):
            runs.append(_measure_once(build_source, spec["terminal"], out_dir, spec["abort_above"]))
    except Exception as error:  # an expected outcome for some cells (an overflow raised)
        _emit({"aborted": False, "error": f"{type(error).__name__}: {error}"[:2000], "runs": runs})
        return
    _emit({"aborted": False, "error": None, "runs": runs})


if __name__ == "__main__":
    main(json.loads(sys.argv[1]))
