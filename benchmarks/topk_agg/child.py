"""One measured cell of the #386 harness, in a process of its own.

``python benchmarks/topk_agg/child.py SPEC`` (``SPEC`` is the JSON that
``run.py`` writes) pins the engine layout, runs the cell's grouped Top-K
query once to warm up, times ``repeat`` runs, executes it ``analyze_repeat``
more times (3) through the ``topk_research`` metrics hook, recording the
process's CPU time over each (the denominator of every stage share), checks
the result against DuckDB, and prints one JSON line.

With ``modes`` in the spec (``compare.py``), the cell instead times the
query under each pruning setting (``_topk_set_prune``: 0 is today's plan,
N is the spike with N slots per partition), interleaved in one process:
a negative N is the spike's control, its waves without pruning; one
warm-up per mode, then ``repeat`` rounds that run every mode once, in
reverse order on odd rounds, so drift and cache state fall on all modes
alike. With ``warmup`` false and ``repeat`` 1 it measures the peak of a
single cold run of one mode, the only peak a later run cannot hide.

A run's time covers building the plan from the Parquet reader and draining
``to_arrow()``; the reader is lazy, so every run scans the file (the page
cache is warm after the warm-up run). Peak memory follows
``benchmarks/m0/child.py``: resident growth over the first measured run,
with the kernel's peak counter reset first.
"""

from __future__ import annotations

import gc
import json
import resource
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "py-ltseq"))
sys.path.insert(0, str(HERE))

from ltseq import LTSeq, ltseq_core  # noqa: E402
from ltseq._testing import pinned_engine  # noqa: E402

import queries  # noqa: E402


def _status_bytes(field: str) -> int:
    with open("/proc/self/status") as status:
        for line in status:
            if line.startswith(field + ":"):
                return int(line.split()[1]) * 1024
    raise LookupError(field)


def _reset_peak() -> None:
    with open("/proc/self/clear_refs", "w") as clear_refs:
        clear_refs.write("5")


def _analyze(table) -> dict:
    before = resource.getrusage(resource.RUSAGE_SELF)
    plan_ns, exec_ns, rows, text, operators = ltseq_core._topk_analyze(table._inner)
    after = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "plan_seconds": plan_ns / 1e9,
        "exec_seconds": exec_ns / 1e9,
        "rows": rows,
        "process_cpu_seconds": (after.ru_utime + after.ru_stime) - (before.ru_utime + before.ru_stime),
        "plan": text,
        "operators": [
            {"depth": d, "name": n, "line": line, "metrics": dict(m)} for d, n, line, m in operators
        ],
    }


def compare(spec: dict) -> None:
    source, key, shape, k = spec["source"], spec["key"], spec["shape"], spec["k"]
    modes = spec["modes"]
    out: dict = {"spec": spec, "modes": {str(m): {"runs": [], "analyze": []} for m in modes}}
    results = {}
    with pinned_engine(target_partitions=spec["partitions"], batch_size=spec.get("batch_size")) as layout:
        out["engine"] = layout

        def run(mode):
            ltseq_core._topk_set_prune(abs(mode), mode > 0)
            table = queries.top_k(LTSeq.read_parquet(source), shape, key, k, spec.get("filter"))
            return table, table.to_arrow()

        if spec.get("warmup", True):
            for mode in modes:
                run(mode)
        for i in range(spec["repeat"]):
            for mode in modes if i % 2 == 0 else modes[::-1]:
                gc.collect()
                baseline = _status_bytes("VmRSS")
                _reset_peak()
                started = time.perf_counter()
                table, results[mode] = run(mode)
                entry = {"seconds": time.perf_counter() - started}
                if not spec.get("warmup", True) and i == 0:
                    entry["peak_above"] = _status_bytes("VmHWM") - baseline
                out["modes"][str(mode)]["runs"].append(entry)
        for mode in modes:
            for i in range(spec.get("analyze_repeat", 3 if spec.get("analyze", True) else 0)):
                ltseq_core._topk_set_prune(abs(mode), mode > 0)
                analysis = _analyze(queries.top_k(LTSeq.read_parquet(source), shape, key, k, spec.get("filter")))
                if i:
                    analysis["plan"] = None
                out["modes"][str(mode)]["analyze"].append(analysis)
        ltseq_core._topk_set_prune(0)
    if spec.get("check", True):
        for mode in modes:
            out["modes"][str(mode)]["check"] = queries.check(
                results[mode], source, shape, key, k, spec.get("filter")
            )
    sys.stdout.write(json.dumps(out) + "\n")


def main(spec: dict) -> None:
    if "modes" in spec:
        return compare(spec)
    source, key, shape, k = spec["source"], spec["key"], spec["shape"], spec["k"]
    out: dict = {"spec": spec}
    with pinned_engine(target_partitions=spec["partitions"], batch_size=spec.get("batch_size")) as layout:
        out["engine"] = layout

        def run():
            table = LTSeq.read_parquet(source)
            return queries.top_k(table, shape, key, k, spec.get("filter")).to_arrow()

        run()  # warm-up: page cache, allocator, footer cache
        runs = []
        result = None
        for i in range(spec["repeat"]):
            gc.collect()
            baseline = _status_bytes("VmRSS")
            _reset_peak()
            started = time.perf_counter()
            result = run()
            seconds = time.perf_counter() - started
            entry = {"seconds": seconds}
            if i == 0:
                entry["peak_above"] = _status_bytes("VmHWM") - baseline
            runs.append(entry)
        out["runs"] = runs

        if spec.get("analyze", True) and hasattr(ltseq_core, "_topk_analyze"):
            out["analyze"] = []
            for i in range(spec.get("analyze_repeat", 3)):
                table = queries.top_k(LTSeq.read_parquet(source), shape, key, k, spec.get("filter"))
                before = resource.getrusage(resource.RUSAGE_SELF)
                plan_ns, exec_ns, rows, text, operators = ltseq_core._topk_analyze(table._inner)
                after = resource.getrusage(resource.RUSAGE_SELF)
                cpu = (after.ru_utime + after.ru_stime) - (before.ru_utime + before.ru_stime)
                out["analyze"].append(
                    {
                        "plan_seconds": plan_ns / 1e9,
                        "exec_seconds": exec_ns / 1e9,
                        "rows": rows,
                        "process_cpu_seconds": cpu,
                        "plan": text if i == 0 else None,
                        "operators": [
                            {"depth": d, "name": n, "line": line, "metrics": dict(m)}
                            for d, n, line, m in operators
                        ],
                    }
                )

    if spec.get("check", True):
        out["check"] = queries.check(result, source, shape, key, k, spec.get("filter"))
    sys.stdout.write(json.dumps(out) + "\n")


if __name__ == "__main__":
    main(json.loads(sys.argv[1]))
