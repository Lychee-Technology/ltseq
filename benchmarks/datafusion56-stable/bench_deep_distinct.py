"""Time LTSeq's ``distinct`` over a deeply computed column (#296, R1-F4).

DataFusion 56 plans a sort or grouping over a column computed by a deep
expression in time that grows about as the cube of the depth. This times
what ``test_operations_on_a_deep_plan_on_a_small_thread`` runs, on release
builds: ``t.derive(s=a + 1 + ... + 1).distinct().count()`` on a sorted
(``t.sort("a")``) and an unsorted table, at each depth, in one pinned layout.

Run it with the interpreter of each engine's environment, then compare:

    python bench_deep_distinct.py --label 55 --out results/deep-distinct-55.json
    python bench_deep_distinct.py --label 56 --out results/deep-distinct-56.json
    python bench_deep_distinct.py --compare results/deep-distinct-55.json \
        results/deep-distinct-56.json

Each case runs in a fresh subprocess (so nothing a previous case cached is
reused), on a thread with a 1 GiB stack, ``--repeat`` times; the median and
every run are recorded.
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import subprocess
import sys
import textwrap

DEPTHS = (25, 50, 100, 200)

CASE = textwrap.dedent(
    """
    import json, sys, threading, time
    sys.setrecursionlimit(50_000)
    from ltseq import LTSeq
    from ltseq._testing import pinned_engine

    depth, shape, partitions, batch = {depth}, {shape!r}, {partitions}, {batch}
    out = {{}}

    def sum_of_depth(r):
        e = r.a
        for _ in range(depth - 1):
            e = e + 1
        return e

    def body():
        with pinned_engine(target_partitions=partitions, batch_size=batch):
            t = LTSeq.from_dict({{"a": [1, 2, 3], "b": [1, 1, 2]}})
            if shape == "sorted":
                t = t.sort("a")
            start = time.perf_counter()
            d = t.derive(s=sum_of_depth)
            built = time.perf_counter()
            n = d.distinct().count()
            done = time.perf_counter()
        out.update(derive_s=built - start, distinct_count_s=done - built, rows=n)

    threading.stack_size(1 << 30)
    th = threading.Thread(target=body)
    th.start()
    th.join()
    print(json.dumps(out))
    """
)


def run_case(depth: int, shape: str, partitions: int, batch: int, timeout: float):
    script = CASE.format(depth=depth, shape=shape, partitions=partitions, batch=batch)
    try:
        run = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return {"timeout_s": timeout}
    if run.returncode != 0 or not run.stdout.strip():
        return {"error": run.stderr[-2000:]}
    return json.loads(run.stdout.strip().splitlines()[-1])


def measure(args) -> dict:
    import ltseq  # noqa: F401  (the build being measured)
    from ltseq import ltseq_core

    cases = []
    for shape in ("sorted", "unsorted"):
        for depth in args.depths:
            runs = [
                run_case(depth, shape, args.partitions, args.batch, args.timeout)
                for _ in range(args.repeat)
            ]
            ok = [r["distinct_count_s"] for r in runs if "distinct_count_s" in r]
            case = {
                "shape": shape,
                "depth": depth,
                "runs": runs,
                "median_distinct_count_s": statistics.median(ok) if ok else None,
            }
            cases.append(case)
            print(
                f"{args.label} {shape:8} {depth:4}: "
                + (f"{case['median_distinct_count_s']:.4f} s" if ok else str(runs[0])),
                file=sys.stderr,
            )
    return {
        "label": args.label,
        "ltseq_core": ltseq_core.__file__,
        "python": sys.version,
        "machine": platform.platform(),
        "processor": platform.machine(),
        "layout": {"target_partitions": args.partitions, "batch_size": args.batch},
        "repeat": args.repeat,
        "cases": cases,
    }


def compare(a_path: str, b_path: str) -> str:
    a = json.load(open(a_path))
    b = json.load(open(b_path))
    by = {(c["shape"], c["depth"]): c for c in b["cases"]}
    lines = [
        f"| shape | depth | {a['label']} median (s) | {b['label']} median (s) | ratio |",
        "|---|---:|---:|---:|---:|",
    ]

    def show(v):
        return "—" if v is None else f"{v:.4f}"

    for c in a["cases"]:
        other = by.get((c["shape"], c["depth"]))
        x = c["median_distinct_count_s"]
        y = other and other["median_distinct_count_s"]
        ratio = f"{y / x:.1f}×" if x and y else "—"
        lines.append(
            f"| {c['shape']} | {c['depth']} | {show(x)} | {show(y)} | {ratio} |"
        )
    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--label")
    p.add_argument("--out")
    p.add_argument("--depths", type=int, nargs="+", default=list(DEPTHS))
    p.add_argument("--repeat", type=int, default=3)
    p.add_argument("--partitions", type=int, default=8)
    p.add_argument("--batch", type=int, default=8192)
    p.add_argument("--timeout", type=float, default=900)
    p.add_argument("--compare", nargs=2, metavar=("A", "B"))
    args = p.parse_args()
    if args.compare:
        print(compare(*args.compare))
        return
    result = measure(args)
    with open(args.out, "w") as f:
        json.dump(result, f, indent=1)
        f.write("\n")


if __name__ == "__main__":
    main()
