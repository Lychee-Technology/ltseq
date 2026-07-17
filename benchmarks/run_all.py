#!/usr/bin/env python3
"""
LTSeq Benchmark Runner
======================

One-shot orchestrator that runs the LTSeq benchmark suite in order and prints a
consolidated PASS / SKIP / FAIL summary. It shells out to the existing scripts
(bench_core.py, bench_vs.py) via ``sys.executable`` and does not reimplement
any measurement logic.

By default it runs a fast smoke test:

  1. maturin develop --release   (rebuild the extension; skip with --skip-build)
  2. bench_core.py               (LTSeq-only micro-benchmarks; no external data)
  3. bench_vs.py --sample        (LTSeq vs DuckDB on the 1M-row sample)

The ClickBench comparison (bench_vs.py) needs prepared parquet data under
benchmarks/data/. This runner never downloads the ~14 GB ClickBench dataset on
its own: if the data is missing, the bench_vs step is SKIPPED with guidance.
Pass --prepare to explicitly invoke prepare_data.py first (this WILL download
the full dataset unless the files already exist).

The ClickBench comparison (bench_vs.py) and data prep (prepare_data.py) import
duckdb + psutil, which live in the optional ``bench`` dependency group. Activate
it with ``uv run --group bench`` so those packages are synced into the venv the
subprocess steps reuse; without it they are not installed and the vs/prepare
steps fail on import. The core-only run does not need the group.

Usage:
    # Core-only smoke test (rebuild + core; no external deps needed)
    uv run python benchmarks/run_all.py --only core

    # Fast smoke test (rebuild + core + sample ClickBench)
    uv run --group bench python benchmarks/run_all.py

    # Full ClickBench comparison on the sorted dataset
    uv run --group bench python benchmarks/run_all.py --full

    # Skip the rebuild (extension already up to date)
    uv run --group bench python benchmarks/run_all.py --skip-build

    # Only the ClickBench comparison
    uv run --group bench python benchmarks/run_all.py --only vs

    # Explicitly prepare data first (downloads ClickBench data if absent)
    uv run --group bench python benchmarks/run_all.py --full --prepare
"""

import argparse
import json
import os
import subprocess
import sys

BENCHMARKS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BENCHMARKS_DIR, "data")
HITS_SORTED = os.path.join(DATA_DIR, "hits_sorted.parquet")
HITS_SAMPLE = os.path.join(DATA_DIR, "hits_sample.parquet")
CLICKBENCH_RESULTS = os.path.join(BENCHMARKS_DIR, "clickbench_results.json")

# Step outcome markers.
PASS = "PASS"
SKIP = "SKIP"
FAIL = "FAIL"


def run_step(name, cmd, cwd=None):
    """Run a subprocess step, echoing the command and returning (name, outcome)."""
    print("\n" + "=" * 70)
    print(f"STEP: {name}")
    print("  $ " + " ".join(cmd))
    print("=" * 70)
    try:
        result = subprocess.run(cmd, cwd=cwd, check=False)
    except FileNotFoundError as exc:
        # e.g. maturin not on PATH; keep the runner self-contained by
        # reporting this as a FAIL step rather than crashing before the summary.
        print(f"\n  -> {name}: {FAIL} (command not found: {exc.filename or cmd[0]})")
        return name, FAIL
    outcome = PASS if result.returncode == 0 else FAIL
    print(f"\n  -> {name}: {outcome} (exit {result.returncode})")
    return name, outcome


def verify_clickbench_pass(name):
    """Confirm the ClickBench comparison actually passed.

    bench_vs.py returns exit code 0 even when a round fails validation or hits an
    infrastructure failure; the authoritative signal is the top-level ``passed``
    flag it writes to clickbench_results.json. Downgrade a zero-exit PASS to FAIL
    when that flag is not true.
    """
    try:
        with open(CLICKBENCH_RESULTS) as f:
            data = json.load(f)
    except (OSError, ValueError) as exc:
        print(f"  -> {name}: {FAIL} (cannot read {CLICKBENCH_RESULTS}: {exc})")
        return name, FAIL
    if data.get("passed") is True:
        return name, PASS
    detail = (
        f"correctness_failures={data.get('correctness_failures', '?')}, "
        f"infra_failures={data.get('infra_failures', '?')}"
    )
    print(f"  -> {name}: {FAIL} (benchmark reported passed=false; {detail})")
    return name, FAIL


def skip_step(name, reason):
    """Record a skipped step with an explanation."""
    print("\n" + "=" * 70)
    print(f"STEP: {name}")
    print(f"  -> SKIP: {reason}")
    print("=" * 70)
    return name, SKIP


def main():
    parser = argparse.ArgumentParser(
        description="Run the LTSeq benchmark suite and print a consolidated summary."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--sample",
        action="store_true",
        help="Run the ClickBench comparison on the 1M-row sample (default).",
    )
    mode.add_argument(
        "--full",
        action="store_true",
        help="Run the ClickBench comparison on the full sorted dataset.",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip the 'maturin develop --release' rebuild step.",
    )
    parser.add_argument(
        "--prepare",
        action="store_true",
        help=(
            "Run prepare_data.py before the ClickBench comparison. "
            "This downloads the full ClickBench dataset (~14 GB) if the parquet "
            "files are not already present."
        ),
    )
    parser.add_argument(
        "--only",
        choices=["core", "vs"],
        help="Run only the core micro-benchmarks ('core') or the ClickBench comparison ('vs').",
    )
    args = parser.parse_args()

    full = args.full
    data_file = HITS_SORTED if full else HITS_SAMPLE
    dataset_label = "full sorted dataset" if full else "1M-row sample"

    run_core = args.only in (None, "core")
    run_vs = args.only in (None, "vs")

    outcomes = []

    # -- Step 1: rebuild the extension ---------------------------------------
    if not args.skip_build:
        outcomes.append(
            run_step("build (maturin develop --release)", ["maturin", "develop", "--release"])
        )
        if outcomes[-1][1] == FAIL:
            print("\nBuild failed; skipping benchmark steps.")
            print_summary(outcomes)
            return 1
    else:
        outcomes.append(skip_step("build", "--skip-build requested"))

    # -- Step 2: core micro-benchmarks ---------------------------------------
    if run_core:
        outcomes.append(
            run_step(
                "bench_core",
                [sys.executable, os.path.join(BENCHMARKS_DIR, "bench_core.py")],
            )
        )
    else:
        outcomes.append(skip_step("bench_core", "--only vs requested"))

    # -- Step 3: prepare data (opt-in only) ----------------------------------
    if run_vs and args.prepare:
        prep_cmd = [sys.executable, os.path.join(BENCHMARKS_DIR, "prepare_data.py")]
        if not full:
            prep_cmd.append("--sample-only")
        outcomes.append(run_step("prepare_data", prep_cmd))

    # -- Step 4: ClickBench comparison ---------------------------------------
    if run_vs:
        if not os.path.exists(data_file):
            outcomes.append(
                skip_step(
                    "bench_vs",
                    (
                        f"{dataset_label} not found at {data_file}. "
                        "Prepare it first with: "
                        f"uv run --group bench python benchmarks/prepare_data.py"
                        f"{' --sample-only' if not full else ''} "
                        "(or re-run this script with --prepare)."
                    ),
                )
            )
        else:
            vs_cmd = [sys.executable, os.path.join(BENCHMARKS_DIR, "bench_vs.py")]
            if not full:
                vs_cmd.append("--sample")
            step = run_step(f"bench_vs ({dataset_label})", vs_cmd)
            if step[1] == PASS:
                step = verify_clickbench_pass(step[0])
            outcomes.append(step)
    else:
        outcomes.append(skip_step("bench_vs", "--only core requested"))

    return print_summary(outcomes)


def print_summary(outcomes):
    """Print a summary table and return a process exit code (non-zero on any FAIL)."""
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    for name, outcome in outcomes:
        print(f"  {outcome:>4}  {name}")
    print()

    failed = [name for name, outcome in outcomes if outcome == FAIL]
    if failed:
        print(f"{len(failed)} step(s) FAILED: {', '.join(failed)}")
        return 1
    print("All executed steps passed (skipped steps do not count as failures).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
