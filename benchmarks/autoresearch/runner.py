#!/usr/bin/env python3
"""
autoresearch/runner.py — Automated performance research for LTSeq.

Research loop:
  1. Run bench_vs.py (LTSeq vs DuckDB) + bench_core.py
  2. Profile hot rounds with py-spy or perf (optional)
  3. Extract Top-N hotspot functions from flamegraph SVGs
  4. Match hotspots against known optimization patterns → hypotheses
  5. Save results + compare with history
  6. Generate Markdown report

Usage:
    # Quick benchmark only (no profiling)
    python benchmarks/autoresearch/runner.py --skip-profile

    # Full research with flamegraphs (1M sample)
    python benchmarks/autoresearch/runner.py --profile

    # Research on full 100M dataset
    python benchmarks/autoresearch/runner.py --profile --full

    # Specific round only
    python benchmarks/autoresearch/runner.py --round 1

    # Trend report without running benchmarks
    python benchmarks/autoresearch/runner.py --report-only

    # Use perf instead of py-spy
    python benchmarks/autoresearch/runner.py --profile --profiler perf
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Ensure the benchmarks/ directory is on sys.path for ltseq imports
REPO_ROOT = Path(__file__).parent.parent.parent
BENCHMARKS_DIR = Path(__file__).parent.parent
AUTORESEARCH_DIR = Path(__file__).parent
RESULTS_DIR = AUTORESEARCH_DIR / "results"
sys.path.insert(0, str(REPO_ROOT / "py-ltseq"))

if __package__ in (None, ""):
    sys.path.insert(0, str(AUTORESEARCH_DIR.parent))
    from autoresearch.analyzer import extract_hotspots
    from autoresearch.hypothesis import generate_hypotheses
    from autoresearch.profiler import profile_round, profile_python_layer, check_dependencies
    from autoresearch.report import generate_report
    from autoresearch.tracker import save_run, load_history
else:
    from .analyzer import extract_hotspots
    from .hypothesis import generate_hypotheses
    from .profiler import profile_round, profile_python_layer, check_dependencies
    from .report import generate_report
    from .tracker import save_run, load_history


# ---------------------------------------------------------------------------
# Benchmark runners
# ---------------------------------------------------------------------------

def run_bench_vs(data_flag: str, rounds: list[int], iterations: int, warmup: int) -> dict | None:
    """Run bench_vs.py and return parsed JSON results."""
    bench_script = BENCHMARKS_DIR / "bench_vs.py"
    results_json = BENCHMARKS_DIR / "clickbench_results.json"

    # Avoid reusing stale benchmark output from a previous successful run.
    if results_json.exists():
        results_json.unlink()

    cmd = [sys.executable, str(bench_script), "--iterations", str(iterations), "--warmup", str(warmup)]
    if data_flag == "--sample":
        cmd.append("--sample")
    elif data_flag.startswith("--data"):
        cmd.extend(data_flag.split("=", 1))

    # If all three rounds, run once together to get the combined JSON
    if sorted(rounds) == [1, 2, 3]:
        print("\n  [bench_vs] Running all rounds...")
        result = subprocess.run(cmd, cwd=str(REPO_ROOT), text=True)
        if result.returncode != 0:
            print(f"  [bench_vs] Benchmark run failed (rc={result.returncode})")
            return None
    else:
        for r in rounds:
            print(f"\n  [bench_vs] Round {r}...")
            result = subprocess.run(cmd + ["--round", str(r)], cwd=str(REPO_ROOT), text=True)
            if result.returncode != 0:
                print(f"  [bench_vs] Round {r} failed (rc={result.returncode})")

        if not results_json.exists():
            return None

    if results_json.exists():
        try:
            return json.loads(results_json.read_text())
        except Exception as e:
            print(f"  [bench_vs] Failed to parse JSON: {e}")
    return None


def run_bench_core() -> list[dict] | None:
    """Run bench_core.py and return parsed results."""
    bench_script = BENCHMARKS_DIR / "bench_core.py"
    results_json = BENCHMARKS_DIR / "results.json"

    print("\n  [bench_core] Running core operation benchmarks...")
    result = subprocess.run(
        [sys.executable, str(bench_script)],
        cwd=str(REPO_ROOT),
        text=True,
    )
    if result.returncode != 0:
        print(f"  [bench_core] Failed (rc={result.returncode})")
        return None

    if results_json.exists():
        try:
            data = json.loads(results_json.read_text())
            if isinstance(data, list):
                return data
            if isinstance(data, dict) and "results" in data:
                return data["results"]
            return data
        except Exception:
            pass
    return None


# ---------------------------------------------------------------------------
# Main research loop
# ---------------------------------------------------------------------------


def describe_dataset(data_flag: str) -> str:
    if data_flag == "--sample":
        return "1M-row sample"
    if data_flag.startswith("--data="):
        data_path = Path(data_flag.split("=", 1)[1])
        if data_path.name == "hits_sample.parquet":
            return "1M-row sample"
        if data_path.name == "hits_sorted.parquet":
            return "full dataset"
        return f"custom dataset ({data_path})"
    return "full dataset"


def run_research(
    rounds: list[int],
    data_flag: str,
    skip_profile: bool,
    profiler: str,
    bench_core: bool,
    iterations: int,
    warmup: int,
    report_only: bool,
    output_dir: str | None,
) -> Path:
    """Execute the full research loop, return path to run directory."""
    date_str = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    if output_dir:
        run_dir = Path(output_dir).expanduser()
        if not run_dir.is_absolute():
            run_dir = REPO_ROOT / run_dir
    else:
        run_dir = RESULTS_DIR / date_str
    run_dir.mkdir(parents=True, exist_ok=True)

    dataset_label = describe_dataset(data_flag)

    print(f"\n{'='*60}")
    print(f"LTSeq autoresearch — {date_str}")
    print(f"Dataset: {dataset_label} | Rounds: {rounds}")
    print(f"{'='*60}")

    bench_vs_results = None
    bench_core_results = None
    hotspots: dict[str, list[dict]] = {}
    hypotheses: list[dict] = []

    if not report_only:
        # --- Step 1: Benchmarks ---
        print("\n[1/4] Running benchmarks...")
        bench_vs_results = run_bench_vs(data_flag, rounds, iterations, warmup)
        if bench_vs_results:
            (run_dir / "bench_vs.json").write_text(json.dumps(bench_vs_results, indent=2))

        if bench_core:
            bench_core_results = run_bench_core()
            if bench_core_results:
                (run_dir / "bench_core.json").write_text(
                    json.dumps(bench_core_results, indent=2)
                )

        # --- Step 2: Profiling ---
        if not skip_profile:
            print("\n[2/4] Generating flamegraphs...")
            deps = check_dependencies()
            if not deps["py-spy"] and not (deps["perf"] and deps["inferno-collapse-perf"]):
                print("  No profiling tools found. Install with: pip install py-spy")
            else:
                for r in rounds:
                    svg = run_dir / f"r{r}_flamegraph.svg"
                    ok = profile_round(r, svg, data_flag=data_flag, prefer=profiler)
                    if ok:
                        hs = extract_hotspots(svg)
                        if hs:
                            hotspots[f"r{r}"] = hs
                            print(f"  -> r{r}: {len(hs)} hotspot functions found")

                py_svg = run_dir / "python_flamegraph.svg"
                ok = profile_python_layer(py_svg, data_flag=data_flag)
                if ok:
                    hs = extract_hotspots(py_svg)
                    if hs:
                        hotspots["python"] = hs
                        print(f"  -> python layer: {len(hs)} hotspot functions found")
        else:
            print("\n[2/4] Skipping profiling (use --profile to enable)")

        # --- Step 3: Hypotheses ---
        print("\n[3/4] Generating optimization hypotheses...")
        all_hotspots = [h for hs in hotspots.values() for h in hs]
        hypotheses = generate_hypotheses(all_hotspots)
        if hypotheses:
            for h in hypotheses[:3]:
                print(f"  -> [{h.get('effort','?')}] {h['label']} ({h.get('expected_gain','?')})")
        else:
            print("  No profiling data — using static known-pattern list")

        # --- Step 4: Save ---
        print("\n[4/4] Saving results...")
        save_run(run_dir, bench_vs_results, bench_core_results, hotspots)
    else:
        print("  [report-only] Skipping benchmarks and profiling")

    history = load_history(10)

    report = generate_report(
        run_dir=run_dir,
        bench_vs_results=bench_vs_results,
        bench_core_results=bench_core_results,
        hotspots=hotspots,
        hypotheses=hypotheses if not report_only else [],
        history=history,
        data_flag=data_flag,
    )
    report_path = run_dir / "report.md"
    report_path.write_text(report)

    print(f"\n{'='*60}")
    print(f"Report saved: {report_path}")
    print(f"{'='*60}\n")
    print(report)

    return run_dir


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="LTSeq autoresearch: automated performance research tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Data selection
    data_group = parser.add_mutually_exclusive_group()
    data_group.add_argument(
        "--sample", action="store_true", default=True,
        help="Use 1M-row sample (default)",
    )
    data_group.add_argument(
        "--full", action="store_true",
        help="Use full 100M-row dataset",
    )
    data_group.add_argument(
        "--data", type=str, metavar="PATH",
        help="Custom Parquet file path",
    )

    # Round selection
    parser.add_argument(
        "--round", type=int, choices=[1, 2, 3], action="append", dest="rounds",
        help="Run specific round (repeatable; default: all)",
    )

    # Profiling
    parser.add_argument(
        "--profile", action="store_true",
        help="Generate flamegraphs (requires py-spy or perf)",
    )
    parser.add_argument(
        "--skip-profile", action="store_true",
        help="Skip profiling, run benchmarks only",
    )
    parser.add_argument(
        "--profiler", choices=["pyspy", "perf"], default="pyspy",
        help="Preferred profiler (default: pyspy)",
    )

    # Bench options
    parser.add_argument(
        "--no-bench-core", action="store_true",
        help="Skip bench_core.py",
    )
    parser.add_argument(
        "--iterations", type=int, default=3,
        help="Number of timed iterations for bench_vs.py (default: 3)",
    )
    parser.add_argument(
        "--warmup", type=int, default=1,
        help="Number of warmup iterations for bench_vs.py (default: 1)",
    )

    # Report only
    parser.add_argument(
        "--report-only", action="store_true",
        help="Generate trend report from history only, do not run benchmarks",
    )
    parser.add_argument(
        "--output-dir", type=str,
        help="Write artifacts to a specific directory instead of a timestamped run dir",
    )

    args = parser.parse_args()

    # Determine data flag
    if args.data:
        data_flag = f"--data={args.data}"
    elif args.full:
        data_flag = ""  # no flag = full dataset
    else:
        data_flag = "--sample"

    rounds = sorted(set(args.rounds)) if args.rounds else [1, 2, 3]
    skip_profile = not args.profile or args.skip_profile

    run_research(
        rounds=rounds,
        data_flag=data_flag,
        skip_profile=skip_profile,
        profiler=args.profiler,
        bench_core=not args.no_bench_core,
        iterations=args.iterations,
        warmup=args.warmup,
        report_only=args.report_only,
        output_dir=args.output_dir,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
