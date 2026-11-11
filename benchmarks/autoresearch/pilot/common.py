from __future__ import annotations

import argparse
import json
import math
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TypeIs

REPO_ROOT = Path(__file__).resolve().parents[3]
AUTORESEARCH_DIR = Path(__file__).resolve().parents[1]
PILOT_DIR = Path(__file__).resolve().parent
REPORT_DIR = PILOT_DIR / "reports"
RUNNER_PATH = AUTORESEARCH_DIR / "runner.py"


@dataclass(frozen=True)
class TargetSpec:
    key: str
    title: str
    rounds: tuple[int, ...]
    target_workloads: tuple[str, ...]
    protected_workloads: tuple[str, ...]
    target_improvement_threshold_pct: float = -3.0
    protected_regression_threshold_pct: float = 5.0
    warmup: int = 1
    iterations: int = 3

    @property
    def required_workloads(self) -> tuple[str, ...]:
        return self.target_workloads + self.protected_workloads


# Thresholds. A target workload wins when its LTSeq median AND p95 both improve
# by at least 3%; a protected workload regresses when its median OR p95 worsens
# by 5% or more (#51). These values are defaults, not calibrated limits: see
# "Thresholds and their limits" in docs/BENCHMARK_AUTORESEARCH.md before
# trusting a decision near them, and calibrate per target there (an A/A run on
# one commit) rather than editing the defaults here.

TARGETS = {
    "clickbench_funnel": TargetSpec(
        key="clickbench_funnel",
        title="ClickBench Funnel",
        rounds=(1, 2, 3),
        target_workloads=("r3_funnel",),
        protected_workloads=("r1_top_urls", "r2_sessionization"),
    ),
    "clickbench_sessionization": TargetSpec(
        key="clickbench_sessionization",
        title="ClickBench Sessionization",
        rounds=(1, 2, 3),
        target_workloads=("r2_sessionization",),
        protected_workloads=("r1_top_urls", "r3_funnel"),
    ),
    "clickbench_top_urls": TargetSpec(
        key="clickbench_top_urls",
        title="ClickBench Top URLs",
        rounds=(1, 2, 3),
        target_workloads=("r1_top_urls",),
        protected_workloads=("r2_sessionization", "r3_funnel"),
    ),
}

ROUND_NAME_TO_ID = {
    "R1: Top URLs": "r1_top_urls",
    "R2: Sessionization": "r2_sessionization",
    "R3: Funnel": "r3_funnel",
}

ROUND_NUMBER_TO_ID = {
    1: "r1_top_urls",
    2: "r2_sessionization",
    3: "r3_funnel",
}


class BenchmarkInputError(Exception):
    """An input the gate cannot evaluate: an unknown target, or a summary file
    that is missing, unreadable or not a benchmark summary."""


def resolve_target(target: str) -> TargetSpec:
    try:
        return TARGETS[target]
    except KeyError as exc:
        available = ", ".join(sorted(TARGETS))
        raise BenchmarkInputError(
            f"unknown benchmark target: {target} (available: {available})"
        ) from exc


def ensure_report_dirs() -> None:
    for phase in ("baseline", "candidates", "diff"):
        (REPORT_DIR / phase).mkdir(parents=True, exist_ok=True)


def describe_dataset(data_file: str | None) -> str:
    if not data_file:
        return "unknown dataset"
    data_path = Path(data_file)
    if data_path.name == "hits_sample.parquet":
        return "1M-row sample"
    if data_path.name == "hits_sorted.parquet":
        return "full dataset"
    return f"custom dataset ({data_path})"


def resolve_report_dir(phase: str, target: str) -> Path:
    return REPORT_DIR / phase / target


def clean_output_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def run_build(skip_build: bool = False, dry_run: bool = False) -> None:
    if skip_build:
        return
    cmd = ["maturin", "develop", "--release"]
    if dry_run:
        print("dry-run command:", shell_join(cmd))
        return
    subprocess.run(cmd, cwd=REPO_ROOT, check=True)


def run_runner(
    output_dir: Path,
    rounds: tuple[int, ...],
    *,
    data_mode: str,
    data_path: str | None,
    iterations: int,
    warmup: int,
    skip_profile: bool = True,
    bench_core: bool = False,
    dry_run: bool = False,
) -> None:
    cmd = [sys.executable, str(RUNNER_PATH), "--output-dir", str(output_dir)]
    if skip_profile:
        cmd.append("--skip-profile")
    if not bench_core:
        cmd.append("--no-bench-core")
    cmd.extend(["--iterations", str(iterations), "--warmup", str(warmup)])
    for round_num in rounds:
        cmd.extend(["--round", str(round_num)])

    if data_path:
        cmd.extend(["--data", data_path])
    elif data_mode == "sample":
        cmd.append("--sample")
    elif data_mode == "full":
        # runner.py defaults to the sample, so the full dataset must be asked for.
        cmd.append("--full")
    else:
        raise SystemExit(f"unsupported data mode: {data_mode}")

    if dry_run:
        print("dry-run command:", shell_join(cmd))
        return

    subprocess.run(cmd, cwd=REPO_ROOT, check=True)


def shell_join(parts: list[str]) -> str:
    return " ".join(json.dumps(part) for part in parts)


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def p95(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * 0.95
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    lower_value = ordered[lower]
    upper_value = ordered[upper]
    return lower_value + (upper_value - lower_value) * (rank - lower)


def normalize_round(round_payload: dict, spec: TargetSpec) -> dict:
    workload_id = round_payload.get("round_id") or ROUND_NAME_TO_ID.get(
        round_payload.get("round_name", ""), round_payload.get("round_name", "unknown")
    )
    validation = round_payload.get("validation", {})
    ltseq = round_payload.get("ltseq", {})
    duckdb = round_payload.get("duckdb", {})
    benchmark_status = round_payload.get("benchmark_status", "completed")
    validation_status = validation.get("status", "unknown")
    return {
        "id": workload_id,
        "name": round_payload.get("round_name", workload_id),
        "role": (
            "target"
            if workload_id in spec.target_workloads
            else "protected"
            if workload_id in spec.protected_workloads
            else "context"
        ),
        "benchmark_status": benchmark_status,
        "passed": benchmark_status == "completed" and validation_status == "pass",
        "validation_status": validation_status,
        "validation_detail": validation.get("detail"),
        "ltseq": {
            "median_s": ltseq.get("median_s"),
            "p95_s": p95(ltseq.get("times", [])),
            "times": ltseq.get("times", []),
            "mem_delta_mb": ltseq.get("mem_delta_mb"),
        },
        "duckdb": {
            "median_s": duckdb.get("median_s"),
            "p95_s": p95(duckdb.get("times", [])),
            "times": duckdb.get("times", []),
            "mem_delta_mb": duckdb.get("mem_delta_mb"),
        },
    }


def expected_workload_ids(spec: TargetSpec) -> tuple[str, ...]:
    return tuple(ROUND_NUMBER_TO_ID[round_num] for round_num in spec.rounds)


def ensure_complete_benchmark_capture(spec: TargetSpec, runner_summary: dict) -> None:
    bench_vs = runner_summary.get("bench_vs")
    if not isinstance(bench_vs, dict):
        raise SystemExit("runner summary missing bench_vs benchmark results")

    rounds = bench_vs.get("rounds")
    if not isinstance(rounds, list) or not rounds:
        expected = ", ".join(expected_workload_ids(spec))
        raise SystemExit(
            f"runner summary missing bench_vs round results; expected workloads: {expected}"
        )

    actual_ids = {
        normalize_round(round_payload, spec).get("id")
        for round_payload in rounds
        if isinstance(round_payload, dict)
    }
    missing = [workload_id for workload_id in expected_workload_ids(spec) if workload_id not in actual_ids]
    if missing:
        raise SystemExit(
            "runner summary missing bench_vs workloads: " + ", ".join(missing)
        )


def build_benchmark_summary(target: str, runner_summary: dict) -> dict:
    spec = resolve_target(target)
    bench_vs = runner_summary.get("bench_vs")
    if not isinstance(bench_vs, dict):
        bench_vs = {}
    rounds = [normalize_round(round_payload, spec) for round_payload in bench_vs.get("rounds", [])]
    correctness_failures = int(
        bench_vs.get(
            "correctness_failures",
            sum(1 for workload in rounds if workload.get("validation_status") == "fail"),
        )
    )
    infra_failures = int(
        bench_vs.get(
            "infra_failures",
            sum(1 for workload in rounds if workload.get("benchmark_status") == "infra_failure"),
        )
    )
    passed = bool(bench_vs.get("passed", correctness_failures == 0 and infra_failures == 0))
    if not rounds:
        passed = False
    return {
        "target": target,
        "title": spec.title,
        "git_sha": runner_summary.get("git_sha"),
        "timestamp": runner_summary.get("timestamp"),
        "data_file": bench_vs.get("data_file"),
        "data_file_bytes": dataset_size(bench_vs.get("data_file")),
        "dataset_label": describe_dataset(bench_vs.get("data_file")),
        "warmup": bench_vs.get("warmup"),
        "iterations": bench_vs.get("iterations"),
        "system": bench_vs.get("system"),
        "passed": passed,
        "correctness_failures": correctness_failures,
        "infra_failures": infra_failures,
        "workloads": rounds,
    }


def render_benchmark_result(summary: dict) -> str:
    lines = [
        f"# {summary['title']} benchmark result",
        "",
        f"- Target: `{summary['target']}`",
        f"- Git SHA: `{summary.get('git_sha', '?')}`",
        f"- Dataset: {summary.get('dataset_label', describe_dataset(summary.get('data_file')))}",
        f"- Passed: `{summary.get('passed')}`",
        f"- Correctness failures: `{summary.get('correctness_failures')}`",
        f"- Infra failures: `{summary.get('infra_failures')}`",
        "",
        "| Workload | Role | LTSeq median (s) | LTSeq p95 (s) | Validation |",
        "|----------|------|------------------|---------------|------------|",
    ]
    for workload in summary.get("workloads", []):
        ltseq = workload.get("ltseq", {})
        lines.append(
            f"| {workload['name']} | {workload['role']} | "
            f"{format_metric(ltseq.get('median_s'))} | {format_metric(ltseq.get('p95_s'))} | "
            f"{workload.get('validation_status', 'unknown')} |"
        )
    lines.append("")
    return "\n".join(lines)


def format_metric(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.4f}"


def workload_map(summary: dict) -> dict[str, dict]:
    return {workload["id"]: workload for workload in summary.get("workloads", [])}


def is_positive_number(value: object) -> TypeIs[int | float]:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


def ltseq_metric(workload: dict | None, metric: str) -> object:
    """A workload's raw LTSeq timing, or None when the workload has no
    `ltseq` object. The value is not checked; see is_positive_number."""
    ltseq = (workload or {}).get("ltseq")
    return ltseq.get(metric) if isinstance(ltseq, dict) else None


def pct_delta(baseline: object, candidate: object) -> float | None:
    """Percent change, or None unless both timings are positive numbers.

    Summaries come from files the gate does not control, so a malformed timing
    gives no delta here; the evaluator reports it as `missing_metrics`.
    """
    if not is_positive_number(baseline) or not is_positive_number(candidate):
        return None
    return ((candidate - baseline) / baseline) * 100.0


def build_diff(target: str, baseline: dict, candidate: dict) -> dict:
    spec = resolve_target(target)
    baseline_workloads = workload_map(baseline)
    candidate_workloads = workload_map(candidate)
    workload_ids = list(spec.target_workloads + spec.protected_workloads)
    diffs = []
    for workload_id in workload_ids:
        base = baseline_workloads.get(workload_id)
        cand = candidate_workloads.get(workload_id)
        diffs.append(
            {
                "id": workload_id,
                "name": (cand or base or {}).get("name", workload_id),
                "role": (cand or base or {}).get("role", "unknown"),
                "baseline": base,
                "candidate": cand,
                "ltseq_median_delta_pct": pct_delta(
                    ltseq_metric(base, "median_s"), ltseq_metric(cand, "median_s")
                ),
                "ltseq_p95_delta_pct": pct_delta(ltseq_metric(base, "p95_s"), ltseq_metric(cand, "p95_s")),
            }
        )
    return {
        "target": target,
        "baseline_git_sha": baseline.get("git_sha"),
        "candidate_git_sha": candidate.get("git_sha"),
        "workloads": diffs,
    }


def render_diff(diff: dict) -> str:
    lines = [
        f"# Benchmark diff for {diff['target']}",
        "",
        "| Workload | Role | Median delta | P95 delta |",
        "|----------|------|--------------|-----------|",
    ]
    for workload in diff.get("workloads", []):
        lines.append(
            f"| {workload['name']} | {workload['role']} | "
            f"{format_delta(workload.get('ltseq_median_delta_pct'))} | "
            f"{format_delta(workload.get('ltseq_p95_delta_pct'))} |"
        )
    lines.append("")
    return "\n".join(lines)


def format_delta(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.2f}%"


def dataset_size(data_file: str | None) -> int | None:
    """Size in bytes of the benchmarked file, so the gate can tell two datasets
    apart even when the baseline and candidate ran from different checkouts."""
    if not data_file:
        return None
    path = Path(data_file)
    if not path.is_absolute():
        path = REPO_ROOT / path
    try:
        return path.stat().st_size
    except OSError:
        return None


def check_summary_shape(summary: object, label: str) -> dict:
    """Raise BenchmarkInputError unless `summary` looks like benchmark-summary.json."""
    if not isinstance(summary, dict):
        raise BenchmarkInputError(f"{label} summary is not a JSON object")
    workloads = summary.get("workloads")
    if not isinstance(workloads, list) or not all(
        isinstance(workload, dict) and isinstance(workload.get("id"), str)
        for workload in workloads
    ):
        raise BenchmarkInputError(f"{label} summary has no valid 'workloads' list")
    return summary


def load_summary(path: Path, label: str) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise BenchmarkInputError(f"{label} summary not found: {path}") from exc
    except (OSError, ValueError) as exc:
        raise BenchmarkInputError(f"{label} summary is unreadable: {path}: {exc}") from exc
    return check_summary_shape(payload, label)


PHASE_REPORT_DIRS = {"baseline": "baseline", "candidate": "candidates"}


def capture_main(phase: str, argv: list[str] | None = None) -> int:
    """Run the benchmark for one target and write `<phase>` artifacts.

    Shared by benchmark_baseline.py and benchmark_candidate.py, which differ
    only in the report directory they write.
    """
    parser = argparse.ArgumentParser(description=f"Capture {phase} benchmark artifacts")
    parser.add_argument("target", nargs="?", default="clickbench_funnel", choices=sorted(TARGETS))
    parser.add_argument("--sample", action="store_true", help="1M-row sample; smoke tests only")
    parser.add_argument("--data", type=str, help="custom Parquet file")
    parser.add_argument("--iterations", type=int)
    parser.add_argument("--warmup", type=int)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    spec = resolve_target(args.target)
    ensure_report_dirs()
    output_dir = resolve_report_dir(PHASE_REPORT_DIRS[phase], spec.key)
    if not args.dry_run:
        clean_output_dir(output_dir)

    run_build(skip_build=args.skip_build, dry_run=args.dry_run)
    run_runner(
        output_dir,
        spec.rounds,
        data_mode="sample" if args.sample else "full",
        data_path=args.data,
        iterations=spec.iterations if args.iterations is None else args.iterations,
        warmup=spec.warmup if args.warmup is None else args.warmup,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        return 0

    runner_summary = load_json(output_dir / "summary.json")
    ensure_complete_benchmark_capture(spec, runner_summary)
    benchmark_summary = build_benchmark_summary(spec.key, runner_summary)
    write_json(output_dir / "benchmark-result.json", runner_summary)
    write_json(output_dir / "benchmark-summary.json", benchmark_summary)
    (output_dir / "benchmark-result.md").write_text(
        render_benchmark_result(benchmark_summary), encoding="utf-8"
    )

    print(f"{phase} artifacts: {output_dir}")
    if not benchmark_summary["passed"]:
        # The artifacts are still written so the gate can name the failure.
        print(
            f"warning: {phase} run did not pass: "
            f"correctness_failures={benchmark_summary['correctness_failures']}, "
            f"infra_failures={benchmark_summary['infra_failures']}",
            file=sys.stderr,
        )
    return 0
