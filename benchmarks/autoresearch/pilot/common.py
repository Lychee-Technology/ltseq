from __future__ import annotations

import json
import math
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
AUTORESEARCH_DIR = Path(__file__).resolve().parents[1]
PILOT_DIR = Path(__file__).resolve().parent
REPORT_DIR = PILOT_DIR / "reports"
RUNNER_PATH = AUTORESEARCH_DIR / "runner.py"


@dataclass(frozen=True)
class TargetSpec:
    key: str
    title: str
    brief_path: Path
    source_files: tuple[str, ...]
    rounds: tuple[int, ...]
    target_workloads: tuple[str, ...]
    protected_workloads: tuple[str, ...]
    target_improvement_threshold_pct: float = -3.0
    protected_regression_threshold_pct: float = 5.0
    warmup: int = 1
    iterations: int = 3


TARGETS = {
    "clickbench_funnel": TargetSpec(
        key="clickbench_funnel",
        title="ClickBench Funnel",
        brief_path=PILOT_DIR / "targets" / "clickbench_funnel_perf.md",
        source_files=("src/ops/pattern_match.rs",),
        rounds=(1, 2, 3),
        target_workloads=("r3_funnel",),
        protected_workloads=("r1_top_urls", "r2_sessionization"),
    ),
    "clickbench_sessionization": TargetSpec(
        key="clickbench_sessionization",
        title="ClickBench Sessionization",
        brief_path=PILOT_DIR / "targets" / "clickbench_sessionization_perf.md",
        source_files=("src/ops/window.rs", "src/ops/derive.rs"),
        rounds=(1, 2, 3),
        target_workloads=("r2_sessionization",),
        protected_workloads=("r1_top_urls", "r3_funnel"),
    ),
}

ROUND_NAME_TO_ID = {
    "R1: Top URLs": "r1_top_urls",
    "R2: Sessionization": "r2_sessionization",
    "R3: Funnel": "r3_funnel",
}


def resolve_target(target: str) -> TargetSpec:
    try:
        return TARGETS[target]
    except KeyError as exc:
        available = ", ".join(sorted(TARGETS))
        raise SystemExit(f"unknown benchmark target: {target} (available: {available})") from exc


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
    elif data_mode != "full":
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


def build_benchmark_summary(target: str, runner_summary: dict) -> dict:
    spec = resolve_target(target)
    bench_vs = runner_summary.get("bench_vs") or {}
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
    passed = bool(
        bench_vs.get("passed", correctness_failures == 0 and infra_failures == 0)
    )
    return {
        "target": target,
        "title": spec.title,
        "brief": str(spec.brief_path.relative_to(REPO_ROOT)),
        "git_sha": runner_summary.get("git_sha"),
        "timestamp": runner_summary.get("timestamp"),
        "data_file": bench_vs.get("data_file"),
        "dataset_label": describe_dataset(bench_vs.get("data_file")),
        "warmup": bench_vs.get("warmup"),
        "iterations": bench_vs.get("iterations"),
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


def pct_delta(baseline: float | None, candidate: float | None) -> float | None:
    if baseline in (None, 0) or candidate is None:
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
                    ((base or {}).get("ltseq") or {}).get("median_s"),
                    ((cand or {}).get("ltseq") or {}).get("median_s"),
                ),
                "ltseq_p95_delta_pct": pct_delta(
                    ((base or {}).get("ltseq") or {}).get("p95_s"),
                    ((cand or {}).get("ltseq") or {}).get("p95_s"),
                ),
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
