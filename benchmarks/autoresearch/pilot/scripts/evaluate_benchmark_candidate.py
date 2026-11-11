#!/usr/bin/env python3
"""Decide keep or discard for a benchmark candidate against its baseline.

The policy (docs/BENCHMARK_AUTORESEARCH.md, "Evaluation contract"):

- the baseline and candidate must be comparable: each records the target,
  dataset, iterations and warmup, and the two records match;
- the baseline must itself be a valid run;
- the candidate must have no correctness or infrastructure failure, every
  target and protected workload, and a median and p95 for each;
- no protected workload may regress by the regression threshold or more in
  median OR p95;
- at least one target workload must improve by the improvement threshold or
  more in median AND p95 (#51).

Every failed check gives `discard` with a `reason_code`.

Exit status of the CLI: 0 keep, 1 discard, 2 when the inputs cannot be
evaluated at all (missing or malformed summary, unknown target, bad option).
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from pilot.common import (
        TARGETS,
        BenchmarkInputError,
        build_diff,
        check_summary_shape,
        format_delta,
        is_positive_number,
        load_summary,
        ltseq_metric,
        resolve_target,
    )
else:
    from ..common import (
        TARGETS,
        BenchmarkInputError,
        build_diff,
        check_summary_shape,
        format_delta,
        is_positive_number,
        load_summary,
        ltseq_metric,
        resolve_target,
    )

EXIT_KEEP = 0
EXIT_DISCARD = 1
EXIT_INPUT_ERROR = 2

# Run settings that must match for two timings to be compared, with the least
# usable value of each.
COMPARED_SETTINGS = {"iterations": 1, "warmup": 0}
# Machine facts that should match; a difference is reported, not rejected,
# because a kernel or library update changes them on the same machine.
COMPARED_SYSTEM_FIELDS = ("platform", "processor", "cpu_count", "python_version")

REASON_LABELS = {
    "correctness_failure": "correctness regressions detected",
    "infra_failure": "infrastructure failures detected",
    "missing_workloads": "missing workloads",
    "unvalidated_workload": "workloads without a passing validation",
    "missing_metrics": "missing performance metrics",
}


def exit_status(evaluation: dict) -> int:
    return EXIT_KEEP if evaluation["recommendation"] == "keep" else EXIT_DISCARD


def _workloads(summary: dict) -> dict[str, dict]:
    return {workload["id"]: workload for workload in summary.get("workloads", [])}


def _count(summary: dict, field: str) -> int:
    try:
        return int(summary.get(field) or 0)
    except (TypeError, ValueError):
        # An unreadable count cannot vouch for a clean run.
        return 1


def _is_count(value: object, minimum: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= minimum


def _compatibility_problems(target: str, baseline: dict, candidate: dict) -> list[str]:
    # A setting both summaries lack would compare equal, so each summary must
    # record every setting before the two are compared.
    problems = []
    labelled = (("baseline", baseline), ("candidate", candidate))
    for label, summary in labelled:
        if summary.get("target") is None:
            problems.append(f"{label} summary does not name its target")
        elif summary.get("target") != target:
            problems.append(f"{label} summary is for target {summary.get('target')!r}, not {target!r}")

    base_file, cand_file = baseline.get("data_file"), candidate.get("data_file")
    if not (isinstance(base_file, str) and base_file and isinstance(cand_file, str) and cand_file):
        problems.append(
            f"dataset unknown: data_file is baseline {base_file!r}, candidate {cand_file!r}"
        )
    else:
        base_bytes, cand_bytes = baseline.get("data_file_bytes"), candidate.get("data_file_bytes")
        if base_bytes is not None and cand_bytes is not None:
            # Both sizes known: compare the file, wherever each checkout keeps it.
            same = Path(base_file).name == Path(cand_file).name and base_bytes == cand_bytes
        else:
            same = base_file == cand_file
        if not same:
            problems.append(
                f"datasets differ: baseline {base_file} ({base_bytes} bytes), "
                f"candidate {cand_file} ({cand_bytes} bytes)"
            )

    for field, minimum in COMPARED_SETTINGS.items():
        unusable = [
            f"{label} {summary.get(field)!r}"
            for label, summary in labelled
            if not _is_count(summary.get(field), minimum)
        ]
        if unusable:
            problems.append(f"{field} unknown: expected an integer >= {minimum}, got " + ", ".join(unusable))
        elif baseline[field] != candidate[field]:
            problems.append(f"{field} differs: baseline {baseline[field]!r}, candidate {candidate[field]!r}")
    return problems


def _system_warnings(baseline: dict, candidate: dict) -> list[str]:
    base_system, cand_system = baseline.get("system"), candidate.get("system")
    if not isinstance(base_system, dict) or not isinstance(cand_system, dict):
        return ["system metadata missing from a summary; cannot confirm both runs used one machine"]
    return [
        f"system {field} differs: baseline {base_system.get(field)!r}, candidate {cand_system.get(field)!r}"
        for field in COMPARED_SYSTEM_FIELDS
        if base_system.get(field) != cand_system.get(field)
    ]


def _run_problems(spec, summary: dict) -> dict[str, list[str]]:
    """Problems that make a run unusable, keyed by reason code, in check order."""
    workloads = _workloads(summary)
    problems: dict[str, list[str]] = {code: [] for code in REASON_LABELS}
    if _count(summary, "correctness_failures") > 0:
        problems["correctness_failure"].append(
            f"correctness_failures={summary.get('correctness_failures')}"
        )
    if _count(summary, "infra_failures") > 0:
        problems["infra_failure"].append(f"infra_failures={summary.get('infra_failures')}")

    for workload_id in spec.required_workloads:
        workload = workloads.get(workload_id)
        if workload is None:
            problems["missing_workloads"].append(workload_id)
            continue
        validation = workload.get("validation_status")
        status = workload.get("benchmark_status")
        if validation == "fail":
            problems["correctness_failure"].append(f"{workload_id} validation failed")
        elif status == "infra_failure" or validation == "infra_failure":
            problems["infra_failure"].append(f"{workload_id} infrastructure failure")
        elif status != "completed" or validation != "pass":
            problems["unvalidated_workload"].append(
                f"{workload_id} benchmark_status={status!r} validation_status={validation!r}"
            )
        for metric in ("median_s", "p95_s"):
            value = ltseq_metric(workload, metric)
            if not is_positive_number(value):
                problems["missing_metrics"].append(f"{workload_id} ltseq.{metric}={value!r}")
    return {code: found for code, found in problems.items() if found}


def resolve_thresholds(
    spec,
    *,
    min_improvement_pct: float | None = None,
    max_regression_pct: float | None = None,
) -> dict:
    overridden = min_improvement_pct is not None or max_regression_pct is not None
    return {
        "target_improvement_threshold_pct": (
            -abs(min_improvement_pct)
            if min_improvement_pct is not None
            else spec.target_improvement_threshold_pct
        ),
        "protected_regression_threshold_pct": (
            abs(max_regression_pct)
            if max_regression_pct is not None
            else spec.protected_regression_threshold_pct
        ),
        "source": "override" if overridden else "target_spec",
    }


def evaluate_candidate(
    target: str,
    baseline: dict,
    candidate: dict,
    *,
    min_improvement_pct: float | None = None,
    max_regression_pct: float | None = None,
) -> dict:
    spec = resolve_target(target)
    check_summary_shape(baseline, "baseline")
    check_summary_shape(candidate, "candidate")
    thresholds = resolve_thresholds(
        spec, min_improvement_pct=min_improvement_pct, max_regression_pct=max_regression_pct
    )
    improvement_bar = thresholds["target_improvement_threshold_pct"]
    regression_bar = thresholds["protected_regression_threshold_pct"]
    warnings = _system_warnings(baseline, candidate)

    def result(reason_code: str, reason: str, **fields) -> dict:
        payload = {
            "target": target,
            "recommendation": "keep" if reason_code == "target_improvement" else "discard",
            "reason_code": reason_code,
            "reason": reason,
            "target_win": "none",
            "protected_status": "n/a",
            "target_wins_detail": [],
            "protected_regressions_detail": [],
            "missing_workloads": [],
            "problems": [],
            "warnings": warnings,
            "baseline_git_sha": baseline.get("git_sha"),
            "candidate_git_sha": candidate.get("git_sha"),
            "thresholds": thresholds,
        }
        payload.update(fields)
        return payload

    incompatible = _compatibility_problems(target, baseline, candidate)
    if incompatible:
        return result(
            "incompatible_inputs",
            "baseline and candidate are not comparable: " + "; ".join(incompatible),
            problems=incompatible,
        )

    baseline_problems = [item for found in _run_problems(spec, baseline).values() for item in found]
    if baseline_problems:
        return result(
            "invalid_baseline",
            "baseline run is not usable: " + "; ".join(baseline_problems),
            problems=baseline_problems,
        )

    candidate_problems = _run_problems(spec, candidate)
    if candidate_problems:
        reason_code, found = next(iter(candidate_problems.items()))
        return result(
            reason_code,
            f"{REASON_LABELS[reason_code]}: " + ", ".join(found),
            problems=[item for items in candidate_problems.values() for item in items],
            missing_workloads=sorted(candidate_problems.get("missing_workloads", [])),
        )

    diff = build_diff(target, baseline, candidate)
    deltas = {workload["id"]: workload for workload in diff["workloads"]}

    def signal(workload: dict) -> dict:
        return {
            "id": workload["id"],
            "name": workload["name"],
            "role": workload.get("role", "unknown"),
            "median_delta_pct": workload["ltseq_median_delta_pct"],
            "p95_delta_pct": workload["ltseq_p95_delta_pct"],
        }

    def describe(workload: dict) -> str:
        return (
            f"{workload['name']}(median={format_delta(workload['ltseq_median_delta_pct'])},"
            f"p95={format_delta(workload['ltseq_p95_delta_pct'])})"
        )

    # Past _run_problems every metric is a positive number, so every delta is too.
    wins = [
        deltas[workload_id]
        for workload_id in spec.target_workloads
        if deltas[workload_id]["ltseq_median_delta_pct"] <= improvement_bar
        and deltas[workload_id]["ltseq_p95_delta_pct"] <= improvement_bar
    ]
    regressions = [
        deltas[workload_id]
        for workload_id in spec.protected_workloads
        if deltas[workload_id]["ltseq_median_delta_pct"] >= regression_bar
        or deltas[workload_id]["ltseq_p95_delta_pct"] >= regression_bar
    ]
    target_win = ", ".join(describe(workload) for workload in wins) or "none"

    if regressions:
        protected_status = ", ".join(describe(workload) for workload in regressions)
        return result(
            "protected_regression",
            "protected workload regression: " + protected_status,
            target_win=target_win,
            protected_status=protected_status,
            target_wins_detail=[signal(workload) for workload in wins],
            protected_regressions_detail=[signal(workload) for workload in regressions],
        )

    if not wins:
        return result(
            "no_target_improvement",
            "no target workload met the improvement threshold in both median and p95",
            protected_status="clean",
        )

    return result(
        "target_improvement",
        "target improvement without protected regression: " + target_win,
        target_win=target_win,
        protected_status="clean",
        target_wins_detail=[signal(workload) for workload in wins],
    )


def positive_pct(text: str) -> float:
    try:
        value = float(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected a percentage, got {text!r}") from exc
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError(f"expected a positive percentage, got {text!r}")
    return value


def add_threshold_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--min-improvement-pct",
        type=positive_pct,
        help="override the target's improvement bar, e.g. 3 for 3%% (calibration runs)",
    )
    parser.add_argument(
        "--max-regression-pct",
        type=positive_pct,
        help="override the target's protected-regression bar, e.g. 5 for 5%% (calibration runs)",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate a benchmark candidate against a baseline",
        epilog="Exit status: 0 keep, 1 discard, 2 inputs cannot be evaluated.",
    )
    parser.add_argument("baseline_summary", type=Path)
    parser.add_argument("candidate_summary", type=Path)
    parser.add_argument("target", choices=sorted(TARGETS))
    add_threshold_options(parser)
    args = parser.parse_args(argv)

    try:
        baseline = load_summary(args.baseline_summary, "baseline")
        candidate = load_summary(args.candidate_summary, "candidate")
        evaluation = evaluate_candidate(
            args.target,
            baseline,
            candidate,
            min_improvement_pct=args.min_improvement_pct,
            max_regression_pct=args.max_regression_pct,
        )
    except BenchmarkInputError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_INPUT_ERROR
    print(json.dumps(evaluation, indent=2))
    return exit_status(evaluation)


if __name__ == "__main__":
    raise SystemExit(main())
