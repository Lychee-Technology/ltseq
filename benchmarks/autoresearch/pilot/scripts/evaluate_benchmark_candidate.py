#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path

if __package__ in (None, ""):
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from pilot.common import build_diff, format_delta, load_json, resolve_target
else:
    from ..common import build_diff, format_delta, load_json, resolve_target


def evaluate_candidate(target: str, baseline: dict, candidate: dict) -> dict:
    spec = resolve_target(target)
    diff = build_diff(target, baseline, candidate)
    workloads = {workload["id"]: workload for workload in diff.get("workloads", [])}

    def workload_signal(workload: dict) -> dict:
        return {
            "id": workload["id"],
            "name": workload["name"],
            "role": workload.get("role", "unknown"),
            "median_delta_pct": workload.get("ltseq_median_delta_pct"),
            "p95_delta_pct": workload.get("ltseq_p95_delta_pct"),
        }

    thresholds = {
        "target_improvement_threshold_pct": spec.target_improvement_threshold_pct,
        "protected_regression_threshold_pct": spec.protected_regression_threshold_pct,
    }

    if int(candidate.get("correctness_failures", 0)) > 0:
        return {
            "target": target,
            "recommendation": "discard",
            "reason": f"correctness regressions detected: {candidate['correctness_failures']}",
            "target_win": "none",
            "protected_status": "n/a",
            "target_wins_detail": [],
            "protected_regressions_detail": [],
            "missing_workloads": [],
            "thresholds": thresholds,
        }

    if int(candidate.get("infra_failures", 0)) > 0:
        return {
            "target": target,
            "recommendation": "discard",
            "reason": f"infrastructure failures detected: {candidate['infra_failures']}",
            "target_win": "none",
            "protected_status": "n/a",
            "target_wins_detail": [],
            "protected_regressions_detail": [],
            "missing_workloads": [],
            "thresholds": thresholds,
        }

    target_wins = []
    target_wins_detail = []
    protected_regressions = []
    protected_regressions_detail = []
    missing_workloads = []

    for workload_id in spec.target_workloads:
        workload = workloads.get(workload_id)
        if not workload or not workload.get("candidate"):
            missing_workloads.append(workload_id)
            continue
        median_delta = workload.get("ltseq_median_delta_pct")
        p95_delta = workload.get("ltseq_p95_delta_pct")
        if (
            median_delta is not None
            and p95_delta is not None
            and median_delta <= spec.target_improvement_threshold_pct
            and p95_delta <= spec.target_improvement_threshold_pct
        ):
            target_wins_detail.append(workload_signal(workload))
            target_wins.append(
                f"{workload['name']}(median={format_delta(median_delta)},p95={format_delta(p95_delta)})"
            )

    for workload_id in spec.protected_workloads:
        workload = workloads.get(workload_id)
        if not workload or not workload.get("candidate"):
            missing_workloads.append(workload_id)
            continue
        median_delta = workload.get("ltseq_median_delta_pct")
        p95_delta = workload.get("ltseq_p95_delta_pct")
        if (
            median_delta is not None and median_delta >= spec.protected_regression_threshold_pct
        ) or (p95_delta is not None and p95_delta >= spec.protected_regression_threshold_pct):
            protected_regressions_detail.append(workload_signal(workload))
            protected_regressions.append(
                f"{workload['name']}(median={format_delta(median_delta)},p95={format_delta(p95_delta)})"
            )

    if missing_workloads:
        return {
            "target": target,
            "recommendation": "discard",
            "reason": "missing workloads: " + ", ".join(sorted(set(missing_workloads))),
            "target_win": "none",
            "protected_status": "n/a",
            "target_wins_detail": [],
            "protected_regressions_detail": [],
            "missing_workloads": sorted(set(missing_workloads)),
            "thresholds": thresholds,
        }

    if protected_regressions:
        return {
            "target": target,
            "recommendation": "discard",
            "reason": "protected workload regression: " + ", ".join(protected_regressions),
            "target_win": ", ".join(target_wins) if target_wins else "none",
            "protected_status": ", ".join(protected_regressions),
            "target_wins_detail": target_wins_detail,
            "protected_regressions_detail": protected_regressions_detail,
            "missing_workloads": [],
            "thresholds": thresholds,
        }

    if not target_wins:
        return {
            "target": target,
            "recommendation": "discard",
            "reason": "no target workload met the improvement threshold",
            "target_win": "none",
            "protected_status": "clean",
            "target_wins_detail": [],
            "protected_regressions_detail": [],
            "missing_workloads": [],
            "thresholds": thresholds,
        }

    return {
        "target": target,
        "recommendation": "keep",
        "reason": "target improvement without protected regression: " + ", ".join(target_wins),
        "target_win": ", ".join(target_wins),
        "protected_status": "clean",
        "target_wins_detail": target_wins_detail,
        "protected_regressions_detail": [],
        "missing_workloads": [],
        "thresholds": thresholds,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate a benchmark candidate against a baseline")
    parser.add_argument("baseline_summary", type=Path)
    parser.add_argument("candidate_summary", type=Path)
    parser.add_argument("target")
    args = parser.parse_args()

    baseline = load_json(args.baseline_summary)
    candidate = load_json(args.candidate_summary)
    evaluation = evaluate_candidate(args.target, baseline, candidate)
    print(json.dumps(evaluation, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
