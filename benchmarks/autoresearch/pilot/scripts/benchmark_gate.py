#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path

if __package__ in (None, ""):
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from pilot.common import (
        build_diff,
        clean_output_dir,
        ensure_report_dirs,
        load_json,
        render_diff,
        resolve_report_dir,
        resolve_target,
        write_json,
    )
    from pilot.scripts.evaluate_benchmark_candidate import evaluate_candidate
else:
    from ..common import (
        build_diff,
        clean_output_dir,
        ensure_report_dirs,
        load_json,
        render_diff,
        resolve_report_dir,
        resolve_target,
        write_json,
    )
    from .evaluate_benchmark_candidate import evaluate_candidate


def render_evaluation(evaluation: dict) -> str:
    return (
        f"recommendation={evaluation['recommendation']}\n"
        f"reason={evaluation['reason']}\n"
        f"target_win={evaluation['target_win']}\n"
        f"protected_status={evaluation['protected_status']}\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare baseline and candidate benchmark artifacts")
    parser.add_argument("target", nargs="?", default="clickbench_funnel")
    args = parser.parse_args()

    spec = resolve_target(args.target)
    ensure_report_dirs()

    baseline_dir = resolve_report_dir("baseline", spec.key)
    candidate_dir = resolve_report_dir("candidates", spec.key)
    diff_dir = resolve_report_dir("diff", spec.key)
    clean_output_dir(diff_dir)

    baseline = load_json(baseline_dir / "benchmark-summary.json")
    candidate = load_json(candidate_dir / "benchmark-summary.json")

    diff = build_diff(spec.key, baseline, candidate)
    evaluation = evaluate_candidate(spec.key, baseline, candidate)

    write_json(diff_dir / "benchmark-diff.json", diff)
    (diff_dir / "benchmark-diff.txt").write_text(render_diff(diff), encoding="utf-8")
    write_json(diff_dir / "evaluation.json", evaluation)
    (diff_dir / "evaluation.txt").write_text(render_evaluation(evaluation), encoding="utf-8")

    print(json.dumps(evaluation, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
