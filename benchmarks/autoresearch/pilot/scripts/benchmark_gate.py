#!/usr/bin/env python3
"""Compare a target's baseline and candidate artifacts and write the decision.

Reads reports/{baseline,candidates}/<target>/benchmark-summary.json and writes
benchmark-diff.{json,txt} and evaluation.{json,txt} under reports/diff/<target>/.

Exit status: 0 keep, 1 discard, 2 when the inputs cannot be evaluated (a
summary is missing or malformed, or an option is invalid). On status 2 the
target's diff directory is left empty, so no earlier evaluation can be mistaken
for this one. The exception is a command line argparse cannot read far enough
to name a target (an unknown target, or an option with no value): it exits
before a target is chosen and changes nothing.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from pilot.common import (
        TARGETS,
        BenchmarkInputError,
        build_diff,
        clean_output_dir,
        ensure_report_dirs,
        load_summary,
        render_diff,
        resolve_report_dir,
        resolve_target,
        write_json,
    )
    from pilot.scripts.evaluate_benchmark_candidate import (
        EXIT_INPUT_ERROR,
        add_threshold_options,
        evaluate_candidate,
        exit_status,
        threshold_overrides,
    )
else:
    from ..common import (
        TARGETS,
        BenchmarkInputError,
        build_diff,
        clean_output_dir,
        ensure_report_dirs,
        load_summary,
        render_diff,
        resolve_report_dir,
        resolve_target,
        write_json,
    )
    from .evaluate_benchmark_candidate import (
        EXIT_INPUT_ERROR,
        add_threshold_options,
        evaluate_candidate,
        exit_status,
        threshold_overrides,
    )


def render_evaluation(evaluation: dict) -> str:
    lines = [
        f"recommendation={evaluation['recommendation']}",
        f"reason_code={evaluation['reason_code']}",
        f"reason={evaluation['reason']}",
        f"target_win={evaluation['target_win']}",
        f"protected_status={evaluation['protected_status']}",
    ]
    lines.extend(f"warning={warning}" for warning in evaluation.get("warnings", []))
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compare baseline and candidate benchmark artifacts",
        epilog="Exit status: 0 keep, 1 discard, 2 inputs cannot be evaluated.",
    )
    parser.add_argument("target", nargs="?", default="clickbench_funnel", choices=sorted(TARGETS))
    add_threshold_options(parser)
    # parse_known_args and text-valued thresholds name the target without
    # rejecting the rest of the line, so the target's previous decision is
    # cleared before any usage error below exits with status 2.
    args, unrecognized = parser.parse_known_args(argv)

    spec = resolve_target(args.target)
    ensure_report_dirs()

    baseline_dir = resolve_report_dir("baseline", spec.key)
    candidate_dir = resolve_report_dir("candidates", spec.key)
    diff_dir = resolve_report_dir("diff", spec.key)
    clean_output_dir(diff_dir)

    if unrecognized:
        parser.error(f"unrecognized arguments: {' '.join(unrecognized)}")
    overrides = threshold_overrides(parser, args)

    try:
        baseline = load_summary(baseline_dir / "benchmark-summary.json", "baseline")
        candidate = load_summary(candidate_dir / "benchmark-summary.json", "candidate")
        evaluation = evaluate_candidate(spec.key, baseline, candidate, **overrides)
    except BenchmarkInputError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_INPUT_ERROR

    diff = build_diff(spec.key, baseline, candidate)
    write_json(diff_dir / "benchmark-diff.json", diff)
    (diff_dir / "benchmark-diff.txt").write_text(render_diff(diff), encoding="utf-8")
    write_json(diff_dir / "evaluation.json", evaluation)
    (diff_dir / "evaluation.txt").write_text(render_evaluation(evaluation), encoding="utf-8")

    print(json.dumps(evaluation, indent=2))
    return exit_status(evaluation)


if __name__ == "__main__":
    raise SystemExit(main())
