#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

if __package__ in (None, ""):
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from pilot.common import (
        build_benchmark_summary,
        clean_output_dir,
        ensure_report_dirs,
        load_json,
        render_benchmark_result,
        resolve_report_dir,
        resolve_target,
        run_build,
        run_runner,
        write_json,
    )
else:
    from ..common import (
        build_benchmark_summary,
        clean_output_dir,
        ensure_report_dirs,
        load_json,
        render_benchmark_result,
        resolve_report_dir,
        resolve_target,
        run_build,
        run_runner,
        write_json,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Capture baseline benchmark artifacts")
    parser.add_argument("target", nargs="?", default="clickbench_funnel")
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--data", type=str)
    parser.add_argument("--iterations", type=int)
    parser.add_argument("--warmup", type=int)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    spec = resolve_target(args.target)
    ensure_report_dirs()
    output_dir = resolve_report_dir("baseline", spec.key)
    clean_output_dir(output_dir)

    run_build(skip_build=args.skip_build, dry_run=args.dry_run)
    run_runner(
        output_dir,
        spec.rounds,
        data_mode="sample" if args.sample else "full",
        data_path=args.data,
        iterations=args.iterations or spec.iterations,
        warmup=args.warmup or spec.warmup,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        return 0

    runner_summary = load_json(output_dir / "summary.json")
    benchmark_summary = build_benchmark_summary(spec.key, runner_summary)
    write_json(output_dir / "benchmark-result.json", runner_summary)
    write_json(output_dir / "benchmark-summary.json", benchmark_summary)
    (output_dir / "benchmark-result.md").write_text(
        render_benchmark_result(benchmark_summary), encoding="utf-8"
    )

    print(f"baseline artifacts: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
