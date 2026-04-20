"""
report.py — Generate Markdown research reports.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from .analyzer import summarize_hotspots, diff_hotspots
from .hypothesis import render_hypotheses
from .tracker import render_trend_table


def generate_report(
    run_dir: Path,
    bench_vs_results: dict | None,
    bench_core_results: dict | None,
    hotspots: dict[str, list[dict]],
    hypotheses: list[dict],
    history: list[dict],
    data_flag: str = "--sample",
) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    git_sha = (history[0].get("git_sha", "?") if history else "?")
    dataset_label = "1M-row sample" if "sample" in data_flag else "full dataset"

    lines = [
        f"# LTSeq autoresearch report — {now}",
        "",
        f"**Git SHA**: `{git_sha}` | **Dataset**: {dataset_label}",
        "",
    ]

    # --- Benchmark comparison ---
    lines += ["## Benchmark comparison (bench_vs)", ""]
    if bench_vs_results and bench_vs_results.get("rounds"):
        lines += [
            "| Round | LTSeq (s) | DuckDB (s) | Ratio | LTSeq mem (MB) |",
            "|-------|-----------|------------|-------|----------------|",
        ]
        for r in bench_vs_results["rounds"]:
            name = r.get("round_name", "?")
            lt = r.get("ltseq", {})
            dk = r.get("duckdb", {})
            lt_s = lt.get("median_s", "-")
            dk_s = dk.get("median_s", "-")
            lt_mem = lt.get("mem_delta_mb", "-")
            if isinstance(lt_s, float) and isinstance(dk_s, float) and dk_s > 0:
                ratio = dk_s / lt_s
                ratio_str = f"{ratio:.1f}x LT" if ratio > 1 else f"{1/ratio:.1f}x DK"
            else:
                ratio_str = "N/A"
            lines.append(f"| {name} | {lt_s} | {dk_s} | {ratio_str} | {lt_mem} |")
        lines.append("")
    else:
        lines += ["_bench_vs not run — no comparison data._", ""]

    # --- Core operation benchmarks ---
    if bench_core_results:
        lines += ["## Core operation benchmarks (bench_core)", ""]
        lines += [
            "| Operation | Rows | Time (ms) | Throughput (rows/s) |",
            "|-----------|------|-----------|---------------------|",
        ]
        for r in bench_core_results:
            rps = r.get("rows_per_sec")
            rps_str = f"{rps:.0f}" if isinstance(rps, (int, float)) else "-"
            lines.append(
                f"| {r.get('name','?')} | {r.get('rows','-')} | "
                f"{r.get('time_ms','-')} | {rps_str} |"
            )
        lines.append("")

    # --- Flamegraph hotspots ---
    lines += ["## Flamegraph hotspot analysis", ""]
    if hotspots:
        prev_hotspots: dict[str, list[dict]] = {}
        if len(history) > 1:
            prev_hs = history[1].get("hotspots", {})
            prev_hotspots = prev_hs if isinstance(prev_hs, dict) else {}

        for round_key, hs in hotspots.items():
            round_label = round_key.upper().replace("_", " ")
            lines.append(f"### {round_label}")
            prev = prev_hotspots.get(round_key, [])
            annotated = diff_hotspots(hs, prev) if prev else hs
            lines.append(summarize_hotspots(annotated))
            lines.append("")
    else:
        lines += ["_No flamegraphs generated. Run with --profile to enable._", ""]

    # --- Optimization hypotheses ---
    lines += ["## Optimization hypotheses", ""]
    lines.append(render_hypotheses(hypotheses))

    # --- Historical trend ---
    lines += ["## Historical trend", ""]
    lines.append(render_trend_table(history))

    # --- Next steps ---
    lines += [
        "## Next steps",
        "",
        "1. Open the flamegraph SVG files in a browser (see `results/` directory)",
        "2. Pick the highest-priority hypothesis above and implement the change",
        "3. Run `pytest py-ltseq/tests/ -q` to verify correctness",
        "4. Re-run autoresearch to confirm the performance improvement",
        "",
        "```bash",
        "# Quick re-run after a change",
        "python benchmarks/autoresearch/runner.py --skip-profile",
        "```",
    ]

    return "\n".join(lines) + "\n"
