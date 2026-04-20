"""
tracker.py — Results history storage and trend analysis.
"""

from __future__ import annotations

import json
import subprocess
from datetime import datetime
from pathlib import Path

RESULTS_DIR = Path(__file__).parent / "results"


def get_git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], text=True
        ).strip()
    except Exception:
        return "unknown"


def save_run(
    run_dir: Path,
    bench_vs_results: dict | None,
    bench_core_results: dict | None,
    hotspots: dict[str, list[dict]],
) -> None:
    summary = {
        "timestamp": datetime.now().isoformat(),
        "git_sha": get_git_sha(),
        "bench_vs": bench_vs_results,
        "bench_core": bench_core_results,
        "hotspots": hotspots,
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2))


def load_history(n: int = 10) -> list[dict]:
    """Load the most recent n research runs."""
    summaries = sorted(RESULTS_DIR.glob("*/summary.json"), reverse=True)
    history = []
    for p in summaries[:n]:
        try:
            history.append(json.loads(p.read_text()))
        except Exception:
            pass
    return history


def render_trend_table(history: list[dict]) -> str:
    """Generate a Markdown trend table from historical runs."""
    if not history:
        return "_No historical data yet. Run autoresearch at least once to start tracking._\n"

    lines = [
        "| Date | Git SHA | R1 LTSeq (s) | R2 LTSeq (s) | R3 LTSeq (s) |",
        "|------|---------|-------------|-------------|-------------|",
    ]

    for run in history:
        ts = run.get("timestamp", "")[:10]
        sha = run.get("git_sha", "?")
        rounds = {}
        if run.get("bench_vs") and run["bench_vs"].get("rounds"):
            for r in run["bench_vs"]["rounds"]:
                name = r.get("round_name", "")
                if "R1" in name:
                    rounds["r1"] = r.get("ltseq", {}).get("median_s", "-")
                elif "R2" in name:
                    rounds["r2"] = r.get("ltseq", {}).get("median_s", "-")
                elif "R3" in name:
                    rounds["r3"] = r.get("ltseq", {}).get("median_s", "-")

        r1 = f"{rounds.get('r1', '-')}"
        r2 = f"{rounds.get('r2', '-')}"
        r3 = f"{rounds.get('r3', '-')}"
        lines.append(f"| {ts} | `{sha}` | {r1} | {r2} | {r3} |")

    return "\n".join(lines) + "\n"


def compute_delta(history: list[dict], key: str) -> str:
    """Compute delta vs previous run for a metric (e.g. 'r1_ltseq_s')."""
    if len(history) < 2:
        return "N/A"
    current = _extract_metric(history[0], key)
    previous = _extract_metric(history[1], key)
    if current is None or previous is None:
        return "N/A"
    delta = current - previous
    sign = "+" if delta > 0 else ""
    return f"{sign}{delta:.3f}s"


def _extract_metric(run: dict, key: str) -> float | None:
    """Extract a metric like 'r1_ltseq_s' from a run summary."""
    mapping = {"r1": "R1", "r2": "R2", "r3": "R3"}
    parts = key.split("_")
    if len(parts) < 2:
        return None
    round_key = parts[0]
    round_name_prefix = mapping.get(round_key)
    if not round_name_prefix:
        return None
    rounds = (run.get("bench_vs") or {}).get("rounds", [])
    for r in rounds:
        if round_name_prefix in r.get("round_name", ""):
            return r.get("ltseq", {}).get("median_s")
    return None
