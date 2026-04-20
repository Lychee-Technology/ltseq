"""
analyzer.py — Parse flamegraph SVG files to extract hotspot functions.

Flamegraph SVGs contain <title> elements with function names and
rect widths proportional to CPU time.
"""

from __future__ import annotations

import re
from pathlib import Path


def extract_hotspots(svg_path: Path, top_n: int = 20) -> list[dict]:
    """Extract top-N hotspot functions from a flamegraph SVG.

    Each flamegraph <g> element has:
      - <title>funcname (N samples, M%)</title>  (inferno format)
      - OR <title>funcname</title> with a width attribute

    Returns list of dicts: {function, self_pct, samples}
    """
    if not svg_path.exists():
        return []

    content = svg_path.read_text(encoding="utf-8", errors="replace")

    # Try inferno format: <title>name (N samples, M%)</title>
    inferno_pattern = re.compile(
        r"<title>(.+?)\s+\((\d+)\s+samples,\s+([\d.]+)%\)</title>"
    )
    hotspots = []
    for m in inferno_pattern.finditer(content):
        func_name = m.group(1).strip()
        samples = int(m.group(2))
        pct = float(m.group(3))
        hotspots.append({"function": func_name, "self_pct": pct, "samples": samples})

    if not hotspots:
        # Try Brendan Gregg format: <title>name (N)</title> with rect width
        gregg_pattern = re.compile(r"<title>(.+?)\s+\((\d+)\)</title>")
        total_samples = 0
        entries = []
        for m in gregg_pattern.finditer(content):
            func_name = m.group(1).strip()
            samples = int(m.group(2))
            entries.append((func_name, samples))
            if not total_samples:
                # First entry is usually the root with total count
                total_samples = samples
        if total_samples > 0:
            for func_name, samples in entries[1:]:  # skip root
                pct = round(samples / total_samples * 100, 2)
                hotspots.append(
                    {"function": func_name, "self_pct": pct, "samples": samples}
                )

    # Deduplicate and sort by self_pct descending
    seen = {}
    for h in hotspots:
        fn = h["function"]
        if fn not in seen or h["self_pct"] > seen[fn]["self_pct"]:
            seen[fn] = h
    sorted_hotspots = sorted(seen.values(), key=lambda x: x["self_pct"], reverse=True)

    return sorted_hotspots[:top_n]


def diff_hotspots(
    current: list[dict], previous: list[dict]
) -> list[dict]:
    """Compare two hotspot lists, annotate with delta."""
    prev_map = {h["function"]: h["self_pct"] for h in previous}
    result = []
    for h in current:
        fn = h["function"]
        prev_pct = prev_map.get(fn)
        delta = None
        if prev_pct is not None:
            delta = round(h["self_pct"] - prev_pct, 2)
        result.append({**h, "prev_pct": prev_pct, "delta": delta})
    return result


def summarize_hotspots(hotspots: list[dict], top_n: int = 10) -> str:
    """Format hotspots as a Markdown table."""
    if not hotspots:
        return "_No profiling data available._\n"

    lines = [
        "| Rank | Function | Self% | Delta |",
        "|------|----------|-------|-------|",
    ]
    for i, h in enumerate(hotspots[:top_n], 1):
        fn = h["function"]
        # Truncate long function names
        if len(fn) > 60:
            fn = fn[:57] + "..."
        pct = h.get("self_pct", 0)
        delta = h.get("delta")
        delta_str = f"{delta:+.1f}%" if delta is not None else "new"
        lines.append(f"| {i} | `{fn}` | {pct:.1f}% | {delta_str} |")

    return "\n".join(lines) + "\n"
