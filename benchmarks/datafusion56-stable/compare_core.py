"""Compare ``bench_core.py`` runs of two engines as Markdown (#296, PR #423).

    python compare_core.py --a 55.0.0 a1.json a2.json --b 56.0.0 b1.json b2.json

Each engine's time is the better of its runs; *spread* is the larger of the
two engines' ratios between their own slowest and fastest runs, a floor on
the noise. Standard library only.
"""

from __future__ import annotations

import argparse
import json
import math


def load(paths: list[str]) -> dict[str, list[float]]:
    times: dict[str, list[float]] = {}
    for path in paths:
        with open(path) as f:
            for r in json.load(f)["results"]:
                times.setdefault(r["name"], []).append(r["time_ms"])
    return times


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--a", nargs="+", required=True, metavar="LABEL_THEN_FILES")
    p.add_argument("--b", nargs="+", required=True, metavar="LABEL_THEN_FILES")
    args = p.parse_args()
    (la, *fa), (lb, *fb) = args.a, args.b
    a, b = load(fa), load(fb)
    lines = [
        f"| Benchmark | {la} (ms) | {lb} (ms) | {lb} / {la} | Spread |",
        "|---|---|---|---|---|",
    ]
    ratios = []
    for name, ta in a.items():
        tb = b.get(name)
        if not tb:
            continue
        ratio = min(tb) / min(ta)
        spread = max(max(ta) / min(ta), max(tb) / min(tb))
        ratios.append(ratio)
        lines.append(
            f"| `{name}` | {min(ta):.2f} | {min(tb):.2f} | {ratio:.2f} | {spread:.2f} |"
        )
    gmean = math.exp(sum(math.log(r) for r in ratios) / len(ratios))
    lines.append("")
    lines.append(
        f"Over the {len(ratios)} benchmarks the geometric mean of {lb} / {la} is "
        f"{gmean:.3f}; the ratios lie between {min(ratios):.2f} and {max(ratios):.2f}."
    )
    print("\n".join(lines))


if __name__ == "__main__":
    main()
