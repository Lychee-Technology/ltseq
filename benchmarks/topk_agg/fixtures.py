"""Synthetic grouped-count fixtures for the Top-K aggregate research (#386).

Each fixture is one Parquet file of ``ROWS`` rows in random order, with
one grouping key stored twice: ``key`` (int64) and ``skey`` (a URL-like
string of about 40 bytes that is unique per ``key``), and a ``flag`` column
(int64, 0..9, uniform) for the filtered case. The rows are drawn with a
fixed seed, so a fixture is the same file on every machine that runs this
version of numpy.

Distributions, over ``cardinality`` distinct keys:

- ``uniform``: every row's key uniform over the keys. The K-th largest
  count is close to the mean, so a count bound can prune little.
- ``zipf``: key rank r drawn with probability proportional to 1/r^1.1,
  ranks shuffled onto key ids. Few heavy keys, a long tail.
- ``hot``: half the rows hold key 0, the rest are uniform.
- ``flat``: every key's count is ``ROWS // cardinality`` plus 0, 1 or 2:
  nearly tied, the adversarial case for a count bound.

``python benchmarks/topk_agg/fixtures.py`` writes every fixture into
``benchmarks/data/topk_agg/`` (ignored by git) and skips a file that is
already there.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
DATA = HERE.parent / "data" / "topk_agg"

ROWS = 20_000_000
ROW_GROUP = 1 << 20
SEED = 386
DISTRIBUTIONS = ("uniform", "zipf", "hot", "flat")
CARDINALITIES = (1_000, 100_000, 1_000_000)


def portable(text: str) -> str:
    """``text`` without this machine's paths: files under the repository
    become relative to it, and any ``.../benchmarks/data/`` directory (the
    ClickBench file may live in another checkout) reads
    ``benchmarks/data/``. Results and plans are committed through it."""
    root = str(REPO).lstrip("/")
    text = re.sub(r"/?" + re.escape(root) + "/", "", text)
    return re.sub(r"[^\s\[\]'\",=]*/benchmarks/data/", "benchmarks/data/", text)


def slim(text: str) -> str:
    """A results file as committed: ``portable``, each scan's one-line
    description cut short (its file ranges repeat in every cell), and plan
    texts kept only for the cells ``representative`` picks."""
    data = json.loads(portable(text))
    for cell in data.get("cells", []):
        result = cell.get("timing", cell)
        keep = "spec" in result and representative(result["spec"])
        analyses = list(result.get("analyze", []))
        analyses += [a for mode in result.get("modes", {}).values() for a in mode.get("analyze", [])]
        for analysis in analyses:
            if not keep:
                analysis["plan"] = None
            for op in analysis.get("operators", []):
                if op["name"] == "DataSourceExec" and len(op["line"]) > 160:
                    op["line"] = op["line"][:160] + "…"
    # One cell per line: readable diffs at a third of the indented size.
    cells = ",\n".join(json.dumps(cell, separators=(",", ":")) for cell in data.pop("cells", []))
    head = json.dumps(data, indent=1)[:-2]
    return f'{head},\n "cells": [\n{cells}\n]}}\n'


def representative(spec: dict) -> bool:
    """The cells whose plans ``results/plans/`` keeps: K=10 over the 1M-key
    fixtures and ClickBench, at one and eight partitions."""
    return spec["k"] == 10 and spec["partitions"] in (1, 8) and (
        "_1000000" in spec["source"] or "hits" in spec["source"]
    )


def name(distribution: str, cardinality: int) -> str:
    return f"{distribution}_{cardinality}"


def path(distribution: str, cardinality: int) -> Path:
    return DATA / f"{name(distribution, cardinality)}.parquet"


def draw_keys(distribution: str, cardinality: int, rows: int, seed: int) -> np.ndarray:
    """The key column of a fixture, in row order."""
    rng = np.random.default_rng([seed, DISTRIBUTIONS.index(distribution), cardinality])
    if distribution == "uniform":
        return rng.integers(0, cardinality, rows, dtype=np.int64)
    if distribution == "zipf":
        weights = 1.0 / np.arange(1, cardinality + 1, dtype=np.float64) ** 1.1
        ranks = rng.choice(cardinality, size=rows, p=weights / weights.sum())
        return rng.permutation(cardinality).astype(np.int64)[ranks]
    if distribution == "hot":
        keys = rng.integers(0, cardinality, rows, dtype=np.int64)
        keys[rng.random(rows) < 0.5] = 0
        return keys
    if distribution == "flat":
        base = rows // cardinality
        counts = np.full(cardinality, base - 1, dtype=np.int64) + rng.integers(0, 3, cardinality)
        keys = np.repeat(np.arange(cardinality, dtype=np.int64), counts)
        return rng.permutation(keys)
    raise ValueError(distribution)


def string_keys(keys: np.ndarray) -> pa.Array:
    """``key`` as a URL-like string: unique per key, about 40 bytes."""
    distinct, inverse = np.unique(keys, return_inverse=True)
    mixed = (distinct.astype(np.uint64) * np.uint64(0x9E3779B97F4A7C15)) >> np.uint64(16)
    text = pa.array(
        [f"http://site{m % 997}.example/page/{k:08d}?r={m:012x}" for k, m in zip(distinct, mixed)]
    )
    return text.take(pa.array(inverse))


def build(distribution: str, cardinality: int, rows: int = ROWS, seed: int = SEED) -> pa.Table:
    keys = draw_keys(distribution, cardinality, rows, seed)
    flags = np.random.default_rng([seed, 99, cardinality]).integers(0, 10, len(keys), dtype=np.int64)
    return pa.table({"key": keys, "skey": string_keys(keys), "flag": flags})


def write(distribution: str, cardinality: int, rows: int = ROWS, force: bool = False) -> Path:
    target = path(distribution, cardinality)
    if target.exists() and not force:
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    table = build(distribution, cardinality, rows)
    tmp = target.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp, row_group_size=ROW_GROUP, compression="snappy")
    tmp.rename(target)
    return target


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--only", help="comma-separated fixture names, e.g. zipf_1000000")
    parser.add_argument("--force", action="store_true", help="rewrite existing files")
    args = parser.parse_args()
    wanted = set(args.only.split(",")) if args.only else None
    for distribution in DISTRIBUTIONS:
        for cardinality in CARDINALITIES:
            if wanted and name(distribution, cardinality) not in wanted:
                continue
            target = write(distribution, cardinality, force=args.force)
            print(f"{target.relative_to(HERE.parent.parent)}: {target.stat().st_size >> 20} MiB")


if __name__ == "__main__":
    main()
