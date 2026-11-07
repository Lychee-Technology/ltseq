"""Randomized checks that the simulated pruner never drops a possible winner.

``python benchmarks/topk_agg/selfcheck.py`` draws small inputs with many
ties (counts from a handful of values, zero-count-free, sizes around K) and
checks, against a brute-force top-K, the invariants REPORT.md's
correctness note rests on:

1. every group whose count is above the K-th count is kept, and at least K
   groups with a count at or above it are kept, globally and in every final
   partition with more than K groups, so the Top-K sort above the
   partitions returns an exact top-K;
2. a skipped group's count is strictly below the K-th count, so a group
   tied with the K-th count is never skipped;
3. the partition-local threshold prunes no group the global K-th count
   would keep (``dynamic`` is a subset of ``oracle``);
4. K = 0 and inputs with at most K groups prune nothing.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))

import ceiling  # noqa: E402


def check_once(rng: np.random.Generator) -> bool:
    """Check one random input; return whether the pruner skipped anything."""
    groups = int(rng.integers(0, 120))
    k = int(rng.integers(0, 15))
    partitions = int(rng.integers(1, 5))
    slots = int(rng.choice([1, 2, 3, 8, 64]))
    values = rng.integers(1, 40, size=int(rng.integers(1, 6)))
    counts = rng.choice(values, size=groups).astype(np.int64)
    if groups and rng.random() < 0.3:
        counts[rng.integers(0, groups)] = 10_000  # a hot key
    hashes = rng.integers(0, 2**63, size=groups, dtype=np.int64).astype(np.uint64)

    dynamic, oracle, kth = ceiling.pruned(hashes, counts, k, partitions, slots)
    if k == 0 or groups <= k:
        assert kth is None and not dynamic.any() and not oracle.any()
        return False
    truth = np.sort(counts)[::-1][k - 1]
    assert kth == truth
    # 3: the local thresholds prune a subset of what the global one does.
    assert not (dynamic & ~oracle).any()
    for member in (dynamic, oracle):
        kept = ~member
        # 1: every count above the K-th is kept, and K at or above it are.
        assert kept[counts > kth].all()
        assert (kept & (counts >= kth)).sum() >= k
        # 2: a skipped group cannot reach the K-th count.
        assert (counts[member] < kth).all()
    part = (ceiling._mix(hashes, 1) % np.uint64(partitions)).astype(np.int64)
    for p in range(partitions):
        mine = part == p
        if mine.sum() <= k:
            assert not dynamic[mine].any()
            continue
        local = np.sort(counts[mine])[::-1][k - 1]
        kept = mine & ~dynamic
        assert kept[mine & (counts > local)].all()
        assert (kept & (counts >= local)).sum() >= k
    return bool(dynamic.any())


def main() -> None:
    rng = np.random.default_rng(386)
    trials = int(sys.argv[1]) if len(sys.argv) > 1 else 20_000
    pruning = sum(check_once(rng) for _ in range(trials))
    # The checks are vacuous on inputs where nothing is skipped.
    assert pruning > trials // 10, pruning
    print(f"selfcheck: {trials} random inputs ({pruning} with skipped groups), all invariants hold")


if __name__ == "__main__":
    main()
