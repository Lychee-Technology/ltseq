"""How much final-aggregation input a slot-bound pruner could skip (#386).

The Pivot-style pruner works after the hash repartition that gives every
group key to exactly one final-aggregation partition. It hashes each
incoming partial row to one of ``slots`` disjoint slots and keeps, per slot,
the sum of the partial counts routed to it. COUNT contributions are
non-negative and a key lives in one slot, so for every group ``g``

    count(g) <= bound(slot(g)) = sum of the partial counts in slot(g)

It then finalizes slots in descending bound order, keeping the K largest
finalized counts; once it holds K groups and the next slot's bound is
*strictly below* the K-th count, no group in that slot or any later one can
enter the result, and their partial rows are never merged.

The bound is kept per final partition: a slot is a (partition, slot) pair.
This module simulates that from the exact per-group counts of a query
(computed by DuckDB, so the simulation does not trust the engine it
judges):

- ``dynamic``: the procedure above, each partition with its own K-th count
  as its threshold. The spike (``src/topk_research/prune.rs``) also shares
  the largest K-th count any partition has finalized, so it can prune more
  than ``dynamic`` says.
- ``oracle``: every slot whose bound is below the global K-th count pruned,
  the most any threshold schedule can prune.

Both report the pruned share of groups and of input rows. The partial rows
the final aggregate merges per group lie between one (partial aggregation
collapsed the group in one input partition) and the group's count
(partial aggregation skipped), so the share of final-merge work that could
be skipped lies between the two shares; ``report.py`` uses the larger one
for the ceiling. Neither counts the pruner's own cost: hashing every row
into a slot, buffering a partition's input until its slot bounds are known,
and the heap.
"""

from __future__ import annotations

import heapq

import duckdb
import numpy as np


def group_counts(source: str, key: str, measure: str = "count(*)", where: str = "") -> tuple[np.ndarray, np.ndarray]:
    """(hash, count) of every group of the query, from DuckDB."""
    con = duckdb.connect()
    con.execute("SET enable_progress_bar = false")
    rows = con.sql(
        f'SELECT hash("{key}") AS h, {measure} AS c FROM read_parquet(\'{source}\') {where} GROUP BY "{key}"'
    ).fetchnumpy()
    return rows["h"].astype(np.uint64), rows["c"].astype(np.int64)


def _mix(hashes: np.ndarray, salt: int) -> np.ndarray:
    mixed = (hashes ^ np.uint64(salt)) * np.uint64(0xD6E8FEB86659FD93)
    return mixed ^ (mixed >> np.uint64(32))


def _dynamic(counts: np.ndarray, slot: np.ndarray, slots: int, k: int) -> np.ndarray:
    """Which slots of one final partition the pruner skips (a bool per slot)."""
    bound = np.bincount(slot, weights=counts, minlength=slots).astype(np.int64)
    order = np.argsort(-bound, kind="stable")
    by_slot = np.argsort(slot, kind="stable")
    starts = np.searchsorted(slot[by_slot], np.arange(slots + 1))
    heap: list[int] = []
    finalized = 0
    for position, s in enumerate(order):
        if len(heap) == k and bound[s] < heap[0]:
            break
        for c in counts[by_slot[starts[s] : starts[s + 1]]]:
            if len(heap) < k:
                heapq.heappush(heap, int(c))
            elif c > heap[0]:
                heapq.heapreplace(heap, int(c))
        finalized = position + 1
    pruned = np.zeros(slots, dtype=bool)
    pruned[order[finalized:]] = True
    return pruned


def pruned(hashes: np.ndarray, counts: np.ndarray, k: int, partitions: int, slots: int):
    """Which groups the pruner skips: ``(dynamic, oracle, kth)``, two bool
    arrays over the groups and the global K-th count (``None`` when there
    are at most K groups, and then nothing is pruned).

    Groups go to final partitions by one hash and to slots within their
    partition by another. ``dynamic`` runs the pruner in each partition on
    its own, with its own K-th count (the partition's exact local top-K is
    all the Top-K sort above it needs); ``oracle`` prunes every slot whose
    bound is below the global K-th count.
    """
    groups = len(counts)
    dynamic = np.zeros(groups, dtype=bool)
    oracle = np.zeros(groups, dtype=bool)
    if k <= 0 or groups <= k:
        return dynamic, oracle, None
    part = (_mix(hashes, 1) % np.uint64(partitions)).astype(np.int64)
    slot = (_mix(hashes, 2) % np.uint64(slots)).astype(np.int64)
    kth = int(np.partition(counts, groups - k)[groups - k])

    cell = part * slots + slot
    bound = np.bincount(cell, weights=counts, minlength=partitions * slots).astype(np.int64)
    oracle = (bound < kth)[cell]

    for p in range(partitions):
        mine = np.flatnonzero(part == p)
        if len(mine) <= k:
            continue  # the partition holds no more groups than K: all are kept
        skipped = _dynamic(counts[mine], slot[mine], slots, k)
        dynamic[mine] = skipped[slot[mine]]
    return dynamic, oracle, kth


def simulate(hashes: np.ndarray, counts: np.ndarray, k: int, partitions: int, slots: int) -> dict:
    """Pruned shares of groups and rows for one K, final-partition count and
    slot count per partition (see ``pruned``)."""
    groups, rows = len(counts), int(counts.sum())
    dynamic, oracle, kth = pruned(hashes, counts, k, partitions, slots)

    def shares(member: np.ndarray) -> dict:
        return {
            "groups": float(member.sum()) / groups if groups else 0.0,
            "rows": float(counts[member].sum()) / rows if rows else 0.0,
        }

    return {
        "groups": groups,
        "rows": rows,
        "partitions": partitions,
        "slots": slots,
        "k": k,
        "kth_count": kth,
        "oracle": shares(oracle),
        "dynamic": shares(dynamic),
    }
