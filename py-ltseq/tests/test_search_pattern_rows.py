"""The rows `search_pattern` returns, over many batches and tied sort keys (#149).

`search_pattern` used to execute the table's plan twice: once over the
columns its steps read, to find the matches, and once over every column, to
take the rows at those positions. Rows with equal sort keys can come back in
another order on the second run, so a position could name a row the steps
never matched. It now executes the plan once and takes the matching rows,
every column, from that execution. These tests check the rows themselves:
each column, including those no step reads, and their order, at every
engine layout and input layout.
"""

from __future__ import annotations

import random

import pyarrow as pa

from ltseq import LTSeq

N = 300


def _rows(*, distinct_keys: int) -> pa.Table:
    """``N`` rows in shuffled order: a sort key ``k`` with ``distinct_keys``
    values, a partition key ``p`` that follows ``k``, a step column ``flag``
    and two columns no step reads, a unique ``id`` and a ``note``."""
    rng = random.Random(149)
    k = [i * distinct_keys // N for i in range(N)]
    flag = [rng.choice([0, 1, 1, 2]) for _ in range(N)]
    order = list(range(N))
    rng.shuffle(order)
    return pa.table(
        {
            "k": [k[i] for i in order],
            "p": [k[i] // 10 for i in order],
            "flag": [flag[i] for i in order],
            "id": order,
            "note": [f"row-{i}" for i in order],
        }
    )


def _expected(rows: pa.Table, *, partitioned: bool) -> list[dict]:
    """The matches of ``flag == 1`` then ``flag == 2`` over ``rows`` read in
    ``k`` order, ``k`` being unique."""
    ordered = sorted(rows.to_pylist(), key=lambda r: r["k"])
    return [
        a
        for a, b in zip(ordered, ordered[1:])
        if a["flag"] == 1 and b["flag"] == 2 and (not partitioned or a["p"] == b["p"])
    ]


def _plain(result: LTSeq) -> list[dict]:
    return [dict(row) for row in result.to_arrow().to_pylist()]


def test_two_steps_return_every_column_in_table_order(data_layout):
    rows = _rows(distinct_keys=N)
    t = data_layout(rows).sort("k")

    for partition_by in (None, "p"):
        result = t.search_pattern(
            lambda r: r.flag == 1, lambda r: r.flag == 2, partition_by=partition_by
        )
        expected = _expected(rows, partitioned=partition_by is not None)
        assert expected, "the rows hold matches"
        assert _plain(result) == expected


def test_rows_with_tied_sort_keys_are_the_rows_that_matched(engine_layout):
    # 30 keys over 300 rows: each key is tied over 10 rows, which the sort may
    # return in any order.
    rows = _rows(distinct_keys=30)
    t = LTSeq.from_arrow(pa.Table.from_batches(rows.to_batches(max_chunksize=7))).sort("k")

    single = _plain(t.search_pattern(lambda r: r.flag == 1))
    matched = sorted((r for r in rows.to_pylist() if r["flag"] == 1), key=lambda r: r["id"])
    assert sorted(single, key=lambda r: r["id"]) == matched
    assert [r["k"] for r in single] == sorted(r["k"] for r in single)

    two = _plain(t.search_pattern(lambda r: r.flag == 1, lambda r: r.flag == 2))
    assert two, "the rows hold matches"
    assert all(r["flag"] == 1 for r in two)
    assert [r["k"] for r in two] == sorted(r["k"] for r in two)
    ids = [r["id"] for r in two]
    assert len(set(ids)) == len(ids)
    by_id = {r["id"]: r for r in rows.to_pylist()}
    assert all(by_id[r["id"]] == r for r in two)
