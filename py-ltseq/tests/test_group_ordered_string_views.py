"""group_ordered().filter().flatten() over a Parquet string key (#338).

A Parquet scan yields `string_view` columns. Arrow's concat of view arrays
keeps every data buffer of every input, so in small batches each sort and
window of the group pipeline multiplied the buffers a batch referenced:
100 rows at batch size 1 did not finish. The pipeline now runs over plain
strings and casts back, so these runs take a fraction of a second.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from ltseq import LTSeq

from . import layouts

PARQUET_LAYOUTS = ("parquet_file", "parquet_row_groups", "parquet_files")


def rows(n: int) -> pa.Table:
    # Keys longer than 12 bytes are not inlined in the views, and runs of 1
    # and 3 rows make the filter keep some groups and drop others.
    g = [f"key-long-enough-{i // 3 if i % 4 else -i}" for i in range(n)]
    return pa.table({"date": list(range(n))[::-1], "g": g[::-1], "v": list(range(n))})


def kept(t: LTSeq) -> pa.Table:
    out = t.sort("date").group_ordered(lambda r: r.g).filter(lambda g: g.count() > 1).flatten()
    return layouts.plain_types(out.to_arrow())


@pytest.mark.parametrize("layout", PARQUET_LAYOUTS)
def test_filter_flatten_over_parquet_string_key(layout, engine_layout, tmp_path):
    table = rows(100)
    got = kept(layouts.build(layout, table, tmp_path / "input"))
    want = kept(LTSeq.from_arrow(table))
    assert got.schema.field("g").type == pa.string()
    assert got.num_rows > 0
    assert got.to_pylist() == want.to_pylist()


def test_result_keeps_string_view_type(tmp_path):
    t = layouts.build("parquet_file", rows(20), tmp_path / "input")
    out = t.sort("date").group_ordered(lambda r: r.g).flatten().to_arrow()
    assert out.schema.field("g").type == pa.string_view()


def test_computed_key_keeps_view_types(tmp_path):
    # The grouping lambda resolves against the input's types: the length of
    # a `string_view` is Int32, so with an Int32 `k` of 2**30 the product
    # wraps to the same key for lengths 13 and 17. Computing the key over
    # the large plain copies would make it Int64 and split the two rows.
    table = pa.table(
        {
            "date": [0, 1],
            "g": ["a" * 13, "b" * 17],
            "k": pa.array([2**30, 2**30], pa.int32()),
        }
    )
    t = layouts.build("parquet_file", table, tmp_path / "input").sort("date")
    assert t.to_arrow().schema.field("g").type == pa.string_view()

    staged = t.derive(key=lambda r: r.g.s.len() * r.k)
    assert staged.to_arrow().column("key").to_pylist() == [2**30, 2**30]
    grouped = [
        t.group_ordered(lambda r: r.g.s.len() * r.k),
        staged.group_ordered(lambda r: r.key),
        # A boundary expression with a window runs on the other path: the
        # product is 2**30 on both rows, so only the first starts a group.
        t.group_ordered(lambda r: (r.g.s.len() * r.k != 2**30) | (r.date.shift(1) > 1)),
    ]
    for g in grouped:
        assert g.filter(lambda g: g.count() > 1).flatten().count() == 2
