"""`sort` in the order of contract §9.4 (#268): test row O3 and property P9.

- **P9.** `t.sort(keys, descending, nulls_last)` equals Python's stable
  `sorted` over the input rows with §9.4's key function, for random inputs
  with heavy ties, NULL, NaN of both signs, `±0.0` and `±inf`, at every
  engine layout and every input layout (one or many batches, Parquet files
  and row groups, CSV files).
- **Stability** at scale: #268's repro, 120,000 rows in 12 batches.
- **NULL placement** by `nulls_last`, per key, in both directions.
- **Floats**: every NaN above `+inf`, whatever its sign bit; `-0.0` and
  `0.0` equal, so they keep their input order.
- **Everywhere**: window `order_by` and ranking order the same way, and so
  does a window over the declared order.

Rows are compared by an `i` column numbering the input rows, so a NaN or a
zero is identified by its row, not by its bits.
"""

from __future__ import annotations

import math
import random
import struct
import zlib

import pyarrow as pa
import pytest

from ltseq import LTSeq, dense_rank, rank, row_number

NEG_NAN = struct.unpack("<d", struct.pack("<Q", 0xFFF8_0000_0000_0000))[0]
INF = float("inf")


def is_negative_nan(value) -> bool:
    return isinstance(value, float) and math.isnan(value) and math.copysign(1.0, value) < 0


def reference(rows: list[dict], keys, descending, nulls_last) -> list[int]:
    """The `i` of `rows` sorted by §9.4: stable, NULL by `nulls_last` in
    either direction, every NaN above `+inf`, `-0.0 == 0.0`.

    One stable pass per key, least significant first, gives the
    lexicographic stable order.
    """

    def value_key(value):
        if isinstance(value, float) and math.isnan(value):
            return (1, 0.0)
        return (0, value)

    out = list(rows)
    for key, desc, last in reversed(list(zip(keys, descending, nulls_last))):
        present = sorted(
            (r for r in out if r[key] is not None),
            key=lambda r: value_key(r[key]),
            reverse=desc,
        )
        missing = [r for r in out if r[key] is None]
        out = present + missing if last else missing + present
    return [r["i"] for r in out]


FLOATS = [NEG_NAN, float("nan"), INF, -INF, 0.0, -0.0, 1.5, -1.5, None]
STRINGS = ["Z", "a", "é", "ab", None]


def random_rows(seed: int, n: int) -> list[dict]:
    rng = random.Random(seed)
    return [
        {
            "i": i,
            "k": rng.choice([0, 1, 2, None]),
            "x": rng.choice(FLOATS),
            "s": rng.choice(STRINGS),
        }
        for i in range(n)
    ]


def as_table(rows: list[dict]) -> pa.Table:
    return pa.table(
        {
            "i": pa.array([r["i"] for r in rows], pa.int64()),
            "k": pa.array([r["k"] for r in rows], pa.int64()),
            "x": pa.array([r["x"] for r in rows], pa.float64()),
            "s": pa.array([r["s"] for r in rows], pa.string()),
        }
    )


def ids(t: LTSeq) -> list[int]:
    return t.to_arrow().column("i").to_pylist()


# (keys, descending, nulls_last): each key alone in both directions and both
# NULL placements, and mixed multi-key orders.
ORDERS = [
    (("x",), (False,), (True,)),
    (("x",), (True,), (True,)),
    (("x",), (False,), (False,)),
    (("x",), (True,), (False,)),
    (("k",), (True,), (True,)),
    (("s",), (False,), (False,)),
    (("k", "x"), (False, True), (True, False)),
    (("s", "k", "x"), (True, False, False), (False, True, True)),
]


@pytest.mark.parametrize("keys,descending,nulls_last", ORDERS)
def test_p9_at_every_engine_layout(engine_layout, keys, descending, nulls_last):
    rows = random_rows(seed=zlib.crc32(repr(keys).encode()), n=200)
    t = LTSeq.from_arrow(
        pa.Table.from_batches(as_table(rows).to_batches(max_chunksize=17))
    )
    out = t.sort(*keys, descending=list(descending), nulls_last=list(nulls_last))
    assert ids(out) == reference(rows, keys, descending, nulls_last)


@pytest.mark.parametrize("keys,descending,nulls_last", ORDERS[1:2] + ORDERS[6:])
def test_p9_at_every_input_layout(engine_config, data_layout, keys, descending, nulls_last):
    engine_config(target_partitions=4, batch_size=7)
    rows = random_rows(seed=7, n=90)
    t = data_layout(as_table(rows))
    out = t.sort(*keys, descending=list(descending), nulls_last=list(nulls_last))
    assert ids(out) == reference(rows, keys, descending, nulls_last)


def test_issue_repro_is_stable(engine_config):
    """#268: 120,000 rows in 12 batches, a key with three values."""
    engine_config(target_partitions=12)
    n = 120_000
    tbl = pa.table({"k": [i % 3 for i in range(n)], "i": range(n)})
    t = LTSeq.from_arrow(pa.Table.from_batches(tbl.to_batches(max_chunksize=n // 12)))
    assert ids(t.sort("k")) == sorted(range(n), key=lambda i: i % 3)
    assert ids(t.sort("k", descending=True)) == sorted(range(n), key=lambda i: -(i % 3))


def test_terminals_after_sort_read_the_stable_order(engine_config):
    engine_config(target_partitions=8, batch_size=5)
    n = 300
    rows = [{"i": i, "k": i % 4} for i in range(n)]
    t = LTSeq.from_arrow(pa.Table.from_pylist(rows)).sort("k")
    expected = sorted(range(n), key=lambda i: i % 4)
    assert ids(t.head(50)) == expected[:50]
    assert ids(t.slice(100, 30)) == expected[100:130]
    assert ids(t.tail(20)) == expected[-20:]
    assert [r["i"] for r in t.to_dicts()] == expected


def test_sort_of_a_sort_is_stable_on_the_first(engine_config):
    engine_config(target_partitions=6, batch_size=3)
    rows = [{"i": i, "a": i % 5, "b": i % 2} for i in range(100)]
    t = LTSeq.from_arrow(pa.Table.from_pylist(rows))
    expected = sorted(sorted(range(100), key=lambda i: i % 5), key=lambda i: i % 2)
    assert ids(t.sort("a").sort("b")) == expected


# --- O3: value order -------------------------------------------------------


def test_descending_puts_null_last():
    """§9.4's example: v0.4 gave [None, nan, inf, 2.0, 1.0]."""
    t = LTSeq.from_dict({"x": [2.0, None, float("nan"), 1.0, INF]})
    out = t.sort("x", descending=True).to_arrow().column("x").to_pylist()
    assert out[1:] == [INF, 2.0, 1.0, None]
    assert math.isnan(out[0])


@pytest.mark.parametrize("descending", [False, True])
def test_nulls_last_false_puts_null_first(descending):
    t = LTSeq.from_dict({"i": [0, 1, 2, 3], "x": [2, None, 1, None]})
    out = t.sort("x", descending=descending, nulls_last=False)
    assert ids(out) == ([1, 3, 0, 2] if descending else [1, 3, 2, 0])


def test_nulls_last_per_key():
    t = LTSeq.from_dict(
        {"i": [0, 1, 2, 3, 4, 5], "a": [1, None, 1, None, 2, 2], "b": [None, 1, 2, None, None, 3]}
    )
    out = t.sort("a", "b", descending=[True, False], nulls_last=[False, True])
    assert ids(out) == [1, 3, 5, 4, 2, 0]


def test_nulls_last_list_of_wrong_length_raises():
    t = LTSeq.from_dict({"a": [1], "b": [2]})
    with pytest.raises(ValueError, match="nulls_last"):
        t.sort("a", "b", nulls_last=[True])


def test_every_nan_is_above_inf_and_zeros_are_equal():
    xs = [1.0, NEG_NAN, -INF, -0.0, 0.0, float("nan"), INF, -0.0]
    t = LTSeq.from_dict({"i": list(range(len(xs))), "x": xs})
    assert ids(t.sort("x")) == [2, 3, 4, 7, 0, 6, 1, 5]
    assert ids(t.sort("x", descending=True)) == [1, 5, 6, 0, 3, 4, 7, 2]
    # The rows keep their values: the NaN with the sign bit is still one.
    out = t.sort("x").to_arrow().column("x").to_pylist()
    assert is_negative_nan(out[6])
    assert [math.copysign(1.0, v) for v in out[1:4]] == [-1.0, 1.0, -1.0]


@pytest.mark.parametrize("dtype", [pa.float32(), pa.float16()])
def test_narrow_floats_order_as_float64(dtype):
    import numpy as np

    values = np.array([1.0, np.nan, -np.inf, -0.0, 0.0], dtype=dtype.to_pandas_dtype())
    values[1] = -values[1]  # a NaN with the sign bit set
    t = LTSeq.from_arrow(pa.table({"i": [0, 1, 2, 3, 4], "x": pa.array(values, dtype)}))
    assert ids(t.sort("x")) == [2, 3, 4, 0, 1]


def test_dictionary_encoded_floats():
    xs = pa.array([1.0, NEG_NAN, -0.0, 0.0, -1.0]).dictionary_encode()
    t = LTSeq.from_arrow(pa.table({"i": [0, 1, 2, 3, 4], "x": xs}))
    assert ids(t.sort("x")) == [4, 2, 3, 0, 1]


def test_strings_by_code_point():
    t = LTSeq.from_dict({"i": [0, 1, 2, 3], "s": ["é", "a", "Z", None]})
    assert ids(t.sort("s")) == [2, 1, 0, 3]


def test_computed_key_uses_the_same_order():
    t = LTSeq.from_dict({"i": [0, 1, 2, 3], "x": [1.0, NEG_NAN, -0.0, None]})
    out = t.sort(lambda r: r.x * 1.0, descending=True)
    assert ids(out) == [1, 0, 2, 3]


# --- declared order --------------------------------------------------------


def test_sort_keys_and_is_sorted_by_carry_nulls_last():
    t = LTSeq.from_dict({"a": [1, None], "b": [None, 2]})
    s = t.sort("a", "b", descending=[True, False], nulls_last=[False, True])
    assert s.sort_keys == [("a", True), ("b", False)]
    assert s.is_sorted_by("a", "b", desc=[True, False], nulls_last=[False, True])
    assert not s.is_sorted_by("a", desc=True)
    assert t.sort("a").is_sorted_by("a")
    assert not t.sort("a").is_sorted_by("a", nulls_last=False)


def test_reads_after_a_filter_keep_the_order(engine_config):
    """A read re-imposes the declared order: NULL placement and float order
    included."""
    engine_config(target_partitions=8, batch_size=3)
    xs = [None, 2.0, NEG_NAN, -0.0, INF, 1.0, None, -INF] * 10
    t = LTSeq.from_dict({"i": list(range(len(xs))), "x": xs})
    s = t.sort("x", descending=True, nulls_last=False)
    out = s.filter(lambda r: r.i >= 0).derive(y=lambda r: r.i + 1).to_arrow()
    xs_out = out.column("x").to_pylist()
    kinds = [
        "null" if v is None else "nan" if math.isnan(v) else v for v in xs_out
    ]
    assert kinds == ["null"] * 20 + ["nan"] * 10 + [INF] * 10 + [2.0] * 10 + [1.0] * 10 + [-0.0] * 10 + [-INF] * 10


def test_sorting_by_the_declared_keys_keeps_rows_in_place():
    rows = [{"i": i, "a": i // 10, "b": -i} for i in range(40)]
    t = LTSeq.from_arrow(pa.Table.from_pylist(rows)).assume_sorted("a", "b", desc=[False, True])
    assert ids(t.sort("a")) == list(range(40))
    assert t.sort("a").sort_keys == [("a", False)]


def test_assume_sorted_declares_float_order_and_null_placement():
    xs = [None, -INF, -0.0, 0.0, 1.0, INF, NEG_NAN]
    t = LTSeq.from_dict({"i": list(range(7)), "x": xs}).assume_sorted("x", nulls_last=False)
    assert t.is_sorted_by("x", nulls_last=False)
    assert ids(t) == list(range(7))
    prev = t.derive(p=lambda r: r.i.shift(1)).to_arrow().column("p").to_pylist()
    assert prev == [None, 0, 1, 2, 3, 4, 5]


def test_assume_sorted_parquet_float_key(tmp_path):
    import pyarrow.parquet as pq

    xs = [-INF, -0.0, 0.0, 1.0, INF, NEG_NAN, None]
    path = tmp_path / "x.parquet"
    pq.write_table(pa.table({"i": list(range(7)), "x": xs}), path)
    t = LTSeq.read_parquet(str(path)).assume_sorted("x")
    assert ids(t.sort("x")) == list(range(7))
    assert ids(t.filter(lambda r: r.i != 3)) == [0, 1, 2, 4, 5, 6]


# --- windows and ranking ---------------------------------------------------


def window_column(t: LTSeq, **derived) -> list:
    name = next(iter(derived))
    return t.derive(**derived).to_arrow().column(name).to_pylist()


def test_ranking_orders_floats_as_sort_does():
    xs = [1.0, NEG_NAN, -INF, -0.0, 0.0, float("nan"), INF, None]
    t = LTSeq.from_dict({"i": list(range(len(xs))), "x": xs}).sort("i")
    ranks = window_column(t, r=lambda r: rank().over(order_by="x"))
    #        1.0 -nan -inf -0.0  0.0  nan  inf  None
    assert ranks == [4, 6, 1, 2, 2, 6, 5, 8]
    dense = window_column(t, r=lambda r: dense_rank().over(order_by="x", descending=True))
    assert dense == [3, 1, 5, 4, 4, 1, 2, 6]


def test_window_order_by_nulls_last_false():
    t = LTSeq.from_dict({"i": [0, 1, 2], "x": [2, None, 1]}).sort("i")
    assert window_column(t, r=lambda r: row_number().over(order_by="x")) == [2, 3, 1]
    assert window_column(
        t, r=lambda r: row_number().over(order_by="x", nulls_last=False)
    ) == [3, 1, 2]
    assert window_column(
        t, r=lambda r: row_number().over(order_by="x", descending=True, nulls_last=False)
    ) == [2, 1, 3]


def test_row_number_ranks_rows_in_sort_order():
    """`row_number().over(order_by=k)` gives each row its position in
    `sort(k)`, ties apart."""
    xs = [None, 2.0, NEG_NAN, -0.0, INF, 1.0]
    t = LTSeq.from_dict({"i": list(range(6)), "x": xs})
    for descending in (False, True):
        for nulls_last in (False, True):
            order = ids(t.sort("x", descending=descending, nulls_last=nulls_last))
            got = window_column(
                t.sort("i"),
                r=lambda r: row_number().over(
                    order_by="x", descending=descending, nulls_last=nulls_last
                ),
            )
            assert [order.index(i) + 1 for i in range(6)] == got


def test_window_over_declared_order_follows_it():
    xs = [None, 2.0, NEG_NAN, -0.0, INF, 1.0]
    t = LTSeq.from_dict({"i": list(range(6)), "x": xs})
    s = t.sort("x", descending=True, nulls_last=False)
    out = s.derive(prev=lambda r: r.i.shift(1)).to_arrow()
    order = out.column("i").to_pylist()
    assert order == [0, 2, 4, 1, 5, 3]
    assert out.column("prev").to_pylist() == [None] + order[:-1]


def test_a_float_key_is_sorted_once(engine_config):
    """Reads re-impose the declared order through the same `order_key`
    expression the sort used, so DataFusion sees it satisfied and plans no
    second sort, after the sort and after a filter."""
    engine_config(target_partitions=4)
    t = LTSeq.from_dict({"i": list(range(6)), "x": [2.0, NEG_NAN, -0.0, 0.0, None, 1.0]})
    for table in (t.sort("x"), t.sort("x").filter(lambda r: r.i > 0)):
        counts = table._explain_pipeline()["operator_counts"]
        assert counts.get("SortExec") == 1, counts
    declared = t.sort("x").assume_sorted("x")
    counts = declared.filter(lambda r: r.i > 0)._explain_pipeline()["operator_counts"]
    assert "SortExec" not in counts, counts
