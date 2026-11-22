"""Windows in one ``derive`` whose inputs differ only by a cast (#318).

DataFusion names a cast by the expression inside it, so ``lag(CAST(g AS
Float64))`` and ``lag(g)`` read as the same text, and a window node that
names its functions by their text rejects the pair as duplicates. Each
window is computed under a name of its own, so one ``derive`` gives what
two ``derive`` calls give.
"""

import pyarrow as pa
import pytest

from ltseq import LTSeq


def _table():
    return LTSeq.from_arrow(pa.table({"m": ["a", "b", "c", "d"], "g": [1, 2, 4, 8]})).sort("m")


def _one_derive_matches_two(a, b):
    together = _table().derive(a=a, b=b).to_arrow()
    apart = _table().derive(a=a).derive(b=b).to_arrow()
    assert together.equals(apart)
    return together


def test_shift_with_and_without_a_cast(engine_layout):
    out = _one_derive_matches_two(
        lambda r: r.g.shift(1), lambda r: r.g.cast("float64").shift(1)
    )
    assert out.schema.field("a").type == pa.int64()
    assert out.schema.field("b").type == pa.float64()
    assert out.column("a").to_pylist() == [None, 1, 2, 4]
    assert out.column("b").to_pylist() == [None, 1.0, 2.0, 4.0]


def test_shift_under_two_different_casts(engine_layout):
    out = _one_derive_matches_two(
        lambda r: r.g.cast("float32").shift(1), lambda r: r.g.cast("float64").shift(1)
    )
    assert out.schema.field("a").type == pa.float32()
    assert out.schema.field("b").type == pa.float64()
    assert out.column("b").to_pylist() == [None, 1.0, 2.0, 4.0]


def test_rolling_sum_with_and_without_a_cast(engine_layout):
    out = _one_derive_matches_two(
        lambda r: r.g.rolling(2).sum(), lambda r: r.g.cast("float64").rolling(2).sum()
    )
    assert out.schema.field("b").type == pa.float64()
    assert out.column("b").to_pylist()[1:] == [3.0, 6.0, 12.0]


def test_pct_change_with_and_without_a_cast(engine_layout):
    out = _one_derive_matches_two(
        lambda r: r.g.pct_change(), lambda r: r.g.cast("float64").pct_change()
    )
    assert out.column("a").to_pylist()[1:] == [1.0, 1.0, 1.0]
    assert out.column("b").to_pylist()[1:] == [1.0, 1.0, 1.0]


def test_nested_windows_with_and_without_a_cast(engine_layout):
    # Each inner window is staged in a hidden column first; the two inner
    # windows have the same text.
    out = _one_derive_matches_two(
        lambda r: r.g.shift(1).shift(1), lambda r: r.g.cast("float64").shift(1).shift(1)
    )
    assert out.column("a").to_pylist() == [None, None, 1, 2]
    assert out.column("b").to_pylist() == [None, None, 1.0, 2.0]


def test_overwriting_the_sort_key_keeps_the_input_order(engine_layout):
    # Replacing the sort key projects through the order-keeping path.
    t = LTSeq.from_arrow(pa.table({"m": [3, 1, 2], "g": [30, 10, 20]})).sort("m")
    out = t.derive(m=lambda r: r.m.shift(1), b=lambda r: r.m.cast("float64").shift(1))
    assert out.to_dicts() == [
        {"m": None, "g": 10, "b": None},
        {"m": 1, "g": 20, "b": 1.0},
        {"m": 2, "g": 30, "b": 2.0},
    ]


def test_cum_sum_with_and_without_a_cast(engine_layout):
    out = _table().cum_sum(lambda r: r.g, lambda r: r.g.cast("float64")).to_arrow()
    assert out.column("g_cumsum").to_pylist() == [1, 3, 7, 15]
    assert out.column("cum_sum_1_cumsum").to_pylist() == [1.0, 3.0, 7.0, 15.0]


@pytest.mark.parametrize("name", ["__ltseq_window__", "lag(g)"])
def test_a_column_with_a_staged_name_is_read_as_itself(name):
    t = LTSeq.from_arrow(pa.table({"m": ["a", "b"], "g": [1, 2], name: [7, 9]})).sort("m")
    out = t.derive(a=lambda r: r.g.shift(1), b=lambda r: r.g.cast("float64").shift(1))
    out = out.to_arrow()
    assert out.column(name).to_pylist() == [7, 9]
    assert out.column("b").to_pylist() == [None, 1.0]
