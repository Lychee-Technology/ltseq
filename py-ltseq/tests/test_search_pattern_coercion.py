"""search_pattern step predicates coerce their operands as filter does (#188).

`search_pattern` and `search_pattern_count` evaluate step predicates with
their own evaluator (`src/ops/pattern_match.rs`), which called Arrow's
comparison and arithmetic kernels on the operands as they came. Those
kernels refuse two operand types, so a predicate `filter` runs raised there:

- a `str` literal, which is `Utf8`, against the `LargeUtf8` strings of
  `from_pandas` or the `Utf8View` strings of a Parquet read:
  `Invalid comparison operation: Utf8View == Utf8`;
- `r.v > 1.5` on an Int64 column (`Int64 > Float64`), `r.close > 2` on a
  Float64 one, an Int32 column against an Int64 one, and a `Decimal`
  literal against a float column.

Since #218 the evaluator casts both operands to DataFusion's common type,
and lowers a comparison with a literal through the transpiler, as `filter`
does. These tests hold it to that: a step selects the rows `filter` selects
with the same predicate, on every string type, input source and engine
layout. The reproductions of the issue, and the numeric cases, also check
rows worked out by hand, so they do not rest on `filter` being right.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq

# ---- reference ----


def _ids(table: LTSeq) -> list[int]:
    return table.to_arrow().column("i").to_pylist()


def _reference(t: LTSeq, steps, partition_by: str | None) -> list[int]:
    """Start rows *i* such that `filter(steps[k])` selects row *i + k* for
    every *k*, all in one partition of ``partition_by``."""
    rows = t.to_arrow()
    order = rows.column("i").to_pylist()
    keys = rows.column(partition_by).to_pylist() if partition_by else None
    selected = [set(_ids(t.filter(step))) for step in steps]
    last = len(steps) - 1
    return [
        order[p]
        for p in range(len(order) - last)
        if all(order[p + k] in selected[k] for k in range(len(steps)))
        and (keys is None or len({keys[p + k] for k in range(len(steps))}) == 1)
    ]


def _assert_steps_select_filter_rows(t: LTSeq, cases) -> None:
    for steps in cases:
        for partition_by in (None, "u"):
            expected = _reference(t, steps, partition_by)
            got = _ids(t.search_pattern(*steps, partition_by=partition_by))
            assert got == expected, (steps, partition_by)
            count = t.search_pattern_count(*steps, partition_by=partition_by)
            assert count == len(expected), (steps, partition_by)


# ---- strings against str literals ----

# Three users' event streams, ordered by (u, ts). Every event value occurs
# in every user, so each comparison selects rows in several partitions.
EVENTS = {
    "i": list(range(12)),
    "u": [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3],
    "ts": [1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4],
    "e": [
        "view", "cart", "order", "view",
        "view", "view", "cart", "order",
        "cart", "view", "cart", "order",
    ],
}

FUNNEL = (
    lambda r: r.e == "view",
    lambda r: r.e == "cart",
    lambda r: r.e == "order",
)

STRING_CASES = [
    (lambda r: r.e == "view",),
    (lambda r: "cart" == r.e,),
    (lambda r: r.e != "order",),
    (lambda r: r.e < "order",),
    (lambda r: r.e >= "view",),
    (lambda r: (r.e == "view") | (r.e == "order"),),
    (lambda r: ~(r.e == "cart") & (r.ts > 1),),
    FUNNEL,
    FUNNEL[:2],
]


def _events(string_type: pa.DataType = pa.string()) -> pa.Table:
    return pa.table(
        {
            "i": pa.array(EVENTS["i"], pa.int64()),
            "u": pa.array(EVENTS["u"], pa.int64()),
            "ts": pa.array(EVENTS["ts"], pa.int64()),
            "e": pa.array(EVENTS["e"], string_type),
        }
    )


def test_string_steps_in_every_input_layout(engine_layout, data_layout):
    """from_arrow, CSV and Parquet input (`Utf8`, and `Utf8View` from
    Parquet), at every engine layout."""
    t = data_layout(_events()).sort("u", "ts")
    _assert_steps_select_filter_rows(t, STRING_CASES)


@pytest.mark.parametrize(
    "string_type",
    [pa.string(), pa.large_string(), pa.string_view()],
    ids=["utf8", "large_utf8", "utf8_view"],
)
def test_string_steps_on_every_string_type(engine_layout, string_type):
    rows = _events(string_type)
    # A NULL event matches no comparison, as in filter.
    events = EVENTS["e"][:5] + [None] + EVENTS["e"][6:]
    rows = rows.set_column(3, "e", pa.array(events, string_type))
    t = LTSeq.from_arrow(rows).sort("u", "ts")
    assert t.to_arrow().schema.field("e").type == string_type
    _assert_steps_select_filter_rows(t, STRING_CASES)


def test_string_steps_from_pandas(engine_layout):
    t = LTSeq.from_pandas(pd.DataFrame(EVENTS)).sort("u", "ts")
    assert t.to_arrow().schema.field("e").type == pa.large_string()
    _assert_steps_select_filter_rows(t, STRING_CASES)


def test_string_steps_from_dict(engine_layout):
    t = LTSeq.from_dict(EVENTS).sort("u", "ts")
    assert t.to_arrow().schema.field("e").type == pa.string()
    _assert_steps_select_filter_rows(t, STRING_CASES)


@pytest.mark.parametrize("row_group_size", [1, 2, 12])
def test_string_steps_on_sorted_parquet(tmp_path, row_group_size):
    """A Parquet table declared sorted keeps its source path, so a count
    with `partition_by` takes the parallel scan, which evaluates the steps
    per row group and on the rows around each row-group seam."""
    path = str(tmp_path / "events.parquet")
    pq.write_table(_events(), path, row_group_size=row_group_size)
    t = LTSeq.read_parquet(path).assume_sorted("u", "ts")
    assert t.to_arrow().schema.field("e").type == pa.string_view()
    _assert_steps_select_filter_rows(t, STRING_CASES)


def test_issue_parquet_reproduction(tmp_path):
    """The reproduction in the comment of 2026-10-20 on #188."""
    path = str(tmp_path / "ev.parquet")
    pq.write_table(
        pa.table({"u": [1, 1, 1], "ts": [1, 2, 3], "e": ["view", "cart", "order"]}), path
    )
    t = LTSeq.read_parquet(path).sort("u", "ts")
    steps = (lambda r: r.e == "view", lambda r: r.e == "cart")
    assert t.search_pattern_count(*steps, partition_by="u") == 1
    assert t.search_pattern_count(*steps) == 1
    assert t.search_pattern(*steps, partition_by="u").to_arrow().column("ts").to_pylist() == [1]
    sorted_t = LTSeq.read_parquet(path).assume_sorted("u", "ts")
    assert sorted_t.search_pattern_count(*steps, partition_by="u") == 1


# The funnel of tutorial 07: u1 converts, u2 stops at cart, u3 skips the
# cart, u4 converts on its second round.
FUNNEL_ROWS = [
    ("u1", 1, "view"), ("u1", 2, "cart"), ("u1", 3, "order"),
    ("u2", 1, "view"), ("u2", 2, "cart"),
    ("u3", 1, "view"), ("u3", 2, "order"),
    ("u4", 1, "view"), ("u4", 2, "view"), ("u4", 3, "cart"), ("u4", 4, "order"),
]


def _funnel_frame() -> pd.DataFrame:
    return pd.DataFrame(FUNNEL_ROWS, columns=["u", "ts", "e"])


def _assert_funnel(events: LTSeq) -> None:
    hits = events.search_pattern(*FUNNEL, partition_by="u").to_arrow()
    assert list(zip(hits.column("u").to_pylist(), hits.column("ts").to_pylist())) == [
        ("u1", 1),
        ("u4", 2),
    ]
    assert events.search_pattern_count(*FUNNEL, partition_by="u") == 2
    assert events.search_pattern_count(*FUNNEL[:2], partition_by="u") == 3


def test_funnel_from_pandas(engine_layout):
    _assert_funnel(LTSeq.from_pandas(_funnel_frame()).sort("u", "ts"))


def test_funnel_from_parquet(engine_layout, tmp_path):
    path = str(tmp_path / "events.parquet")
    _funnel_frame().to_parquet(path, index=False)
    # `sort`, not `assume_sorted`: on a Parquet table declared sorted, a count
    # partitioned by a string key raises instead of leaving the parallel scan
    # (#211).
    _assert_funnel(LTSeq.read_parquet(path).sort("u", "ts"))


# ---- numbers of two types ----

NUMBERS = pa.table(
    {
        "i": pa.array(range(8), pa.int64()),
        "u": pa.array([1, 1, 1, 1, 2, 2, 2, 2], pa.int64()),
        "v": pa.array([1, 2, 3, 4, 1, None, 3, 4], pa.int64()),
        "a": pa.array([1, 5, 2, 7, 3, 3, None, 0], pa.int32()),
        "b": pa.array([2, 5, 1, 6, 3, 4, 1, None], pa.int64()),
        "close": pa.array([1.5, 2.0, 2.5, 3.0, 0.5, 2.0, None, 4.25], pa.float64()),
        "w": pa.array([1, 2, 3, 4, 5, 6, 7, 8], pa.uint32()),
        "p": pa.array(
            [Decimal(x) if x else None for x in
             ["1.25", "2.50", "3.75", "0.10", "9.99", None, "2.50", "1.00"]],
            pa.decimal128(5, 2),
        ),
        "d": pa.array([date(2024, 1, 1) + timedelta(days=k) for k in range(8)], pa.date32()),
    }
)


@pytest.mark.parametrize(
    ("pred", "expected"),
    [
        # The title's case.
        (lambda r: r.v > 1.5, [1, 2, 3, 6, 7]),
        (lambda r: r.close > 2, [2, 3, 7]),
        # Int32 against Int64 columns.
        (lambda r: r.a > r.b, [2, 3]),
        (lambda r: r.a == r.b, [1, 4]),
        # A Decimal literal against a float column, and an Int32 one.
        (lambda r: r.close >= Decimal("2.5"), [2, 3, 7]),
        (lambda r: r.a < Decimal("2.5"), [0, 2, 7]),
        # A decimal column against a float literal, a Decimal literal of
        # another scale and a float column.
        (lambda r: r.p > 1.5, [1, 2, 4, 6]),
        (lambda r: r.p == Decimal("2.500"), [1, 6]),
        (lambda r: r.p > r.close, [1, 2, 4]),
        # A date column against a datetime literal compares instants.
        (lambda r: r.d > datetime(2024, 1, 3, 12), [3, 4, 5, 6, 7]),
        # Arithmetic of two types, then a comparison.
        (lambda r: r.a + r.close > 6, [1, 3]),
        (lambda r: r.v * r.close <= 2, [0, 4]),
        (lambda r: r.w - r.v == 4, [4, 6, 7]),
    ],
    ids=[
        "int64_gt_float",
        "float64_gt_int",
        "int32_gt_int64",
        "int32_eq_int64",
        "float64_ge_decimal",
        "int32_lt_decimal",
        "decimal_gt_float",
        "decimal_eq_decimal_of_other_scale",
        "decimal_gt_float64",
        "date32_gt_datetime",
        "int32_plus_float64",
        "int64_times_float64",
        "uint32_minus_int64",
    ],
)
def test_numeric_steps_select_filter_rows(engine_layout, pred, expected):
    t = LTSeq.from_arrow(NUMBERS).sort("i")
    assert _ids(t.filter(pred)) == expected
    assert _ids(t.search_pattern(pred)) == expected
    assert t.search_pattern_count(pred) == len(expected)
    _assert_steps_select_filter_rows(t, [(pred,), (pred, pred), (lambda r: r.v >= 1, pred)])


def test_issue_reproduction_from_pandas():
    """The reproduction in the body of #188."""
    t = LTSeq.from_pandas(pd.DataFrame({"v": [1, 2, 3, 4]})).sort("v")
    hits = t.search_pattern(lambda r: r.v > 1.5, lambda r: r.v >= 3)
    assert hits.to_arrow().column("v").to_pylist() == [2, 3]
    assert t.search_pattern_count(lambda r: r.v > 1.5, lambda r: r.v >= 3) == 2
