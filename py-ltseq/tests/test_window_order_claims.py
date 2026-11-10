"""A sort by a rolling or running aggregate sorts (#354).

DataFusion 55 reads an aggregate's set monotonicity (`max`, `count` and
unsigned `sum` only rise as values join the set, `min` only falls) as an
ordering of the window column, and drops a later sort that ordering
satisfies. The orderings it derives do not hold for LTSeq's windows:

- a sliding frame with no `partition_by` over a sorted argument is claimed
  ascending (descending for `min`) whichever way the argument is sorted, and
  a rolling `count` falls once NULLs enter the frame;
- a running frame is claimed `ASC NULLS LAST`, but its value is NULL until
  the first non-NULL argument (apache/datafusion#27267);
- an unsigned `sum` wraps on overflow.

So every window aggregate LTSeq builds claims no order, and these tests check
the sorted rows against an oracle computed in Python, not only against the
same rows sorted after `collect()`.
"""

from __future__ import annotations

import datetime
from collections import Counter
from decimal import Decimal

import pyarrow as pa
import pytest

from ltseq import LTSeq

# Distinct values, so a sort by `x` fixes the table order.
VALUES = {
    "int64": (pa.int64(), [5, -3, 7, 0, 2, -8, 4]),
    "uint64": (pa.uint64(), [5, 3, 7, 0, 2, 8, 4]),
    "float64": (pa.float64(), [0.5, -3.25, 7.0, 0.0, 2.5, -8.0, 4.0]),
    "decimal": (
        pa.decimal128(10, 2),
        [Decimal(v) for v in ("5.10", "-3.25", "7.00", "0.00", "2.50", "-8.75", "4.05")],
    ),
    "string": (pa.string(), ["e", "c", "g", "a", "b", "h", "d"]),
    "date": (
        pa.date32(),
        [datetime.date(2024, 1, d) for d in (5, 3, 7, 1, 2, 8, 4)],
    ),
}
NUMERIC = ("int64", "uint64", "float64", "decimal")

# Which rows of VALUES are NULL.
NULLS = {
    "no_nulls": set(),
    "one_null": {1},
    "three_nulls": {0, 3, 5},
    "all_null": set(range(7)),
}

# The table's order: by the argument either way, or by row id, which leaves
# the argument (and its NULLs) unordered.
ORDERS = {
    "x_asc": (("x", "i"), [False, False]),
    "x_desc": (("x", "i"), [True, False]),
    "row_id": (("i",), [False]),
}

# (id, frame width or None for a running frame, aggregate, argument types)
WINDOWS = [
    *[
        (f"rolling({w}).{agg}", w, agg, types)
        for w in (2, 3)
        for agg, types in (
            ("max", tuple(VALUES)),
            ("min", tuple(VALUES)),
            ("count", tuple(VALUES)),
            ("sum", NUMERIC),
        )
    ],
    ("cum_max", None, "max", tuple(VALUES)),
    ("cum_min", None, "min", tuple(VALUES)),
    ("cum_sum", None, "sum", NUMERIC),
]


def derive_m(t, width, agg, partitioned):
    by = {"partition_by": "g"} if partitioned else {}
    if width is None:
        return t.derive(m=lambda r: getattr(r.x, f"cum_{agg}")(**by))
    return t.derive(m=lambda r: getattr(r.x.rolling(width, **by), agg)())


def window_value(agg, frame):
    present = [v for v in frame if v is not None]
    if agg == "count":
        return len(present)
    if not present:
        return None
    return {"max": max, "min": min, "sum": sum}[agg](present)


def in_sort_order(values, desc):
    """`values` where `sort` puts them: NULLs last ascending, first
    descending, which is what it asks DataFusion for."""
    nulls = [v for v in values if v is None]
    present = sorted((v for v in values if v is not None), reverse=desc)
    return nulls + present if desc else present + nulls


def oracle(rows, order, width, agg, partitioned):
    """The rows in table order (`ORDERS[order]`), each with its window
    value `m`, computed from the frame's values."""
    if order == "row_id":
        ordered = sorted(rows, key=lambda r: r["i"])
    else:
        desc = order == "x_desc"
        nulls = sorted((r for r in rows if r["x"] is None), key=lambda r: r["i"])
        present = sorted(
            (r for r in rows if r["x"] is not None), key=lambda r: r["x"], reverse=desc
        )
        ordered = nulls + present if desc else present + nulls
    seen: dict[object, list] = {}
    out = []
    for row in ordered:
        part = seen.setdefault(row["g"] if partitioned else None, [])
        part.append(row["x"])
        frame = part if width is None else part[-width:]
        out.append({**row, "m": window_value(agg, frame)})
    return out


def table_rows(type_name, nulls):
    arrow_type, values = VALUES[type_name]
    xs = [None if index in NULLS[nulls] else v for index, v in enumerate(values)]
    return [{"i": i, "g": i % 2, "x": x} for i, x in enumerate(xs)], pa.table(
        {
            "i": pa.array(range(len(xs)), pa.int64()),
            "g": pa.array([i % 2 for i in range(len(xs))], pa.int64()),
            "x": pa.array(xs, arrow_type),
        }
    )


def as_tuple(row):
    return (row["i"], row["g"], row["x"], row["m"])


def check_sorted(t, expected_rows, label):
    """`t.sort("m")` and `t.sort("m", desc=True)`, whole and through a
    TopK, against the oracle's rows."""
    n = len(expected_rows)
    expected_counts = Counter(map(as_tuple, expected_rows))
    for desc in (False, True):
        expected_ms = in_sort_order([r["m"] for r in expected_rows], desc)
        sorted_t = t.sort("m", desc=desc)
        got = sorted_t.to_dicts()
        assert [r["m"] for r in got] == expected_ms, f"{label} desc={desc}"
        assert Counter(map(as_tuple, got)) == expected_counts, f"{label} desc={desc}"
        # The differential cross-check the issue states its criteria in.
        collected = t.collect().sort("m", desc=desc).to_dicts()
        assert [r["m"] for r in collected] == expected_ms, f"{label} desc={desc} collected"
        top = sorted_t.slice(0, n - 1).to_dicts()
        assert [r["m"] for r in top] == expected_ms[: n - 1], f"{label} desc={desc} slice"
        assert not Counter(map(as_tuple, top)) - expected_counts, f"{label} desc={desc} slice"


def window_cases():
    for name, width, agg, types in WINDOWS:
        for type_name in types:
            yield pytest.param(width, agg, type_name, id=f"{name}-{type_name}")


@pytest.mark.parametrize("width, agg, type_name", list(window_cases()))
def test_sort_by_window_sorts(engine_config, width, agg, type_name):
    engine_config(target_partitions=2, batch_size=3)
    for nulls in NULLS:
        rows, arrow_rows = table_rows(type_name, nulls)
        for order, (keys, desc) in ORDERS.items():
            for partitioned in (False, True):
                t = LTSeq.from_arrow(arrow_rows).sort(*keys, desc=desc)
                t = derive_m(t, width, agg, partitioned)
                expected = oracle(rows, order, width, agg, partitioned)
                label = f"{nulls} {order} partitioned={partitioned}"
                got = Counter(map(as_tuple, t.to_dicts()))
                assert got == Counter(map(as_tuple, expected)), label
                check_sorted(t, expected, label)


# The issue's cases, and the running ones found with them, in every engine
# and input layout.
ISSUE_CASES = [
    ("rolling(2).max int64 desc", pa.int64(), [3, 2, 1, 0], True, 2, "max"),
    ("rolling(2).max string desc", pa.string(), ["a", "b", "c"], True, 2, "max"),
    ("rolling(2).sum uint64 desc", pa.uint64(), [0, 1, 2, 3], True, 2, "sum"),
    ("rolling(2).count nulls asc", pa.int64(), [1, 2, None, None], False, 2, "count"),
    ("rolling(2).min int64 asc", pa.int64(), [0, 1, 2, 3], False, 2, "min"),
    ("cum_max leading NULL", pa.int64(), [None, 1, 2], True, None, "max"),
    ("cum_sum uint64 leading NULL", pa.uint64(), [None, 1, 2], True, None, "sum"),
]


@pytest.mark.parametrize("case", ISSUE_CASES, ids=[c[0] for c in ISSUE_CASES])
def test_issue_cases_in_every_layout(engine_layout, data_layout, case):
    name, arrow_type, xs, x_desc, width, agg = case
    arrow_rows = pa.table(
        {
            "i": pa.array(range(len(xs)), pa.int64()),
            "g": pa.array([0] * len(xs), pa.int64()),
            "x": pa.array(xs, arrow_type),
        }
    )
    t = data_layout(arrow_rows)
    read_type = t.to_arrow().schema.field("x").type
    # Parquet strings come back as views (layouts.py); CSV infers int64 for
    # unsigned values, whose sum DataFusion never claimed ordered.
    as_written = read_type == arrow_type or (
        read_type == pa.string_view() and arrow_type == pa.string()
    )
    if not as_written:
        pytest.skip(f"this input layout reads x back as {read_type}")
    t = derive_m(t.sort("x", "i", desc=[x_desc, False]), width, agg, partitioned=False)
    rows = [{"i": i, "g": 0, "x": x} for i, x in enumerate(xs)]
    expected = oracle(rows, "x_desc" if x_desc else "x_asc", width, agg, partitioned=False)
    check_sorted(t, expected, name)


def test_wrapped_cum_sum_sorts():
    """An unsigned running sum wraps on overflow, so it is not monotonic."""
    t = (
        LTSeq.from_arrow(pa.table({"i": [0, 1], "x": pa.array([2**64 - 1, 1], pa.uint64())}))
        .sort("i")
        .derive(m=lambda r: r.x.cum_sum())
    )
    assert [r["m"] for r in t.sort("m").to_dicts()] == [0, 2**64 - 1]


def test_table_cum_sum_sorts():
    """`LTSeq.cum_sum` builds its window apart from `derive`."""
    t = (
        LTSeq.from_arrow(pa.table({"x": pa.array([None, 1, 2], pa.uint64())}))
        .sort("x", desc=True)
        .cum_sum("x")
    )
    assert [r["x_cumsum"] for r in t.sort("x_cumsum").to_dicts()] == [2, 3, None]


def test_head_tail_and_writers_read_the_sorted_order(tmp_path):
    t = (
        LTSeq.from_dict({"x": [3, 2, 1, 0]})
        .sort("x", desc=True)
        .derive(m=lambda r: r.x.rolling(2).max())
        .sort("m")
    )
    assert [r["m"] for r in t.head(2).to_dicts()] == [1, 2]
    assert [r["m"] for r in t.tail(2).to_dicts()] == [3, 3]
    path = tmp_path / "sorted.parquet"
    t.write_parquet(str(path))
    assert [r["m"] for r in LTSeq.read_parquet(str(path)).to_dicts()] == [1, 2, 3, 3]
    csv_path = tmp_path / "sorted.csv"
    t.write_csv(str(csv_path))
    assert [r["m"] for r in LTSeq.read_csv(str(csv_path)).to_dicts()] == [1, 2, 3, 3]


def test_plan_keeps_the_sort():
    """The physical plan sorts by the window column above the window."""
    t = (
        LTSeq.from_dict({"x": [3, 2, 1, 0]})
        .sort("x", desc=True)
        .derive(m=lambda r: r.x.rolling(2).max())
    )
    physical = t.sort("m").explain_plan()[1]
    lines = physical.splitlines()
    window = next(i for i, line in enumerate(lines) if "BoundedWindowAggExec" in line)
    assert any(
        "SortExec" in line and "CURRENT ROW@" in line and "ASC NULLS LAST" in line
        for line in lines[:window]
    ), physical
