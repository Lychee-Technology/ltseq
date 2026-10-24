"""An order required after an offset slice is met above the offset (#339).

DataFusion 55 moves an order requirement beneath a limit that skips rows.
With a length, the operator beneath gets the limit's fetch without its
offset, so `sort("date").tail(5).sort("date", "volume")` returned no rows
(B1). Without one, a sort or window in another order ran before the offset
and kept the wrong rows (B2). Where nothing above needed the order, the sort
beneath the offset was dropped, so `len()` and `count()` after `slice(k)` and
a filter counted the wrong rows and `tail()` took them (B3). On a table with
no declared order, every sort after a slice with a length lost rows as in B1.

The tests follow #339's acceptance criteria. Each declared-order test runs at
every engine layout (partition counts 1, 2 and twice the CPU count, batch
sizes 1, 7 and the default) on an in-memory table, a Parquet file of several
row groups and a directory of Parquet files, over the slices of 100 rows at
offsets 1, 3, 50, 99, 100 and 105 with lengths 1, 5, more than the rows left
and none, and `tail` at offsets 99, 95 and 1:

- a sort that extends the declared key, directly, after a `select` that
  keeps it and after `collect()`, with ties and NULLs in the key;
- after a slice without a length, sorts in other orders, and `len()`,
  `count()` and `tail()` after a filter, a `semi_join` and an `anti_join`;
- `search_pattern` and `search_pattern_count`, with one and two steps and
  with `partition_by`;
- windows with `partition_by` and with their own order;
- `group_ordered`'s count fast path (the linear scan), twenty runs for
  three slices with a length and one for the others;
- slices of slices, and a two-key order;
- the shapes #339 lists as unaffected, after both kinds of slice; and the
  plans of `tail(5)` followed by a sort that extends the key, and of
  `slice(10)`;
- (derived) the same after `assume_sorted` on Parquet; a sort or window
  after a slice with a length of a table with no declared order keeps the
  slice's rows.

Results are compared with a pandas reference for their rows, values and
order, and `len()` of each slice and count with the rows a terminal returns.
"""

from __future__ import annotations

import re
from collections.abc import Callable

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from ltseq import LTSeq, rank, row_number

from . import layouts
from .test_declared_order import (
    assert_rows,
    by_date,
    input_rows,
    kept_runs_reference,
    partitioned_cum_sum_reference,
    partitioned_rolling_reference,
    partitioned_shift_reference,
    ranked_reference,
    row_number_reference,
)

N = 100
OFFSETS = (1, 3, N // 2, N - 1, N, N + 5)
# 2 * N is more than the rows left after any offset; None is no length.
LENGTHS = (1, 5, 2 * N, None)
INPUTS = ("many_batches", "parquet_row_groups", "parquet_files")

Shape = Callable[[LTSeq], LTSeq]
Reference = Callable[[pd.DataFrame], pd.DataFrame]


class Slice:
    """A slice of the table and the same slice of its reference."""

    def __init__(self, what: str, cut: Shape, cut_reference: Reference, bounded: bool):
        self.what, self.cut, self.cut_reference, self.bounded = what, cut, cut_reference, bounded


def rows_from(d: pd.DataFrame, k: int, m: int | None) -> pd.DataFrame:
    return d.iloc[k:] if m is None else d.iloc[k : k + m]


def offset_slices() -> list[Slice]:
    """Every offset with every length, and `tail` at offsets 99, 95 and 1."""
    slices = [
        Slice(
            f"slice({k}, {m})",
            lambda t, k=k, m=m: t.slice(k, m),
            lambda d, k=k, m=m: rows_from(d, k, m),
            m is not None,
        )
        for k in OFFSETS
        for m in LENGTHS
    ]
    slices += [
        Slice(f"tail({m})", lambda t, m=m: t.tail(m), lambda d, m=m: d.tail(m), True)
        for m in (1, 5, N - 1)
    ]
    return slices


# Slices of slices: (outer, inner) pairs of (offset, length).
NESTED = (
    ((3, 60), (5, 20)),
    ((10, 50), (45, 10)),
    ((60, 30), (40, 5)),
    ((3, None), (5, None)),
    ((50, None), (49, None)),
    ((20, None), (5, 10)),
    ((5, 50), (3, None)),
)


def nested_slices() -> list[Slice]:
    return [
        Slice(
            f"slice{outer}.slice{inner}",
            lambda t, o=outer, i=inner: t.slice(*o).slice(*i),
            lambda d, o=outer, i=inner: rows_from(rows_from(d, *o), *i),
            inner[1] is not None,
        )
        for outer, inner in NESTED
    ]


def check(result: LTSeq, reference: pd.DataFrame, what: str) -> None:
    assert_rows(result.to_arrow(), reference, what)


def check_slice(s: LTSeq, d: pd.DataFrame, what: str) -> None:
    table = s.to_arrow()
    assert_rows(table, d, what)
    assert len(s) == table.num_rows, (what, "len", len(s), table.num_rows)


def declared(layout: str, tmp_path) -> tuple[LTSeq, pd.DataFrame]:
    rows = input_rows(N)
    return layouts.build(layout, rows, tmp_path / "input").sort("date"), by_date(rows.to_pandas())


# ---------------------------------------------------------------------------
# Shapes after a slice: the LTSeq call and its pandas reference
# ---------------------------------------------------------------------------

# The declared key extended: B1.
EXTENDED: tuple[Shape, Reference] = (
    lambda s: s.sort("date", "volume"),
    lambda d: d.sort_values(["date", "volume"]),
)

# Other orders after a slice without a length: B2.
OTHER_ORDERS: dict[str, tuple[Shape, Reference]] = {
    "sort(volume)": (lambda s: s.sort("volume"), lambda d: d.sort_values("volume")),
    "sort(date, desc)": (
        lambda s: s.sort("date", desc=True),
        lambda d: d.sort_values("date", ascending=False),
    ),
    "sort(volume, date)": (
        lambda s: s.sort("volume", "date"),
        lambda d: d.sort_values(["volume", "date"]),
    ),
    "computed sort": (
        lambda s: s.sort(lambda r: r.volume * -1),
        lambda d: d.sort_values("volume", ascending=False),
    ),
}


def partitioned_row_number_reference(df: pd.DataFrame) -> pd.DataFrame:
    d = by_date(df)
    d["rn"] = d.groupby("g").volume.rank(method="first").astype("int64")
    return d


WINDOWS: dict[str, tuple[Shape, Reference]] = {
    "shift(partition_by)": (
        lambda s: s.derive(prev=lambda r: r.close.shift(1, partition_by="g")),
        partitioned_shift_reference,
    ),
    "rolling(partition_by)": (
        lambda s: s.derive(ma=lambda r: r.close.rolling(3, partition_by="g").mean()),
        partitioned_rolling_reference,
    ),
    "cum_sum(partition_by)": (
        lambda s: s.derive(running=lambda r: r.volume.cum_sum(partition_by="g")),
        partitioned_cum_sum_reference,
    ),
    "rank().over(order_by)": (
        lambda s: s.derive(rk=lambda r: rank().over(order_by=r.close)),
        ranked_reference,
    ),
    "row_number().over(order_by)": (
        lambda s: s.derive(rn=lambda r: row_number().over(order_by=r.volume)),
        row_number_reference,
    ),
    "row_number().over(partition_by, order_by)": (
        lambda s: s.derive(rn=lambda r: row_number().over(partition_by=r.g, order_by=r.volume)),
        partitioned_row_number_reference,
    ),
}

EVEN_DATES = pa.table({"date": np.arange(0, N, 2, dtype=np.int64)})

# Counted after a slice without a length: B3.
COUNTED: dict[str, tuple[Shape, Reference]] = {
    "filter": (lambda s: s.filter(lambda r: r.close > 105), lambda d: d[d.close > 105]),
    "semi_join": (
        lambda s: s.semi_join(LTSeq.from_arrow(EVEN_DATES), on="date"),
        lambda d: d[d.date % 2 == 0],
    ),
    "anti_join": (
        lambda s: s.anti_join(LTSeq.from_arrow(EVEN_DATES), on="date"),
        lambda d: d[d.date % 2 == 1],
    ),
}

# search_pattern's steps are evaluated without type coercion (#188), so the
# float column is compared with a float.
STEP_CLOSE = (lambda r: r.close > 103.0, lambda d: d.close > 103.0)
STEP_VOLUME = (lambda r: r.volume > 400, lambda d: d.volume > 400)
PATTERNS = (
    ("one step", (STEP_CLOSE,), None),
    ("two steps", (STEP_CLOSE, STEP_VOLUME), None),
    ("two steps", (STEP_CLOSE, STEP_VOLUME), "g"),
)


def pattern_reference(d: pd.DataFrame, steps, partition_by: str | None) -> pd.DataFrame:
    """The rows where step 1 matches and each later step matches the row
    after, all in one run of `partition_by`."""
    d = d.reset_index(drop=True)
    hits = [np.asarray(step(d)) for _, step in steps]
    last = len(steps) - 1
    groups = None if partition_by is None else d[partition_by].to_numpy()
    starts = [
        i
        for i in range(len(d) - last)
        if all(hits[j][i + j] for j in range(len(steps)))
        and (groups is None or all(groups[i + j] == groups[i] for j in range(len(steps))))
    ]
    return d.iloc[starts]


def runs_of_g(d: pd.DataFrame) -> int:
    """`group_ordered(g != g.shift(1))`'s groups: the first row's previous
    `g` is NULL, which starts a group too."""
    return int((d.g != d.g.shift(1)).sum())


def check_orders(s: LTSeq, d: pd.DataFrame, cut: Slice, what: str) -> None:
    shape, reference = EXTENDED
    check(shape(s), reference(d), f"{what}.sort(date, volume)")
    if not cut.bounded:
        for name, (shape, reference) in OTHER_ORDERS.items():
            check(shape(s), reference(d), f"{what}.{name}")


def check_counts(s: LTSeq, d: pd.DataFrame, what: str, tails=(1, 5, 2 * N)) -> None:
    for name, (shape, reference) in COUNTED.items():
        kept, expected = shape(s), reference(d)
        assert len(kept) == kept.count() == len(expected), (what, name, len(kept), len(expected))
        for m in tails:
            assert_rows(kept.tail(m).to_arrow(), expected.tail(m), f"{what}.{name}.tail({m})")


def check_patterns(s: LTSeq, d: pd.DataFrame, what: str, patterns=PATTERNS) -> None:
    for name, steps, partition_by in patterns:
        expected = pattern_reference(d, steps, partition_by)
        label = f"{what}.search_pattern({name}, partition_by={partition_by})"
        predicates = [step for step, _ in steps]
        check(s.search_pattern(*predicates, partition_by=partition_by), expected, label)
        counted = s.search_pattern_count(*predicates, partition_by=partition_by)
        assert counted == len(expected), (label, "count", counted, len(expected))


def check_windows(s: LTSeq, d: pd.DataFrame, what: str) -> None:
    for name, (shape, reference) in WINDOWS.items():
        check(shape(s), reference(d), f"{what}.{name}")


def check_group_count(s: LTSeq, d: pd.DataFrame, what: str, runs: int) -> None:
    expected = runs_of_g(d)
    for _ in range(runs):
        counted = s.group_ordered(lambda r: r.g != r.g.shift(1)).first().count()
        assert counted == expected, (what, "group count", counted, expected)


# ---------------------------------------------------------------------------
# A declared order
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("layout", INPUTS)
def test_a_sort_after_an_offset_slice_keeps_its_rows(layout, engine_layout, tmp_path):
    t, ref = declared(layout, tmp_path)
    for cut in offset_slices():
        s, d = cut.cut(t), cut.cut_reference(ref)
        check_slice(s, d, cut.what)
        check_orders(s, d, cut, cut.what)


@pytest.mark.parametrize("layout", INPUTS)
def test_a_count_after_an_offset_slice_counts_its_rows(layout, engine_layout, tmp_path):
    t, ref = declared(layout, tmp_path)
    for cut in offset_slices():
        if not cut.bounded:
            check_counts(cut.cut(t), cut.cut_reference(ref), cut.what)


@pytest.mark.parametrize("layout", INPUTS)
def test_search_pattern_after_an_offset_slice_matches_its_rows(layout, engine_layout, tmp_path):
    t, ref = declared(layout, tmp_path)
    for cut in offset_slices():
        check_patterns(cut.cut(t), cut.cut_reference(ref), cut.what)


@pytest.mark.parametrize("layout", INPUTS)
def test_a_window_after_an_offset_slice_computes_over_its_rows(layout, engine_layout, tmp_path):
    t, ref = declared(layout, tmp_path)
    for cut in offset_slices():
        check_windows(cut.cut(t), cut.cut_reference(ref), cut.what)


# The slices whose group count runs twenty times.
REPEATED = ("tail(5)", "slice(3, 5)", "slice(50, 200)")


@pytest.mark.parametrize("layout", INPUTS)
def test_the_group_count_after_an_offset_slice_counts_its_runs(layout, engine_layout, tmp_path):
    """The linear scan's count after `tail` lost rows in 20-60% of runs, so
    three slices with a length count twenty times and the others once."""
    t, ref = declared(layout, tmp_path)
    for cut in offset_slices():
        runs = 20 if cut.what in REPEATED else 1
        check_group_count(cut.cut(t), cut.cut_reference(ref), cut.what, runs)


@pytest.mark.parametrize("layout", INPUTS)
def test_a_slice_of_a_slice_keeps_its_rows(layout, engine_layout, tmp_path):
    t, ref = declared(layout, tmp_path)
    for cut in nested_slices():
        s, d = cut.cut(t), cut.cut_reference(ref)
        check_slice(s, d, cut.what)
        check_orders(s, d, cut, cut.what)
        if not cut.bounded:
            check_counts(s, d, cut.what, tails=(5,))
        check_patterns(s, d, cut.what, patterns=PATTERNS[2:])
        check_windows(s, d, cut.what)
        check_group_count(s, d, cut.what, runs=1)


@pytest.mark.parametrize("layout", INPUTS)
def test_an_offset_slice_of_a_two_key_order(layout, engine_layout, tmp_path):
    rows = input_rows(N)
    t = layouts.build(layout, rows, tmp_path / "input").sort("g", "date")
    ref = rows.to_pandas().sort_values(["g", "date"]).reset_index(drop=True)
    for cut in offset_slices():
        s, d = cut.cut(t), cut.cut_reference(ref)
        what = cut.what
        check_slice(s, d, what)
        check(s.sort("g", "date", "volume"), d.sort_values(["g", "date", "volume"]), f"{what}.extended")
        check(
            s.derive(rn=lambda r: row_number().over(order_by=r.volume)),
            d.assign(rn=d.volume.rank(method="first").astype("int64")),
            f"{what}.row_number",
        )
        if not cut.bounded:
            check(s.sort("volume"), d.sort_values("volume"), f"{what}.sort(volume)")
            filtered = s.filter(lambda r: r.close > 105)
            assert len(filtered) == filtered.count() == int((d.close > 105).sum()), what


QUOTE_TIMES = np.arange(0, 3 * N, 3, dtype=np.int64)

# The shapes #339 lists as unaffected, which the change must keep that way:
# (call, reference, whether the result has an order to compare). A re-sort
# in another order is unaffected only after a slice with a length; after one
# without, it is B2 (`OTHER_ORDERS`).
RESORTS = {"sort(volume)": OTHER_ORDERS["sort(volume)"], "computed sort": OTHER_ORDERS["computed sort"]}
UNAFFECTED: dict[str, tuple[Shape, Reference, bool]] = {
    "filter().sort()": (
        lambda s: s.filter(lambda r: r.close > 105).sort("date", "volume"),
        lambda d: d[d.close > 105],
        True,
    ),
    "derive().sort()": (
        lambda s: s.derive(z=lambda r: r.close * 2).sort("date", "volume"),
        lambda d: d.assign(z=d.close * 2),
        True,
    ),
    "shift": (
        lambda s: s.derive(prev=lambda r: r.close.shift(1)),
        lambda d: d.assign(prev=d.close.shift(1)),
        True,
    ),
    "cum_sum": (lambda s: s.cum_sum("volume"), lambda d: d.assign(volume_cumsum=d.volume.cumsum()), True),
    "kept runs": (
        lambda s: s.group_ordered(lambda r: r.g).filter(lambda g: g.count() > 1).flatten(),
        kept_runs_reference,
        True,
    ),
    "rvs": (lambda s: s.rvs(), lambda d: d.iloc[::-1], True),
    "step(3)": (lambda s: s.step(3), lambda d: d.iloc[::3], True),
    "fold": (
        lambda s: s.fold(lambda state, row: state + 1, init=0, into="n"),
        lambda d: d.assign(n=np.arange(1, len(d) + 1)),
        True,
    ),
    "head(2)": (lambda s: s.head(2), lambda d: d.head(2), True),
    "tail(2)": (lambda s: s.tail(2), lambda d: d.tail(2), True),
    "slice(1, 2)": (lambda s: s.slice(1, 2), lambda d: d.iloc[1:3], True),
    # These declare no order.
    "distinct(g)": (lambda s: s.distinct("g"), lambda d: d.drop_duplicates("g"), False),
    "delete(0)": (lambda s: s.delete(0), lambda d: d.iloc[1:], False),
    "group_by(g).agg": (
        lambda s: s.group_by("g").agg(total=lambda r: r.volume.sum()),
        lambda d: d.groupby("g", as_index=False).agg(total=("volume", "sum")),
        False,
    ),
}


@pytest.mark.parametrize("layout", INPUTS)
def test_the_shapes_an_offset_slice_never_broke_still_work(layout, engine_layout, tmp_path):
    t, ref = declared(layout, tmp_path)
    quotes = LTSeq.from_arrow(pa.table({"time": QUOTE_TIMES, "px": QUOTE_TIMES / 2})).sort("time")
    cuts = [c for c in offset_slices() if c.what in ("tail(5)", "slice(3, 5)", "slice(99, 5)")]
    cuts += [c for c in offset_slices() if c.what in ("slice(1, None)", "slice(50, None)", "slice(99, None)")]
    for cut in cuts:
        s, d = cut.cut(t), cut.cut_reference(ref)
        if cut.bounded:
            for name, (shape, reference) in RESORTS.items():
                check(shape(s), reference(d), f"{cut.what}.{name}")
        for name, (shape, reference, ordered) in UNAFFECTED.items():
            what = f"{cut.what}.{name}"
            result, expected = shape(s).to_pandas(), reference(d)
            if not ordered:
                key = list(expected.columns)[0]
                result, expected = result.sort_values(key), expected.sort_values(key)
            assert_rows(pa.Table.from_pandas(result, preserve_index=False), expected, what)
        first = s.search_first(lambda r: r.close > 105)
        matches = d[d.close > 105].head(1)
        if first is None:
            assert matches.empty, cut.what
        else:
            check(first, matches, f"{cut.what}.search_first")
        joined = s.asof_join(quotes, left_on="date", right_on="time").to_pandas().sort_values("date")
        assert joined.date.tolist() == d.date.tolist(), cut.what
        assert joined.time.tolist() == [v - v % 3 for v in d.date], cut.what


def test_a_sort_after_tail_is_planned_above_the_offset():
    """The plans #339 asks the PR to record: a sort that extends the declared
    key after `tail(5)` is a top-5 above the offset limit, and a slice without
    a length numbers the rows after a full sort, not a top-k heap."""
    t = LTSeq.from_arrow(input_rows(N)).sort("date")
    _, physical = t.tail(5).sort("date", "volume").explain_plan()
    lines = physical.splitlines()
    limit = next(i for i, line in enumerate(lines) if "GlobalLimitExec: skip=95, fetch=5" in line)
    sort = next(i for i, line in enumerate(lines) if re.search(r"SortExec: TopK\(fetch=5\), expr=\[date@\d+ ASC NULLS LAST, volume", line))
    assert sort < limit, physical

    _, physical = t.slice(10).explain_plan()
    assert "GlobalLimitExec" not in physical, physical
    assert "BoundedWindowAggExec" in physical, physical
    assert "TopK" not in physical, physical


# ---------------------------------------------------------------------------
# Ties and NULLs in the declared key
# ---------------------------------------------------------------------------


def tied_rows(n: int) -> pa.Table:
    """`a` holds runs of three equal values and two NULLs; `b` is unique, and
    so is `id`, which names each row."""
    ids = np.arange(n, dtype=np.int64)
    a = pa.array([None if i >= n - 2 else int(i // 3) for i in range(n)], pa.int64())
    b = (ids * 7919) % (n * 11)
    table = pa.table({"id": ids, "a": a, "b": b})
    return table.take(np.random.default_rng(339).permutation(n))


def assert_slice_of_ties(
    result: pa.Table, rows: pd.DataFrame, expected: pd.DataFrame, desc: bool, what: str
) -> None:
    """`result` is `expected` up to which rows of a run of equal `a` a slice
    boundary keeps, which the declared order leaves open.

    Its rows are input rows, each once; its `a` values are `expected`'s in
    order; and it is sorted by `a` (NULLs last ascending, first descending)
    and then by `b`.
    """
    got = result.to_pandas()
    assert len(got) == len(expected), (what, len(got), len(expected))
    assert got.id.is_unique, what
    original = rows.set_index("id").loc[got.id]
    assert original.a.fillna(-1).tolist() == got.a.fillna(-1).tolist(), what
    assert original.b.tolist() == got.b.tolist(), what
    assert got.a.fillna(-1).tolist() == expected.a.fillna(-1).tolist(), what
    ordered = got.sort_values(
        ["a", "b"], ascending=[not desc, True], na_position="first" if desc else "last"
    )
    assert got.id.tolist() == ordered.id.tolist(), what


@pytest.mark.parametrize("desc", [False, True], ids=["asc", "desc"])
@pytest.mark.parametrize("layout", INPUTS)
def test_an_order_that_extends_the_key_after_an_offset_slice_of_ties_and_nulls(
    layout, desc, engine_layout, tmp_path
):
    """A sort by `(a, b)` and a window partitioned by `a` and ordered by `b`
    after a slice of `sort("a")`: both orders extend the slice's, the B1
    shape (#339's `sort("g").tail(10).derive(rk=...)`)."""
    rows = tied_rows(N)
    t = layouts.build(layout, rows, tmp_path / "input").sort("a", desc=desc)
    frame = rows.to_pandas()
    nulls = "first" if desc else "last"
    ref = frame.sort_values("a", ascending=not desc, na_position=nulls, kind="stable")
    shapes = {
        "sort(a, b)": lambda s: s.sort("a", "b", desc=[desc, False]),
        "select(keys).sort(a, b)": lambda s: s.select("id", "a", "b").sort("a", "b", desc=[desc, False]),
        "collect().sort(a, b)": lambda s: s.collect().sort("a", "b", desc=[desc, False]),
    }
    for cut in offset_slices():
        s = cut.cut(t)
        expected = cut.cut_reference(ref).sort_values(
            ["a", "b"], ascending=[not desc, True], na_position=nulls
        )
        assert len(s) == len(expected), (cut.what, "len")
        for name, shape in shapes.items():
            assert_slice_of_ties(shape(s).to_arrow(), frame, expected, desc, f"{cut.what}.{name}")

        what = f"{cut.what}.rank().over(partition_by=a, order_by=b)"
        ranked = s.derive(rk=lambda r: rank().over(partition_by=r.a, order_by=r.b)).to_pandas()
        assert len(ranked) == len(expected) and ranked.id.is_unique, what
        assert sorted(ranked.a.fillna(-1)) == sorted(expected.a.fillna(-1)), what
        # Ranked within the rows the slice kept: NULL keys are one partition.
        own = ranked.groupby(ranked.a.fillna(-1)).b.rank(method="min").astype("int64")
        assert ranked.rk.tolist() == own.tolist(), what


# ---------------------------------------------------------------------------
# (derived) An order declared with assume_sorted
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("files,row_groups", [(1, 10), (4, 3)], ids=["row_groups", "files"])
def test_an_offset_slice_after_assume_sorted(files, row_groups, engine_layout, tmp_path):
    ref = by_date(input_rows(N).to_pandas())
    rows = pa.Table.from_pandas(ref, preserve_index=False)
    path = layouts.write_parquet(rows, tmp_path / "input", files=files, row_groups=row_groups)
    t = LTSeq.read_parquet(str(path)).assume_sorted("date")
    shift, shift_reference = WINDOWS["shift(partition_by)"]
    for cut in offset_slices():
        s, d = cut.cut(t), cut.cut_reference(ref)
        check_slice(s, d, cut.what)
        check_orders(s, d, cut, cut.what)
        check(shift(s), shift_reference(d), f"{cut.what}.shift(partition_by)")
        if not cut.bounded:
            filtered = s.filter(lambda r: r.close > 105)
            assert len(filtered) == filtered.count() == int((d.close > 105).sum()), cut.what


# ---------------------------------------------------------------------------
# (derived) No declared order
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("layout", ["one_batch", "many_batches"])
def test_a_sort_after_an_offset_slice_of_an_unsorted_table_keeps_its_rows(
    layout, engine_layout, tmp_path
):
    """A table with no declared order has no order its slice must follow,
    but a sort or a window after a slice with a length must keep the slice's
    rows: as many as it has, and values computed over them.

    Read from several partitions, such a table returns its rows in the order
    the partitions deliver them, so a slice of it can keep other rows on the
    next read: each result is checked against its own rows. One batch is
    read in order, so there each result also keeps the rows the slice reads.

    Without a length a later sort still runs before the offset, so
    `slice(95).sort("volume")` keeps the 5 rows of highest volume. Which
    rows a slice of such a table skips is M1's to define (#148).
    """
    rows = input_rows(N)
    frame = rows.to_pandas()
    t = layouts.build(layout, rows, tmp_path / "input")
    shapes = {
        "sort(date)": (lambda s: s.sort("date"), lambda d: d.sort_values("date")),
        "sort(volume, date)": (
            lambda s: s.sort("volume", "date"),
            lambda d: d.sort_values(["volume", "date"]),
        ),
        "rank().over(order_by)": (
            lambda s: s.derive(rk=lambda r: rank().over(order_by=r.close)).sort("date"),
            lambda d: d.assign(rk=d.close.rank(method="min").astype("int64")).sort_values("date"),
        ),
        "row_number().over(partition_by, order_by)": (
            lambda s: s.derive(
                rn=lambda r: row_number().over(partition_by=r.g, order_by=r.volume)
            ).sort("date"),
            lambda d: d.assign(
                rn=d.groupby("g").volume.rank(method="first").astype("int64")
            ).sort_values("date"),
        ),
    }
    for cut in offset_slices():
        if not cut.bounded:
            continue
        s = cut.cut(t)
        count = len(cut.cut_reference(frame))
        assert len(s) == count, (cut.what, "len")
        read = sorted(s.to_pandas().date) if layout == "one_batch" else None
        for name, (shape, reference) in shapes.items():
            what = f"{cut.what}.{name}"
            result = shape(s).to_arrow()
            kept = result.select(list(frame.columns)).to_pandas()
            assert len(kept) == count and kept.date.is_unique, (what, len(kept), count)
            assert_rows(result, reference(kept), what)
            if read is not None:
                assert sorted(kept.date) == read, what
