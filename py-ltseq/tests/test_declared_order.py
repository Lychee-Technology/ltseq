"""A table with a declared order returns its rows in that order from every
consumer (#311, #148).

DataFusion keeps a sort's order only up to the next operator it chooses to
repartition, so a filter, a computed column or a window after `sort` used to
return rows in partition arrival order while `sort_keys` still reported the
sort. The table's declared order is now required at the root of every plan
that reads rows in table order.

The tests follow #321's ordering acceptance, items 1 and 3, and #148's 0.3.0
slice:

- seven pipelines, each read through every consumer and compared row for row
  with a pandas reference: at 100 rows under every engine layout (partition
  counts 1, 2 and twice the CPU count, batch sizes 1, 7 and the default) and
  every input layout, and at 40,000 rows at 2 and 2×CPU partitions;
- the same consumers after `assume_sorted`, on every input layout, with a
  transform before or after the declaration, at 100 and 100,000 rows;
- the operations that count rows in table order (`rvs`, `step`, `distinct`,
  the positional mutations, `asof_join`'s search, an offset `slice`);
- a projection that drops or replaces a sort key;
- seeded chains of transforms after a `sort` or an `assume_sorted`, after
  which every terminal's rows are sorted by `sort_keys`.

The input rows of a `sort` arrive shuffled, so every order in an output comes
from the pipeline's `sort`. The rows of an `assume_sorted` arrive in the order
it declares, as its contract requires.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq, rank

from . import layouts
from .layouts import CPUS, LAYOUTS

# Consecutive runs of `g` in date order: lengths cycle through these, labels
# through LABELS, so neighbouring runs always differ and some runs are a
# single row.
RUN_LENGTHS = (1, 3, 2, 1, 4)
LABELS = (1, 2, 3)


def input_rows(n: int) -> pa.Table:
    """`n` rows in a shuffled order.

    `date` and `volume` are unique, so every sort and every probe below has
    one answer. `close` repeats every 41 dates, which gives `rank` ties.
    There is no string column: read back from Parquet in small batches,
    one makes `group_ordered().filter()` take minutes on 100 rows, a
    slowdown unrelated to order (#338).
    """
    date = np.arange(n, dtype=np.int64)
    g: list[int] = []
    run = 0
    while len(g) < n:
        g.extend([LABELS[run % len(LABELS)]] * RUN_LENGTHS[run % len(RUN_LENGTHS)])
        run += 1
    table = pa.table(
        {
            "date": date,
            "close": 100.0 + (date * 37 % 41) / 4 + 0.125,
            "volume": (date * 7919) % (n * 11) + 1,
            "g": g[:n],
        }
    )
    return table.take(np.random.default_rng(311).permutation(n))


def by_date(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values("date", kind="stable").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Pipelines: the LTSeq version and its pandas reference
# ---------------------------------------------------------------------------


def quickstart(t: LTSeq) -> LTSeq:
    """The README Quickstart, verbatim apart from its CSV path."""
    t = t.sort(lambda r: r.date)
    result = t.derive(
        lambda r: {
            "prev_close": r.close.shift(1),
            "price_change": r.close - r.close.shift(1),
            "ma_5": r.close.rolling(5).mean(),
        }
    )
    result = result.cum_sum("volume")
    return result.filter(lambda r: r.price_change > 0).select(
        lambda r: [r.date, r.close, r.price_change, r.ma_5]
    )


def quickstart_reference(df: pd.DataFrame) -> pd.DataFrame:
    d = by_date(df)
    d["price_change"] = d.close - d.close.shift(1)
    # LTSeq's rolling mean averages the rows the window has so far.
    d["ma_5"] = d.close.rolling(5, min_periods=1).mean()
    return d[d.price_change > 0][["date", "close", "price_change", "ma_5"]]


def computed_derive_reference(df: pd.DataFrame) -> pd.DataFrame:
    d = by_date(df)
    d["z"] = d.close * 2 + d.volume
    return d


def dropped_key_reference(df: pd.DataFrame) -> pd.DataFrame:
    d = by_date(df)
    return pd.DataFrame({"volume": d.volume, "close2": d.close * 2})


def sliced_select_reference(df: pd.DataFrame) -> pd.DataFrame:
    return by_date(df).tail(70)[["date", "close", "volume"]].reset_index(drop=True)


def partitioned_shift_reference(df: pd.DataFrame) -> pd.DataFrame:
    d = by_date(df)
    d["prev"] = d.groupby("g").close.shift(1)
    return d


def ranked_reference(df: pd.DataFrame) -> pd.DataFrame:
    d = by_date(df)
    d["rk"] = d.close.rank(method="min").astype("int64")
    return d


def kept_runs_reference(df: pd.DataFrame) -> pd.DataFrame:
    """Runs of `g` longer than one row, renumbered as `flatten` does."""

    def numbered(d: pd.DataFrame) -> pd.DataFrame:
        run = (d.g != d.g.shift(1)).cumsum().astype("int64")
        d = d.assign(__group_id__=run)
        size = d.groupby("__group_id__").g.transform("size").astype("int64")
        position = d.groupby("__group_id__").cumcount().astype("int64") + 1
        return d.assign(__group_count__=size, __rn__=position)

    d = numbered(by_date(df))
    d = d[d.__group_count__ > 1][["date", "close", "volume", "g"]].reset_index(drop=True)
    return numbered(d)


@dataclass(frozen=True)
class Pipeline:
    build: Callable[[LTSeq], LTSeq]
    reference: Callable[[pd.DataFrame], pd.DataFrame]
    # search_first's probe: the first row in table order where `column` is
    # above the median of its reference values.
    probe: str


PIPELINES = {
    "quickstart": Pipeline(quickstart, quickstart_reference, "close"),
    "computed_derive": Pipeline(
        lambda t: t.sort("date").derive(z=lambda r: r.close * 2 + r.volume),
        computed_derive_reference,
        "volume",
    ),
    # `sort_keys` is None after the select, yet the select's rows keep the
    # order of the sort before it.
    "dropped_key": Pipeline(
        lambda t: t.sort("date").select(lambda r: [r.volume, r.close * 2]),
        dropped_key_reference,
        "volume",
    ),
    # The select keeps the key, so it adds no sort of its own above the
    # tail's offset limit, and DataFusion drops its projection as a no-op.
    "sliced_select": Pipeline(
        lambda t: t.sort("date").tail(70).select("date", "close", "volume"),
        sliced_select_reference,
        "volume",
    ),
    "partitioned_shift": Pipeline(
        lambda t: t.sort("date").derive(prev=lambda r: r.close.shift(1, partition_by="g")),
        partitioned_shift_reference,
        "volume",
    ),
    "ranked": Pipeline(
        lambda t: t.sort("date").derive(rk=lambda r: rank().over(order_by=r.close)),
        ranked_reference,
        "volume",
    ),
    "kept_runs": Pipeline(
        lambda t: t.sort("date")
        .group_ordered(lambda r: r.g)
        .filter(lambda g: g.count() > 1)
        .flatten(),
        kept_runs_reference,
        "volume",
    ),
}


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


def assert_rows(actual: pa.Table, expected: pd.DataFrame, what: str) -> None:
    """`actual` holds `expected`'s rows in `expected`'s order.

    Columns are compared by position: a computed column's name is LTSeq's.
    Floats compare to 1e-12 relative, NULL and NaN alike.
    """
    assert actual.num_columns == expected.shape[1], (what, actual.schema, expected.dtypes)
    assert actual.num_rows == len(expected), (what, actual.num_rows, len(expected))
    for index in range(actual.num_columns):
        got = actual.column(index).to_pandas().to_numpy()
        want = expected.iloc[:, index].to_numpy()
        if want.dtype.kind == "f" or got.dtype.kind == "f":
            np.testing.assert_allclose(
                got.astype(float), want.astype(float), rtol=1e-12, equal_nan=True, err_msg=what
            )
        else:
            assert list(got) == list(want), (what, actual.column_names[index])


def shown_first_column(output: str) -> list[str]:
    """The first cell of every data row `show()` printed."""
    rows = [line for line in output.splitlines() if line.startswith("|")]
    return [row.strip("|").split("|")[0].strip() for row in rows[1:]]


def write_and_read_parquet(t: LTSeq, directory: Path) -> pa.Table:
    path = directory / "out.parquet"
    t.write_parquet(str(path))
    return pq.read_table(path)


def write_and_read_csv(t: LTSeq, directory: Path) -> pa.Table:
    path = directory / "out.csv"
    t.write_csv(str(path))
    return pacsv.read_csv(path)


def check_consumers(t: LTSeq, pipeline: Pipeline, ref: pd.DataFrame, tmp_path: Path, capsys) -> None:
    """Every consumer of #321's ordering acceptance against the reference."""
    assert_rows(t.to_arrow(), ref, "to_arrow")
    assert_rows(pa.Table.from_pandas(t.to_pandas(), preserve_index=False), ref, "to_pandas")
    assert_rows(pa.Table.from_pylist(t.to_dicts()), ref, "to_dicts")
    assert_rows(pa.table(t), ref, "pa.table")
    assert_rows(t.head(5).to_arrow(), ref.head(5), "head")
    assert_rows(t.tail(5).to_arrow(), ref.tail(5), "tail")
    assert_rows(t.slice(3, 4).to_arrow(), ref.iloc[3:7], "slice")
    assert_rows(t.collect().to_arrow(), ref, "collect")
    assert_rows(write_and_read_parquet(t, tmp_path), ref, "write_parquet")
    assert_rows(write_and_read_csv(t, tmp_path), ref, "write_csv")

    threshold = ref[pipeline.probe].median()
    first = t.search_first(lambda r: getattr(r, pipeline.probe) > threshold)
    assert_rows(first.to_arrow(), ref[ref[pipeline.probe] > threshold].head(1), "search_first")

    capsys.readouterr()
    t.show(7)
    shown = shown_first_column(capsys.readouterr().out)
    assert shown == [str(value) for value in ref.iloc[:7, 0]], ("show", shown)

    if t.sort_keys is not None:
        counted = t.fold(lambda state, row: state + 1, init=0, into="n").to_arrow()
        assert_rows(counted, ref.assign(n=np.arange(1, len(ref) + 1)), "fold")


@pytest.mark.parametrize("name", list(PIPELINES))
def test_every_consumer_reads_rows_in_order(name, engine_layout, data_layout, tmp_path, capsys):
    pipeline = PIPELINES[name]
    rows = input_rows(100)
    t = pipeline.build(data_layout(rows))
    check_consumers(t, pipeline, pipeline.reference(rows.to_pandas()), tmp_path, capsys)


@pytest.mark.parametrize("partitions", [2, 2 * CPUS], ids=["p2", "p2xcpu"])
@pytest.mark.parametrize("name", list(PIPELINES))
def test_every_consumer_reads_rows_in_order_at_scale(
    name, partitions, engine_config, data_layout, tmp_path, capsys
):
    """40,000 rows at the default batch size: several batches per partition."""
    engine_config(target_partitions=partitions)
    pipeline = PIPELINES[name]
    rows = input_rows(40_000)
    t = pipeline.build(data_layout(rows))
    check_consumers(t, pipeline, pipeline.reference(rows.to_pandas()), tmp_path, capsys)


# ---------------------------------------------------------------------------
# Order declared with assume_sorted (#148)
# ---------------------------------------------------------------------------


def in_date_order(n: int) -> pa.Table:
    """`input_rows(n)` in the order `assume_sorted("date")` declares."""
    return input_rows(n).sort_by("date")


def filtered_reference(df: pd.DataFrame) -> pd.DataFrame:
    return by_date(df[df.volume % 3 != 0])


def halves_then_declared(t: LTSeq) -> LTSeq:
    """The earlier and the later half of the rows, concatenated by `union`."""
    half = len(t) // 2
    earlier = t.filter(lambda r: r.date < half)
    later = t.filter(lambda r: r.date >= half)
    return earlier.union(later).assume_sorted("date")


# On Parquet, `assume_sorted` reads the files again with their order declared.
# On any other input, or after a transform, it collects the rows and declares
# the order on them, so the rows must be collected in the input's order.
ASSUMED = {
    "declared": Pipeline(lambda t: t.assume_sorted("date"), by_date, "volume"),
    "declared_then_computed": Pipeline(
        lambda t: t.assume_sorted("date").derive(z=lambda r: r.close * 2 + r.volume),
        computed_derive_reference,
        "volume",
    ),
    "declared_then_partitioned_shift": Pipeline(
        lambda t: t.assume_sorted("date").derive(
            prev=lambda r: r.close.shift(1, partition_by="g")
        ),
        partitioned_shift_reference,
        "volume",
    ),
    "filtered_then_declared": Pipeline(
        lambda t: t.filter(lambda r: r.volume % 3 != 0).assume_sorted("date"),
        filtered_reference,
        "volume",
    ),
    "computed_then_declared": Pipeline(
        lambda t: t.derive(z=lambda r: r.close * 2 + r.volume).assume_sorted("date"),
        computed_derive_reference,
        "volume",
    ),
    "halves_then_declared": Pipeline(halves_then_declared, by_date, "volume"),
}


@pytest.mark.parametrize("name", list(ASSUMED))
def test_every_consumer_reads_the_assumed_order(name, engine_layout, data_layout, tmp_path, capsys):
    pipeline = ASSUMED[name]
    rows = in_date_order(100)
    t = pipeline.build(data_layout(rows))
    check_consumers(t, pipeline, pipeline.reference(rows.to_pandas()), tmp_path, capsys)


@pytest.mark.parametrize("partitions", [2, 2 * CPUS], ids=["p2", "p2xcpu"])
@pytest.mark.parametrize("name", ["declared", "filtered_then_declared"])
def test_every_consumer_reads_the_assumed_order_at_scale(
    name, partitions, engine_config, data_layout, tmp_path, capsys
):
    """100,000 rows at the default batch size. Each Parquet layout then holds
    more than 1 MiB, from which DataFusion may split its files into byte
    ranges that it reads in parallel."""
    engine_config(target_partitions=partitions)
    pipeline = ASSUMED[name]
    rows = in_date_order(100_000)
    t = pipeline.build(data_layout(rows))
    check_consumers(t, pipeline, pipeline.reference(rows.to_pandas()), tmp_path, capsys)


# ---------------------------------------------------------------------------
# Operations that count rows in table order
# ---------------------------------------------------------------------------


def dates(t: LTSeq) -> list[int]:
    return t.to_arrow().column("date").to_pylist()


def sorted_then_computed() -> LTSeq:
    """A sorted table whose plan repartitions above the sort."""
    return LTSeq.from_arrow(input_rows(100)).sort("date").derive(z=lambda r: r.close * 2)


def test_reverse_and_step_count_positions_in_order(engine_layout):
    t = sorted_then_computed()
    assert dates(t.rvs()) == list(range(99, -1, -1))
    assert dates(t.step(7)) == list(range(0, 100, 7))


def test_distinct_keeps_the_first_row_in_order(engine_layout):
    reference = by_date(input_rows(100).to_pandas())
    first_of_each = reference.drop_duplicates("g").date.tolist()
    # distinct's own result has no declared order.
    assert sorted(dates(sorted_then_computed().distinct("g"))) == sorted(first_of_each)


def test_positional_mutations_count_in_order(engine_layout):
    """A mutation's result declares no order, so these check which row the
    position named, not where the result puts it."""
    t = sorted_then_computed()
    assert set(range(100)) - set(dates(t.delete(3))) == {3}
    modified = t.modify(5, close=0.0).to_pandas().set_index("date")
    assert modified.close[5] == 0.0
    assert (modified.drop(index=5).close != 0.0).all()


def test_asof_join_searches_a_side_sorted_before_a_derive(engine_layout):
    times = np.arange(0, 300, 3, dtype=np.int64)
    quotes = pa.table({"time": times, "px": times / 2}).take(
        np.random.default_rng(1).permutation(len(times))
    )
    right = LTSeq.from_arrow(quotes).sort("time").derive(bid=lambda r: r.px - 1)
    left = LTSeq.from_arrow(input_rows(100)).sort("date")
    joined = left.asof_join(right, left_on="date", right_on="time").to_pandas()
    joined = joined.sort_values("date")
    assert joined.time.tolist() == [d - d % 3 for d in range(100)]
    assert joined.bid.tolist() == [(d - d % 3) / 2 - 1 for d in range(100)]


def test_sort_after_an_offset_slice_keeps_the_slice(engine_layout):
    """A slice keeps its sort under its limit. Sorting it again by the same
    key must not stack a second sort on the limit: with a window below,
    DataFusion 55 then plans the limit short by its offset."""
    t = LTSeq.from_arrow(input_rows(100)).sort("date").derive(
        rk=lambda r: rank().over(order_by=r.close)
    )
    assert dates(t.tail(5).sort("date")) == [95, 96, 97, 98, 99]
    assert dates(t.slice(3, 4).sort("date")) == [3, 4, 5, 6]
    assert dates(t.slice(3, 4).slice(1, 2)) == [4, 5]


def test_a_projection_after_an_offset_slice_keeps_the_slice(engine_layout):
    """A select that only picks columns leaves a projection between the
    slice's limit and the root sort. DataFusion drops it as a no-op, so a
    root sort there stacks on the limit, which DataFusion 55 then plans
    short by its offset even with no window below."""
    t = LTSeq.from_arrow(input_rows(100)).sort("date")
    assert dates(t.tail(5).select("date", "close")) == [95, 96, 97, 98, 99]
    assert dates(t.tail(70).select("date", "volume")) == list(range(30, 100))
    assert dates(t.slice(10, 5).select("date", "close")) == list(range(10, 15))
    assert dates(t.slice(50, 50).select("date").select("date")) == list(range(50, 100))
    assert dates(t.tail(5).select("date", "close").sort("date")) == [95, 96, 97, 98, 99]
    renamed = t.slice(10, 5).select("date", "close").rename(date="day")
    assert renamed.sort_keys == [("day", False)]
    assert renamed.to_arrow().column("day").to_pylist() == list(range(10, 15))
    assert renamed.collect().to_arrow().column("day").to_pylist() == list(range(10, 15))

    two_keys = LTSeq.from_arrow(input_rows(100)).sort("g", "date")
    expected = by_date(input_rows(100).to_pandas()).sort_values("g", kind="stable")
    sliced = two_keys.slice(20, 30).select("g", "date", "close")
    assert dates(sliced) == expected.date.tolist()[20:50]
    # Dropping the second key keeps the order through hidden copies of both.
    assert sliced.select("g", "close").to_arrow().column("close").to_pylist() == (
        expected.close.tolist()[20:50]
    )


# ---------------------------------------------------------------------------
# Projections that drop or replace a sort key
# ---------------------------------------------------------------------------


def test_dropping_a_later_key_keeps_the_full_order(engine_layout):
    """`sort_keys` keeps the keys before the dropped one; the rows of the
    projection itself keep every key's order, ties on `g` included."""
    rows = input_rows(100)
    t = LTSeq.from_arrow(rows).sort("g", "date").select(lambda r: [r.g, r.volume, r.close * 2])
    assert t.sort_keys == [("g", False)]
    expected = rows.to_pandas().sort_values(["g", "date"], kind="stable")
    assert t.to_arrow().column("volume").to_pylist() == expected.volume.tolist()


def test_replacing_a_key_keeps_the_input_order(engine_layout):
    t = LTSeq.from_arrow(input_rows(100)).sort("date")
    negated = t.derive(date=lambda r: 0 - r.date, z=lambda r: r.close * 2)
    assert negated.sort_keys is None
    assert dates(negated) == [-d for d in range(100)]
    shifted = t.derive(date=lambda r: r.date.shift(1))
    assert shifted.sort_keys is None
    assert dates(shifted) == [None, *range(99)]


def test_a_computed_column_named_like_a_key_does_not_keep_the_key(engine_layout):
    """A cast keeps its input's name. Its values sort differently, so it is
    not the key, and its rows keep the input's order."""
    t = LTSeq.from_arrow(input_rows(100)).sort("date").select(lambda r: [r.date.cast("string")])
    assert t.sort_keys is None
    assert t.to_arrow().column(0).to_pylist() == [str(d) for d in range(100)]


# ---------------------------------------------------------------------------
# Property: whenever sort_keys is not None, every terminal is sorted by it
# ---------------------------------------------------------------------------

# Orders a chain starts from: unique and tied keys, both directions.
ORDERS = (
    (("date",), False),
    (("g", "date"), False),
    (("g", "close"), [False, True]),
    (("close",), True),
    (("volume",), False),
)


def _derived(name: str) -> Callable[[LTSeq, int], LTSeq]:
    """A step that adds one column, named after its position in the chain."""
    exprs = {
        "computed": lambda r: r.close * 2 + r.volume,
        "shift": lambda r: r.close.shift(1),
        "partitioned_shift": lambda r: r.close.shift(1, partition_by="g"),
        "rolling": lambda r: r.close.rolling(3).mean(),
        "rank": lambda r: rank().over(order_by=r.volume),
    }
    return lambda t, i: t.derive(**{f"{name}_{i}": exprs[name]})


def _in_order(step: Callable[[LTSeq, int], LTSeq]) -> Callable[[LTSeq, int], LTSeq]:
    """A step that needs declared keys, skipped once an earlier step cleared them."""
    return lambda t, i: step(t, i) if t.sort_keys else t


def _reads(*columns: str, step: Callable[[LTSeq, int], LTSeq]) -> Callable[[LTSeq, int], LTSeq]:
    """A step skipped once an earlier step dropped or renamed a column it reads."""
    return lambda t, i: step(t, i) if set(columns) <= set(t.columns) else t


def _drop_last_key(t: LTSeq, i: int) -> LTSeq:
    keys = [key for key, _ in t.sort_keys or []]
    if len(keys) < 2:
        return t
    return t.select(*[c for c in t.columns if c != keys[-1]])


def _drop_a_non_key(t: LTSeq, i: int) -> LTSeq:
    """A select that keeps every key and only picks columns."""
    keys = {key for key, _ in t.sort_keys or []}
    others = [c for c in t.columns if c not in keys]
    if not keys or not others:
        return t
    return t.select(*[c for c in t.columns if c != others[-1]])


def _replace_first_key(t: LTSeq, i: int) -> LTSeq:
    keys = [key for key, _ in t.sort_keys or []]
    if not keys or t.schema[keys[0]] not in ("int64", "float64"):
        return t
    return t.derive(**{keys[0]: lambda r: 0 - getattr(r, keys[0])})


def _rename_first_key(t: LTSeq, i: int) -> LTSeq:
    keys = [key for key, _ in t.sort_keys or []]
    return t.rename({keys[0]: f"renamed_{i}"}) if keys else t


STEPS: dict[str, Callable[[LTSeq, int], LTSeq]] = {
    "filter": _reads("volume", step=lambda t, i: t.filter(lambda r: r.volume % 3 != 0)),
    "computed": _reads("close", "volume", step=_derived("computed")),
    "shift": _reads("close", step=_in_order(_derived("shift"))),
    "partitioned_shift": _reads("close", "g", step=_in_order(_derived("partitioned_shift"))),
    "rolling": _reads("close", step=_in_order(_derived("rolling"))),
    "rank": _reads("volume", step=_derived("rank")),
    "cum_sum": _reads(
        "volume",
        step=_in_order(
            lambda t, i: t.cum_sum("volume") if "volume_cumsum" not in t.columns else t
        ),
    ),
    "drop_last_key": _drop_last_key,
    "drop_non_key": _drop_a_non_key,
    "replace_first_key": _replace_first_key,
    "rename_first_key": _rename_first_key,
    "head": lambda t, i: t.head(70),
    "tail": lambda t, i: t.tail(70),
    "slice": lambda t, i: t.slice(5, 60),
    "collect": lambda t, i: t.collect(),
    "group_derive": _reads(
        "g",
        step=_in_order(
            lambda t, i: t.group_ordered(lambda r: r.g).derive(
                lambda g: {f"size_{i}": g.count()}
            )
        ),
    ),
    "semi_join": _reads(
        "volume",
        step=lambda t, i: t.semi_join(
            LTSeq.from_arrow(input_rows(100)).filter(lambda r: r.volume % 2 == 0),
            on="volume",
        ),
    ),
}


def chain(seed: int, declare: str) -> tuple[Callable[[LTSeq], LTSeq], str, pa.Table]:
    """A seeded order, declared by `sort` or `assume_sorted`, followed by one
    to four steps, and the rows to build the table from.

    A `sort` gets the shuffled rows. An `assume_sorted` gets them in the order
    it declares, with ties in their shuffled order.
    """
    rng = np.random.default_rng(seed)
    keys, desc = ORDERS[seed % len(ORDERS)]
    names = list(rng.choice(sorted(STEPS), size=int(rng.integers(1, 5))))
    rows = input_rows(100)
    if declare == "assume_sorted":
        descending = desc if isinstance(desc, list) else [desc] * len(keys)
        rows = rows.sort_by(
            [(key, "descending" if d else "ascending") for key, d in zip(keys, descending)]
        )

    def build(t: LTSeq) -> LTSeq:
        t = getattr(t, declare)(*keys, desc=desc)
        for i, name in enumerate(names):
            t = STEPS[name](t, i)
        return t

    return build, f"{declare}{keys} → {' → '.join(names)}", rows


def assert_sorted_by(table: pa.Table, keys: list[tuple[str, bool]], rows: int, what: str) -> None:
    """`table` has `rows` rows, and adjacent ones do not sort after each
    other under `keys`. The count keeps a terminal that lost rows from
    passing: an empty table is sorted by anything."""
    assert table.num_rows == rows, (what, table.num_rows, rows)
    columns = [table.column(key).to_pylist() for key, _ in keys]
    for row in range(1, table.num_rows):
        for column, (key, descending) in zip(columns, keys):
            before, after = column[row - 1], column[row]
            if before == after:
                continue
            assert (before > after) if descending else (before < after), (what, key, row, before, after)
            break


@pytest.mark.parametrize("declare", ["sort", "assume_sorted"])
@pytest.mark.parametrize("seed", range(40))
def test_terminals_are_sorted_by_sort_keys(seed, declare, engine_layout, tmp_path):
    build, description, rows = chain(seed, declare)
    layout = LAYOUTS[seed % len(LAYOUTS)]
    t = build(layouts.build(layout, rows, tmp_path / "input"))
    keys = t.sort_keys
    if keys is None:
        # Only a step that drops or replaces the first key clears them all.
        assert "replace_first_key" in description, description
        return
    what = f"{description} over {layout}"
    # `len` counts through the plan without the root sort.
    rows = len(t)
    assert_sorted_by(t.to_arrow(), keys, rows, f"to_arrow: {what}")
    assert_sorted_by(pa.Table.from_pandas(t.to_pandas()), keys, rows, f"to_pandas: {what}")
    assert_sorted_by(pa.Table.from_pylist(t.to_dicts()), keys, rows, f"to_dicts: {what}")
    assert_sorted_by(pa.Table.from_pylist(list(t)), keys, rows, f"iteration: {what}")
    assert_sorted_by(pa.table(t), keys, rows, f"pa.table: {what}")
    assert_sorted_by(t.collect().to_arrow(), keys, rows, f"collect: {what}")
    assert_sorted_by(write_and_read_parquet(t, tmp_path), keys, rows, f"write_parquet: {what}")
    assert_sorted_by(write_and_read_csv(t, tmp_path), keys, rows, f"write_csv: {what}")
