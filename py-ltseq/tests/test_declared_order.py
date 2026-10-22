"""A sorted table returns its rows in order from every consumer (#311).

DataFusion keeps a sort's order only up to the next operator it chooses to
repartition, so a filter, a computed column or a window after `sort` used to
return rows in partition arrival order while `sort_keys` still reported the
sort. The table's declared order is now required at the root of every plan
that reads rows in table order.

These are #321's ordering acceptance pipelines and consumers. Each output is
compared row for row with a pandas reference, under every engine layout
(partition counts 1, 2 and twice the CPU count, batch sizes 1, 7 and the
default) and every input layout. The input rows arrive shuffled, so every
order in an output comes from the pipeline's `sort`.
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
    slowdown unrelated to order.
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
