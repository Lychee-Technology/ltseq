"""group_ordered() returns its groups in sequence order (#196).

The group windows partition by group, so DataFusion hash-repartitions the
rows by `__group_id__` and returns the groups in the order its partitions
finish, which changes from run to run. Each group operation ends with a sort
by group and row number. `first()` and `last()` used to get that order only
from the sort that ends `flatten()`, when DataFusion pushed their filter
beneath it. Their results, and `agg`'s, declare no `sort_keys`, so nothing
else restores the order where they are read.

The tests follow #196's 0.3.0 slice:

- the issue's 8-row reproduction, at every engine layout (partition counts 1,
  2 and twice the CPU count, batch sizes 1, 7 and the default);
- `first`, `last`, `agg`, and `first` and `last` after `filter`, at 100 rows
  in several batches, read through every consumer at every engine layout;
- those and `filter(...).flatten()` at 50,000 rows, at 2 and 2×CPU
  partitions, on every input layout.

Every shape sorts its input first, so the input layout reaches the group
windows only through the sort's merge; the 100-row runs therefore use one
input layout. `filter(...).flatten()` through every consumer, at every engine
and input layout, is test_declared_order.py's `kept_runs` pipeline.

The input's runs of `g` never put two kept groups with equal keys next to
each other, so `first` and `last` after `filter`, which group the kept rows
again, give the groups `filter` kept (M11 owns the group identity of
`filter`).
"""

from __future__ import annotations

import pandas as pd
import pyarrow as pa
import pytest

from ltseq import LTSeq

from . import layouts
from .layouts import CPUS
from .test_declared_order import (
    Pipeline,
    by_date,
    check_consumers,
    input_rows,
    kept_runs_reference,
)


def runs(df: pd.DataFrame) -> pd.DataFrame:
    """The rows in date order, with `run` numbering the runs of `g`."""
    d = by_date(df)
    return d.assign(run=(d.g != d.g.shift(1)).cumsum())


def kept(df: pd.DataFrame) -> pd.DataFrame:
    """The rows of the runs longer than one row, as `filter` keeps them."""
    d = runs(df)
    return d[d.groupby("run").g.transform("size") > 1]


def first_rows(d: pd.DataFrame) -> pd.DataFrame:
    return d.groupby("run").head(1).drop(columns="run").reset_index(drop=True)


def last_rows(d: pd.DataFrame) -> pd.DataFrame:
    return d.groupby("run").tail(1).drop(columns="run").reset_index(drop=True)


def spans_reference(df: pd.DataFrame) -> pd.DataFrame:
    grouped = runs(df).groupby("run")
    return pd.DataFrame(
        {
            "start": grouped.date.first(),
            "end": grouped.date.last(),
            "n": grouped.size(),
            "volume": grouped.volume.sum(),
        }
    ).reset_index(drop=True)


def grouped(t: LTSeq):
    return t.sort("date").group_ordered(lambda r: r.g)


def kept_groups(t: LTSeq):
    return grouped(t).filter(lambda g: g.count() > 1)


SHAPES = {
    "first": Pipeline(lambda t: grouped(t).first(), lambda df: first_rows(runs(df)), "volume"),
    "last": Pipeline(lambda t: grouped(t).last(), lambda df: last_rows(runs(df)), "volume"),
    "agg": Pipeline(
        lambda t: grouped(t).agg(
            lambda g: {
                "start": g.first().date,
                "end": g.last().date,
                "n": g.count(),
                "volume": g.sum("volume"),
            }
        ),
        spans_reference,
        "volume",
    ),
    "filter_first": Pipeline(
        lambda t: kept_groups(t).first(), lambda df: first_rows(kept(df)), "volume"
    ),
    "filter_last": Pipeline(
        lambda t: kept_groups(t).last(), lambda df: last_rows(kept(df)), "volume"
    ),
}

AT_SCALE = {
    **SHAPES,
    "filter_flatten": Pipeline(lambda t: kept_groups(t).flatten(), kept_runs_reference, "volume"),
}


def test_issue_reproduction(engine_layout):
    """The 8-row reproduction from #196, which keeps every group."""
    t = LTSeq.from_arrow(
        pa.table({"g": [1, 1, 2, 2, 3, 3, 4, 4], "x": [1, 2, 3, 4, 5, 6, 7, 8]})
    ).sort(lambda r: r.g)
    nested = t.group_ordered(lambda r: r.g)
    kept_all = nested.filter(lambda g: g.sum("x") > 2)

    def xs(result: LTSeq) -> list[int]:
        return result.to_arrow().column("x").to_pylist()

    assert xs(kept_all.flatten()) == [1, 2, 3, 4, 5, 6, 7, 8]
    assert xs(nested.first()) == [1, 3, 5, 7]
    assert xs(nested.last()) == [2, 4, 6, 8]
    assert xs(kept_all.first()) == [1, 3, 5, 7]
    assert xs(kept_all.last()) == [2, 4, 6, 8]
    spans = nested.agg(lambda g: {"first_x": g.first().x, "total": g.sum("x")})
    assert spans.to_arrow().to_pydict() == {"first_x": [1, 3, 5, 7], "total": [3, 7, 11, 15]}


@pytest.mark.parametrize("name", list(SHAPES))
def test_every_consumer_reads_groups_in_order(name, engine_layout, tmp_path, capsys):
    shape = SHAPES[name]
    rows = input_rows(100)
    t = shape.build(layouts.build("many_batches", rows, tmp_path / "input"))
    check_consumers(t, shape, shape.reference(rows.to_pandas()), tmp_path, capsys)


@pytest.mark.parametrize("partitions", [2, 2 * CPUS], ids=["p2", "p2xcpu"])
@pytest.mark.parametrize("name", list(AT_SCALE))
def test_groups_come_back_in_order_at_scale(name, partitions, engine_config, data_layout):
    """50,000 rows at the default batch size: several batches per partition,
    and groups from every part of the input in each hash partition."""
    engine_config(target_partitions=partitions)
    shape = AT_SCALE[name]
    rows = input_rows(50_000)
    actual = shape.build(data_layout(rows)).to_pandas()
    expected = shape.reference(rows.to_pandas())
    assert list(actual.columns) == list(expected.columns)
    pd.testing.assert_frame_equal(actual, expected, check_dtype=False)
