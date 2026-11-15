"""Joins of in-memory tables whose schema metadata clash (issue #301).

`pyarrow.Table.from_pandas` gives each table its own `pandas` schema
metadata, and a join's schema merges its inputs' metadata, the left input's
value winning the key both carry (the right one's in a right join). When
DataFusion's `join_selection` swapped a join's inputs, the winner changed and
the physical optimizer failed its own schema check at collect ("Schema
metadata mismatch"), for instance for a filter on a right-side string column.
`join_selection` now keeps each join's inputs (#273, X5), which also keeps
the merged metadata; `rewrite_guard`'s
`join_selection_keeps_clashing_schema_metadata` shows the unguarded failure.

These tests build every table inside the test, under each engine layout,
from inputs that carry clashing metadata, and check the rows.
"""

import pandas as pd
import pyarrow as pa
import pytest

from ltseq import LTSeq

LEFT = {"id": [1, 2, 3], "v": [10.0, None, 30.0]}
RIGHT = {"id": [1, 2, 4], "x": [None, "p", None], "n": [5, 6, 7]}


def _pandas_metadata(table: pa.Table) -> bytes:
    return (table.schema.metadata or {})[b"pandas"]


def _from_pandas(left, right):
    return LTSeq.from_pandas(pd.DataFrame(left)), LTSeq.from_pandas(pd.DataFrame(right))


def _from_arrow_with_pandas_metadata(left, right):
    tables = [pa.Table.from_pandas(pd.DataFrame(d), preserve_index=False) for d in (left, right)]
    assert _pandas_metadata(tables[0]) != _pandas_metadata(tables[1])
    return tuple(LTSeq.from_arrow(t) for t in tables)


def _from_arrow_with_other_metadata(left, right):
    tables = [
        pa.table(d).replace_schema_metadata({"origin": origin})
        for d, origin in ((left, "left"), (right, "right"))
    ]
    return tuple(LTSeq.from_arrow(t) for t in tables)


SOURCES = {
    "from_pandas": _from_pandas,
    "from_arrow-pandas-metadata": _from_arrow_with_pandas_metadata,
    "from_arrow-other-metadata": _from_arrow_with_other_metadata,
}


@pytest.fixture(params=list(SOURCES.values()), ids=list(SOURCES))
def tables(request, engine_layout):
    return request.param(LEFT, RIGHT)


def _ids(t: LTSeq) -> list:
    return sorted(t.to_arrow().column("id").to_pylist())


def _is_p(q):
    return q.x == "p"


# The issue's table, and the narrowing select of LINKING_GUIDE's former
# caveat. Each case reads a right-side column after (or below) the join.
CASES = {
    "filter-right-string-after-join": (lambda l, r: l.join(r, on="id").filter(_is_p), [2]),
    "filter-right-string-before-join": (
        lambda l, r: l.join(r.filter(lambda q: q.x.is_not_null()), on="id"),
        [2],
    ),
    "filter-right-int-after-join": (lambda l, r: l.join(r, on="id").filter(lambda q: q.n == 6), [2]),
    "filter-left-after-join": (lambda l, r: l.join(r, on="id").filter(lambda q: q.v == 10.0), [1]),
    "select-after-join": (lambda l, r: l.join(r, on="id").select("id", "x"), [1, 2]),
    "filter-then-select-after-join": (
        lambda l, r: l.join(r, on="id").filter(_is_p).select("id", "x"),
        [2],
    ),
    "left-join-filter": (lambda l, r: l.join(r, on="id", how="left").filter(_is_p), [2]),
    "full-join-filter": (lambda l, r: l.join(r, on="id", how="full").filter(_is_p), [2]),
    "link-filter": (
        lambda l, r: l.link(r, on=lambda a, b: a.id == b.id, alias="b").filter(lambda q: q.b_x == "p"),
        [2],
    ),
    "link-select": (
        lambda l, r: l.link(r, on=lambda a, b: a.id == b.id, alias="b").select("id", "b_x"),
        [1, 2],
    ),
    "semi-join-on-filtered-right": (lambda l, r: l.semi_join(r.filter(_is_p), on="id"), [2]),
}


@pytest.mark.parametrize("case", list(CASES.values()), ids=list(CASES))
def test_join_over_clashing_schema_metadata(tables, case):
    build, expected = case
    left, right = tables
    assert _ids(build(left, right)) == expected
