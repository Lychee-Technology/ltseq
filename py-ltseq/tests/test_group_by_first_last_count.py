"""`g.count()`, `first` and `last` in `group_by().agg()` and `LTSeq.agg()` (#314).

`g.count()` counts a group's rows. `g.col.first()` and `g.col.last()` read
each group's rows in table order (contract §14.1-§14.2), NULL included, so
they need a declared order and raise `SortRequiredError` without one
(positional tier, §9.3). `g.first()` is not a group attribute (§8.1): it reads
as a column, and its error names the column form.
"""

import pytest

from ltseq import LTSeq
from ltseq.exceptions import SortRequiredError


def by_key(table, key="k"):
    return {row[key]: row for row in table.to_dicts()}


ROWS = [
    {"k": "a", "ts": 3, "v": 30},
    {"k": "b", "ts": 5, "v": None},
    {"k": "a", "ts": 1, "v": None},
    {"k": "a", "ts": 2, "v": 20},
    {"k": "b", "ts": 4, "v": 40},
]


class TestCount:
    def test_issue_repro(self):
        t = LTSeq.from_rows([{"k": "a", "v": 1}, {"k": "a", "v": 2}, {"k": "b", "v": 3}])
        out = by_key(t.group_by("k").agg(n=lambda g: g.count()))
        assert {k: r["n"] for k, r in out.items()} == {"a": 2, "b": 1}

    def test_counts_rows_with_null_values(self):
        out = by_key(
            LTSeq.from_rows(ROWS)
            .group_by("k")
            .agg(rows=lambda g: g.count(), values=lambda g: g.v.count())
        )
        assert {k: (r["rows"], r["values"]) for k, r in out.items()} == {
            "a": (3, 2),
            "b": (2, 1),
        }

    def test_needs_no_order(self):
        # Counting reads no row order, so an unsorted table is fine.
        t = LTSeq.from_rows(ROWS)
        assert t.sort_keys is None or not t.sort_keys
        assert t.agg(n=lambda g: g.count()).to_dicts() == [{"n": 5}]

    def test_combines_with_column_aggregates(self):
        out = by_key(
            LTSeq.from_rows(ROWS)
            .group_by(lambda r: r.k)
            .agg(n=lambda g: g.count(), total=lambda g: g.v.sum())
        )
        assert out["a"]["n"] == 3 and out["a"]["total"] == 50

    def test_column_named_count_keeps_its_name(self):
        t = LTSeq.from_rows([{"k": "a", "count": 4}, {"k": "a", "count": 6}])
        out = t.group_by("k").agg(total=lambda g: g.count.sum()).to_dicts()
        assert out == [{"k": "a", "total": 10}]


class TestFirstLast:
    def test_reads_table_order_with_nulls(self):
        t = LTSeq.from_rows(ROWS).sort("ts")
        out = by_key(t.group_by("k").agg(f=lambda g: g.v.first(), l=lambda g: g.v.last()))
        # a in ts order: v = None, 20, 30; b: 40, None. NULL is a value here.
        assert (out["a"]["f"], out["a"]["l"]) == (None, 30)
        assert (out["b"]["f"], out["b"]["l"]) == (40, None)

    def test_descending_order(self):
        t = LTSeq.from_rows(ROWS).sort("ts", descending=True)
        out = by_key(
            t.group_by("k").agg(f=lambda g: g.ts.first(), l=lambda g: g.ts.last())
        )
        assert (out["a"]["f"], out["a"]["l"]) == (3, 1)
        assert (out["b"]["f"], out["b"]["l"]) == (5, 4)

    def test_multi_key_order_and_null_keys(self):
        rows = [
            {"k": "a", "d": 1, "ts": None, "v": "null-ts"},
            {"k": "a", "d": 1, "ts": 7, "v": "d1-7"},
            {"k": "a", "d": 0, "ts": 9, "v": "d0-9"},
            {"k": "a", "d": 1, "ts": 2, "v": "d1-2"},
        ]
        # NULL sorts last by default, and first with nulls_last=False.
        t = LTSeq.from_rows(rows).sort("d", "ts")
        out = by_key(t.group_by("k").agg(f=lambda g: g.v.first(), l=lambda g: g.v.last()))
        assert (out["a"]["f"], out["a"]["l"]) == ("d0-9", "null-ts")

        t = LTSeq.from_rows(rows).sort("d", "ts", nulls_last=False)
        out = by_key(t.group_by("k").agg(f=lambda g: g.v.first(), l=lambda g: g.v.last()))
        assert (out["a"]["f"], out["a"]["l"]) == ("d0-9", "d1-7")

        t = LTSeq.from_rows(rows).sort("d", "ts", descending=[False, True])
        out = by_key(t.group_by("k").agg(f=lambda g: g.v.first(), l=lambda g: g.v.last()))
        assert (out["a"]["f"], out["a"]["l"]) == ("d0-9", "null-ts")

    def test_assume_sorted_declares_the_order(self):
        rows = sorted(ROWS, key=lambda r: r["ts"])
        t = LTSeq.from_rows(rows).assume_sorted("ts")
        out = by_key(t.group_by("k").agg(f=lambda g: g.ts.first(), l=lambda g: g.ts.last()))
        assert (out["a"]["f"], out["a"]["l"]) == (1, 3)

    def test_whole_table_agg(self):
        t = LTSeq.from_rows(ROWS).sort("ts")
        out = t.agg(f=lambda g: g.ts.first(), l=lambda g: g.v.last(), n=lambda g: g.count())
        assert out.to_dicts() == [{"f": 1, "l": None, "n": 5}]

    def test_agg_by(self):
        t = LTSeq.from_rows(ROWS).sort("ts")
        out = t.agg(by=lambda r: r.k, f=lambda g: g.v.first())
        assert {r["k"]: r["f"] for r in out.to_dicts()} == {"a": None, "b": 40}

    @pytest.mark.parametrize(
        "spelling",
        [lambda g: g.v.first(), lambda g: g.v.last()],
        ids=["first", "last"],
    )
    def test_without_declared_order_raises(self, spelling):
        t = LTSeq.from_rows(ROWS)
        with pytest.raises(SortRequiredError, match=r"table order.*\.sort\(") as info:
            t.group_by("k").agg(x=spelling)
        assert "'x'" in str(info.value)
        assert ".min()" in str(info.value)
        with pytest.raises(SortRequiredError):
            t.agg(x=spelling)

    @pytest.mark.parametrize("name", ["first", "last"])
    def test_group_row_proxy_names_the_column_form(self, name):
        # Contract §8.1: the group has no first()/last() attribute; the error
        # names g.<col>.first() instead of only listing the columns.
        from ltseq.exceptions import ColumnNotFoundError

        t = LTSeq.from_rows(ROWS).sort("ts")
        with pytest.raises(ColumnNotFoundError, match=rf"g\.<col>\.{name}\(\)"):
            t.group_by("k").agg(x=lambda g: getattr(g, name)().v)

    def test_column_named_first_keeps_its_name(self):
        t = LTSeq.from_rows([{"k": "a", "first": 1}, {"k": "a", "first": 2}])
        assert t.group_by("k").agg(x=lambda g: g.first.max()).to_dicts() == [
            {"k": "a", "x": 2}
        ]

    def test_engine_layouts(self, engine_layout):
        # Many groups spread over every row of the input, read back from a
        # multi-partition plan, against a reference over the sorted rows.
        n = 2_000
        rows = [{"k": i % 37, "ts": (i * 7919) % n, "v": i} for i in range(n)]
        t = LTSeq.from_rows(rows).sort("ts")
        out = by_key(
            t.group_by("k").agg(
                f=lambda g: g.v.first(), l=lambda g: g.v.last(), n=lambda g: g.count()
            )
        )
        expected = {}
        for row in sorted(rows, key=lambda r: r["ts"]):
            first, _, count = expected.get(row["k"], (row["v"], None, 0))
            expected[row["k"]] = (first, row["v"], count + 1)
        assert {k: (r["f"], r["l"], r["n"]) for k, r in out.items()} == expected


def test_nested_table_agg_unchanged():
    # NestedTable.agg passes its own in-group row number as the order.
    t = LTSeq.from_rows(ROWS).sort("ts")
    out = t.group_ordered(lambda r: r.k).agg(
        lambda g: {"f": g.first().ts, "l": g.last().ts, "n": g.count()}
    )
    assert out.to_dicts() == [
        {"f": 1, "l": 3, "n": 3},
        {"f": 4, "l": 5, "n": 2},
    ]
