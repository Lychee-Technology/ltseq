"""NULL semantics across the API, on one nullable fixture (#154).

Most of these behaviors were correct before #154 but untested. Three of
them were not:

- `r.col == None` / `r.col != None` compared against SQL `NULL`, so
  `filter` returned no rows and `derive` an all-NULL column. They now build
  the same expression as `.is_null()` / `.is_not_null()`, in row lambdas
  and in group predicates (`g.first().col == None`).
- `to_dicts()` went through pandas: a nullable Int64 column came back as
  floats and a string NULL as `nan`, and `fold()`, which rebuilds its result
  from those rows, raised on any table with a NULL in a string column.
- Ranking and sequence windows with `.over(order_by=...)` placed NULLs first
  in ascending order, the opposite of `sort()`. Both now treat NULL as the
  largest value: last when ascending, first when descending.

Row order of a derived table is not guaranteed to follow the input (see
#202), so results are compared by `id`.
"""

import math
import sys

import pyarrow as pa
import pytest

from ltseq import LTSeq, dense_rank, if_else, rank, row_number
from ltseq.expr.types import ColumnExpr

NAN = float("nan")

# id: row identity, never NULL.
# grp: string with NULLs (group / partition / distinct key).
# k: Int64 join key with NULLs.
# v: Int64 values with NULLs.
# f: Float64 with both NULL (ids 2, 5) and NaN (id 3).
TABLE = pa.table(
    {
        "id": pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
        "grp": pa.array(["a", None, "a", None, "b", "b"], pa.string()),
        "k": pa.array([10, None, 20, None, 10, 30], pa.int64()),
        "v": pa.array([1, None, 3, 4, None, 6], pa.int64()),
        "f": pa.array([1.0, None, NAN, 4.0, None, 6.0], pa.float64()),
    }
)
V_NULL_IDS = [2, 5]
V_NOT_NULL_IDS = [1, 3, 4, 6]


@pytest.fixture
def t():
    return LTSeq.from_arrow(TABLE)


def _rows(table):
    return table.to_arrow().to_pylist()


def _ids(table):
    return sorted(r["id"] for r in _rows(table))


def _by_id(table, column):
    return {r["id"]: r[column] for r in _rows(table)}


class TestToDicts:
    def test_null_becomes_none_and_int_stays_int(self, t):
        rows = {r["id"]: r for r in t.to_dicts()}
        assert rows[2] == {"id": 2, "grp": None, "k": None, "v": None, "f": None}
        assert type(rows[1]["v"]) is int and rows[1]["v"] == 1
        assert type(rows[1]["k"]) is int
        assert rows[1]["grp"] == "a"

    def test_nan_stays_distinct_from_null(self, t):
        rows = {r["id"]: r for r in t.to_dicts()}
        assert math.isnan(rows[3]["f"])
        assert rows[2]["f"] is None

    def test_other_types_keep_none(self):
        import datetime as dt

        table = LTSeq.from_arrow(
            pa.table(
                {
                    "b": pa.array([True, None]),
                    "d": pa.array([dt.date(2024, 1, 2), None], pa.date32()),
                    "ts": pa.array([dt.datetime(2024, 1, 2, 3, 4), None], pa.timestamp("us")),
                    "l": pa.array([[1, 2], None], pa.list_(pa.int64())),
                }
            )
        )
        assert table.to_dicts() == [
            {"b": True, "d": dt.date(2024, 1, 2), "ts": dt.datetime(2024, 1, 2, 3, 4), "l": [1, 2]},
            {"b": None, "d": None, "ts": None, "l": None},
        ]

    def test_iter_yields_the_same_rows(self, t):
        no_nan = t.select("id", "grp", "k", "v")
        assert list(no_nan) == no_nan.to_dicts()

    def test_empty_result(self, t):
        assert t.filter(lambda r: r.id > 100).to_dicts() == []

    def test_does_not_need_pandas(self, t, monkeypatch):
        # pandas is a dev dependency only; to_dicts() must not import it.
        monkeypatch.setitem(sys.modules, "pandas", None)
        assert len(t.to_dicts()) == 6


class TestFoldWithNulls:
    def test_fold_sees_none_and_keeps_nullable_columns(self, t):
        # Used to raise ArrowTypeError: the string NULL reached fold() as nan.
        out = t.sort("id").fold(
            lambda s, r: s + (r["v"] or 0), init=0, into="acc"
        )
        assert _rows(out.select("id", "grp", "v", "acc")) == [
            {"id": 1, "grp": "a", "v": 1, "acc": 1},
            {"id": 2, "grp": None, "v": None, "acc": 1},
            {"id": 3, "grp": "a", "v": 3, "acc": 4},
            {"id": 4, "grp": None, "v": 4, "acc": 8},
            {"id": 5, "grp": "b", "v": None, "acc": 8},
            {"id": 6, "grp": "b", "v": 6, "acc": 14},
        ]

    def test_null_partition_key_is_one_partition(self, t):
        out = t.sort("id").fold(lambda s, r: s + 1, init=0, into="n", partition_by="grp")
        assert _by_id(out, "n") == {1: 1, 2: 1, 3: 2, 4: 2, 5: 1, 6: 2}

    def test_fold_keeps_source_column_types(self):
        # The result is not rebuilt from Python values, so nothing is
        # re-inferred: an all-NULL Int64 column would otherwise become the
        # Arrow null type (and SUM over it would fail).
        from decimal import Decimal

        types = {
            "i": pa.int64(),
            "all_null": pa.int64(),
            "d": pa.decimal128(10, 2),
            "ts": pa.timestamp("ns"),
            "u": pa.uint64(),
        }
        table = pa.table(
            {
                "i": pa.array([1, 2], types["i"]),
                "all_null": pa.array([None, None], types["all_null"]),
                "d": pa.array([Decimal("1.10"), None], types["d"]),
                "ts": pa.array([1, None], types["ts"]),
                "u": pa.array([2**64 - 1, 1], types["u"]),
            }
        )
        out = LTSeq.from_arrow(table).sort("i").fold(lambda s, r: s + 1, init=0, into="n")
        arrow = out.to_arrow()
        assert {f.name: f.type for f in arrow.schema} == {**types, "n": pa.int64()}
        assert arrow.column("u").to_pylist() == [2**64 - 1, 1]
        assert out.agg(s=lambda g: g.all_null.sum()).to_dicts() == [{"s": None}]

    def test_into_column_with_no_values_is_float64(self, t):
        out = t.sort("id").fold(lambda s, r: None, init=None, into="n")
        assert out.to_arrow().schema.field("n").type == pa.float64()


class TestEqNone:
    """`== None` / `!= None` mean `.is_null()` / `.is_not_null()`."""

    def test_builds_the_null_check_expression(self):
        # The same expression, not merely the same rows on the DataFusion
        # path: the search_pattern and linear-scan evaluators read the
        # serialized form, so a different node could still diverge there.
        col = ColumnExpr("v")
        assert (col == None).serialize() == col.is_null().serialize()  # noqa: E711
        assert (col != None).serialize() == col.is_not_null().serialize()  # noqa: E711
        assert (None == col).serialize() == col.is_null().serialize()  # noqa: E711
        assert (None != col).serialize() == col.is_not_null().serialize()  # noqa: E711

    def test_filter(self, t):
        assert _ids(t.filter(lambda r: r.v == None)) == V_NULL_IDS  # noqa: E711
        assert _ids(t.filter(lambda r: r.v != None)) == V_NOT_NULL_IDS  # noqa: E711
        assert _ids(t.filter(lambda r: None == r.v)) == V_NULL_IDS  # noqa: E711
        assert _ids(t.filter(lambda r: r.grp == None)) == [2, 4]  # noqa: E711

    def test_matches_is_null_methods(self, t):
        assert _ids(t.filter(lambda r: r.v == None)) == _ids(t.filter(lambda r: r.v.is_null()))  # noqa: E711
        assert _ids(t.filter(lambda r: r.v != None)) == _ids(  # noqa: E711
            t.filter(lambda r: r.v.is_not_null())
        )

    def test_derive_gives_booleans_not_null(self, t):
        out = t.derive(eq=lambda r: r.v == None, ne=lambda r: r.v != None)  # noqa: E711
        assert _by_id(out, "eq") == {1: False, 2: True, 3: False, 4: False, 5: True, 6: False}
        assert _by_id(out, "ne") == {1: True, 2: False, 3: True, 4: True, 5: False, 6: True}

    def test_python_variable_holding_none(self, t):
        wanted = None
        assert _ids(t.filter(lambda r: r.grp == wanted)) == [2, 4]

    def test_in_compound_predicates_and_if_else(self, t):
        assert _ids(t.filter(lambda r: (r.v == None) | (r.v > 3))) == [2, 4, 5, 6]  # noqa: E711
        out = t.derive(w=lambda r: if_else(r.v == None, 0, r.v))  # noqa: E711
        assert _by_id(out, "w") == {1: 1, 2: 0, 3: 3, 4: 4, 5: 0, 6: 6}

    def test_without_source(self, t):
        # Operator overloading needs no source, unlike the `is None` rewrite.
        assert _ids(t.filter(eval("lambda r: r.v == None"))) == V_NULL_IDS

    def test_nan_is_not_none(self, t):
        assert _ids(t.filter(lambda r: r.f == None)) == V_NULL_IDS  # noqa: E711

    def test_column_comparison_keeps_sql_semantics(self, t):
        # Only a literal None operand is a null test; NULL = NULL between two
        # columns is still NULL, so NULL rows are not selected.
        same = t.derive(w=lambda r: r.v)
        assert _ids(same.filter(lambda r: r.v == r.w)) == V_NOT_NULL_IDS

    def test_search_first_and_search_pattern(self, t):
        s = t.sort("id")
        assert _ids(s.search_first(lambda r: r.v == None)) == [2]  # noqa: E711
        matches = s.search_pattern(lambda r: r.v == None, lambda r: r.v != None)  # noqa: E711
        assert _ids(matches) == [2, 5]


class TestEqNoneInGroupPredicates:
    @pytest.fixture
    def groups(self):
        # Groups by g: 1 -> first v NULL, 2 -> no NULL, 3 -> only NULL.
        table = pa.table(
            {
                "id": pa.array([1, 2, 3, 4, 5], pa.int64()),
                "g": pa.array([1, 1, 2, 2, 3], pa.int64()),
                "v": pa.array([None, 5, 7, 8, None], pa.int64()),
            }
        )
        return LTSeq.from_arrow(table).sort("id").group_ordered(lambda r: r.g)

    @staticmethod
    def _kept(nested):
        return _ids(nested.flatten())

    def test_first_last(self, groups):
        assert self._kept(groups.filter(lambda g: g.first().v == None)) == [1, 2, 5]  # noqa: E711
        assert self._kept(groups.filter(lambda g: g.first().v != None)) == [3, 4]  # noqa: E711
        assert self._kept(groups.filter(lambda g: g.last().v == None)) == [5]  # noqa: E711
        assert self._kept(groups.filter(lambda g: None == g.first().v)) == [1, 2, 5]  # noqa: E711

    def test_methods(self, groups):
        assert self._kept(groups.filter(lambda g: g.first().v.is_null())) == [1, 2, 5]
        assert self._kept(groups.filter(lambda g: g.first().v.is_not_null())) == [3, 4]
        assert self._kept(groups.filter(lambda g: ~g.first().v.is_null())) == [3, 4]

    def test_is_none_points_to_the_null_checks(self, groups):
        # Group lambdas are not rewritten (#144 covers row lambdas), so
        # `is None` is a plain Python bool; the error names what works.
        with pytest.raises(ValueError, match=r"is_null\(\)"):
            groups.filter(lambda g: g.first().v is None)

    def test_aggregate(self, groups):
        # SUM over only NULLs is NULL.
        assert self._kept(groups.filter(lambda g: g.sum("v") == None)) == [5]  # noqa: E711
        combined = groups.filter(lambda g: (g.sum("v") != None) & (g.count() > 1))  # noqa: E711
        assert self._kept(combined) == [1, 2, 3, 4]

    def test_quantifiers(self, groups):
        assert self._kept(groups.filter(lambda g: g.any(lambda r: r.v == None))) == [1, 2, 5]  # noqa: E711
        assert self._kept(groups.filter(lambda g: g.all(lambda r: r.v != None))) == [3, 4]  # noqa: E711


class TestWindowsWithNulls:
    def test_cum_sum_skips_null(self, t):
        s = t.sort("id")
        expected = {1: 1, 2: 1, 3: 4, 4: 8, 5: 8, 6: 14}
        assert _by_id(s.cum_sum("v"), "v_cumsum") == expected
        assert _by_id(s.derive(c=lambda r: r.v.cum_sum()), "c") == expected

    def test_shift_and_diff_propagate_null(self, t):
        s = t.sort("id")
        assert _by_id(s.derive(p=lambda r: r.v.shift(1)), "p") == {
            1: None, 2: 1, 3: None, 4: 3, 5: 4, 6: None,
        }
        assert _by_id(s.derive(d=lambda r: r.v.diff()), "d") == {
            1: None, 2: None, 3: None, 4: 1, 5: None, 6: None,
        }

    def test_rolling_aggregates_skip_null(self, t):
        s = t.sort("id")
        assert _by_id(s.derive(m=lambda r: r.v.rolling(2).sum()), "m") == {
            1: 1, 2: 1, 3: 3, 4: 7, 5: 4, 6: 6,
        }
        assert _by_id(s.derive(m=lambda r: r.v.rolling(2).mean()), "m") == {
            1: 1.0, 2: 1.0, 3: 3.0, 4: 3.5, 5: 4.0, 6: 6.0,
        }
        assert _by_id(s.derive(m=lambda r: r.v.rolling(2).count()), "m") == {
            1: 1, 2: 1, 3: 1, 4: 2, 5: 1, 6: 1,
        }


class TestJoinWithNullKeys:
    """A NULL key matches nothing, not even a NULL key on the other side."""

    @pytest.fixture
    def right(self):
        return LTSeq.from_arrow(
            pa.table(
                {
                    "k": pa.array([10, None, 30], pa.int64()),
                    "name": ["ten", "null", "thirty"],
                }
            )
        )

    def test_inner_and_left(self, t, right):
        assert _ids(t.join(right, on="k")) == [1, 5, 6]
        left = t.join(right, on="k", how="left")
        assert _by_id(left, "name") == {
            1: "ten", 2: None, 3: None, 4: None, 5: "ten", 6: "thirty",
        }

    def test_merge_strategy(self, t, right):
        merged = t.sort("k").join(right.sort("k"), on="k", strategy="merge")
        assert _ids(merged) == [1, 5, 6]

    def test_semi_and_anti(self, t, right):
        assert _ids(t.semi_join(right, on="k")) == [1, 5, 6]
        assert _ids(t.anti_join(right, on="k")) == [2, 3, 4]


class TestNullOrdering:
    """NULL is the largest value: last when ascending, first when descending."""

    def test_sort(self, t):
        assert [r["v"] for r in _rows(t.sort("v"))] == [1, 3, 4, 6, None, None]
        assert [r["v"] for r in _rows(t.sort("v", desc=True))] == [None, None, 6, 4, 3, 1]
        assert [r["grp"] for r in _rows(t.sort("grp", "id"))] == ["a", "a", "b", "b", None, None]

    def test_rank_and_dense_rank(self, t):
        asc = t.derive(
            rk=lambda r: rank().over(order_by=r.v),
            drk=lambda r: dense_rank().over(order_by=r.v),
        )
        assert _by_id(asc, "rk") == {1: 1, 3: 2, 4: 3, 6: 4, 2: 5, 5: 5}
        assert _by_id(asc, "drk") == {1: 1, 3: 2, 4: 3, 6: 4, 2: 5, 5: 5}
        desc = t.derive(rk=lambda r: rank().over(order_by=r.v, descending=True))
        assert _by_id(desc, "rk") == {2: 1, 5: 1, 6: 3, 4: 4, 3: 5, 1: 6}

    def test_row_number_matches_sort_position(self, t):
        for descending in (False, True):
            numbered = _by_id(
                t.derive(rn=lambda r: row_number().over(order_by=r.v, descending=descending)),
                "rn",
            )
            order = _rows(t.sort("v", desc=descending))
            # NULL ties may be numbered in either order; compare as a set.
            null_positions = {i + 1 for i, r in enumerate(order) if r["v"] is None}
            assert {numbered[i] for i in V_NULL_IDS} == null_positions
            for position, row in enumerate(order, start=1):
                if row["v"] is not None:
                    assert numbered[row["id"]] == position

    def test_sequence_window_over_matches_sort(self, t):
        # cum_sum of id in v order; only non-NULL rows have a well-defined
        # value (the two NULL rows tie).
        for descending, expected in ((False, {1: 1, 3: 4, 4: 8, 6: 14}), (True, {6: 13, 4: 17, 3: 20, 1: 21})):
            over = _by_id(
                t.derive(c=lambda r: r.id.cum_sum().over(order_by=r.v, descending=descending)),
                "c",
            )
            sorted_first = _by_id(
                t.sort("v", desc=descending).derive(c=lambda r: r.id.cum_sum()), "c"
            )
            for i in V_NOT_NULL_IDS:
                assert over[i] == sorted_first[i] == expected[i]

    def test_null_partition_is_its_own_partition(self, t):
        out = t.derive(rk=lambda r: rank().over(partition_by=r.grp, order_by=r.id))
        assert _by_id(out, "rk") == {1: 1, 3: 2, 2: 1, 4: 2, 5: 1, 6: 2}


class TestGroupingAndDistinct:
    def test_group_by_null_key_is_its_own_group(self, t):
        out = t.group_by("grp").agg(n=lambda g: g.id.count(), s=lambda g: g.v.sum())
        assert {r["grp"]: (r["n"], r["s"]) for r in _rows(out)} == {
            None: (2, 4),
            "a": (2, 4),
            "b": (2, 6),
        }
        by_lambda = t.agg(by=lambda r: r.grp, n=lambda g: g.id.count())
        assert {r["grp"]: r["n"] for r in _rows(by_lambda)} == {None: 2, "a": 2, "b": 2}

    def test_distinct_keeps_one_null(self, t):
        assert sorted(_rows(t.select("grp").distinct()), key=repr) == sorted(
            [{"grp": None}, {"grp": "a"}, {"grp": "b"}], key=repr
        )
        assert sorted((r["grp"] for r in _rows(t.distinct("grp"))), key=repr) == sorted(
            [None, "a", "b"], key=repr
        )


class TestAggregatesSkipNull:
    def test_sum_avg_count_min_max(self, t):
        out = t.agg(
            s=lambda g: g.v.sum(),
            a=lambda g: g.v.avg(),
            c=lambda g: g.v.count(),
            rows=lambda g: g.id.count(),
            mn=lambda g: g.v.min(),
            mx=lambda g: g.v.max(),
        )
        assert _rows(out) == [{"s": 14, "a": 3.5, "c": 4, "rows": 6, "mn": 1, "mx": 6}]

    def test_all_null_input(self, t):
        out = t.filter(lambda r: r.v.is_null()).agg(
            s=lambda g: g.v.sum(), c=lambda g: g.v.count()
        )
        assert _rows(out) == [{"s": None, "c": 0}]


class TestNanIsNotNull:
    def test_is_null_ignores_nan(self, t):
        assert _ids(t.filter(lambda r: r.f.is_null())) == V_NULL_IDS

    def test_nan_compares_greater_than_numbers(self, t):
        # DataFusion orders NaN above every number, so `NaN > 0` is true,
        # unlike Python where `nan > 0` is False.
        assert _ids(t.filter(lambda r: r.f > 0)) == [1, 3, 4, 6]
        assert _ids(t.filter(lambda r: r.f > float("inf"))) == [3]
        assert _ids(t.filter(lambda r: r.f == NAN)) == [3]

    def test_nan_sorts_after_numbers_and_before_null(self, t):
        assert [r["id"] for r in _rows(t.sort("f", "id"))] == [1, 4, 6, 3, 2, 5]
        assert math.isnan(t.agg(m=lambda g: g.f.max()).to_dicts()[0]["m"])
