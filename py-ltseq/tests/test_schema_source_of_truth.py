"""Regression tests for issue #93: Rust is the single source of truth for schema.

Python's _schema used to be a hand-maintained lossy mirror: select kept
projected-away columns, derive/agg/pivot invented "Unknown" placeholder
types, and rename of a sort column diverged Python _sort_keys (remapped)
from Rust sort_specs (cleared by select validation).
"""

from typing import Any

import pandas as pd
import pytest

from ltseq import LTSeq


def make_t():
    return LTSeq.from_pandas(
        pd.DataFrame({"a": [1, 2], "b": ["x", "y"], "c": [1.5, 2.5]})
    )


class TestSelectSchema:
    def test_select_columns_match_arrow(self):
        """B3: .columns must match the actual Arrow output after select."""
        r = make_t().select("a", "b")
        assert r.columns == r.to_arrow().column_names == ["a", "b"]


class TestNoPlaceholderTypes:
    def test_derive_type_not_unknown(self):
        r = make_t().derive(lambda r: {"d": r.a + 1})
        assert r.schema["d"] != "Unknown"

    def test_agg_type_not_unknown(self):
        r = make_t().agg(total=lambda g: g.a.sum())
        assert all(v != "Unknown" for v in r.schema.values()), r.schema

    def test_grouped_agg_type_not_unknown(self):
        df = pd.DataFrame({"region": ["E", "E", "W"], "sales": [1, 2, 3]})
        r = LTSeq.from_pandas(df).agg(
            by=lambda r: r.region, total=lambda g: g.sales.sum()
        )
        assert all(v != "Unknown" for v in r.schema.values()), r.schema

    def test_pivot_no_placeholder_column(self):
        df = pd.DataFrame(
            {"k": ["p", "p", "q"], "cat": ["A", "B", "A"], "v": [1, 2, 3]}
        )
        r = LTSeq.from_pandas(df).pivot(index="k", columns="cat", values="v")
        assert "_pivot_columns" not in r.columns
        assert r.columns == r.to_arrow().column_names


class TestRenameSortMetadata:
    def test_rename_sort_column_keeps_window_order(self):
        """Renaming a sort column must keep Rust-side ordering for windows."""
        df = pd.DataFrame({"k": [3, 1, 2], "v": [30.0, 10.0, 20.0]})
        r = LTSeq.from_pandas(df).sort("k").rename(k="k2")
        assert r.is_sorted_by("k2")
        # The Rust kernel (not just the Python mirror) must know the new name,
        # or window ORDER BY silently degrades to physical order.
        assert r._inner.get_sort_keys() == [("k2", False)]
        out = r.derive(lambda r: {"prev": r.v.shift(1)}).to_pandas()
        s = out.sort_values("k2").reset_index(drop=True)
        assert s["prev"].tolist()[1:] == [10.0, 20.0]


class TestDropSortPrefix:
    def test_drop_secondary_sort_key_keeps_prefix(self):
        """sorted by (a,b) then drop b: still sorted by the prefix (a)."""
        df = pd.DataFrame({"a": [1, 1, 2], "b": [2, 1, 1], "v": [1.0, 2.0, 3.0]})
        r = LTSeq.from_pandas(df).sort("a", "b").drop("b")
        assert r.is_sorted_by("a")
        assert r._inner.get_sort_keys() == [("a", False)]


class TestSemiAntiJoinSortPropagation:
    """semi/anti joins are order-preserving filters on the left table: the
    Rust kernel must propagate the left table's sort specs (a #98 over-reach
    cleared them along with regular joins)."""

    def _sorted_left(self):
        df = pd.DataFrame({"k": [3, 1, 2], "v": [30.0, 10.0, 20.0]})
        return LTSeq.from_pandas(df).sort("k")

    def test_semi_join_preserves_sort_order(self):
        t = self._sorted_left()
        matches = LTSeq.from_pandas(pd.DataFrame({"k": [1, 3]}))
        r = t.semi_join(matches, on=lambda a, b: a.k == b.k)
        assert r.is_sorted_by("k")
        assert r._inner.get_sort_keys() == [("k", False)]
        assert r.to_pandas()["k"].tolist() == [1, 3]

    def test_anti_join_preserves_sort_order(self):
        t = self._sorted_left()
        matches = LTSeq.from_pandas(pd.DataFrame({"k": [2]}))
        r = t.anti_join(matches, on=lambda a, b: a.k == b.k)
        assert r.is_sorted_by("k")
        assert r._inner.get_sort_keys() == [("k", False)]
        assert r.to_pandas()["k"].tolist() == [1, 3]


class TestLazyFirstSortKeysNoMaterialize:
    def test_sort_keys_read_does_not_materialize(self):
        """_LazyFirstLTSeq._sort_keys must answer from the source table
        instead of materializing the first-row join as a side effect."""
        df = pd.DataFrame({"g": [1, 1, 2], "v": [1.0, 2.0, 3.0]})
        t = LTSeq.from_pandas(df).sort("g")
        # .first() is statically typed as LTSeq; the lazy wrapper's
        # _materialized attribute is an implementation detail under test.
        lazy_first: Any = t.group_ordered(lambda r: r.g != r.g.shift(1)).first()
        assert lazy_first._sort_keys == [("g", False)]
        assert lazy_first._materialized is None


class TestEmptyTableGuard:
    def test_empty_schema_still_empty_dict(self):
        t = LTSeq()
        assert t.schema == {}
