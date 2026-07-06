"""Integration tests for the native group-window path (issue #91 PR 4).

NestedTable.derive/filter now send serialized group-dialect dicts to Rust
(derive_group_window / filter_group_window) instead of SQL strings.
"""

import pandas as pd
import pytest

from ltseq import LTSeq


@pytest.fixture
def grouped():
    df = pd.DataFrame(
        {
            "g": [1, 1, 1, 2, 2, 3],
            "v": [5.0, 1.0, 3.0, 10.0, 20.0, 7.0],
        }
    )
    t = LTSeq.from_pandas(df).sort("g")
    return t.group_ordered(lambda r: r.g != r.g.shift(1))


class TestDeriveHygiene:
    def test_internals_stripped_and_order_preserved(self, grouped):
        """All three internal columns are absent (the legacy SQL path leaked
        __group_count__), derived columns come after originals, and row order
        matches the input."""
        result = grouped.derive(lambda g: {"n": g.count()})
        df = result.to_pandas()
        assert list(df.columns) == ["g", "v", "n"]
        assert df["g"].tolist() == [1, 1, 1, 2, 2, 3]
        assert df["v"].tolist() == [5.0, 1.0, 3.0, 10.0, 20.0, 7.0]
        assert df["n"].tolist() == [3, 3, 3, 2, 2, 1]

    def test_derived_column_has_real_dtype(self, grouped):
        """Rust owns the schema: count is a real integer type, not a guess."""
        result = grouped.derive(lambda g: {"n": g.count()})
        assert result.schema["n"] == "int64"


class TestShiftBasedGroupingLambda:
    def test_flatten_keeps_user_columns(self, grouped):
        """Pre-existing hole fixed: shift-based grouping lambdas used to route
        through the metadata-only linear scan, dropping every user column
        from flatten()/derive()/filter()."""
        flat = grouped.flatten()
        df = flat.to_pandas()
        assert "g" in df.columns and "v" in df.columns
        assert len(df) == 6

    def test_derive_span(self, grouped):
        result = grouped.derive(
            lambda g: {"span": g.last().v - g.first().v}
        )
        df = result.to_pandas()
        assert df["span"].tolist() == [-2.0, -2.0, -2.0, 10.0, 10.0, 0.0]


class TestStagedFilterCombos:
    def test_multiple_quantifiers_and_aggregate(self, grouped):
        result = grouped.filter(
            lambda g: g.all(lambda r: r.v > 0) & (g.count() >= 2)
        )
        df = result.to_pandas()
        assert df["g"].tolist() == [1, 1, 1, 2, 2]

    def test_quantifier_or_count(self, grouped):
        result = grouped.filter(
            lambda g: g.any(lambda r: r.v > 15.0) | (g.count() >= 3)
        )
        df = result.to_pandas()
        # group 1 (count 3) and group 2 (has v=20) pass; group 3 fails
        assert df["g"].tolist() == [1, 1, 1, 2, 2]

    def test_negation(self, grouped):
        result = grouped.filter(lambda g: ~(g.count() > 2))
        df = result.to_pandas()
        assert df["g"].tolist() == [2, 2, 3]

    def test_window_vs_window(self, grouped):
        """Comparing two group windows: groups where last < first."""
        result = grouped.filter(lambda g: g.last().v < g.first().v)
        df = result.to_pandas()
        assert df["g"].tolist() == [1, 1, 1]

    def test_none_quantifier(self, grouped):
        result = grouped.filter(lambda g: g.none(lambda r: r.v > 15.0))
        df = result.to_pandas()
        assert sorted(df["g"].unique().tolist()) == [1, 3]


class TestChains:
    def test_filter_then_derive(self, grouped):
        result = grouped.filter(lambda g: g.count() >= 2).derive(
            lambda g: {"vmax": g.max("v")}
        )
        df = result.to_pandas()
        assert list(df.columns) == ["g", "v", "vmax"]
        assert df["vmax"].tolist() == [5.0, 5.0, 5.0, 20.0, 20.0]

    def test_filter_then_filter(self, grouped):
        result = grouped.filter(lambda g: g.count() >= 2).filter(
            lambda g: g.max("v") > 10.0
        )
        df = result.to_pandas()
        assert df["g"].tolist() == [2, 2]


class TestEdges:
    def test_empty_table(self):
        df = pd.DataFrame({"g": pd.Series([], dtype="int64"), "v": pd.Series([], dtype="float64")})
        t = LTSeq.from_pandas(df)
        grouped = t.group_ordered(lambda r: r.g)
        result = grouped.derive(lambda g: {"n": g.count()})
        assert len(result) == 0

    def test_single_group(self):
        df = pd.DataFrame({"g": [7, 7], "v": [1.0, 2.0]})
        grouped = LTSeq.from_pandas(df).sort("g").group_ordered(lambda r: r.g)
        result = grouped.filter(lambda g: g.count() == 2)
        assert len(result.to_pandas()) == 2

    def test_quantifier_with_is_null(self):
        """Widened surface: inner predicates get the full row dialect."""
        df = pd.DataFrame({"g": [1, 1, 2], "v": [1.0, None, 3.0]})
        grouped = LTSeq.from_pandas(df).sort("g").group_ordered(lambda r: r.g)
        result = grouped.filter(lambda g: g.none(lambda r: r.v.is_null()))
        assert result.to_pandas()["g"].tolist() == [2]

    def test_first_value_with_leading_null(self):
        """first/last respect nulls (parity with the legacy SQL FIRST_VALUE)."""
        df = pd.DataFrame({"g": [1, 1, 2, 2], "v": [None, 2.0, 3.0, 4.0]})
        grouped = LTSeq.from_pandas(df).sort("g").group_ordered(lambda r: r.g)
        result = grouped.derive(lambda g: {"first_v": g.first().v})
        vals = result.to_pandas()["first_v"].tolist()
        assert pd.isna(vals[0]) and pd.isna(vals[1])
        assert vals[2:] == [3.0, 3.0]


class TestErrors:
    def test_quantifier_rejects_window_functions(self, grouped):
        with pytest.raises(Exception, match="cannot contain window functions"):
            grouped.filter(lambda g: g.all(lambda r: r.v.shift(1) > 0))

    def test_quantifier_unknown_column_raises_at_capture(self, grouped):
        with pytest.raises(AttributeError):
            grouped.filter(lambda g: g.all(lambda r: r.nonexistent > 0))
