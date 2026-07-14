"""Ordered-API invariants (issue #125 findings 1/2/3/5).

Every API whose semantics depend on row order must require a declared
order (.sort() or .assume_sorted()) and raise SortRequiredError otherwise;
sort metadata must always be a valid prefix of the actual sort order.
"""

import pandas as pd
import pytest

from ltseq import LTSeq
from ltseq.exceptions import SortRequiredError


@pytest.fixture
def unsorted():
    df = pd.DataFrame({"g": [1, 2, 1], "v": [10.0, 20.0, 30.0]})
    return LTSeq.from_pandas(df)


class TestCumSumRequiresOrder:
    def test_unsorted_raises(self, unsorted):
        with pytest.raises(SortRequiredError):
            unsorted.cum_sum("v")

    def test_after_sort_works(self, unsorted):
        result = unsorted.sort("g").cum_sum("v")
        assert "v_cumsum" in result.columns

    def test_after_assume_sorted_works(self):
        df = pd.DataFrame({"g": [1, 1, 2], "v": [1.0, 2.0, 3.0]})
        t = LTSeq.from_pandas(df).assume_sorted("g")
        result = t.cum_sum("v")
        assert result.to_pandas()["v_cumsum"].tolist() == [1.0, 3.0, 6.0]


class TestGroupSortedValidates:
    def test_unsorted_raises(self, unsorted):
        with pytest.raises(SortRequiredError):
            unsorted.group_sorted(lambda r: r.g)

    def test_wrong_key_raises(self):
        """Table sorted by v, grouped by g — interleaved keys would be split
        into multiple runs instead of one group per key."""
        df = pd.DataFrame({"g": ["A", "B", "A"], "v": [1, 2, 3]})
        t = LTSeq.from_pandas(df).sort("v")
        with pytest.raises(SortRequiredError):
            t.group_sorted(lambda r: r.g)

    def test_matching_sort_key_works(self):
        df = pd.DataFrame({"g": ["A", "A", "B"], "v": [1, 2, 3]})
        t = LTSeq.from_pandas(df).sort("g")
        groups = t.group_sorted(lambda r: r.g)
        assert groups.first().count() == 2

    def test_sort_key_prefix_works(self):
        """Grouping by the FIRST sort key of a multi-key sort is valid."""
        df = pd.DataFrame({"g": ["A", "A", "B"], "v": [2, 1, 3]})
        t = LTSeq.from_pandas(df).sort("g", "v")
        groups = t.group_sorted(lambda r: r.g)
        assert groups.first().count() == 2

    def test_assume_sorted_is_the_trust_escape_hatch(self):
        """Callers who know their data is ordered declare it — no extra
        unsafe API needed."""
        df = pd.DataFrame({"g": ["A", "A", "B"], "v": [1, 2, 3]})
        t = LTSeq.from_pandas(df).assume_sorted("g")
        groups = t.group_sorted(lambda r: r.g)
        assert groups.first().count() == 2


class TestGroupOrderedRequiresOrder:
    def test_unsorted_raises(self, unsorted):
        with pytest.raises(SortRequiredError):
            unsorted.group_ordered(lambda r: r.g)

    def test_after_sort_works(self, unsorted):
        groups = unsorted.sort("g").group_ordered(lambda r: r.g)
        assert groups.first().count() == 2

    def test_after_assume_sorted_works(self):
        df = pd.DataFrame({"g": [1, 1, 2], "v": [1.0, 2.0, 3.0]})
        t = LTSeq.from_pandas(df).assume_sorted("g")
        groups = t.group_ordered(lambda r: r.g)
        assert groups.first().count() == 2


class TestComputedSortMetadataPrefix:
    def test_pure_computed_sort_has_no_keys(self):
        df = pd.DataFrame({"a": [3, 1, 2]})
        t = LTSeq.from_pandas(df).sort(lambda r: r.a * 2)
        assert t.sort_keys is None
        assert t._inner.get_sort_keys() == []

    def test_mixed_sort_truncates_at_first_computed_key(self):
        """sort("a", computed, "c"): metadata must be [a], never [a, c] —
        [a, c] is not a prefix of the true order (a, b*2, c)."""
        df = pd.DataFrame({"a": [1, 1, 2], "b": [3, 1, 2], "c": [9, 8, 7]})
        t = LTSeq.from_pandas(df).sort("a", lambda r: r.b * 2, "c")
        assert t._inner.get_sort_keys() == [("a", False)]
        assert t.is_sorted_by("a")
        assert not t.is_sorted_by("a", "c")

    def test_assume_sorted_mixed_truncates_too(self):
        df = pd.DataFrame({"a": [1, 1, 2], "b": [1, 2, 3], "c": [9, 8, 7]})
        t = LTSeq.from_pandas(df).assume_sorted("a", lambda r: r.b * 2, "c")
        assert t._inner.get_sort_keys() == [("a", False)]

    def test_window_after_mixed_sort_uses_prefix(self):
        """Windows after a mixed computed sort run off the truncated prefix
        metadata. The declared order is only the prefix — tie order among
        rows equal on the prefix is unspecified — so the guarantee is
        self-consistency: output sorted by the prefix, and shift(1) matching
        the output's own row sequence."""
        df = pd.DataFrame({"a": [1, 1, 2], "b": [3, 1, 2], "c": [9, 8, 7]})
        t = LTSeq.from_pandas(df).sort("a", lambda r: r.b * 2, "c")
        result = t.derive(prev_c=lambda r: r.c.shift(1)).to_pandas()
        assert result["a"].tolist() == sorted(result["a"].tolist())
        prev = result["prev_c"].tolist()
        assert pd.isna(prev[0])
        assert prev[1:] == [float(x) for x in result["c"].tolist()[:-1]]


class TestMultiBatchRegression:
    """Ordered ops must stay correct when data spans multiple record
    batches (issue #125 finding 3: undeclared order is unstable on
    multi-batch/multi-partition plans — declared order must not be)."""

    def test_group_ordered_and_cum_sum_across_batches(self, tmp_path):
        n = 50_000
        run = 1_000
        csv = tmp_path / "multibatch.csv"
        with open(csv, "w") as f:
            f.write("cat,v\n")
            for i in range(n):
                f.write(f"{i // run},1\n")

        t = LTSeq.read_csv(str(csv)).assume_sorted("cat")

        sized = t.group_ordered(lambda r: r.cat).derive(
            lambda g: {"sz": g.count()}
        )
        df = sized.to_pandas()
        assert len(df) == n
        assert (df["sz"] == run).all()
        assert df["cat"].tolist() == [i // run for i in range(n)]

        cs = t.cum_sum("v").to_pandas()["v_cumsum"]
        assert cs.tolist() == list(range(1, n + 1))
