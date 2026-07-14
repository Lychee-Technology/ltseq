"""Ordered-API invariants (issue #125 findings 1/2/3/5).

Every API whose semantics depend on row order must require a declared
order (.sort() or .assume_sorted()) and raise SortRequiredError otherwise;
sort metadata must always be a valid prefix of the actual sort order.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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

    def test_computed_key_rejected_on_unrelated_sort(self):
        """PR #126 review P1: a computed grouping key cannot be checked
        against column-level sort metadata — accepting it silently splits
        logical keys into multiple runs (here 3 runs for 2 key values)."""
        df = pd.DataFrame({"g": [1, 2, 1], "v": [1, 2, 3]})
        t = LTSeq.from_pandas(df).sort("v")
        with pytest.raises(SortRequiredError, match="derive"):
            t.group_sorted(lambda r: r.g + 0)

    def test_computed_key_rejected_even_when_source_column_is_sorted(self):
        """Sorted by g does not prove sorted by f(g) for arbitrary f, so the
        computed key is rejected uniformly; the migration is derive-then-sort."""
        df = pd.DataFrame({"g": [1, 1, 2], "v": [1, 2, 3]})
        t = LTSeq.from_pandas(df).sort("g")
        with pytest.raises(SortRequiredError):
            t.group_sorted(lambda r: r.g + 0)

    def test_derive_then_sort_is_the_computed_key_migration(self):
        df = pd.DataFrame({"g": [1, 2, 1], "v": [1, 2, 3]})
        t = LTSeq.from_pandas(df).derive(k=lambda r: r.g + 0).sort("k")
        groups = t.group_sorted(lambda r: r.k)
        sized = groups.derive(lambda gr: {"sz": gr.count()})
        assert sorted(sized.to_pandas()["sz"].tolist()) == [1, 2, 2]

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


class TestComputedOnlySortGuidance:
    """PR #126 review P2: after sort(lambda r: r.a * 2) the user HAS sorted,
    but computed keys cannot be represented in column-level sort metadata, so
    ordered APIs still (correctly) refuse — window ORDER BY built from that
    metadata would otherwise silently run unordered, which is the original
    finding-3 bug. The error must explain this and point at the
    derive-then-sort migration instead of just saying 'call .sort()'."""

    def test_cum_sum_error_explains_computed_keys(self):
        df = pd.DataFrame({"a": [3, 1, 2]})
        t = LTSeq.from_pandas(df).sort(lambda r: r.a * 2)
        with pytest.raises(SortRequiredError, match="[Cc]omputed"):
            t.cum_sum("a")

    def test_group_ordered_error_explains_computed_keys(self):
        df = pd.DataFrame({"a": [3, 1, 2]})
        t = LTSeq.from_pandas(df).sort(lambda r: r.a * 2)
        with pytest.raises(SortRequiredError, match="derive"):
            t.group_ordered(lambda r: r.a)

    def test_derive_then_sort_migration_works(self):
        df = pd.DataFrame({"a": [3, 1, 2]})
        t = LTSeq.from_pandas(df).derive(k=lambda r: r.a * 2).sort("k")
        result = t.cum_sum("a").to_pandas()
        assert result["a_cumsum"].tolist() == [1.0, 3.0, 6.0]


class TestEmptyTableDeclaredOrder:
    """PR #126 review P2: assume_sorted() on a filtered-to-empty table must
    keep a queryable zero-row table (schema intact, ordered ops working) —
    it is the one-line migration path for the breaking change, so it cannot
    degrade to a schema-only stub that raises 'No data loaded'."""

    @pytest.fixture
    def empty_declared(self):
        df = pd.DataFrame({"g": [1, 1, 2], "v": [1.0, 2.0, 3.0]})
        t = LTSeq.from_pandas(df).sort("v")
        return t.filter(lambda r: r.v < 0).assume_sorted("v")

    def test_len_is_zero(self, empty_declared):
        assert len(empty_declared) == 0

    def test_cum_sum_keeps_result_column(self, empty_declared):
        out = empty_declared.cum_sum("v")
        assert "v_cumsum" in out.columns
        assert len(out.to_pandas()) == 0

    def test_group_ordered_yields_no_groups(self, empty_declared):
        sized = empty_declared.group_ordered(lambda r: r.g).derive(
            lambda gr: {"sz": gr.count()}
        )
        assert len(sized.to_pandas()) == 0

    def test_sort_metadata_survives(self, empty_declared):
        assert empty_declared._inner.get_sort_keys() == [("v", False)]


class TestMultiPartitionRegression:
    """Issue #125 finding 3 acceptance: the original bug came from partition
    merge order, so ordered ops must be exercised on a genuinely
    multi-partition plan. Multiple Parquet files through the lazy
    assume_sorted path keep separate file groups (the non-Parquet path
    collect()s into a single MemTable partition and cannot cover this)."""

    def test_group_ordered_and_cum_sum_multi_partition(self, tmp_path):
        n = 40_000
        run = 700  # does not divide the per-file row count: runs span files
        files = 4
        per_file = n // files
        pdir = tmp_path / "parts"
        pdir.mkdir()
        for f in range(files):
            start = f * per_file
            pq.write_table(
                pa.table(
                    {
                        "cat": [(start + i) // run for i in range(per_file)],
                        "v": [1] * per_file,
                    }
                ),
                pdir / f"part-{f}.parquet",
            )

        t = LTSeq.read_parquet(str(pdir)).assume_sorted("cat")

        # Premise check: the physical plan must actually read multiple
        # file groups, otherwise this test silently degrades to the
        # single-partition case it exists to go beyond.
        import re

        _, physical = t.explain_plan()
        m = re.search(r"file_groups=\{(\d+) group", physical)
        assert m is not None and int(m.group(1)) >= 2, physical

        sized = t.group_ordered(lambda r: r.cat).derive(
            lambda gr: {"sz": gr.count()}
        )
        df = sized.to_pandas()
        expected_cat = [i // run for i in range(n)]
        expected_sz = [run if (c + 1) * run <= n else n - c * run for c in expected_cat]
        assert len(df) == n
        assert df["cat"].tolist() == expected_cat
        assert df["sz"].tolist() == expected_sz

        cs = t.cum_sum("v").to_pandas()["v_cumsum"]
        assert cs.tolist() == list(range(1, n + 1))


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
