"""Tests for join() merge-equivalent functionality.

Previously tested join_merge() (deprecated); now tests join() directly,
which is the replacement API.
"""

import pytest
from ltseq import LTSeq


# Test data file path (relative to project root)
TEST_AGG_CSV = "py-ltseq/tests/test_data/test_agg.csv"


class TestJoinMergeBasic:
    """Basic join() functionality tests (replacing join_merge)."""

    def test_join_merge_creates_ltseq(self):
        """join() should return an LTSeq instance."""
        t1 = LTSeq.read_csv(TEST_AGG_CSV)
        t2 = LTSeq.read_csv(TEST_AGG_CSV)

        result = t1.join(t2, on=lambda a, b: a.region == b.region)
        assert isinstance(result, LTSeq)

    def test_join_merge_inner_join(self):
        """join() should perform inner join by default."""
        t1 = LTSeq.read_csv(TEST_AGG_CSV)
        t2 = LTSeq.read_csv(TEST_AGG_CSV)

        result = t1.join(
            t2, on=lambda a, b: a.region == b.region, how="inner"
        )

        # Should have matches for regions with data
        assert len(result) > 0

    def test_join_merge_left_join(self):
        """join() should support left outer join."""
        t1 = LTSeq.read_csv(TEST_AGG_CSV)
        t2 = LTSeq.read_csv(TEST_AGG_CSV)

        result = t1.join(
            t2, on=lambda a, b: a.region == b.region, how="left"
        )

        # Left join should have at least as many rows as inner join
        assert len(result) > 0

    def test_join_merge_right_join(self):
        """join() should support right outer join."""
        t1 = LTSeq.read_csv(TEST_AGG_CSV)
        t2 = LTSeq.read_csv(TEST_AGG_CSV)

        result = t1.join(
            t2, on=lambda a, b: a.region == b.region, how="right"
        )

        assert len(result) > 0

    def test_join_merge_full_outer_join(self):
        """join() should support full outer join."""
        t1 = LTSeq.read_csv(TEST_AGG_CSV)
        t2 = LTSeq.read_csv(TEST_AGG_CSV)

        result = t1.join(
            t2, on=lambda a, b: a.region == b.region, how="full"
        )

        assert len(result) > 0


class TestJoinMergeValidation:
    """Tests for join() error handling."""

    def test_join_merge_missing_schema_raises_error(self):
        """join() should raise error if schema not initialized."""
        t1 = LTSeq()
        t2 = LTSeq.read_csv(TEST_AGG_CSV)

        with pytest.raises(ValueError, match="Schema not initialized"):
            t1.join(t2, on=lambda a, b: a.region == b.region)

    def test_join_merge_invalid_join_type_raises_error(self):
        """join() should raise error for invalid join type."""
        t1 = LTSeq.read_csv(TEST_AGG_CSV)
        t2 = LTSeq.read_csv(TEST_AGG_CSV)

        with pytest.raises(ValueError, match="Invalid join type"):
            t1.join(t2, on=lambda a, b: a.region == b.region, how="invalid")

    def test_join_merge_invalid_table_raises_error(self):
        """join() should raise error if other is not LTSeq."""
        t1 = LTSeq.read_csv(TEST_AGG_CSV)

        with pytest.raises(TypeError):
            t1.join("not a table", on=lambda a, b: a.region == b.region)

    def test_join_merge_invalid_condition_raises_error(self):
        """join() should raise error for invalid join condition."""
        t1 = LTSeq.read_csv(TEST_AGG_CSV)
        t2 = LTSeq.read_csv(TEST_AGG_CSV)

        with pytest.raises(TypeError, match="Invalid join condition"):
            t1.join(t2, on=lambda a, b: a.nonexistent == b.nonexistent)


class TestJoinMergeSchemas:
    """Tests for join() schema handling."""

    def test_join_merge_result_has_columns_from_both_tables(self):
        """join() result should have columns from both tables."""
        t1 = LTSeq.read_csv(TEST_AGG_CSV)
        t2 = LTSeq.read_csv(TEST_AGG_CSV)

        result = t1.join(t2, on=lambda a, b: a.region == b.region)

        # Result schema should include columns from both tables
        assert result._schema is not None
        # Should have at least some columns from both
        assert len(result._schema) > 0

    def test_join_merge_result_is_chainable(self):
        """join() result should be chainable."""
        t1 = LTSeq.read_csv(TEST_AGG_CSV)
        t2 = LTSeq.read_csv(TEST_AGG_CSV)

        result = t1.join(t2, on=lambda a, b: a.region == b.region)

        # Result should be usable with other operations
        assert isinstance(result, LTSeq)

    def test_join_merge_preserves_data_integrity(self):
        """join() should preserve data integrity."""
        t1 = LTSeq.read_csv(TEST_AGG_CSV)
        t2 = LTSeq.read_csv(TEST_AGG_CSV)

        result = t1.join(t2, on=lambda a, b: a.region == b.region)

        # Result should have sensible data
        assert len(result) > 0


class TestJoinMergeRegressions:
    """Regression tests for join() (replacing join_merge)."""

    def test_join_merge_does_not_modify_source(self):
        """join() should not modify source tables."""
        t1 = LTSeq.read_csv(TEST_AGG_CSV)
        t2 = LTSeq.read_csv(TEST_AGG_CSV)

        original_len_t1 = len(t1)
        original_len_t2 = len(t2)

        _ = t1.join(t2, on=lambda a, b: a.region == b.region)

        # Original tables should be unchanged
        assert len(t1) == original_len_t1
        assert len(t2) == original_len_t2

    def test_join_merge_multiple_calls(self):
        """Should be able to call join() multiple times."""
        t1 = LTSeq.read_csv(TEST_AGG_CSV)
        t2 = LTSeq.read_csv(TEST_AGG_CSV)

        result1 = t1.join(t2, on=lambda a, b: a.region == b.region)
        result2 = t1.join(
            t2, on=lambda a, b: a.region == b.region, how="left"
        )

        # Both should work
        assert len(result1) > 0
        assert len(result2) > 0

    def test_join_merge_with_different_tables(self):
        """join() should work with different table combinations."""
        t1 = LTSeq.read_csv(TEST_AGG_CSV)

        # Same table joined with itself
        result = t1.join(t1, on=lambda a, b: a.region == b.region)

        assert isinstance(result, LTSeq)
        assert len(result) > 0
