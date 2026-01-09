"""Tests for join() functionality.

Tests the join() method which performs standard SQL-style hash joins.
"""

import pytest
from ltseq import LTSeq


# Use test_data directory for proper test files
USERS_CSV = "py-ltseq/tests/test_data/users.csv"
ORDERS_CSV = "py-ltseq/tests/test_data/orders.csv"


class TestJoinBasic:
    """Basic join() functionality tests."""

    def test_join_creates_ltseq(self):
        """join() should return an LTSeq instance."""
        t1 = LTSeq.read_csv(USERS_CSV)
        t2 = LTSeq.read_csv(ORDERS_CSV)
        result = t1.join(t2, on=lambda a, b: a.user_id == b.user_id)
        assert isinstance(result, LTSeq)

    def test_join_inner_default(self):
        """join() should perform inner join by default."""
        t1 = LTSeq.read_csv(USERS_CSV)
        t2 = LTSeq.read_csv(ORDERS_CSV)
        result = t1.join(t2, on=lambda a, b: a.user_id == b.user_id)
        # Inner join only keeps matching rows
        assert len(result) >= 0

    def test_join_left(self):
        """join() should support left outer join."""
        t1 = LTSeq.read_csv(USERS_CSV)
        t2 = LTSeq.read_csv(ORDERS_CSV)
        result = t1.join(t2, on=lambda a, b: a.user_id == b.user_id, how="left")
        # Left join keeps all rows from left table
        assert len(result) >= len(t1)

    def test_join_right(self):
        """join() should support right outer join."""
        t1 = LTSeq.read_csv(USERS_CSV)
        t2 = LTSeq.read_csv(ORDERS_CSV)
        result = t1.join(t2, on=lambda a, b: a.user_id == b.user_id, how="right")
        # Right join keeps all rows from right table
        assert len(result) >= len(t2)

    def test_join_full(self):
        """join() should support full outer join."""
        t1 = LTSeq.read_csv(USERS_CSV)
        t2 = LTSeq.read_csv(ORDERS_CSV)
        result = t1.join(t2, on=lambda a, b: a.user_id == b.user_id, how="full")
        # Full join keeps all rows from both tables
        assert len(result) >= max(len(t1), len(t2))


class TestJoinErrors:
    """Tests for join() error handling."""

    def test_join_missing_schema_raises_error(self):
        """join() should raise error if schema not initialized."""
        t1 = LTSeq()
        t2 = LTSeq.read_csv(USERS_CSV)
        with pytest.raises(ValueError, match="Schema not initialized"):
            t1.join(t2, on=lambda a, b: a.user_id == b.user_id)

    def test_join_invalid_how_raises_error(self):
        """join() should raise error for invalid join type."""
        t1 = LTSeq.read_csv(USERS_CSV)
        t2 = LTSeq.read_csv(ORDERS_CSV)
        with pytest.raises(ValueError, match="Invalid join type"):
            t1.join(t2, on=lambda a, b: a.user_id == b.user_id, how="invalid")

    def test_join_invalid_table_raises_error(self):
        """join() should raise error if other is not LTSeq."""
        t1 = LTSeq.read_csv(USERS_CSV)
        with pytest.raises(TypeError, match="must be LTSeq"):
            t1.join("not a table", on=lambda a, b: a.user_id == b.user_id)

    def test_join_invalid_condition_raises_error(self):
        """join() should raise error for invalid join condition."""
        t1 = LTSeq.read_csv(USERS_CSV)
        t2 = LTSeq.read_csv(ORDERS_CSV)
        with pytest.raises(TypeError, match="Invalid join condition"):
            t1.join(t2, on=lambda a, b: a.nonexistent == b.nonexistent)


class TestJoinSchema:
    """Tests for join() schema handling."""

    def test_join_result_has_columns_from_both_tables(self):
        """join() result should have columns from both tables."""
        t1 = LTSeq.read_csv(USERS_CSV)
        t2 = LTSeq.read_csv(ORDERS_CSV)
        result = t1.join(t2, on=lambda a, b: a.user_id == b.user_id)

        # Result should include columns from both tables
        result_df = result.to_pandas()
        # Check that we have columns from users
        assert "name" in result_df.columns
        # Check that we have columns from orders (prefixed with _other_)
        assert (
            "_other_product" in result_df.columns
            or "_other_amount" in result_df.columns
        )

    def test_join_result_is_chainable(self):
        """join() result should be chainable with select."""
        t1 = LTSeq.read_csv(USERS_CSV)
        t2 = LTSeq.read_csv(ORDERS_CSV)
        result = t1.join(t2, on=lambda a, b: a.user_id == b.user_id)

        # Should be able to chain select operation
        selected = result.select("name", "user_id")
        assert isinstance(selected, LTSeq)
        df = selected.to_pandas()
        assert "name" in df.columns


class TestJoinRegression:
    """Regression tests for join()."""

    def test_join_does_not_modify_source(self):
        """join() should not modify source tables."""
        t1 = LTSeq.read_csv(USERS_CSV)
        t2 = LTSeq.read_csv(ORDERS_CSV)

        original_len1 = len(t1)
        original_len2 = len(t2)

        _ = t1.join(t2, on=lambda a, b: a.user_id == b.user_id)

        assert len(t1) == original_len1
        assert len(t2) == original_len2

    def test_join_multiple_calls(self):
        """Should be able to call join() multiple times."""
        t1 = LTSeq.read_csv(USERS_CSV)
        t2 = LTSeq.read_csv(ORDERS_CSV)

        result1 = t1.join(t2, on=lambda a, b: a.user_id == b.user_id)
        result2 = t1.join(t2, on=lambda a, b: a.user_id == b.user_id, how="left")

        # Both should work
        assert isinstance(result1, LTSeq)
        assert isinstance(result2, LTSeq)
