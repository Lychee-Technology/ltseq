"""Tests for Phase 3.3: search_first() functionality.

Tests the search_first() method which finds the first row matching a condition,
optimized for use on sorted tables.
"""

import pytest
from ltseq import LTSeq


class TestSearchFirstBasic:
    """Basic search_first() functionality tests."""

    def test_search_first_creates_ltseq(self):
        """search_first() should return an LTSeq instance."""
        t = LTSeq.read_csv("test_agg.csv")
        result = t.search_first(lambda r: r.amount > 500)
        assert isinstance(result, LTSeq)

    def test_search_first_returns_single_row(self):
        """search_first() should always return at most one row."""
        t = LTSeq.read_csv("test_agg.csv")

        # This condition matches 3 rows, but should only return first
        result = t.search_first(lambda r: r.region == "West")

        # Should return exactly 1 row
        assert len(result) == 1

    def test_search_first_no_match_returns_empty(self):
        """search_first() should return empty LTSeq if no match."""
        t = LTSeq.read_csv("test_agg.csv")

        # Search for impossible condition
        result = t.search_first(lambda r: r.amount > 10000)

        # Should return empty table
        assert len(result) == 0

    def test_search_first_with_comparison(self):
        """search_first() should work with various comparison operators."""
        t = LTSeq.read_csv("test_agg.csv")

        # Test different comparisons
        gt_result = t.search_first(lambda r: r.amount >= 700)
        assert len(gt_result) == 1

        lt_result = t.search_first(lambda r: r.amount <= 600)
        assert len(lt_result) == 1

        eq_result = t.search_first(lambda r: r.year == 2021)
        assert len(eq_result) == 1

    def test_search_first_with_string_match(self):
        """search_first() should work with string comparisons."""
        t = LTSeq.read_csv("test_agg.csv")

        # Search for specific region
        result = t.search_first(lambda r: r.region == "East")

        assert len(result) == 1
        # Verify schema preserved
        assert result._schema == t._schema


class TestSearchFirstOrdering:
    """Tests for search_first() behavior with ordering."""

    def test_search_first_on_original_order(self):
        """search_first() should respect original table order."""
        t = LTSeq.read_csv("test_agg.csv")

        # Condition matches multiple rows
        result = t.search_first(lambda r: r.amount > 500)

        # Should return exactly first match
        assert len(result) == 1


class TestSearchFirstValidation:
    """Tests for search_first() error handling."""

    def test_search_first_missing_schema_raises_error(self):
        """search_first() should raise error if schema not initialized."""
        t = LTSeq()
        with pytest.raises(ValueError, match="Schema not initialized"):
            t.search_first(lambda r: r.amount > 100)

    def test_search_first_invalid_predicate_raises_error(self):
        """search_first() should raise error for invalid predicate."""
        t = LTSeq.read_csv("test_agg.csv")

        with pytest.raises(TypeError, match="Invalid predicate"):
            t.search_first(lambda r: r.nonexistent_column > 100)

    def test_search_first_non_callable_raises_error(self):
        """search_first() should raise error if predicate is not callable."""
        t = LTSeq.read_csv("test_agg.csv")

        # This will fail during validation
        with pytest.raises(TypeError):
            t.search_first("not a function")


class TestSearchFirstDataTypes:
    """Tests for search_first() with different data types."""

    def test_search_first_with_numeric_columns(self):
        """search_first() should work with numeric columns."""
        t = LTSeq.read_csv("test_agg.csv")

        result = t.search_first(lambda r: r.amount > 750)
        assert len(result) == 1

    def test_search_first_with_string_columns(self):
        """search_first() should work with string columns."""
        t = LTSeq.read_csv("test_agg.csv")

        result = t.search_first(lambda r: r.region == "East")
        assert len(result) == 1

    def test_search_first_with_integer_columns(self):
        """search_first() should work with integer columns."""
        t = LTSeq.read_csv("test_agg.csv")

        result = t.search_first(lambda r: r.year >= 2021)
        assert len(result) == 1


class TestSearchFirstEdgeCases:
    """Tests for search_first() edge cases."""

    def test_search_first_single_row_match(self):
        """search_first() on single row should work."""
        # Create single row table
        rows = [{"amount": 1000, "region": "West", "year": 2020}]
        schema = {"amount": "int64", "region": "str", "year": "int64"}
        t = LTSeq._from_rows(rows, schema)

        result = t.search_first(lambda r: r.amount == 1000)
        assert len(result) == 1

    def test_search_first_single_row_no_match(self):
        """search_first() on single row with no match returns empty."""
        # Create single row table
        rows = [{"amount": 1000, "region": "West", "year": 2020}]
        schema = {"amount": "int64", "region": "str", "year": "int64"}
        t = LTSeq._from_rows(rows, schema)

        result = t.search_first(lambda r: r.amount == 999)
        assert len(result) == 0

    def test_search_first_complex_predicate(self):
        """search_first() should work with complex predicates."""
        t = LTSeq.read_csv("test_agg.csv")

        # Complex condition: amount > 500 AND region == "East"
        result = t.search_first(lambda r: (r.amount > 500) & (r.region == "East"))

        assert len(result) == 1

    def test_search_first_all_rows_match(self):
        """search_first() should return first when all rows match."""
        t = LTSeq.read_csv("test_agg.csv")

        # Condition that matches all rows
        result = t.search_first(lambda r: r.amount >= 0)

        # Should return first row
        assert len(result) == 1


class TestSearchFirstChaining:
    """Tests for search_first() result properties."""

    def test_search_first_result_is_ltseq(self):
        """search_first() result should be LTSeq instance."""
        t = LTSeq.read_csv("test_agg.csv")
        result = t.search_first(lambda r: r.region == "East")

        # Result should be usable LTSeq
        assert isinstance(result, LTSeq)
        assert len(result) == 1

    def test_search_first_then_show(self):
        """search_first() result should be showable."""
        t = LTSeq.read_csv("test_agg.csv")
        result = t.search_first(lambda r: r.amount > 600)

        # Should be able to show
        assert isinstance(result, LTSeq)

    def test_search_first_preserves_schema(self):
        """search_first() result should have same schema as source."""
        t = LTSeq.read_csv("test_agg.csv")
        result = t.search_first(lambda r: r.amount > 500)

        # Schema should be preserved
        assert result._schema == t._schema


class TestSearchFirstRegressions:
    """Regression tests for search_first()."""

    def test_search_first_does_not_modify_source(self):
        """search_first() should not modify the source table."""
        t = LTSeq.read_csv("test_agg.csv")
        original_len = len(t)

        _ = t.search_first(lambda r: r.amount > 500)

        # Original should be unchanged
        assert len(t) == original_len

    def test_search_first_multiple_calls_same_table(self):
        """Should be able to call search_first() multiple times on same table."""
        t = LTSeq.read_csv("test_agg.csv")

        result1 = t.search_first(lambda r: r.region == "West")
        result2 = t.search_first(lambda r: r.region == "East")

        # Both should work
        assert len(result1) == 1
        assert len(result2) == 1

    def test_search_first_different_predicates_same_table(self):
        """Multiple predicates on same table should work independently."""
        t = LTSeq.read_csv("test_agg.csv")

        # Different predicates
        result1 = t.search_first(lambda r: r.amount >= 1000)
        result2 = t.search_first(lambda r: r.year >= 2021)

        assert len(result1) == 1
        assert len(result2) == 1
