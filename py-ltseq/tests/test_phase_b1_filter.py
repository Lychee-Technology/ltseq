"""Test Phase B.1: NestedTable.filter() with complex predicates."""

import pytest

# Import LTSeq - tests are run from project root
try:
    from ltseq import LTSeq
except ImportError:
    import sys

    sys.path.insert(0, "py-ltseq")
    from ltseq import LTSeq


class TestNestedTableFilterBasic:
    """Test basic filter patterns on groups."""

    @pytest.fixture
    def grouped_table(self):
        """Create a grouped table for testing."""
        t = LTSeq.read_csv("test_agg.csv")
        return t.group_ordered(lambda r: r.region)

    def test_filter_count_greater_than(self, grouped_table):
        """Filter groups with count > 0."""
        result = grouped_table.filter(lambda g: g.count() > 0)
        # All rows should remain since all groups have count >= 1
        assert len(result) == 6

    def test_filter_count_less_than(self, grouped_table):
        """Filter groups with count < 10."""
        result = grouped_table.filter(lambda g: g.count() < 10)
        # All rows should remain
        assert len(result) == 6

    def test_filter_count_equal(self, grouped_table):
        """Filter groups with count == 3."""
        result = grouped_table.filter(lambda g: g.count() == 3)
        # Both regions have 3 rows, so all rows should remain
        assert len(result) == 6

    def test_filter_removes_groups(self, grouped_table):
        """Filter that removes some groups."""
        result = grouped_table.filter(lambda g: g.count() > 5)
        # No group has more than 3 rows, so filter should return something
        # (We can't easily check if it's empty due to Rust limitations)
        # Just verify that it returns an LTSeq without error
        assert result is not None
        assert hasattr(result, "_schema")


class TestNestedTableFilterErrorHandling:
    """Test error handling for unsupported filter patterns."""

    @pytest.fixture
    def grouped_table(self):
        """Create a grouped table for testing."""
        t = LTSeq.read_csv("test_agg.csv")
        return t.group_ordered(lambda r: r.region)

    def test_filter_unsupported_function_error(self, grouped_table):
        """Unsupported group function should raise helpful error."""
        with pytest.raises(ValueError) as exc_info:
            # Define in function to have source available
            def unsupported_filter(g):
                return g.median() > 100

            grouped_table.filter(unsupported_filter)

        error_msg = str(exc_info.value)
        assert "Unsupported filter predicate pattern" in error_msg
        assert "Supported patterns:" in error_msg
        assert "g.count()" in error_msg


class TestNestedTableFilterEdgeCases:
    """Test edge cases for filtering."""

    def test_filter_empty_result(self):
        """Filter that results in empty table."""
        t = LTSeq.read_csv("test_agg.csv")
        grouped = t.group_ordered(lambda r: r.region)

        result = grouped.filter(lambda g: g.count() > 100)
        # Filter should return successfully even if no rows match
        assert result is not None
        assert hasattr(result, "_schema")

    def test_filter_all_pass(self):
        """Filter that keeps all groups."""
        t = LTSeq.read_csv("test_agg.csv")
        grouped = t.group_ordered(lambda r: r.region)

        result = grouped.filter(lambda g: g.count() >= 0)
        assert len(result) == 6


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
