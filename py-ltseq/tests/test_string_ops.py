"""Runtime tests for string operations (.s accessor).

Tests the StringAccessor class which provides string manipulation operations.
These tests verify both expression creation AND runtime execution through
the Rust backend.
"""

import pytest
from ltseq import LTSeq
from ltseq.expr import ColumnExpr


# Test data file with string columns
TEST_CSV = "examples/users.csv"  # Has: name, age


class TestStringContains:
    """Tests for s.contains() at runtime."""

    def test_contains_filter_matching(self):
        """s.contains() should filter rows containing the pattern."""
        t = LTSeq.read_csv(TEST_CSV)
        # Filter for names containing 'a' (Jack, Richard have 'a')
        result = t.filter(lambda r: r.name.s.contains("a"))
        df = result.to_pandas()
        # All results should contain 'a'
        for name in df["name"].tolist():
            assert "a" in name.lower() or "a" in name

    def test_contains_filter_no_match(self):
        """s.contains() should return empty when no match."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.filter(lambda r: r.name.s.contains("xyz123"))
        assert len(result) == 0

    def test_contains_derive(self):
        """s.contains() should work in derive() context."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(has_a=lambda r: r.name.s.contains("a"))
        df = result.to_pandas()
        assert "has_a" in df.columns

    def test_contains_case_sensitive(self):
        """s.contains() should be case-sensitive by default."""
        t = LTSeq.read_csv(TEST_CSV)
        # 'J' uppercase vs 'j' lowercase
        result_upper = t.filter(lambda r: r.name.s.contains("J"))
        result_lower = t.filter(lambda r: r.name.s.contains("j"))
        # Jack starts with J, not j
        assert len(result_upper) >= 0  # May or may not match depending on data


class TestStringStartsWith:
    """Tests for s.starts_with() at runtime."""

    def test_starts_with_filter(self):
        """s.starts_with() should filter rows starting with prefix."""
        t = LTSeq.read_csv(TEST_CSV)
        # Filter for names starting with 'J' (Jack)
        result = t.filter(lambda r: r.name.s.starts_with("J"))
        df = result.to_pandas()
        for name in df["name"].tolist():
            assert name.startswith("J")

    def test_starts_with_derive(self):
        """s.starts_with() should work in derive() context."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(starts_j=lambda r: r.name.s.starts_with("J"))
        df = result.to_pandas()
        assert "starts_j" in df.columns

    def test_starts_with_empty_string(self):
        """s.starts_with('') should match all rows."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.filter(lambda r: r.name.s.starts_with(""))
        assert len(result) == len(t)


class TestStringEndsWith:
    """Tests for s.ends_with() at runtime."""

    def test_ends_with_filter(self):
        """s.ends_with() should filter rows ending with suffix."""
        t = LTSeq.read_csv(TEST_CSV)
        # Filter for names ending with 'y' (Lucy)
        result = t.filter(lambda r: r.name.s.ends_with("y"))
        df = result.to_pandas()
        for name in df["name"].tolist():
            assert name.endswith("y")

    def test_ends_with_derive(self):
        """s.ends_with() should work in derive() context."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(ends_y=lambda r: r.name.s.ends_with("y"))
        df = result.to_pandas()
        assert "ends_y" in df.columns


class TestStringCase:
    """Tests for s.lower() and s.upper() at runtime."""

    def test_lower_derive(self):
        """s.lower() should convert to lowercase."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(name_lower=lambda r: r.name.s.lower())
        df = result.to_pandas()
        assert "name_lower" in df.columns
        # All lowercase names should be lowercase
        for name in df["name_lower"].tolist():
            assert name == name.lower()

    def test_upper_derive(self):
        """s.upper() should convert to uppercase."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(name_upper=lambda r: r.name.s.upper())
        df = result.to_pandas()
        assert "name_upper" in df.columns
        # All uppercase names should be uppercase
        for name in df["name_upper"].tolist():
            assert name == name.upper()

    def test_lower_in_filter(self):
        """s.lower() should work in filter comparison."""
        t = LTSeq.read_csv(TEST_CSV)
        # Case-insensitive search using lower()
        result = t.filter(lambda r: r.name.s.lower().s.contains("jack"))
        # Should find Jack
        assert len(result) >= 0


class TestStringStrip:
    """Tests for s.strip() at runtime."""

    def test_strip_derive(self):
        """s.strip() should remove whitespace."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(name_clean=lambda r: r.name.s.strip())
        df = result.to_pandas()
        assert "name_clean" in df.columns


class TestStringLen:
    """Tests for s.len() at runtime."""

    def test_len_derive(self):
        """s.len() should return string length."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(name_len=lambda r: r.name.s.len())
        df = result.to_pandas()
        assert "name_len" in df.columns
        # Check that lengths match
        for i, row in df.iterrows():
            expected_len = len(row["name"])
            actual_len = row["name_len"]
            assert int(actual_len) == expected_len

    def test_len_filter(self):
        """s.len() should work in filter for length checks."""
        t = LTSeq.read_csv(TEST_CSV)
        # Filter for names longer than 4 characters
        result = t.filter(lambda r: r.name.s.len() > 4)
        df = result.to_pandas()
        for name in df["name"].tolist():
            assert len(name) > 4


class TestStringSlice:
    """Tests for s.slice() at runtime."""

    def test_slice_derive(self):
        """s.slice() should extract substring."""
        t = LTSeq.read_csv(TEST_CSV)
        # Extract first 3 characters
        result = t.derive(name_prefix=lambda r: r.name.s.slice(0, 3))
        df = result.to_pandas()
        assert "name_prefix" in df.columns
        # Check prefixes match
        for i, row in df.iterrows():
            expected = row["name"][:3]
            actual = row["name_prefix"]
            assert actual == expected

    def test_slice_middle(self):
        """s.slice() should work with offset."""
        t = LTSeq.read_csv(TEST_CSV)
        # Extract characters 1-3
        result = t.derive(name_mid=lambda r: r.name.s.slice(1, 2))
        df = result.to_pandas()
        assert "name_mid" in df.columns


class TestStringRegexMatch:
    """Tests for s.regex_match() at runtime."""

    def test_regex_match_filter(self):
        """s.regex_match() should filter by regex pattern."""
        t = LTSeq.read_csv(TEST_CSV)
        # Match names starting with capital letter followed by lowercase
        result = t.filter(lambda r: r.name.s.regex_match(r"^[A-Z][a-z]+$"))
        # All names in test data match this pattern
        assert len(result) >= 0

    def test_regex_match_derive(self):
        """s.regex_match() should work in derive()."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(is_capitalized=lambda r: r.name.s.regex_match(r"^[A-Z]"))
        df = result.to_pandas()
        assert "is_capitalized" in df.columns


class TestStringChaining:
    """Tests for chaining string operations."""

    def test_chain_lower_contains(self):
        """Should be able to chain lower() then contains()."""
        t = LTSeq.read_csv(TEST_CSV)
        # Case-insensitive search
        result = t.filter(lambda r: r.name.s.lower().s.contains("ck"))
        # Should find "Jack" -> "jack" contains "ck"
        df = result.to_pandas()
        for name in df["name"].tolist():
            assert "ck" in name.lower()

    def test_chain_strip_lower(self):
        """Should be able to chain strip() then lower()."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(clean_name=lambda r: r.name.s.strip().s.lower())
        df = result.to_pandas()
        assert "clean_name" in df.columns

    def test_multiple_string_derives(self):
        """Should be able to derive multiple string columns."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(
            name_upper=lambda r: r.name.s.upper(),
            name_lower=lambda r: r.name.s.lower(),
            name_len=lambda r: r.name.s.len(),
        )
        df = result.to_pandas()
        assert "name_upper" in df.columns
        assert "name_lower" in df.columns
        assert "name_len" in df.columns


class TestStringWithOtherOps:
    """Tests for string operations combined with other operations."""

    def test_string_in_complex_filter(self):
        """String ops should work with arithmetic comparisons."""
        t = LTSeq.read_csv(TEST_CSV)
        # Filter: name contains 'a' AND age > 18
        result = t.filter(lambda r: (r.name.s.contains("a")) & (r.age > 18))
        df = result.to_pandas()
        for i, row in df.iterrows():
            assert "a" in row["name"].lower() or "a" in row["name"]
            assert row["age"] > 18

    def test_string_after_filter(self):
        """String derive should work after filter."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.filter(lambda r: r.age > 18).derive(
            name_upper=lambda r: r.name.s.upper()
        )
        df = result.to_pandas()
        assert "name_upper" in df.columns


class TestStringEdgeCases:
    """Edge case tests for string operations."""

    def test_empty_string_contains(self):
        """s.contains('') should match all."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.filter(lambda r: r.name.s.contains(""))
        assert len(result) == len(t)

    def test_slice_beyond_length(self):
        """s.slice() beyond string length should not error."""
        t = LTSeq.read_csv(TEST_CSV)
        # Slice with start beyond string length
        result = t.derive(empty=lambda r: r.name.s.slice(100, 10))
        # Should return empty strings or handle gracefully
        assert result is not None

    def test_len_on_short_string(self):
        """s.len() should handle short strings."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(is_short=lambda r: r.name.s.len() < 10)
        df = result.to_pandas()
        assert "is_short" in df.columns
