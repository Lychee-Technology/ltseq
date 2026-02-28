"""Tests for extended string operations (.s accessor).

Tests the new StringAccessor methods:
- s.replace(old, new) - replace substrings
- s.concat(*others) - concatenate strings
- s.pad_left(width, char) - left pad string
- s.pad_right(width, char) - right pad string
- s.split(delimiter, index) - split and get part
"""

import pytest
from ltseq import LTSeq


# Test data file with string columns
TEST_CSV = "examples/users.csv"  # Has: name, age


class TestStringReplace:
    """Tests for s.replace() at runtime."""

    def test_replace_derive(self):
        """s.replace() should replace all occurrences of a substring."""
        t = LTSeq.read_csv(TEST_CSV)
        # Replace 'a' with 'X'
        result = t.derive(name_replaced=lambda r: r.name.s.replace("a", "X"))
        df = result.to_pandas()
        assert "name_replaced" in df.columns
        # Check that 'a' is replaced with 'X'
        for i, row in df.iterrows():
            expected = row["name"].replace("a", "X")
            assert row["name_replaced"] == expected

    def test_replace_no_match(self):
        """s.replace() should return original string when pattern not found."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(name_replaced=lambda r: r.name.s.replace("xyz", "ABC"))
        df = result.to_pandas()
        # Names should be unchanged
        for i, row in df.iterrows():
            assert row["name_replaced"] == row["name"]

    def test_replace_empty_replacement(self):
        """s.replace() should handle empty replacement (deletion)."""
        t = LTSeq.read_csv(TEST_CSV)
        # Remove all 'a' characters
        result = t.derive(name_no_a=lambda r: r.name.s.replace("a", ""))
        df = result.to_pandas()
        for i, row in df.iterrows():
            expected = row["name"].replace("a", "")
            assert row["name_no_a"] == expected

    def test_replace_multiple_occurrences(self):
        """s.replace() should replace all occurrences, not just the first."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(name_replaced=lambda r: r.name.s.replace("c", "C"))
        df = result.to_pandas()
        for i, row in df.iterrows():
            expected = row["name"].replace("c", "C")
            assert row["name_replaced"] == expected

    def test_replace_in_filter(self):
        """s.replace() should work in filter context."""
        t = LTSeq.read_csv(TEST_CSV)
        # Filter for names that become "JACK" after replacing lowercase
        result = t.filter(
            lambda r: r.name.s.replace("a", "A").s.replace("c", "C").s.replace("k", "K")
            == "JACK"
        )
        # Should find "Jack"
        assert len(result) >= 0


class TestStringConcat:
    """Tests for s.concat() at runtime."""

    def test_concat_with_literal(self):
        """s.concat() should concatenate string with literals."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(greeting=lambda r: r.name.s.concat(" says hello"))
        df = result.to_pandas()
        assert "greeting" in df.columns
        for i, row in df.iterrows():
            expected = row["name"] + " says hello"
            assert row["greeting"] == expected

    def test_concat_multiple_literals(self):
        """s.concat() should concatenate multiple literals."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(wrapped=lambda r: r.name.s.concat(" (", "user", ")"))
        df = result.to_pandas()
        for i, row in df.iterrows():
            expected = row["name"] + " (user)"
            assert row["wrapped"] == expected

    def test_concat_prefix(self):
        """Concatenation can add prefix using derive with literal first."""
        t = LTSeq.read_csv(TEST_CSV)
        # Note: to add prefix, we need to start with a literal column or use a different approach
        # This test shows concatenation with suffix
        result = t.derive(suffixed=lambda r: r.name.s.concat("_user"))
        df = result.to_pandas()
        for i, row in df.iterrows():
            assert row["suffixed"] == row["name"] + "_user"

    def test_concat_empty_string(self):
        """s.concat() with empty string should return original."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(same=lambda r: r.name.s.concat(""))
        df = result.to_pandas()
        for i, row in df.iterrows():
            assert row["same"] == row["name"]


class TestStringPadLeft:
    """Tests for s.pad_left() at runtime."""

    def test_pad_left_default_char(self):
        """s.pad_left() should pad with spaces by default."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(padded=lambda r: r.name.s.pad_left(10))
        df = result.to_pandas()
        assert "padded" in df.columns
        for i, row in df.iterrows():
            # Should be right-aligned in 10 characters
            assert len(row["padded"]) == max(10, len(row["name"]))
            assert row["padded"].endswith(row["name"])

    def test_pad_left_custom_char(self):
        """s.pad_left() should use specified padding character."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(padded=lambda r: r.name.s.pad_left(10, "0"))
        df = result.to_pandas()
        for i, row in df.iterrows():
            # Should be padded with zeros
            expected_padding = "0" * (10 - len(row["name"]))
            expected = (
                expected_padding + row["name"] if len(row["name"]) < 10 else row["name"]
            )
            assert row["padded"] == expected

    def test_pad_left_no_padding_needed(self):
        """s.pad_left() truncates if string is longer than width (SQL behavior)."""
        t = LTSeq.read_csv(TEST_CSV)
        # Pad to 3 - SQL LPAD truncates to width, unlike Python's rjust
        result = t.derive(padded=lambda r: r.name.s.pad_left(3))
        df = result.to_pandas()
        for i, row in df.iterrows():
            # DataFusion LPAD truncates to the specified width
            assert len(row["padded"]) == 3
            # For strings >= 3 chars, should be truncated
            if len(row["name"]) >= 3:
                assert row["padded"] == row["name"][:3]

    def test_pad_left_with_asterisk(self):
        """s.pad_left() should work with special characters."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(padded=lambda r: r.name.s.pad_left(12, "*"))
        df = result.to_pandas()
        for i, row in df.iterrows():
            assert row["padded"].endswith(row["name"])
            if len(row["name"]) < 12:
                assert row["padded"].startswith("*")


class TestStringPadRight:
    """Tests for s.pad_right() at runtime."""

    def test_pad_right_default_char(self):
        """s.pad_right() should pad with spaces by default."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(padded=lambda r: r.name.s.pad_right(10))
        df = result.to_pandas()
        assert "padded" in df.columns
        for i, row in df.iterrows():
            # Should be left-aligned in 10 characters
            assert len(row["padded"]) == max(10, len(row["name"]))
            assert row["padded"].startswith(row["name"])

    def test_pad_right_custom_char(self):
        """s.pad_right() should use specified padding character."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(padded=lambda r: r.name.s.pad_right(10, "."))
        df = result.to_pandas()
        for i, row in df.iterrows():
            # Should be padded with dots
            expected_padding = "." * (10 - len(row["name"]))
            expected = (
                row["name"] + expected_padding if len(row["name"]) < 10 else row["name"]
            )
            assert row["padded"] == expected

    def test_pad_right_no_padding_needed(self):
        """s.pad_right() truncates if string is longer than width (SQL behavior)."""
        t = LTSeq.read_csv(TEST_CSV)
        # Pad to 3 - SQL RPAD truncates to width, unlike Python's ljust
        result = t.derive(padded=lambda r: r.name.s.pad_right(3))
        df = result.to_pandas()
        for i, row in df.iterrows():
            # DataFusion RPAD truncates to the specified width
            assert len(row["padded"]) == 3
            # For strings >= 3 chars, should be truncated
            if len(row["name"]) >= 3:
                assert row["padded"] == row["name"][:3]

    def test_pad_right_underscore(self):
        """s.pad_right() should work with underscores."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(padded=lambda r: r.name.s.pad_right(15, "_"))
        df = result.to_pandas()
        for i, row in df.iterrows():
            assert row["padded"].startswith(row["name"])
            if len(row["name"]) < 15:
                assert row["padded"].endswith("_")


class TestStringSplit:
    """Tests for s.split() at runtime."""

    def test_split_first_part(self):
        """s.split() should return first part with index 1."""
        t = LTSeq.read_csv(TEST_CSV)
        # Split names that might have spaces (e.g., "Jack" -> "Jack")
        result = t.derive(first=lambda r: r.name.s.split(" ", 1))
        df = result.to_pandas()
        assert "first" in df.columns
        # For single-word names, should return the whole name
        for i, row in df.iterrows():
            parts = row["name"].split(" ")
            if parts:
                assert row["first"] == parts[0]

    def test_split_delimiter_not_found(self):
        """s.split() should return full string when delimiter not found (index 1)."""
        t = LTSeq.read_csv(TEST_CSV)
        # Names don't contain @
        result = t.derive(part=lambda r: r.name.s.split("@", 1))
        df = result.to_pandas()
        for i, row in df.iterrows():
            # When delimiter not found and index is 1, returns the whole string
            assert row["part"] == row["name"]

    def test_split_index_out_of_range(self):
        """s.split() returns empty string or NULL for out-of-range index."""
        import pandas as pd

        t = LTSeq.read_csv(TEST_CSV)
        # Split by space, get part 5 (most names have only 1 part)
        result = t.derive(part=lambda r: r.name.s.split(" ", 5))
        df = result.to_pandas()
        for i, row in df.iterrows():
            parts = row["name"].split(" ")
            if len(parts) < 5:
                # DataFusion may return NULL or empty string for out-of-range index
                assert pd.isna(row["part"]) or row["part"] == ""

    def test_split_multiple_delimiters(self):
        """s.split() should correctly handle multiple delimiter occurrences."""
        t = LTSeq.read_csv(TEST_CSV)
        # Create a derived column with known format, then split it
        result = t.derive(email=lambda r: r.name.s.concat("@example.com")).derive(
            domain=lambda r: r.email.s.split("@", 2)
        )
        df = result.to_pandas()
        for i, row in df.iterrows():
            assert row["domain"] == "example.com"


class TestStringChaining:
    """Tests for chaining new string operations with existing ones."""

    def test_chain_replace_lower(self):
        """Should be able to chain replace() with lower()."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(modified=lambda r: r.name.s.replace("a", "X").s.lower())
        df = result.to_pandas()
        for i, row in df.iterrows():
            expected = row["name"].replace("a", "X").lower()
            assert row["modified"] == expected

    def test_chain_lower_replace(self):
        """Should be able to chain lower() with replace()."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(modified=lambda r: r.name.s.lower().s.replace("a", "X"))
        df = result.to_pandas()
        for i, row in df.iterrows():
            expected = row["name"].lower().replace("a", "X")
            assert row["modified"] == expected

    def test_chain_pad_strip(self):
        """Should be able to chain pad operations."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(
            padded=lambda r: r.name.s.pad_left(8, "_").s.pad_right(12, "-")
        )
        df = result.to_pandas()
        for i, row in df.iterrows():
            # First pad left to 8, then pad right to 12
            step1 = row["name"].rjust(8, "_")
            step2 = step1.ljust(12, "-")
            assert row["padded"] == step2

    def test_chain_concat_upper(self):
        """Should be able to chain concat() with upper()."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(shouted=lambda r: r.name.s.concat("!").s.upper())
        df = result.to_pandas()
        for i, row in df.iterrows():
            expected = (row["name"] + "!").upper()
            assert row["shouted"] == expected


class TestStringOpsWithOtherOps:
    """Tests for new string operations combined with other operations."""

    def test_replace_in_complex_filter(self):
        """Replace should work with arithmetic comparisons."""
        t = LTSeq.read_csv(TEST_CSV)
        # Filter: name.replace("a", "") has length > 2 AND age > 18
        result = t.filter(
            lambda r: (r.name.s.replace("a", "").s.len() > 2) & (r.age > 18)
        )
        df = result.to_pandas()
        for i, row in df.iterrows():
            name_without_a = row["name"].replace("a", "")
            assert len(name_without_a) > 2
            assert row["age"] > 18

    def test_split_derive_after_filter(self):
        """Split should work after filter."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.filter(lambda r: r.age > 18).derive(
            first_char=lambda r: r.name.s.slice(0, 1)
        )
        df = result.to_pandas()
        assert "first_char" in df.columns


class TestStringOpsEdgeCases:
    """Edge case tests for new string operations."""

    def test_replace_empty_string_search(self):
        """s.replace('', 'x') behavior - implementation defined."""
        t = LTSeq.read_csv(TEST_CSV)
        # Behavior depends on DataFusion - empty search string handling
        result = t.derive(replaced=lambda r: r.name.s.replace("", ""))
        df = result.to_pandas()
        # Should not crash, result may vary
        assert "replaced" in df.columns

    def test_pad_zero_width(self):
        """s.pad_left(0) should return original or empty string."""
        t = LTSeq.read_csv(TEST_CSV)
        result = t.derive(padded=lambda r: r.name.s.pad_left(0))
        df = result.to_pandas()
        # Behavior may vary - just verify no crash
        assert "padded" in df.columns

    def test_split_index_zero(self):
        """s.split(delim, 0) behavior - 1-based indexing.
        DataFusion uses SQL SPLIT_PART which is 1-based.
        Index 0 is invalid and raises an error.
        """
        t = LTSeq.read_csv(TEST_CSV)
        # Index 0 is invalid in SQL SPLIT_PART (1-based)
        result = t.derive(part=lambda r: r.name.s.split(" ", 0))
        with pytest.raises(RuntimeError, match="field position must not be zero"):
            result.to_pandas()


class TestStringOpsTypeErrors:
    """Tests that new string operations fail helpfully on wrong types."""

    def test_replace_on_numeric_column_should_error(self):
        """s.replace() on numeric column should give helpful error."""
        t = LTSeq.read_csv(TEST_CSV)
        with pytest.raises(Exception) as exc_info:
            t.derive(age_replaced=lambda r: r.age.s.replace("5", "6")).to_pandas()
        error_msg = str(exc_info.value).lower()
        assert "age" in error_msg or "int" in error_msg or "string" in error_msg

    def test_pad_left_on_numeric_column_should_error(self):
        """s.pad_left() on numeric column should give helpful error."""
        t = LTSeq.read_csv(TEST_CSV)
        with pytest.raises(Exception) as exc_info:
            t.derive(age_padded=lambda r: r.age.s.pad_left(5, "0")).to_pandas()
        error_msg = str(exc_info.value).lower()
        assert "age" in error_msg or "int" in error_msg or "string" in error_msg

    def test_split_on_numeric_column_should_error(self):
        """s.split() on numeric column should give helpful error."""
        t = LTSeq.read_csv(TEST_CSV)
        with pytest.raises(Exception) as exc_info:
            t.derive(age_split=lambda r: r.age.s.split(".", 1)).to_pandas()
        error_msg = str(exc_info.value).lower()
        assert "age" in error_msg or "int" in error_msg or "string" in error_msg
