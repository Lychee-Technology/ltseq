"""Tests for sort order tracking functionality.

Tests the sort_keys property and is_sorted_by() method for tracking
sort order through table operations.
"""

import os
import tempfile
import unittest

from ltseq import LTSeq


class TestSortKeysProperty(unittest.TestCase):
    """Tests for the sort_keys property."""

    def setUp(self):
        """Create test CSV file."""
        self.csv_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.csv_file.write("id,name,value,date\n")
        self.csv_file.write("1,Alice,100,2024-01-01\n")
        self.csv_file.write("2,Bob,200,2024-01-02\n")
        self.csv_file.write("3,Charlie,150,2024-01-03\n")
        self.csv_file.write("4,Diana,300,2024-01-04\n")
        self.csv_file.close()

    def tearDown(self):
        """Clean up test files."""
        os.unlink(self.csv_file.name)

    def test_unsorted_table_has_none_sort_keys(self):
        """Unsorted table should have sort_keys = None."""
        t = LTSeq.read_csv(self.csv_file.name)
        self.assertIsNone(t.sort_keys)

    def test_sorted_table_has_sort_keys(self):
        """Sorted table should have sort_keys populated."""
        t = LTSeq.read_csv(self.csv_file.name)
        t_sorted = t.sort("id")
        self.assertIsNotNone(t_sorted.sort_keys)
        self.assertEqual(t_sorted.sort_keys, [("id", False)])

    def test_sorted_descending_has_desc_flag(self):
        """Descending sort should set desc flag to True."""
        t = LTSeq.read_csv(self.csv_file.name)
        t_sorted = t.sort("id", desc=True)
        self.assertEqual(t_sorted.sort_keys, [("id", True)])

    def test_multi_key_sort(self):
        """Multi-key sort should track all keys."""
        t = LTSeq.read_csv(self.csv_file.name)
        t_sorted = t.sort("name", "id")
        self.assertEqual(t_sorted.sort_keys, [("name", False), ("id", False)])

    def test_multi_key_mixed_directions(self):
        """Multi-key sort with mixed directions."""
        t = LTSeq.read_csv(self.csv_file.name)
        t_sorted = t.sort("name", "id", desc=[False, True])
        self.assertEqual(t_sorted.sort_keys, [("name", False), ("id", True)])


class TestIsSortedBy(unittest.TestCase):
    """Tests for the is_sorted_by() method."""

    def setUp(self):
        """Create test CSV file."""
        self.csv_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.csv_file.write("id,name,value,date\n")
        self.csv_file.write("1,Alice,100,2024-01-01\n")
        self.csv_file.write("2,Bob,200,2024-01-02\n")
        self.csv_file.write("3,Charlie,150,2024-01-03\n")
        self.csv_file.close()

    def tearDown(self):
        """Clean up test files."""
        os.unlink(self.csv_file.name)

    def test_unsorted_returns_false(self):
        """Unsorted table should return False for any is_sorted_by()."""
        t = LTSeq.read_csv(self.csv_file.name)
        self.assertFalse(t.is_sorted_by("id"))
        self.assertFalse(t.is_sorted_by("name"))

    def test_sorted_by_exact_key(self):
        """Should return True for exact key match."""
        t = LTSeq.read_csv(self.csv_file.name).sort("id")
        self.assertTrue(t.is_sorted_by("id"))

    def test_sorted_by_prefix(self):
        """Should return True for prefix of sort keys."""
        t = LTSeq.read_csv(self.csv_file.name).sort("id", "name")
        self.assertTrue(t.is_sorted_by("id"))
        self.assertTrue(t.is_sorted_by("id", "name"))

    def test_sorted_by_wrong_key(self):
        """Should return False for non-matching key."""
        t = LTSeq.read_csv(self.csv_file.name).sort("id")
        self.assertFalse(t.is_sorted_by("name"))

    def test_sorted_by_wrong_order(self):
        """Should return False when key order doesn't match."""
        t = LTSeq.read_csv(self.csv_file.name).sort("id", "name")
        self.assertFalse(t.is_sorted_by("name"))  # name is second, not first
        self.assertFalse(t.is_sorted_by("name", "id"))  # reversed order

    def test_sorted_by_wrong_direction(self):
        """Should return False when direction doesn't match."""
        t = LTSeq.read_csv(self.csv_file.name).sort("id", desc=True)
        self.assertFalse(t.is_sorted_by("id"))  # default desc=False
        self.assertTrue(t.is_sorted_by("id", desc=True))

    def test_sorted_by_empty_keys_returns_false(self):
        """is_sorted_by() with no keys should return False."""
        t = LTSeq.read_csv(self.csv_file.name).sort("id")
        self.assertFalse(t.is_sorted_by())


class TestSortPreservingOps(unittest.TestCase):
    """Tests for sort-preserving operations."""

    def setUp(self):
        """Create test CSV file."""
        self.csv_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.csv_file.write("id,name,value,date\n")
        self.csv_file.write("1,Alice,100,2024-01-01\n")
        self.csv_file.write("2,Bob,200,2024-01-02\n")
        self.csv_file.write("3,Charlie,150,2024-01-03\n")
        self.csv_file.write("4,Diana,300,2024-01-04\n")
        self.csv_file.close()

    def tearDown(self):
        """Clean up test files."""
        os.unlink(self.csv_file.name)

    def test_filter_preserves_sort(self):
        """filter() should preserve sort order."""
        t = LTSeq.read_csv(self.csv_file.name).sort("id")
        t_filtered = t.filter(lambda r: r.value > 100)
        self.assertEqual(t_filtered.sort_keys, [("id", False)])

    def test_derive_preserves_sort(self):
        """derive() should preserve sort order."""
        t = LTSeq.read_csv(self.csv_file.name).sort("id")
        t_derived = t.derive(doubled=lambda r: r.value * 2)
        self.assertEqual(t_derived.sort_keys, [("id", False)])

    def test_slice_preserves_sort(self):
        """slice() should preserve sort order."""
        t = LTSeq.read_csv(self.csv_file.name).sort("id")
        t_sliced = t.slice(1, 2)
        self.assertEqual(t_sliced.sort_keys, [("id", False)])

    def test_select_preserves_sort_when_keys_present(self):
        """select() should preserve sort if sort keys are included."""
        t = LTSeq.read_csv(self.csv_file.name).sort("id")
        t_selected = t.select("id", "name")
        self.assertEqual(t_selected.sort_keys, [("id", False)])

    def test_select_clears_sort_when_keys_missing(self):
        """select() should clear sort if sort keys are excluded."""
        t = LTSeq.read_csv(self.csv_file.name).sort("id")
        t_selected = t.select("name", "value")
        self.assertIsNone(t_selected.sort_keys)

    def test_cum_sum_preserves_sort(self):
        """cum_sum() should preserve sort order."""
        t = LTSeq.read_csv(self.csv_file.name).sort("id")
        t_cumsum = t.cum_sum("value")
        self.assertEqual(t_cumsum.sort_keys, [("id", False)])


class TestSortInvalidatingOps(unittest.TestCase):
    """Tests for sort-invalidating operations."""

    def setUp(self):
        """Create test CSV files."""
        self.csv_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.csv_file.write("id,name,value\n")
        self.csv_file.write("1,Alice,100\n")
        self.csv_file.write("2,Bob,200\n")
        self.csv_file.write("1,Alice,150\n")  # Duplicate id for distinct test
        self.csv_file.close()

        self.csv_file2 = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.csv_file2.write("id,name,value\n")
        self.csv_file2.write("3,Eve,300\n")
        self.csv_file2.write("4,Frank,400\n")
        self.csv_file2.close()

    def tearDown(self):
        """Clean up test files."""
        os.unlink(self.csv_file.name)
        os.unlink(self.csv_file2.name)

    def test_distinct_clears_sort(self):
        """distinct() should clear sort order."""
        t = LTSeq.read_csv(self.csv_file.name).sort("id")
        t_distinct = t.distinct("id")
        self.assertIsNone(t_distinct.sort_keys)

    def test_union_clears_sort(self):
        """union() should clear sort order."""
        t1 = LTSeq.read_csv(self.csv_file.name).sort("id")
        t2 = LTSeq.read_csv(self.csv_file2.name)
        t_union = t1.union(t2)
        self.assertIsNone(t_union.sort_keys)


class TestChainedOperations(unittest.TestCase):
    """Tests for chained operations maintaining correct sort state."""

    def setUp(self):
        """Create test CSV file."""
        self.csv_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.csv_file.write("id,name,value,category\n")
        self.csv_file.write("1,Alice,100,A\n")
        self.csv_file.write("2,Bob,200,B\n")
        self.csv_file.write("3,Charlie,150,A\n")
        self.csv_file.write("4,Diana,300,B\n")
        self.csv_file.close()

    def tearDown(self):
        """Clean up test files."""
        os.unlink(self.csv_file.name)

    def test_sort_filter_derive_chain(self):
        """Chained sort -> filter -> derive should preserve sort."""
        t = (
            LTSeq.read_csv(self.csv_file.name)
            .sort("id")
            .filter(lambda r: r.value > 100)
            .derive(doubled=lambda r: r.value * 2)
        )
        self.assertEqual(t.sort_keys, [("id", False)])

    def test_re_sort_updates_keys(self):
        """Re-sorting should update sort keys."""
        t = LTSeq.read_csv(self.csv_file.name).sort("id")
        self.assertEqual(t.sort_keys, [("id", False)])

        t2 = t.sort("name")
        self.assertEqual(t2.sort_keys, [("name", False)])

    def test_sort_after_invalidating_op(self):
        """Sort after invalidating operation should set new keys."""
        t = LTSeq.read_csv(self.csv_file.name).sort("id")
        t_distinct = t.distinct("id")
        self.assertIsNone(t_distinct.sort_keys)

        t_resorted = t_distinct.sort("name")
        self.assertEqual(t_resorted.sort_keys, [("name", False)])


if __name__ == "__main__":
    unittest.main()
