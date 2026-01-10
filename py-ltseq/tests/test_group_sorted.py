"""Tests for group_sorted() functionality.

Tests the group_sorted() method which performs efficient one-pass grouping
on pre-sorted data. Unlike group_ordered() which groups only consecutive
identical values, group_sorted() assumes the data is globally sorted by
the grouping key.
"""

import os
import tempfile
import unittest

from ltseq import LTSeq, NestedTable


class TestGroupSortedBasic(unittest.TestCase):
    """Basic group_sorted() functionality tests."""

    def setUp(self):
        """Create test CSV files."""
        # Create events data sorted by user_id
        self.events_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.events_csv.write("user_id,event,timestamp\n")
        self.events_csv.write("1,login,2024-01-01\n")
        self.events_csv.write("1,click,2024-01-02\n")
        self.events_csv.write("1,logout,2024-01-03\n")
        self.events_csv.write("2,login,2024-01-01\n")
        self.events_csv.write("2,click,2024-01-02\n")
        self.events_csv.write("3,login,2024-01-01\n")
        self.events_csv.write("3,click,2024-01-02\n")
        self.events_csv.write("3,purchase,2024-01-03\n")
        self.events_csv.write("3,logout,2024-01-04\n")
        self.events_csv.close()

    def tearDown(self):
        """Clean up test files."""
        os.unlink(self.events_csv.name)

    def test_group_sorted_returns_nested_table(self):
        """group_sorted() should return a NestedTable instance."""
        t = LTSeq.read_csv(self.events_csv.name)
        groups = t.group_sorted(lambda r: r.user_id)
        self.assertIsInstance(groups, NestedTable)

    def test_group_sorted_first_returns_ltseq(self):
        """group_sorted().first() should return an LTSeq instance."""
        t = LTSeq.read_csv(self.events_csv.name)
        groups = t.group_sorted(lambda r: r.user_id)
        first_rows = groups.first()
        self.assertIsInstance(first_rows, LTSeq)

    def test_group_sorted_last_returns_ltseq(self):
        """group_sorted().last() should return an LTSeq instance."""
        t = LTSeq.read_csv(self.events_csv.name)
        groups = t.group_sorted(lambda r: r.user_id)
        last_rows = groups.last()
        self.assertIsInstance(last_rows, LTSeq)

    def test_group_sorted_first_row_per_group(self):
        """group_sorted().first() should return first row of each group."""
        t = LTSeq.read_csv(self.events_csv.name)
        groups = t.group_sorted(lambda r: r.user_id)
        first_rows = groups.first()

        # Should have 3 users
        self.assertEqual(len(first_rows), 3)

        # Check first row values using to_pandas
        df = first_rows.to_pandas()
        # First events should all be 'login'
        self.assertTrue(all(df["event"] == "login"))

    def test_group_sorted_last_row_per_group(self):
        """group_sorted().last() should return last row of each group."""
        t = LTSeq.read_csv(self.events_csv.name)
        groups = t.group_sorted(lambda r: r.user_id)
        last_rows = groups.last()

        # Should have 3 users
        self.assertEqual(len(last_rows), 3)

        # Check last row values using to_pandas
        df = last_rows.to_pandas()
        # User 1 and 3 should have 'logout', user 2 should have 'click'
        events = df["event"].tolist()
        self.assertIn("logout", events)
        self.assertIn("click", events)


class TestGroupSortedFilter(unittest.TestCase):
    """Tests for group_sorted() with filter operations."""

    def setUp(self):
        """Create test CSV files."""
        # Create events data sorted by user_id
        self.events_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.events_csv.write("user_id,event,timestamp\n")
        self.events_csv.write("1,login,2024-01-01\n")
        self.events_csv.write("1,click,2024-01-02\n")
        self.events_csv.write("1,logout,2024-01-03\n")
        self.events_csv.write("2,login,2024-01-01\n")
        self.events_csv.write("2,click,2024-01-02\n")
        self.events_csv.write("3,login,2024-01-01\n")
        self.events_csv.write("3,click,2024-01-02\n")
        self.events_csv.write("3,purchase,2024-01-03\n")
        self.events_csv.write("3,logout,2024-01-04\n")
        self.events_csv.close()

    def tearDown(self):
        """Clean up test files."""
        os.unlink(self.events_csv.name)

    def test_group_sorted_filter_by_count(self):
        """group_sorted().filter() should filter groups by count."""
        t = LTSeq.read_csv(self.events_csv.name)
        groups = t.group_sorted(lambda r: r.user_id)

        # Filter to groups with more than 2 events
        filtered = groups.filter(lambda g: g.count() > 2)

        # User 1 has 3 events, user 2 has 2 events, user 3 has 4 events
        # So only users 1 and 3 should remain
        # Total rows: 3 + 4 = 7
        self.assertIsInstance(filtered, NestedTable)

    def test_group_sorted_filter_preserves_is_sorted(self):
        """group_sorted().filter() should preserve is_sorted flag."""
        t = LTSeq.read_csv(self.events_csv.name)
        groups = t.group_sorted(lambda r: r.user_id)

        # Filter to groups with more than 2 events
        filtered = groups.filter(lambda g: g.count() > 2)

        # The filtered NestedTable should still have is_sorted=True
        self.assertTrue(filtered._is_sorted)


class TestGroupSortedVsGroupOrdered(unittest.TestCase):
    """Tests comparing group_sorted() and group_ordered() behavior."""

    def setUp(self):
        """Create test CSV files."""
        # Create data that's already sorted
        self.sorted_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.sorted_csv.write("category,value\n")
        self.sorted_csv.write("A,10\n")
        self.sorted_csv.write("A,20\n")
        self.sorted_csv.write("A,30\n")
        self.sorted_csv.write("B,40\n")
        self.sorted_csv.write("B,50\n")
        self.sorted_csv.write("C,60\n")
        self.sorted_csv.close()

        # Create data with interleaved categories (not sorted)
        self.unsorted_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.unsorted_csv.write("category,value\n")
        self.unsorted_csv.write("A,10\n")
        self.unsorted_csv.write("B,20\n")
        self.unsorted_csv.write("A,30\n")
        self.unsorted_csv.write("B,40\n")
        self.unsorted_csv.write("A,50\n")
        self.unsorted_csv.close()

    def tearDown(self):
        """Clean up test files."""
        os.unlink(self.sorted_csv.name)
        os.unlink(self.unsorted_csv.name)

    def test_group_sorted_on_sorted_data(self):
        """group_sorted() on sorted data should group correctly."""
        t = LTSeq.read_csv(self.sorted_csv.name)
        groups = t.group_sorted(lambda r: r.category)
        first_rows = groups.first()

        # Should have 3 categories
        self.assertEqual(len(first_rows), 3)

    def test_group_ordered_on_sorted_data(self):
        """group_ordered() on sorted data should behave same as group_sorted()."""
        t = LTSeq.read_csv(self.sorted_csv.name)

        # Both should produce same results on sorted data
        sorted_groups = t.group_sorted(lambda r: r.category)
        ordered_groups = t.group_ordered(lambda r: r.category)

        sorted_first = sorted_groups.first()
        ordered_first = ordered_groups.first()

        self.assertEqual(len(sorted_first), len(ordered_first))

    def test_group_sorted_vs_ordered_on_unsorted_data(self):
        """group_ordered() on unsorted data groups consecutive only."""
        t = LTSeq.read_csv(self.unsorted_csv.name)

        # group_ordered groups consecutive identical values
        ordered_groups = t.group_ordered(lambda r: r.category)
        ordered_first = ordered_groups.first()

        # Data: A, B, A, B, A - so 5 consecutive groups
        self.assertEqual(len(ordered_first), 5)

        # If we sort first, then group_sorted works correctly
        t_sorted = t.sort("category")
        sorted_groups = t_sorted.group_sorted(lambda r: r.category)
        sorted_first = sorted_groups.first()

        # After sorting: A, A, A, B, B - so 2 groups
        self.assertEqual(len(sorted_first), 2)


class TestGroupSortedDerive(unittest.TestCase):
    """Tests for group_sorted() with derive operations."""

    def setUp(self):
        """Create test CSV files."""
        self.sales_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.sales_csv.write("region,amount,date\n")
        self.sales_csv.write("East,100,2024-01-01\n")
        self.sales_csv.write("East,200,2024-01-02\n")
        self.sales_csv.write("East,150,2024-01-03\n")
        self.sales_csv.write("West,300,2024-01-01\n")
        self.sales_csv.write("West,400,2024-01-02\n")
        self.sales_csv.close()

    def tearDown(self):
        """Clean up test files."""
        os.unlink(self.sales_csv.name)

    def test_group_sorted_derive_count(self):
        """group_sorted().derive() should add group count column."""
        t = LTSeq.read_csv(self.sales_csv.name)
        groups = t.group_sorted(lambda r: r.region)

        derived = groups.derive(lambda g: {"group_size": g.count()})

        # Should have same number of rows
        self.assertEqual(len(derived), 5)

        # Check the derived column exists
        df = derived.to_pandas()
        self.assertIn("group_size", df.columns)

        # East has 3 rows, West has 2 rows
        # All rows should have their group's count
        east_sizes = df[df["region"] == "East"]["group_size"].unique()
        west_sizes = df[df["region"] == "West"]["group_size"].unique()

        self.assertEqual(list(east_sizes), [3])
        self.assertEqual(list(west_sizes), [2])


class TestGroupSortedErrors(unittest.TestCase):
    """Tests for group_sorted() error handling."""

    def test_group_sorted_missing_schema_raises_error(self):
        """group_sorted() should raise error if schema not initialized."""
        t = LTSeq()  # No read_csv call
        with self.assertRaises(ValueError):
            t.group_sorted(lambda r: r.id)

    def test_group_sorted_invalid_column_raises_error(self):
        """group_sorted() should raise error for non-existent column when evaluated."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("id,name\n")
        csv.write("1,Alice\n")
        csv.close()

        try:
            t = LTSeq.read_csv(csv.name)
            with self.assertRaises(AttributeError):
                # group_sorted is lazy, error is raised when flatten() triggers evaluation
                t.group_sorted(lambda r: r.nonexistent_column).flatten()
        finally:
            os.unlink(csv.name)


class TestGroupSortedChaining(unittest.TestCase):
    """Tests for chaining operations after group_sorted()."""

    def setUp(self):
        """Create test CSV file."""
        self.csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        self.csv.write("user_id,action,score\n")
        self.csv.write("1,view,10\n")
        self.csv.write("1,click,20\n")
        self.csv.write("1,purchase,100\n")
        self.csv.write("2,view,5\n")
        self.csv.write("2,click,15\n")
        self.csv.write("3,view,8\n")
        self.csv.close()

    def tearDown(self):
        """Clean up test files."""
        os.unlink(self.csv.name)

    def test_group_sorted_filter_then_first(self):
        """Should be able to chain filter().first() on group_sorted()."""
        t = LTSeq.read_csv(self.csv.name)
        groups = t.group_sorted(lambda r: r.user_id)

        # Filter to users with more than 1 action, then get first
        result = groups.filter(lambda g: g.count() > 1).first()

        # Users 1 (3 actions) and 2 (2 actions) pass, user 3 (1 action) doesn't
        self.assertEqual(len(result), 2)

    def test_group_sorted_filter_then_derive(self):
        """Should be able to chain filter().derive() on group_sorted()."""
        t = LTSeq.read_csv(self.csv.name)
        groups = t.group_sorted(lambda r: r.user_id)

        # Filter to users with more than 1 action, then derive
        filtered = groups.filter(lambda g: g.count() > 1)
        result = filtered.derive(lambda g: {"total_actions": g.count()})

        # Check the result has the derived column
        df = result.to_pandas()
        self.assertIn("total_actions", df.columns)


if __name__ == "__main__":
    unittest.main()
