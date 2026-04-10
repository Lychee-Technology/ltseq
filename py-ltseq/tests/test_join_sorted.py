"""Tests for join() with strategy='merge' functionality.

Previously tested join_sorted() (deprecated); now tests join(..., strategy="merge")
directly, which validates sort order before performing a merge join.
"""

import os
import tempfile
import unittest

from ltseq import LTSeq


class TestJoinSortedBasic(unittest.TestCase):
    """Basic join(..., strategy='merge') functionality tests."""

    def setUp(self):
        """Create test CSV files."""
        # Users table
        self.users_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.users_csv.write("user_id,name\n")
        self.users_csv.write("1,Alice\n")
        self.users_csv.write("2,Bob\n")
        self.users_csv.write("3,Charlie\n")
        self.users_csv.close()

        # Orders table
        self.orders_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.orders_csv.write("order_id,user_id,amount\n")
        self.orders_csv.write("101,1,100\n")
        self.orders_csv.write("102,1,150\n")
        self.orders_csv.write("103,2,200\n")
        self.orders_csv.write("104,3,50\n")
        self.orders_csv.close()

    def tearDown(self):
        """Clean up test files."""
        os.unlink(self.users_csv.name)
        os.unlink(self.orders_csv.name)

    def test_join_sorted_succeeds_when_both_sorted(self):
        """join(strategy='merge') should succeed when both tables are sorted by join keys."""
        users = LTSeq.read_csv(self.users_csv.name).sort("user_id")
        orders = LTSeq.read_csv(self.orders_csv.name).sort("user_id")

        result = users.join(orders, on=lambda u, o: u.user_id == o.user_id, strategy="merge")

        # Should return an LTSeq with joined data
        self.assertIsInstance(result, LTSeq)
        self.assertGreater(len(result), 0)

    def test_join_sorted_raises_when_left_unsorted(self):
        """join(strategy='merge') should raise ValueError when left table is unsorted."""
        users = LTSeq.read_csv(self.users_csv.name)  # Not sorted
        orders = LTSeq.read_csv(self.orders_csv.name).sort("user_id")

        with self.assertRaises(ValueError) as ctx:
            users.join(orders, on=lambda u, o: u.user_id == o.user_id, strategy="merge")

        self.assertIn("Left table is not sorted", str(ctx.exception))

    def test_join_sorted_raises_when_right_unsorted(self):
        """join(strategy='merge') should raise ValueError when right table is unsorted."""
        users = LTSeq.read_csv(self.users_csv.name).sort("user_id")
        orders = LTSeq.read_csv(self.orders_csv.name)  # Not sorted

        with self.assertRaises(ValueError) as ctx:
            users.join(orders, on=lambda u, o: u.user_id == o.user_id, strategy="merge")

        self.assertIn("Right table is not sorted", str(ctx.exception))

    def test_join_sorted_raises_when_both_unsorted(self):
        """join(strategy='merge') should raise ValueError when both tables are unsorted."""
        users = LTSeq.read_csv(self.users_csv.name)  # Not sorted
        orders = LTSeq.read_csv(self.orders_csv.name)  # Not sorted

        with self.assertRaises(ValueError) as ctx:
            users.join(orders, on=lambda u, o: u.user_id == o.user_id, strategy="merge")

        # Should fail on left table first
        self.assertIn("Left table is not sorted", str(ctx.exception))


class TestJoinSortedDirections(unittest.TestCase):
    """Tests for join(strategy='merge') with different sort directions."""

    def setUp(self):
        """Create test CSV files."""
        self.users_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.users_csv.write("user_id,name\n")
        self.users_csv.write("1,Alice\n")
        self.users_csv.write("2,Bob\n")
        self.users_csv.write("3,Charlie\n")
        self.users_csv.close()

        self.orders_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.orders_csv.write("order_id,user_id,amount\n")
        self.orders_csv.write("101,1,100\n")
        self.orders_csv.write("102,2,200\n")
        self.orders_csv.write("103,3,50\n")
        self.orders_csv.close()

    def tearDown(self):
        """Clean up test files."""
        os.unlink(self.users_csv.name)
        os.unlink(self.orders_csv.name)

    def test_join_sorted_both_ascending(self):
        """join(strategy='merge') should succeed when both tables sorted ascending."""
        users = LTSeq.read_csv(self.users_csv.name).sort("user_id")
        orders = LTSeq.read_csv(self.orders_csv.name).sort("user_id")

        result = users.join(orders, on=lambda u, o: u.user_id == o.user_id, strategy="merge")
        self.assertIsInstance(result, LTSeq)

    def test_join_sorted_both_descending(self):
        """join(strategy='merge') should succeed when both tables sorted descending."""
        users = LTSeq.read_csv(self.users_csv.name).sort("user_id", desc=True)
        orders = LTSeq.read_csv(self.orders_csv.name).sort("user_id", desc=True)

        result = users.join(orders, on=lambda u, o: u.user_id == o.user_id, strategy="merge")
        self.assertIsInstance(result, LTSeq)

    def test_join_sorted_raises_when_directions_mismatch(self):
        """join(strategy='merge') should raise ValueError when sort directions don't match."""
        users = LTSeq.read_csv(self.users_csv.name).sort("user_id", desc=False)
        orders = LTSeq.read_csv(self.orders_csv.name).sort("user_id", desc=True)

        with self.assertRaises(ValueError) as ctx:
            users.join(orders, on=lambda u, o: u.user_id == o.user_id, strategy="merge")

        self.assertIn("Sort directions don't match", str(ctx.exception))


class TestJoinSortedJoinTypes(unittest.TestCase):
    """Tests for join(strategy='merge') with different join types."""

    def setUp(self):
        """Create test CSV files."""
        self.users_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.users_csv.write("user_id,name\n")
        self.users_csv.write("1,Alice\n")
        self.users_csv.write("2,Bob\n")
        self.users_csv.write("3,Charlie\n")
        self.users_csv.close()

        self.orders_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.orders_csv.write("order_id,user_id,amount\n")
        self.orders_csv.write("101,1,100\n")
        self.orders_csv.write("102,2,200\n")
        self.orders_csv.close()

    def tearDown(self):
        """Clean up test files."""
        os.unlink(self.users_csv.name)
        os.unlink(self.orders_csv.name)

    def test_join_sorted_inner(self):
        """join(strategy='merge', how='inner') should work."""
        users = LTSeq.read_csv(self.users_csv.name).sort("user_id")
        orders = LTSeq.read_csv(self.orders_csv.name).sort("user_id")

        result = users.join(
            orders, on=lambda u, o: u.user_id == o.user_id, how="inner", strategy="merge"
        )
        self.assertIsInstance(result, LTSeq)

    def test_join_sorted_left(self):
        """join(strategy='merge', how='left') should work."""
        users = LTSeq.read_csv(self.users_csv.name).sort("user_id")
        orders = LTSeq.read_csv(self.orders_csv.name).sort("user_id")

        result = users.join(
            orders, on=lambda u, o: u.user_id == o.user_id, how="left", strategy="merge"
        )
        self.assertIsInstance(result, LTSeq)

    def test_join_sorted_invalid_type_raises(self):
        """join() with invalid join type should raise ValueError."""
        users = LTSeq.read_csv(self.users_csv.name).sort("user_id")
        orders = LTSeq.read_csv(self.orders_csv.name).sort("user_id")

        with self.assertRaises(ValueError) as ctx:
            users.join(
                orders, on=lambda u, o: u.user_id == o.user_id, how="invalid", strategy="merge"
            )

        self.assertIn("Invalid join type", str(ctx.exception))


class TestJoinSortedAfterOps(unittest.TestCase):
    """Tests for join(strategy='merge') after sort-preserving/invalidating operations."""

    def setUp(self):
        """Create test CSV files."""
        self.users_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.users_csv.write("user_id,name,active\n")
        self.users_csv.write("1,Alice,1\n")
        self.users_csv.write("2,Bob,1\n")
        self.users_csv.write("3,Charlie,0\n")
        self.users_csv.close()

        self.orders_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.orders_csv.write("order_id,user_id,amount\n")
        self.orders_csv.write("101,1,100\n")
        self.orders_csv.write("102,2,200\n")
        self.orders_csv.close()

    def tearDown(self):
        """Clean up test files."""
        os.unlink(self.users_csv.name)
        os.unlink(self.orders_csv.name)

    def test_join_sorted_after_filter_preserves_sort(self):
        """join(strategy='merge') should work after filter() which preserves sort."""
        users = (
            LTSeq.read_csv(self.users_csv.name)
            .sort("user_id")
            .filter(lambda r: r.active == 1)
        )
        orders = LTSeq.read_csv(self.orders_csv.name).sort("user_id")

        # Should succeed because filter preserves sort order
        result = users.join(orders, on=lambda u, o: u.user_id == o.user_id, strategy="merge")
        self.assertIsInstance(result, LTSeq)

    def test_join_sorted_after_distinct_fails(self):
        """join(strategy='merge') should fail after distinct() which invalidates sort."""
        users = LTSeq.read_csv(self.users_csv.name).sort("user_id").distinct("user_id")
        orders = LTSeq.read_csv(self.orders_csv.name).sort("user_id")

        # Should fail because distinct invalidates sort order
        with self.assertRaises(ValueError) as ctx:
            users.join(orders, on=lambda u, o: u.user_id == o.user_id, strategy="merge")

        self.assertIn("Left table is not sorted", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
