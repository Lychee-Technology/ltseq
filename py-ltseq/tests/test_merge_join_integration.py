"""Tests for merge join integration (Phase 1.4).

Tests the CPU two-pointer merge join path that runs when
strategy='merge' is used with join(). GPU acceleration is
transparent — same API, same results.
"""

import os
import tempfile
import unittest

from ltseq import LTSeq


class TestMergeJoinInner(unittest.TestCase):
    """Inner merge join tests."""

    def setUp(self):
        self.left_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.left_csv.write("id,name\n")
        self.left_csv.write("1,Alice\n")
        self.left_csv.write("2,Bob\n")
        self.left_csv.write("3,Charlie\n")
        self.left_csv.write("5,Eve\n")
        self.left_csv.close()

        self.right_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.right_csv.write("id,score\n")
        self.right_csv.write("1,90\n")
        self.right_csv.write("2,85\n")
        self.right_csv.write("4,70\n")
        self.right_csv.write("5,95\n")
        self.right_csv.close()

    def tearDown(self):
        os.unlink(self.left_csv.name)
        os.unlink(self.right_csv.name)

    def test_inner_merge_join_basic(self):
        """Inner merge join returns only matching rows."""
        left = LTSeq.read_csv(self.left_csv.name).sort("id")
        right = LTSeq.read_csv(self.right_csv.name).sort("id")

        result = left.join(right, on=lambda a, b: a.id == b.id, strategy="merge")

        self.assertIsInstance(result, LTSeq)
        # Matching ids: 1, 2, 5
        self.assertEqual(len(result), 3)

    def test_inner_merge_join_result_columns(self):
        """Inner merge join produces correct column schema."""
        left = LTSeq.read_csv(self.left_csv.name).sort("id")
        right = LTSeq.read_csv(self.right_csv.name).sort("id")

        result = left.join(right, on=lambda a, b: a.id == b.id, strategy="merge")

        df = result.to_pandas()
        # Should have left columns + aliased right columns
        self.assertIn("id", df.columns)
        self.assertIn("name", df.columns)
        self.assertIn("_other_id", df.columns)
        self.assertIn("_other_score", df.columns)

    def test_inner_merge_join_data_correctness(self):
        """Inner merge join matches correct rows."""
        left = LTSeq.read_csv(self.left_csv.name).sort("id")
        right = LTSeq.read_csv(self.right_csv.name).sort("id")

        result = left.join(right, on=lambda a, b: a.id == b.id, strategy="merge")

        df = result.to_pandas().sort_values("id").reset_index(drop=True)
        # id=1: Alice, score=90
        self.assertEqual(df.loc[0, "name"], "Alice")
        self.assertEqual(df.loc[0, "_other_score"], 90)
        # id=2: Bob, score=85
        self.assertEqual(df.loc[1, "name"], "Bob")
        self.assertEqual(df.loc[1, "_other_score"], 85)
        # id=5: Eve, score=95
        self.assertEqual(df.loc[2, "name"], "Eve")
        self.assertEqual(df.loc[2, "_other_score"], 95)

    def test_inner_merge_join_no_matches(self):
        """Inner merge join with no matching keys returns empty."""
        left_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        left_csv.write("id,val\n1,a\n2,b\n")
        left_csv.close()

        right_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        right_csv.write("id,val\n10,x\n20,y\n")
        right_csv.close()

        try:
            left = LTSeq.read_csv(left_csv.name).sort("id")
            right = LTSeq.read_csv(right_csv.name).sort("id")

            result = left.join(right, on=lambda a, b: a.id == b.id, strategy="merge")
            self.assertEqual(len(result), 0)
        finally:
            os.unlink(left_csv.name)
            os.unlink(right_csv.name)


class TestMergeJoinLeft(unittest.TestCase):
    """Left merge join tests."""

    def setUp(self):
        self.left_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.left_csv.write("id,name\n")
        self.left_csv.write("1,Alice\n")
        self.left_csv.write("2,Bob\n")
        self.left_csv.write("3,Charlie\n")
        self.left_csv.close()

        self.right_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.right_csv.write("id,score\n")
        self.right_csv.write("1,90\n")
        self.right_csv.write("3,70\n")
        self.right_csv.close()

    def tearDown(self):
        os.unlink(self.left_csv.name)
        os.unlink(self.right_csv.name)

    def test_left_merge_join_basic(self):
        """Left merge join returns all left rows."""
        left = LTSeq.read_csv(self.left_csv.name).sort("id")
        right = LTSeq.read_csv(self.right_csv.name).sort("id")

        result = left.join(
            right, on=lambda a, b: a.id == b.id, how="left", strategy="merge"
        )

        # All 3 left rows should be present
        self.assertEqual(len(result), 3)

    def test_left_merge_join_unmatched_have_nulls(self):
        """Left merge join fills unmatched right columns with None."""
        left = LTSeq.read_csv(self.left_csv.name).sort("id")
        right = LTSeq.read_csv(self.right_csv.name).sort("id")

        result = left.join(
            right, on=lambda a, b: a.id == b.id, how="left", strategy="merge"
        )

        df = result.to_pandas().sort_values("id").reset_index(drop=True)
        # id=1: matched (score=90)
        self.assertEqual(df.loc[0, "_other_score"], 90)
        # id=2: unmatched (score=NaN)
        import pandas as pd

        self.assertTrue(pd.isna(df.loc[1, "_other_score"]))
        # id=3: matched (score=70)
        self.assertEqual(df.loc[2, "_other_score"], 70)


class TestMergeJoinDuplicateKeys(unittest.TestCase):
    """Tests for merge join with duplicate keys (many-to-many)."""

    def test_many_to_many_cross_product(self):
        """Duplicate keys produce correct cross product."""
        left_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        left_csv.write("id,left_val\n")
        left_csv.write("1,a\n")
        left_csv.write("1,b\n")
        left_csv.write("2,c\n")
        left_csv.close()

        right_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        right_csv.write("id,right_val\n")
        right_csv.write("1,x\n")
        right_csv.write("1,y\n")
        right_csv.write("3,z\n")
        right_csv.close()

        try:
            left = LTSeq.read_csv(left_csv.name).sort("id")
            right = LTSeq.read_csv(right_csv.name).sort("id")

            result = left.join(
                right, on=lambda a, b: a.id == b.id, strategy="merge"
            )

            # id=1: 2 left x 2 right = 4 matches
            # id=2: no match in right
            # id=3: no match in left
            self.assertEqual(len(result), 4)

            # Verify all combinations of id=1
            df = result.to_pandas()
            id1 = df[df["id"] == 1]
            pairs = set(
                zip(id1["left_val"].tolist(), id1["_other_right_val"].tolist())
            )
            self.assertEqual(len(pairs), 4)
            self.assertIn(("a", "x"), pairs)
            self.assertIn(("a", "y"), pairs)
            self.assertIn(("b", "x"), pairs)
            self.assertIn(("b", "y"), pairs)
        finally:
            os.unlink(left_csv.name)
            os.unlink(right_csv.name)


class TestMergeJoinFallback(unittest.TestCase):
    """Tests that merge join falls back correctly for unsupported join types."""

    def setUp(self):
        self.left_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.left_csv.write("id,name\n1,Alice\n2,Bob\n")
        self.left_csv.close()

        self.right_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        self.right_csv.write("id,score\n1,90\n3,70\n")
        self.right_csv.close()

    def tearDown(self):
        os.unlink(self.left_csv.name)
        os.unlink(self.right_csv.name)

    def test_right_join_falls_back_to_sql(self):
        """Right join via strategy='merge' falls back to SQL."""
        left = LTSeq.read_csv(self.left_csv.name).sort("id")
        right = LTSeq.read_csv(self.right_csv.name).sort("id")

        result = left.join(
            right, on=lambda a, b: a.id == b.id, how="right", strategy="merge"
        )

        # Right join: all right rows (ids 1, 3)
        self.assertEqual(len(result), 2)

    def test_full_join_falls_back_to_sql(self):
        """Full join via strategy='merge' falls back to SQL."""
        left = LTSeq.read_csv(self.left_csv.name).sort("id")
        right = LTSeq.read_csv(self.right_csv.name).sort("id")

        result = left.join(
            right, on=lambda a, b: a.id == b.id, how="full", strategy="merge"
        )

        # Full join: ids 1, 2, 3
        self.assertEqual(len(result), 3)


class TestMergeJoinSortOrder(unittest.TestCase):
    """Tests that merge join preserves sort order in result."""

    def test_result_is_sorted_by_join_key(self):
        """Merge join result should be sorted by the left join key."""
        left_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        left_csv.write("id,name\n1,Alice\n3,Charlie\n5,Eve\n7,Grace\n")
        left_csv.close()

        right_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        right_csv.write("id,score\n1,90\n3,70\n5,60\n7,80\n")
        right_csv.close()

        try:
            left = LTSeq.read_csv(left_csv.name).sort("id")
            right = LTSeq.read_csv(right_csv.name).sort("id")

            result = left.join(
                right, on=lambda a, b: a.id == b.id, strategy="merge"
            )

            df = result.to_pandas()
            ids = df["id"].tolist()
            self.assertEqual(ids, sorted(ids))
        finally:
            os.unlink(left_csv.name)
            os.unlink(right_csv.name)


class TestMergeJoinStringKeys(unittest.TestCase):
    """Tests merge join with string key columns (CPU only, no GPU)."""

    def test_string_key_inner_join(self):
        """Merge join works with string key columns."""
        left_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        left_csv.write("region,sales\n")
        left_csv.write("east,100\n")
        left_csv.write("north,200\n")
        left_csv.write("west,300\n")
        left_csv.close()

        right_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        right_csv.write("region,population\n")
        right_csv.write("east,5000\n")
        right_csv.write("south,3000\n")
        right_csv.write("west,8000\n")
        right_csv.close()

        try:
            left = LTSeq.read_csv(left_csv.name).sort("region")
            right = LTSeq.read_csv(right_csv.name).sort("region")

            result = left.join(
                right, on=lambda a, b: a.region == b.region, strategy="merge"
            )

            # Matching: east, west
            self.assertEqual(len(result), 2)
            df = result.to_pandas()
            regions = sorted(df["region"].tolist())
            self.assertEqual(regions, ["east", "west"])
        finally:
            os.unlink(left_csv.name)
            os.unlink(right_csv.name)


if __name__ == "__main__":
    unittest.main()
