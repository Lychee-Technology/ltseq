"""Tests for set algebra operations: union, intersect, diff, is_subset."""

import pytest
import tempfile
import os
from ltseq import LTSeq


class TestUnion:
    """Tests for union() operation."""

    def test_union_basic(self):
        """Union combines all rows from both tables."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create two tables with same schema
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n")
            with open(path2, "w") as f:
                f.write("id,name\n3,Charlie\n4,Diana\n")

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            result = t1.union(t2)
            assert len(result) == 4

    def test_union_with_duplicates(self):
        """Union preserves duplicates (UNION ALL behavior)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n")
            with open(path2, "w") as f:
                f.write("id,name\n1,Alice\n3,Charlie\n")

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            result = t1.union(t2)
            # Should have 4 rows: 1,Alice appears twice
            assert len(result) == 4

    def test_union_empty_left(self):
        """Union with empty left table returns right table rows."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name\n")
            with open(path2, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n")

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            result = t1.union(t2)
            assert len(result) == 2

    def test_union_empty_right(self):
        """Union with empty right table returns left table rows."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n")
            with open(path2, "w") as f:
                f.write("id,name\n")

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            result = t1.union(t2)
            assert len(result) == 2

    def test_union_schema_mismatch_raises(self):
        """Union with different schemas raises error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name\n1,Alice\n")
            with open(path2, "w") as f:
                f.write("id,age\n1,25\n")  # Different column name

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            with pytest.raises(ValueError, match="[Ss]chema"):
                t1.union(t2)


class TestIntersect:
    """Tests for intersect() operation."""

    def test_intersect_basic(self):
        """Intersect returns rows present in both tables."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n3,Charlie\n")
            with open(path2, "w") as f:
                f.write("id,name\n2,Bob\n3,Charlie\n4,Diana\n")

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            # Intersect on all columns
            result = t1.intersect(t2)
            assert len(result) == 2  # Bob and Charlie are in both

    def test_intersect_with_key(self):
        """Intersect on specific key column."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n3,Charlie\n")
            with open(path2, "w") as f:
                f.write("id,name\n2,Barbara\n3,Charles\n4,Diana\n")

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            # Intersect on id only
            result = t1.intersect(t2, on=lambda r: r.id)
            assert len(result) == 2  # ids 2 and 3 are in both

    def test_intersect_no_common_rows(self):
        """Intersect returns empty when no common rows."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n")
            with open(path2, "w") as f:
                f.write("id,name\n3,Charlie\n4,Diana\n")

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            result = t1.intersect(t2)
            assert len(result) == 0

    def test_intersect_identical_tables(self):
        """Intersect of identical tables returns all rows."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "t.csv")

            with open(path, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n")

            t1 = LTSeq.read_csv(path)
            t2 = LTSeq.read_csv(path)

            result = t1.intersect(t2)
            assert len(result) == 2

    def test_intersect_empty_left(self):
        """Intersect with empty left table returns empty."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name\n")
            with open(path2, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n")

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            result = t1.intersect(t2)
            assert len(result) == 0


class TestDiff:
    """Tests for diff() operation."""

    def test_diff_basic(self):
        """Diff returns rows in first table but not in second."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n3,Charlie\n")
            with open(path2, "w") as f:
                f.write("id,name\n2,Bob\n3,Charlie\n4,Diana\n")

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            # Diff on all columns
            result = t1.diff(t2)
            assert len(result) == 1  # Only Alice is unique to t1

    def test_diff_with_key(self):
        """Diff on specific key column."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n3,Charlie\n")
            with open(path2, "w") as f:
                f.write("id,name\n2,Barbara\n3,Charles\n4,Diana\n")

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            # Diff on id only
            result = t1.diff(t2, on=lambda r: r.id)
            assert len(result) == 1  # Only id=1 is unique to t1

    def test_diff_all_common(self):
        """Diff returns empty when all rows are in both."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n")
            with open(path2, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n3,Charlie\n")

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            result = t1.diff(t2)
            assert len(result) == 0  # All t1 rows are in t2

    def test_diff_no_common(self):
        """Diff returns all rows when no overlap."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n")
            with open(path2, "w") as f:
                f.write("id,name\n3,Charlie\n4,Diana\n")

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            result = t1.diff(t2)
            assert len(result) == 2  # All t1 rows are unique

    def test_diff_empty_right(self):
        """Diff with empty right returns all left rows."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n")
            with open(path2, "w") as f:
                f.write("id,name\n")

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            result = t1.diff(t2)
            assert len(result) == 2

    def test_diff_use_case_churned_customers(self):
        """Practical use case: find churned customers."""
        with tempfile.TemporaryDirectory() as tmpdir:
            all_customers = os.path.join(tmpdir, "all.csv")
            active_customers = os.path.join(tmpdir, "active.csv")

            with open(all_customers, "w") as f:
                f.write(
                    "customer_id,name\n101,Alice\n102,Bob\n103,Charlie\n104,Diana\n"
                )
            with open(active_customers, "w") as f:
                f.write("customer_id,name\n102,Bob\n104,Diana\n")

            all_t = LTSeq.read_csv(all_customers)
            active_t = LTSeq.read_csv(active_customers)

            # Find churned customers (in all but not in active)
            churned = all_t.diff(active_t, on=lambda r: r.customer_id)
            assert len(churned) == 2  # Alice and Charlie churned


class TestIsSubset:
    """Tests for is_subset() operation."""

    def test_is_subset_true(self):
        """Subset check returns True when all rows are in other."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n")
            with open(path2, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n3,Charlie\n")

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            assert t1.is_subset(t2) is True

    def test_is_subset_false(self):
        """Subset check returns False when some rows are missing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n3,Charlie\n")
            with open(path2, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n")

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            assert t1.is_subset(t2) is False

    def test_is_subset_with_key(self):
        """Subset check on specific key column."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n")
            with open(path2, "w") as f:
                f.write("id,name\n1,Alicia\n2,Bobby\n3,Charlie\n")

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            # On id only, t1 is a subset (ids 1,2 are in t2)
            assert t1.is_subset(t2, on=lambda r: r.id) is True
            # On all columns, t1 is NOT a subset (names differ)
            assert t1.is_subset(t2) is False

    def test_is_subset_identical(self):
        """Identical tables are subsets of each other."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "t.csv")

            with open(path, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n")

            t1 = LTSeq.read_csv(path)
            t2 = LTSeq.read_csv(path)

            assert t1.is_subset(t2) is True
            assert t2.is_subset(t1) is True

    def test_is_subset_empty_is_subset(self):
        """Empty table is subset of any table."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name\n")
            with open(path2, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n")

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            assert t1.is_subset(t2) is True

    def test_is_subset_no_overlap(self):
        """Tables with no overlap: neither is subset."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name\n1,Alice\n2,Bob\n")
            with open(path2, "w") as f:
                f.write("id,name\n3,Charlie\n4,Diana\n")

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            assert t1.is_subset(t2) is False
            assert t2.is_subset(t1) is False


class TestSetOpsEdgeCases:
    """Edge cases and error handling for set operations."""

    def test_union_type_error(self):
        """Union with non-LTSeq raises TypeError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "t.csv")
            with open(path, "w") as f:
                f.write("id\n1\n")
            t = LTSeq.read_csv(path)

            with pytest.raises(TypeError):
                t.union("not a table")

    def test_intersect_type_error(self):
        """Intersect with non-LTSeq raises TypeError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "t.csv")
            with open(path, "w") as f:
                f.write("id\n1\n")
            t = LTSeq.read_csv(path)

            with pytest.raises(TypeError):
                t.intersect([1, 2, 3])

    def test_diff_type_error(self):
        """Diff with non-LTSeq raises TypeError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "t.csv")
            with open(path, "w") as f:
                f.write("id\n1\n")
            t = LTSeq.read_csv(path)

            with pytest.raises(TypeError):
                t.diff({"id": 1})

    def test_is_subset_type_error(self):
        """is_subset with non-LTSeq raises TypeError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "t.csv")
            with open(path, "w") as f:
                f.write("id\n1\n")
            t = LTSeq.read_csv(path)

            with pytest.raises(TypeError):
                t.is_subset(None)

    def test_set_ops_preserve_schema(self):
        """Set operations preserve schema from left table."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "t1.csv")
            path2 = os.path.join(tmpdir, "t2.csv")

            with open(path1, "w") as f:
                f.write("id,name,age\n1,Alice,30\n")
            with open(path2, "w") as f:
                f.write("id,name,age\n1,Alice,30\n2,Bob,25\n")

            t1 = LTSeq.read_csv(path1)
            t2 = LTSeq.read_csv(path2)

            union_result = t1.union(t2)
            intersect_result = t1.intersect(t2)
            diff_result = t1.diff(t2)

            # All should have same columns as t1
            for result in [union_result, intersect_result, diff_result]:
                assert set(result._schema.keys()) == {"id", "name", "age"}
