"""Tests for Phase 3.1: partition() functionality.

Tests the partition() method which groups rows with the same key value
regardless of their position in the table (unlike group_ordered() which
handles only consecutive groups).
"""

import pytest
from ltseq import LTSeq, PartitionedTable


class TestPartitionBasic:
    """Basic partition() functionality tests."""

    def test_partition_creates_partitioned_table(self):
        """partition() should return a PartitionedTable."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        result = t.partition(by=lambda r: r.region)
        assert isinstance(result, PartitionedTable)

    def test_partition_dict_like_access(self):
        """PartitionedTable should support dict-like access by key."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        partitions = t.partition(by=lambda r: r.region)

        # Access a partition by key
        west = partitions["West"]
        assert isinstance(west, LTSeq)
        assert len(west) > 0

    def test_partition_missing_key_raises_keyerror(self):
        """Accessing non-existent partition key should raise KeyError."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        partitions = t.partition(by=lambda r: r.region)

        with pytest.raises(KeyError):
            _ = partitions["NonExistentRegion"]

    def test_partition_keys(self):
        """PartitionedTable.keys() should return all partition keys."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        partitions = t.partition(by=lambda r: r.region)

        keys = partitions.keys()
        assert isinstance(keys, list)
        assert len(keys) > 0
        assert "West" in keys
        assert "East" in keys

    def test_partition_values(self):
        """PartitionedTable.values() should return all partition tables."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        partitions = t.partition(by=lambda r: r.region)

        values = partitions.values()
        assert isinstance(values, list)
        assert len(values) > 0
        assert all(isinstance(v, LTSeq) for v in values)

    def test_partition_items(self):
        """PartitionedTable.items() should iterate (key, table) pairs."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        partitions = t.partition(by=lambda r: r.region)

        items = list(partitions.items())
        assert len(items) > 0

        for key, table in items:
            assert isinstance(key, str)
            assert isinstance(table, LTSeq)

    def test_partition_iteration(self):
        """PartitionedTable should be iterable over partition tables."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        partitions = t.partition(by=lambda r: r.region)

        tables = list(partitions)
        assert len(tables) > 0
        assert all(isinstance(t, LTSeq) for t in tables)

    def test_partition_len(self):
        """len(PartitionedTable) should return number of partitions."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        partitions = t.partition(by=lambda r: r.region)

        count = len(partitions)
        assert isinstance(count, int)
        assert count > 0
        assert count == len(partitions.keys())


class TestPartitionMap:
    """Tests for PartitionedTable.map() functionality."""

    def test_partition_map_basic(self):
        """map() should apply function to each partition."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        partitions = t.partition(by=lambda r: r.region)

        # Simple identity map - just return each partition as-is
        result = partitions.map(lambda p: p)
        assert isinstance(result, PartitionedTable)
        assert len(result) == len(partitions)

    def test_partition_map_preserves_keys(self):
        """map() should preserve partition keys."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        partitions = t.partition(by=lambda r: r.region)
        original_keys = partitions.keys()

        result = partitions.map(lambda p: p)
        result_keys = result.keys()

        assert set(original_keys) == set(result_keys)


class TestPartitionRowCoverage:
    """Tests that partition() covers all rows."""

    def test_partition_no_rows_lost(self):
        """All rows should be in some partition."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        original_count = len(t)

        partitions = t.partition(by=lambda r: r.region)

        # Sum rows across all partitions
        total_rows = sum(len(p) for p in partitions)
        assert total_rows == original_count

    def test_partition_no_duplicate_rows(self):
        """Rows should not appear in multiple partitions."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        partitions = t.partition(by=lambda r: r.region)

        # Count rows in each partition
        row_counts = [len(p) for p in partitions]
        total_rows = sum(row_counts)

        # If no duplicates, total should match original
        assert total_rows == len(t)


class TestPartitionWithDifferentKeys:
    """Tests partition with different types of partition keys."""

    def test_partition_string_key(self):
        """partition() should work with string keys."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")

        partitions = t.partition(by=lambda r: r.region)
        assert len(partitions) >= 2
        assert "West" in partitions.keys()
        assert "East" in partitions.keys()

    def test_partition_numeric_key(self):
        """partition() should work with numeric partition keys."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")

        # Partition by amount range
        partitions = t.partition(by=lambda r: "low" if int(r.amount) < 1000 else "high")

        assert len(partitions) >= 1
        keys = partitions.keys()
        assert "low" in keys or "high" in keys


class TestPartitionErrorHandling:
    """Tests error handling in partition()."""

    def test_partition_on_uninitialized_table_raises(self):
        """partition() on table without schema should raise."""
        t = LTSeq()

        with pytest.raises(ValueError, match="Schema not initialized"):
            t.partition(by=lambda r: r.region)

    def test_partition_with_invalid_column_raises(self):
        """partition() with non-existent column should raise."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        partitions = t.partition(by=lambda r: r.nonexistent_column)

        # Error should occur when we try to access partitions
        with pytest.raises(RuntimeError):
            _ = partitions.keys()


class TestPartitionCaching:
    """Tests that partitions are cached after first access."""

    def test_partition_cache_efficiency(self):
        """Multiple accesses to same partition should use cache."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        partitions = t.partition(by=lambda r: r.region)

        # First access - computes partitions
        first_access = partitions["West"]

        # Second access - should use cache
        second_access = partitions["West"]

        # Both should have same data
        assert len(first_access) == len(second_access)


class TestPartitionScenarios:
    """Real-world partition scenarios."""

    def test_partition_multiway_split(self):
        """Partition by year to split data multiple ways."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        partitions = t.partition(by=lambda r: r.year)

        # Should have partitions for each year
        assert len(partitions) > 1
        for year_key, partition in partitions.items():
            assert len(partition) > 0

    def test_partition_nonconsequtive_groups(self):
        """partition() should group non-consecutive rows with same key."""
        # This is a key difference from group_ordered()
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")

        # Partition by region
        partitions = t.partition(by=lambda r: r.region)

        # Check West partition has all 3 West rows
        west = partitions["West"]
        assert len(west) == 3  # All 3 West rows grouped together


class TestFromRows:
    """Tests for _from_rows() classmethod used internally by partition()."""

    def test_from_rows_creates_ltseq(self):
        """_from_rows() should create LTSeq from row list."""
        rows = [
            {"region": "West", "amount": "1000", "year": "2020"},
            {"region": "East", "amount": "500", "year": "2021"},
        ]
        schema = {"region": "string", "amount": "string", "year": "string"}

        t = LTSeq._from_rows(rows, schema)
        assert isinstance(t, LTSeq)
        assert len(t) == 2

    def test_from_rows_empty_list(self):
        """_from_rows() should handle empty row list."""
        rows = []
        schema = {"region": "string", "amount": "string"}

        t = LTSeq._from_rows(rows, schema)
        assert isinstance(t, LTSeq)
        assert len(t) == 0

    def test_from_rows_preserves_schema(self):
        """_from_rows() should preserve schema."""
        rows = [{"region": "West", "amount": "1000"}]
        schema = {"region": "string", "amount": "string"}

        t = LTSeq._from_rows(rows, schema)
        assert t._schema == schema
