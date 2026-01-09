"""Tests for partition_by() method - SQL-based column partitioning."""

import pytest
from ltseq import LTSeq


class TestPartitionBySingle:
    """Test partition_by() with single column."""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create a sample CSV file for testing."""
        csv_file = tmp_path / "test_partition.csv"
        csv_file.write_text(
            "date,region,amount\n"
            "2023-01-01,West,100\n"
            "2023-01-02,East,200\n"
            "2023-01-03,West,150\n"
            "2023-01-04,Central,300\n"
            "2023-01-05,East,250\n"
        )
        return str(csv_file)

    def test_partition_by_single_column(self, sample_csv):
        """Partition by single column should create correct groups."""
        t = LTSeq.read_csv(sample_csv)
        partitions = t.partition_by("region")

        # Check that we have the right number of partitions
        assert len(partitions) == 3

        # Check partition keys
        keys = set(partitions.keys())
        assert keys == {"West", "East", "Central"}

    def test_partition_by_single_column_access(self, sample_csv):
        """Access partitions by key should work."""
        t = LTSeq.read_csv(sample_csv)
        partitions = t.partition_by("region")

        # Access specific partition
        west = partitions["West"]
        assert isinstance(west, LTSeq)
        assert len(west) == 2  # Two West entries

    def test_partition_by_iteration(self, sample_csv):
        """Iterate over partitions should work."""
        t = LTSeq.read_csv(sample_csv)
        partitions = t.partition_by("region")

        partition_dict = dict(partitions.items())
        assert len(partition_dict) == 3
        assert len(partition_dict["West"]) == 2
        assert len(partition_dict["East"]) == 2
        assert len(partition_dict["Central"]) == 1


class TestPartitionByMultiple:
    """Test partition_by() with multiple columns."""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create a sample CSV file for testing."""
        csv_file = tmp_path / "test_partition_multi.csv"
        csv_file.write_text(
            "year,region,amount\n"
            "2023,West,100\n"
            "2023,East,200\n"
            "2024,West,150\n"
            "2024,Central,300\n"
            "2023,East,250\n"
        )
        return str(csv_file)

    def test_partition_by_multiple_columns(self, sample_csv):
        """Partition by multiple columns should create correct groups."""
        t = LTSeq.read_csv(sample_csv)
        partitions = t.partition_by("year", "region")

        # Check that we have the right number of partitions
        assert len(partitions) == 4

    def test_partition_by_multiple_columns_access(self, sample_csv):
        """Access partitions by tuple key should work."""
        t = LTSeq.read_csv(sample_csv)
        partitions = t.partition_by("year", "region")

        # Access specific partition (tuple key)
        p_2023_west = partitions[(2023, "West")]
        assert isinstance(p_2023_west, LTSeq)
        assert len(p_2023_west) == 1

    def test_partition_by_multiple_columns_iteration(self, sample_csv):
        """Iterate over multi-column partitions should work."""
        t = LTSeq.read_csv(sample_csv)
        partitions = t.partition_by("year", "region")

        partition_dict = dict(partitions.items())
        assert len(partition_dict) == 4
        assert len(partition_dict[(2023, "West")]) == 1
        assert len(partition_dict[(2024, "West")]) == 1
        assert len(partition_dict[(2023, "East")]) == 2
        assert len(partition_dict[(2024, "Central")]) == 1


class TestPartitionByValidation:
    """Test error handling for partition_by()."""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create a sample CSV file for testing."""
        csv_file = tmp_path / "test_partition_validation.csv"
        csv_file.write_text(
            "date,region,amount\n2023-01-01,West,100\n2023-01-02,East,200\n"
        )
        return str(csv_file)

    def test_partition_by_invalid_column(self, sample_csv):
        """partition_by() should raise error for non-existent column."""
        t = LTSeq.read_csv(sample_csv)

        with pytest.raises(AttributeError):
            t.partition_by("nonexistent")

    def test_partition_by_without_schema(self):
        """partition_by() should raise error if schema not initialized."""
        t = LTSeq()

        with pytest.raises(ValueError):
            t.partition_by("region")


class TestPartitionByEquivalence:
    """Test that partition_by() produces same results as partition()."""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create a sample CSV file for testing."""
        csv_file = tmp_path / "test_partition_equiv.csv"
        csv_file.write_text(
            "date,region,amount\n"
            "2023-01-01,West,100\n"
            "2023-01-02,East,200\n"
            "2023-01-03,West,150\n"
            "2023-01-04,Central,300\n"
            "2023-01-05,East,250\n"
        )
        return str(csv_file)

    def test_partition_by_vs_partition_single_column(self, sample_csv):
        """partition_by and partition should produce same results for single column."""
        t = LTSeq.read_csv(sample_csv)

        p1 = t.partition_by("region")
        p2 = t.partition(by=lambda r: r.region)

        # Same number of partitions
        assert len(p1) == len(p2)

        # Same keys (accounting for format differences)
        keys1 = set(p1.keys())
        keys2 = set(p2.keys())
        assert keys1 == keys2

        # Same data in each partition
        for key in keys1:
            assert len(p1[key]) == len(p2[key])

    def test_partition_by_vs_partition_multiple_columns(self, sample_csv):
        """partition_by and partition should produce same results for multiple columns."""
        t = LTSeq.read_csv(sample_csv)

        # partition_by("region") on this data
        p1 = t.partition_by("region")
        p2 = t.partition(by=lambda r: r.region)

        # Both should have same partitions
        assert len(p1) == len(p2)
        assert set(p1.keys()) == set(p2.keys())


class TestPartitionByChaining:
    """Test that partition_by() results support chaining operations."""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create a sample CSV file for testing."""
        csv_file = tmp_path / "test_partition_chain.csv"
        csv_file.write_text(
            "date,region,amount\n"
            "2023-01-01,West,100\n"
            "2023-01-02,East,200\n"
            "2023-01-03,West,150\n"
            "2023-01-04,Central,300\n"
            "2023-01-05,East,250\n"
        )
        return str(csv_file)

    def test_partition_by_map_operation(self, sample_csv):
        """partition_by() result should support map() for applying operations."""
        t = LTSeq.read_csv(sample_csv)
        partitions = t.partition_by("region")

        # Map should work on partitions
        results = partitions.map(lambda partition: len(partition))

        # Results should be convertible to list with counts for each partition
        result_list = results.to_list()
        assert len(result_list) == 3
        assert set(result_list) == {1, 2}  # Central has 1, West and East have 2 each


class TestPartitionByEmptyResult:
    """Test partition_by() with various data scenarios."""

    def test_partition_by_single_row(self, tmp_path):
        """partition_by() should work with single row table."""
        csv_file = tmp_path / "test_single_row.csv"
        csv_file.write_text("region,amount\nWest,100\n")

        t = LTSeq.read_csv(str(csv_file))
        partitions = t.partition_by("region")

        assert len(partitions) == 1
        assert len(partitions["West"]) == 1

    def test_partition_by_all_same_key(self, tmp_path):
        """partition_by() should work when all rows have same key."""
        csv_file = tmp_path / "test_same_key.csv"
        csv_file.write_text("region,amount\nWest,100\nWest,200\nWest,300\n")

        t = LTSeq.read_csv(str(csv_file))
        partitions = t.partition_by("region")

        assert len(partitions) == 1
        assert len(partitions["West"]) == 3
