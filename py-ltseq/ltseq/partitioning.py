"""Partitioning support for LTSeq tables.

This module provides PartitionedTable for non-consecutive grouping of rows.
Unlike group_ordered() which handles consecutive groups, partition() groups
all rows with the same key value regardless of position.
"""

from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple


class PartitionedTable:
    """
    Container for partitioned data with dict-like access and iteration.

    Partitions a table into groups based on a key function, but unlike
    group_ordered(), includes all rows with the same key regardless of
    their position in the table.

    Supports:
    - Dict-like access: partitioned[key]
    - Iteration: for key, table in partitioned.items()
    - Mapping: partitioned.map(lambda t: t.agg(...))
    - Value access: partitioned.values()

    Example:
        >>> t = LTSeq.read_csv("sales.csv")
        >>> partitions = t.partition(by=lambda r: r.region)
        >>> west_sales = partitions["West"]
        >>> for region, data in partitions.items():
        ...     print(f"{region}: {len(data)} rows")
    """

    def __init__(self, ltseq_instance: "LTSeq", partition_fn: Callable):
        """
        Initialize PartitionedTable.

        Args:
            ltseq_instance: The LTSeq table to partition
            partition_fn: Lambda that returns partition key (e.g., lambda r: r.region)
        """
        self._ltseq = ltseq_instance
        self._partition_fn = partition_fn
        self._partitions_cache: Optional[Dict[Any, "LTSeq"]] = None

    def __getitem__(self, key: Any) -> "LTSeq":
        """
        Access partition by key.

        Args:
            key: The partition key value

        Returns:
            LTSeq table containing all rows with that partition key

        Raises:
            KeyError: If the key doesn't exist in the partitions

        Example:
            >>> west_sales = partitions["West"]
        """
        if self._partitions_cache is None:
            self._materialize_partitions()

        if key not in self._partitions_cache:
            raise KeyError(f"Partition key '{key}' not found")

        return self._partitions_cache[key]

    def keys(self) -> List[Any]:
        """
        Return all partition keys.

        Returns:
            List of unique partition key values

        Example:
            >>> regions = partitions.keys()
            >>> print(regions)  # ['West', 'East', 'Central']
        """
        if self._partitions_cache is None:
            self._materialize_partitions()
        return list(self._partitions_cache.keys())

    def values(self) -> List["LTSeq"]:
        """
        Return all partition tables.

        Returns:
            List of LTSeq objects, one per partition
        """
        if self._partitions_cache is None:
            self._materialize_partitions()
        return list(self._partitions_cache.values())

    def items(self) -> Iterator[Tuple[Any, "LTSeq"]]:
        """
        Iterate through (key, table) pairs.

        Returns:
            Iterator of (partition_key, partition_table) tuples

        Example:
            >>> for region, data in partitions.items():
            ...     print(f"{region}: {len(data)} rows")
        """
        if self._partitions_cache is None:
            self._materialize_partitions()
        return iter(self._partitions_cache.items())

    def __iter__(self) -> Iterator["LTSeq"]:
        """
        Iterate through partition tables (values only).

        Returns:
            Iterator of LTSeq partition tables
        """
        return iter(self.values())

    def __len__(self) -> int:
        """
        Return number of partitions.

        Returns:
            Count of distinct partition keys
        """
        if self._partitions_cache is None:
            self._materialize_partitions()
        return len(self._partitions_cache)

    def map(self, fn: Callable[["LTSeq"], "LTSeq"]) -> "PartitionedTable":
        """
        Apply function to each partition and return new PartitionedTable.

        Useful for parallel processing: apply same operation to each partition.

        Args:
            fn: Function that takes LTSeq and returns LTSeq

        Returns:
            New PartitionedTable with transformed partitions

        Example:
            >>> # Get per-region totals
            >>> totals = partitions.map(lambda t: t.agg(total=lambda g: g.sales.sum()))
            >>> for region, total_row in totals.items():
            ...     print(f"{region}: {total_row}")
        """
        if self._partitions_cache is None:
            self._materialize_partitions()

        # Apply function to each partition
        transformed_partitions = {}
        for key, partition_table in self._partitions_cache.items():
            try:
                result = fn(partition_table)
                transformed_partitions[key] = result
            except Exception as e:
                raise RuntimeError(f"Error applying function to partition '{key}': {e}")

        # Return a new PartitionedTable wrapper with transformed data
        return _PrecomputedPartitionedTable(transformed_partitions)

    def to_list(self) -> List["LTSeq"]:
        """
        Convert to list of LTSeq objects.

        Returns:
            List of partition tables in arbitrary order
        """
        return self.values()

    def _materialize_partitions(self) -> None:
        """
        Compute the partitions (triggered on first access).

        This method:
        1. Gets data from the underlying table via pandas
        2. Evaluates partition function on each row
        3. Groups rows by partition key
        4. Creates LTSeq for each partition
        5. Caches results

        Note: Requires pandas. If not installed, raises RuntimeError.
        """
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError(
                "partition() requires pandas. Install it with: pip install pandas"
            )

        # Get data as pandas DataFrame
        df = self._ltseq.to_pandas()
        rows = df.to_dict("records")

        # Evaluate partition function on each row and group
        partitions_dict: Dict[Any, List[Dict]] = {}

        for row_idx, row_data in enumerate(rows):
            try:
                # Create a proxy-like object to pass to the partition function
                row_proxy = _RowProxy(row_data)
                partition_key = self._partition_fn(row_proxy)

                if partition_key not in partitions_dict:
                    partitions_dict[partition_key] = []
                partitions_dict[partition_key].append(row_data)
            except Exception as e:
                raise RuntimeError(
                    f"Error evaluating partition function on row {row_idx}: {e}"
                )

        # Convert grouped rows back to LTSeq tables
        self._partitions_cache = {}
        for key, row_list in partitions_dict.items():
            try:
                partition_table = self._ltseq.__class__._from_rows(
                    row_list, self._ltseq._schema
                )
                self._partitions_cache[key] = partition_table
            except Exception as e:
                raise RuntimeError(f"Error creating LTSeq for partition '{key}': {e}")


class _PrecomputedPartitionedTable(PartitionedTable):
    """
    A PartitionedTable with pre-computed partitions (no lazy evaluation).

    Used internally by map() to return results without recomputing partitions.
    """

    def __init__(self, precomputed_partitions: Dict[Any, "LTSeq"]):
        """
        Initialize with pre-computed partition dictionary.

        Args:
            precomputed_partitions: Dict mapping partition keys to LTSeq tables
        """
        # Don't call super().__init__() since we don't have partition_fn
        self._ltseq = None
        self._partition_fn = None
        self._partitions_cache = precomputed_partitions

    def _materialize_partitions(self) -> None:
        """Override: partitions are already materialized."""
        pass  # No-op since _partitions_cache is already set


class _RowProxy:
    """
    Proxy object for accessing row data in partition function.

    Allows syntax like: partition_fn(lambda r: r.region)
    """

    def __init__(self, row_data: Dict[str, Any]):
        """Initialize with row dictionary."""
        self._data = row_data

    def __getattr__(self, name: str) -> Any:
        """Get column value by name."""
        if name.startswith("_"):
            # Allow access to internal attributes
            return object.__getattribute__(self, name)

        if name not in self._data:
            raise AttributeError(f"Column '{name}' not found in row")

        return self._data[name]
