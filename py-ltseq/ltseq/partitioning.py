"""Partitioning support for LTSeq tables.

This module provides PartitionedTable for non-consecutive grouping of rows.
Unlike group_ordered() which handles consecutive groups, partition() groups
all rows with the same key value regardless of position.
"""

from typing import Any, Callable, Iterator

class SQLPartitionedTable:
    """
    SQL-based partitioned table for fast column-based partitioning.

    This implementation uses SQL (DISTINCT, WHERE) for partitioning instead
    of evaluating Python lambdas on each row. Much faster for large datasets.

    Supports the same interface as PartitionedTable:
    - Dict-like access: partitioned[key]
    - Iteration: for key, table in partitioned.items()
    - Mapping: partitioned.map(lambda t: t.agg(...))

    Example:
        >>> t = LTSeq.read_csv("sales.csv")
        >>> partitions = t.partition_by("region")
        >>> west_sales = partitions["West"]  # Single column: key is the value
        >>> partitions = t.partition_by("year", "region")
        >>> 2023_west = partitions[(2023, "West")]  # Multiple columns: key is tuple
    """

    def __init__(self, ltseq_instance: "LTSeq", columns: tuple[str, ...]):
        """
        Initialize SQLPartitionedTable.

        Args:
            ltseq_instance: The LTSeq table to partition
            columns: Tuple of column names to partition by
        """
        self._ltseq = ltseq_instance
        self._columns = columns
        self._keys_cache: list[Any] | None = None
        self._partitions_cache: dict[Any, "LTSeq"] | None = None

    def __getitem__(self, key: Any) -> "LTSeq":
        """
        Access partition by key.

        Args:
            key: The partition key value (single value or tuple)

        Returns:
            LTSeq table containing all rows with that partition key

        Raises:
            KeyError: If the key doesn't exist in the partitions
        """
        if self._partitions_cache is None:
            self._partitions_cache = {}

        key_tuple = self._normalize_key(key)

        if self._keys_cache is None:
            self._compute_keys()
        if key not in self._keys_cache and key_tuple not in self._keys_cache:
            raise KeyError(f"Partition key '{key}' not found")

        # Check cache first
        if key_tuple in self._partitions_cache:
            return self._partitions_cache[key_tuple]

        # Build WHERE clause for this key
        conditions = []
        for col, val in zip(self._columns, key_tuple):
            if val is None:
                conditions.append(f'"{col}" IS NULL')
            elif isinstance(val, str):
                escaped_val = val.replace("'", "''")
                conditions.append(f"\"{col}\" = '{escaped_val}'")
            elif isinstance(val, bool):
                conditions.append(f'"{col}" = {str(val).lower()}')
            else:
                conditions.append(f'"{col}" = {val}')

        where_clause = " AND ".join(conditions)

        # Use filter_where to get the partition
        try:
            filtered_inner = self._ltseq._inner.filter_where(where_clause)
            partition = self._ltseq.__class__()
            partition._schema = self._ltseq._schema.copy()
            partition._inner = filtered_inner
            self._partitions_cache[key_tuple] = partition
            return partition
        except Exception as e:
            raise KeyError(f"Failed to access partition for key {key}: {e}")

    def _normalize_key(self, key: Any) -> tuple[Any, ...]:
        if len(self._columns) == 1:
            return (key,) if not isinstance(key, tuple) else key
        return key if isinstance(key, tuple) else (key,)

    def keys(self) -> list[Any]:
        """
        Return all partition keys.

        Returns:
            List of unique partition key values
        """
        if self._keys_cache is None:
            self._compute_keys()
        return self._keys_cache  # type: ignore[return-value]

    def _compute_keys(self) -> None:
        """Compute distinct keys using Arrow (avoids pandas round-trip)."""
        distinct_ltseq = self._ltseq.select(*self._columns).distinct()
        pa_table = distinct_ltseq.to_arrow()

        keys = []
        if len(self._columns) == 1:
            col_name = self._columns[0]
            col = pa_table.column(col_name)
            keys = col.to_pylist()
        else:
            columns = [pa_table.column(c).to_pylist() for c in self._columns]
            for i in range(pa_table.num_rows):
                keys.append(tuple(columns[j][i] for j in range(len(self._columns))))

        self._keys_cache = keys

    def values(self) -> list["LTSeq"]:
        """
        Return all partition tables.

        Returns:
            List of LTSeq objects, one per partition
        """
        if self._keys_cache is None:
            self._compute_keys()

        return [self[key] for key in self._keys_cache]  # type: ignore[union-attr]

    def items(self) -> Iterator[tuple[Any, "LTSeq"]]:
        """
        Iterate through (key, table) pairs.

        Returns:
            Iterator of (partition_key, partition_table) tuples
        """
        if self._keys_cache is None:
            self._compute_keys()

        for key in self._keys_cache:
            yield key, self[key]

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
        if self._keys_cache is None:
            self._compute_keys()
        return len(self._keys_cache)

    def map(self, fn: Callable[["LTSeq"], "LTSeq"]) -> "PartitionedTable":
        """
        Apply function to each partition and return new PartitionedTable.

        Args:
            fn: Function that takes LTSeq and returns LTSeq

        Returns:
            New PartitionedTable with transformed partitions
        """
        if self._keys_cache is None:
            self._compute_keys()

        transformed_partitions: dict[Any, "LTSeq"] = {}
        for key in self._keys_cache:  # type: ignore[union-attr]
            try:
                result = fn(self[key])
                transformed_partitions[key] = result
            except Exception as e:
                raise RuntimeError(f"Error applying function to partition '{key}': {e}")

        return _PrecomputedPartitionedTable(transformed_partitions)

    def to_list(self) -> list["LTSeq"]:
        """
        Convert to list of LTSeq objects.

        Returns:
            List of partition tables in arbitrary order
        """
        return self.values()


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
        self._partitions_cache: dict[Any, "LTSeq"] | None = None
        self._delegate = self._build_delegate()

    def _build_delegate(self) -> SQLPartitionedTable:
        """Build the SQL-backed delegate for capturable partition expressions."""
        try:
            expr_dict = self._ltseq._capture_expr(self._partition_fn)
        except Exception as e:
            raise ValueError(
                "partition(by=callable) only supports simple column expressions such as "
                "lambda r: r.region. Complex Python logic is no longer supported "
                "because it forces internal materialization. "
                f"Capture failed with: {e}"
            ) from e

        columns = _partition_expr_to_columns(expr_dict)
        if columns is None:
            raise ValueError(
                "partition(by=callable) only supports simple column expressions such as "
                "lambda r: r.region or lambda r: r.year. Derived Python expressions "
                "must use explicit column partitioning or another API."
            )

        return SQLPartitionedTable(self._ltseq, columns)

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
        return self._delegate[key]

    def keys(self) -> list[Any]:
        """
        Return all partition keys.

        Returns:
            List of unique partition key values

        Example:
            >>> regions = partitions.keys()
            >>> print(regions)  # ['West', 'East', 'Central']
        """
        return self._delegate.keys()

    def values(self) -> list["LTSeq"]:
        """
        Return all partition tables.

        Returns:
            List of LTSeq objects, one per partition
        """
        return self._delegate.values()

    def items(self) -> Iterator[tuple[Any, "LTSeq"]]:
        """
        Iterate through (key, table) pairs.

        Returns:
            Iterator of (partition_key, partition_table) tuples

        Example:
            >>> for region, data in partitions.items():
            ...     print(f"{region}: {len(data)} rows")
        """
        return self._delegate.items()

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
        return len(self._delegate)

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
        transformed_partitions: dict[Any, "LTSeq"] = {}
        for key, partition_table in self._delegate.items():
            try:
                result = fn(partition_table)
                transformed_partitions[key] = result
            except Exception as e:
                raise RuntimeError(f"Error applying function to partition '{key}': {e}")

        # Return a new PartitionedTable wrapper with transformed data
        return _PrecomputedPartitionedTable(transformed_partitions)

    def to_list(self) -> list["LTSeq"]:
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
        1. Gets data from the underlying table via Arrow
        2. Evaluates partition function on each row
        3. Groups rows by partition key
        4. Creates LTSeq for each partition
        5. Caches results
        """
        # Retained for backward compatibility with tests/internal calls.
        # PartitionedTable now delegates to SQLPartitionedTable instead of
        # materializing Arrow data in Python.
        self._partitions_cache = {key: table for key, table in self._delegate.items()}


def _partition_expr_to_columns(expr_dict: dict[str, Any]) -> tuple[str, ...] | None:
    """Convert a captured partition lambda into SQL partition columns.

    Strict-mode partitioning accepts only direct column references so the
    implementation can stay inside Rust/DataFusion without internal Arrow
    materialization.
    """
    if expr_dict.get("type") == "Column":
        return (expr_dict["name"],)
    return None


class _PrecomputedPartitionedTable(PartitionedTable):
    """
    A PartitionedTable with pre-computed partitions (no lazy evaluation).

    Used internally by map() to return results without recomputing partitions.
    """

    def __init__(self, precomputed_partitions: dict[Any, "LTSeq"]):
        """
        Initialize with pre-computed partition dictionary.

        Args:
            precomputed_partitions: Dict mapping partition keys to LTSeq tables
        """
        # Don't call super().__init__() since we don't have partition_fn
        self._ltseq = None
        self._partition_fn = None
        self._partitions_cache = precomputed_partitions
        self._delegate = None

    def __getitem__(self, key: Any) -> "LTSeq":
        if key not in self._partitions_cache:
            raise KeyError(f"Partition key '{key}' not found")
        return self._partitions_cache[key]

    def keys(self) -> list[Any]:
        return list(self._partitions_cache.keys())

    def values(self) -> list["LTSeq"]:
        return list(self._partitions_cache.values())

    def items(self) -> Iterator[tuple[Any, "LTSeq"]]:
        return iter(self._partitions_cache.items())

    def __len__(self) -> int:
        return len(self._partitions_cache)

    def _materialize_partitions(self) -> None:
        """Override: partitions are already materialized."""
        pass  # No-op since _partitions_cache is already set
