"""Partitioning support for LTSeq tables.

This module provides PartitionedTable for non-consecutive grouping of rows.
Unlike group_ordered() which handles consecutive groups, partition() groups
all rows with the same key value regardless of position.
"""

import math
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Iterator

if TYPE_CHECKING:
    from .core import LTSeq

# What a NaN key is looked up by: NaN equals no value, itself included.
_NAN_KEY = object()


def _lookup_key(key: Any) -> Any:
    """The value a partition key is looked up by.

    Python compares two datetimes that share a tzinfo by wall clock and
    ignores ``fold`` (PEP 495), so the two occurrences of a repeated hour,
    01:30 EDT and 01:30 EST, compare and hash equal. An aware key is looked
    up by its UTC instant, which tells them apart; a NaN key is one value.
    """
    if isinstance(key, tuple):
        return tuple(_lookup_key(value) for value in key)
    if isinstance(key, datetime) and key.utcoffset() is not None:
        return key.astimezone(timezone.utc)
    if isinstance(key, float) and math.isnan(key):
        return _NAN_KEY
    return key


def _positions(keys: list[Any]) -> dict[Any, int]:
    """Each key's position in ``keys``, by the value it is looked up by."""
    return {_lookup_key(key): position for position, key in enumerate(keys)}


class SQLPartitionedTable:
    """
    Column-based partitioned table.

    The distinct keys are found with one query; each partition is a lazy
    filter on its key's values, compared as values of the key columns' own
    types (a NULL key matches NULL). Python lambdas never run per row.

    Supports the same interface as PartitionedTable:
    - Dict-like access: partitioned[key]
    - Iteration: for key, table in partitioned.items()
    - Mapping: partitioned.map(lambda t: t.agg(...))

    A key is looked up as Python compares it, except that an aware
    ``datetime`` is looked up by its instant and any NaN finds the NaN key.

    Example:
        >>> t = LTSeq.read_csv("sales.csv")
        >>> partitions = t.partition("region")
        >>> west_sales = partitions["West"]  # Single column: key is the value
        >>> partitions = t.partition("year", "region")
        >>> west_2023 = partitions[(2023, "West")]  # Multiple columns: key is tuple
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
        # The distinct keys as Arrow, row i holding keys()[i]
        self._key_rows: Any = None
        self._positions: dict[Any, int] | None = None
        self._partitions_cache: dict[int, "LTSeq"] = {}

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
        return self._partition(self._position(key))

    def _position(self, key: Any) -> int:
        """The position in keys() of the key that ``key`` looks up."""
        keys = self.keys()
        if self._positions is None:
            self._positions = _positions(keys)
        if len(self._columns) == 1 and isinstance(key, tuple) and len(key) == 1:
            key = key[0]
        position = self._positions.get(_lookup_key(key))
        if position is None:
            raise KeyError(f"Partition key '{key}' not found")
        return position

    def _partition(self, position: int) -> "LTSeq":
        """The rows of the key at ``position`` in keys(), built once."""
        partition = self._partitions_cache.get(position)
        if partition is None:
            key_row = self._key_rows.slice(position, 1)
            try:
                inner = self._ltseq._inner.filter_key_row(key_row)
            except Exception as e:
                raise KeyError(
                    f"Failed to access partition for key {self.keys()[position]}: {e}"
                )
            partition = type(self._ltseq)._from_inner(inner)
            self._partitions_cache[position] = partition
        return partition

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

        # Keys looked up as one value are listed once. `distinct` keeps NaNs
        # of other bits apart (#290), and a NaN key's partition holds every
        # NaN row.
        first: dict[Any, int] = {}
        try:
            for position, key in enumerate(keys):
                first.setdefault(_lookup_key(key), position)
        except TypeError:  # a list or struct key is unhashable: none is merged
            first = {}
        if 0 < len(first) < len(keys):
            kept = list(first.values())
            keys = [keys[position] for position in kept]
            pa_table = pa_table.take(kept)

        self._key_rows = pa_table
        self._keys_cache = keys

    def values(self) -> list["LTSeq"]:
        """
        Return all partition tables.

        Returns:
            List of LTSeq objects, one per partition
        """
        return [self._partition(i) for i in range(len(self.keys()))]

    def items(self) -> Iterator[tuple[Any, "LTSeq"]]:
        """
        Iterate through (key, table) pairs.

        Returns:
            Iterator of (partition_key, partition_table) tuples
        """
        for position, key in enumerate(self.keys()):
            yield key, self._partition(position)

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
        assert self._keys_cache is not None
        return len(self._keys_cache)

    def map(self, fn: Callable[["LTSeq"], "LTSeq"]) -> "PartitionedTable":
        """
        Apply function to each partition and return new PartitionedTable.

        Args:
            fn: Function that takes LTSeq and returns LTSeq

        Returns:
            New PartitionedTable with transformed partitions
        """
        return _map_partitions(self.items(), fn)

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
        return _map_partitions(self.items(), fn)

    def to_list(self) -> list["LTSeq"]:
        """
        Convert to list of LTSeq objects.

        Returns:
            List of partition tables in arbitrary order
        """
        return self.values()

def _partition_expr_to_columns(expr_dict: dict[str, Any]) -> tuple[str, ...] | None:
    """Convert a captured partition lambda into SQL partition columns.

    Strict-mode partitioning accepts only direct column references so the
    implementation can stay inside Rust/DataFusion without internal Arrow
    materialization.
    """
    if expr_dict.get("type") == "Column":
        return (expr_dict["name"],)
    return None


def _map_partitions(
    items: Iterator[tuple[Any, "LTSeq"]], fn: Callable[["LTSeq"], "LTSeq"]
) -> "_PrecomputedPartitionedTable":
    """Apply ``fn`` to each partition, keeping every key, in order."""
    keys: list[Any] = []
    results: list["LTSeq"] = []
    for key, partition in items:
        try:
            results.append(fn(partition))
        except Exception as e:
            raise RuntimeError(f"Error applying function to partition '{key}': {e}")
        keys.append(key)
    return _PrecomputedPartitionedTable(keys, results)


class _PrecomputedPartitionedTable(PartitionedTable):
    """
    A PartitionedTable with pre-computed partitions (no lazy evaluation).

    Used internally by map() to return results without recomputing partitions.
    Keys are looked up as in SQLPartitionedTable, so the partitions of two keys
    that Python compares equal, such as the two occurrences of a repeated hour,
    stay apart.
    """

    def __init__(self, keys: list[Any], partitions: list["LTSeq"]):
        """
        Initialize with pre-computed partitions.

        Args:
            keys: The partition keys
            partitions: The table of each key, in the order of ``keys``
        """
        # Don't call super().__init__() since we don't have partition_fn
        self._ltseq = None
        self._partition_fn = None
        self._keys = keys
        self._partitions = partitions
        self._positions = _positions(keys)
        self._delegate = None

    def __getitem__(self, key: Any) -> "LTSeq":
        position = self._positions.get(_lookup_key(key))
        if position is None:
            raise KeyError(f"Partition key '{key}' not found")
        return self._partitions[position]

    def keys(self) -> list[Any]:
        return list(self._keys)

    def values(self) -> list["LTSeq"]:
        return list(self._partitions)

    def items(self) -> Iterator[tuple[Any, "LTSeq"]]:
        return iter(zip(self._keys, self._partitions))

    def __len__(self) -> int:
        return len(self._keys)
