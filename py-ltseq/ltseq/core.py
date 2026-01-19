"""Core LTSeq table class with data operations."""

from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from .expr import SchemaProxy, _lambda_to_expr
from .helpers import _infer_schema_from_csv

# Import mixin classes
from .io_ops import IOMixin
from .transforms import TransformMixin
from .joins import JoinMixin
from .aggregation import AggregationMixin
from .advanced_ops import SetOpsMixin, AdvancedOpsMixin

try:
    from . import ltseq_core

    HAS_RUST_BINDING = True
except ImportError:
    HAS_RUST_BINDING = False


class LTSeq(
    IOMixin,
    TransformMixin,
    JoinMixin,
    AggregationMixin,
    SetOpsMixin,
    AdvancedOpsMixin,
):
    """Python-visible LTSeq wrapper backed by the native Rust kernel."""

    def __init__(self):
        if not HAS_RUST_BINDING:
            raise RuntimeError(
                "Rust extension ltseq_core not available. "
                "Please rebuild with `maturin develop`."
            )
        self._inner = ltseq_core.LTSeqTable()
        self._schema: Dict[str, str] = {}
        self._csv_path: Optional[str] = None  # Track original CSV file for to_pandas()
        self._has_header: bool = True  # Track header setting for to_pandas()
        self._sort_keys: Optional[List[Tuple[str, bool]]] = (
            None  # [(col, is_desc), ...]
        )
        self._name: Optional[str] = None  # Table name for lookup operations

    def show(self, n: int = 10) -> None:
        """
        Display the data as a pretty-printed ASCII table.

        Args:
            n: Maximum number of rows to display (default 10)

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> t.show()
        """
        out = self._inner.show(n)
        print(out)

    def to_pandas(self):
        """
        Convert the table to a pandas DataFrame.

        Returns:
            A pandas DataFrame containing the table data

        Requires pandas to be installed.

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> df = t.to_pandas()
        """
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError(
                "to_pandas() requires pandas. Install it with: pip install pandas"
            )

        # If we have the original CSV path and it still exists, read it directly
        if self._csv_path:
            import os

            if os.path.exists(self._csv_path):
                has_header = getattr(self, "_has_header", True)
                if has_header:
                    return pd.read_csv(self._csv_path)
                else:
                    df = pd.read_csv(self._csv_path, header=None)
                    df.columns = [f"column_{i + 1}" for i in range(len(df.columns))]
                    return df

        # Otherwise, try to use CSV round-trip via write_csv
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            temp_path = f.name

        try:
            try:
                self.write_csv(temp_path)
                df = pd.read_csv(temp_path)
                return df
            except (AttributeError, RuntimeError):
                # Empty table or no data loaded - return empty DataFrame with schema
                return pd.DataFrame(columns=self._schema.keys())
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def __len__(self) -> int:
        """
        Get the number of rows in the table.

        Returns:
            The count of rows

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> print(len(t))  # Number of rows
        """
        return self._inner.count()

    def count(self) -> int:
        """
        Return the number of rows in the table.

        Returns:
            Integer row count

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> n = t.filter(lambda r: r.status == "active").count()
        """
        return len(self)

    def collect(self) -> List[Dict[str, Any]]:
        """
        Materialize all rows as a list of dictionaries.

        Returns:
            List of row dictionaries, where each dict maps column names to values

        Raises:
            MemoryError: If the dataset is too large to fit in memory
            RuntimeError: If execution fails

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> rows = t.filter(lambda r: r.age > 18).collect()
            >>> for row in rows:
            ...     print(row["name"])
        """
        df = self.to_pandas()
        return df.to_dict("records")

    @property
    def schema(self) -> Dict[str, str]:
        """
        Return the table schema with column names and types.

        Returns:
            Dictionary mapping column names to their data types

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> print(t.schema)  # {"id": "Int64", "name": "Utf8", ...}
        """
        return self._schema.copy()

    @property
    def columns(self) -> List[str]:
        """
        Return list of column names.

        Returns:
            List of column name strings

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> print(t.columns)  # ["id", "name", "age"]
        """
        return list(self._schema.keys())

    @property
    def sort_keys(self) -> Optional[List[Tuple[str, bool]]]:
        """
        Get the current sort keys for this table.

        Returns the list of (column_name, is_descending) tuples representing
        the sort order, or None if the sort order is unknown.

        Returns:
            List of (column_name, is_desc) tuples, or None if unknown

        Example:
            >>> t = LTSeq.read_csv("data.csv").sort("date", "id")
            >>> t.sort_keys
            [('date', False), ('id', False)]
        """
        return self._sort_keys

    def is_sorted_by(self, *keys: str, desc: Union[bool, List[bool]] = False) -> bool:
        """
        Check if this table is sorted by the specified keys.

        Uses prefix matching: if the table is sorted by ["a", "b", "c"],
        then is_sorted_by("a") and is_sorted_by("a", "b") both return True.

        Args:
            *keys: Column names to check (in order)
            desc: Expected descending flag(s). Can be:
                  - Single boolean: applies to all keys
                  - List of booleans: one per key
                  Default: False (all ascending)

        Returns:
            True if table is sorted by the specified keys with matching direction

        Raises:
            ValueError: If desc list length doesn't match number of keys

        Example:
            >>> t = LTSeq.read_csv("data.csv").sort("date", "id")
            >>> t.is_sorted_by("date")  # True (prefix match)
            >>> t.is_sorted_by("date", "id")  # True (exact match)
        """
        if self._sort_keys is None:
            return False

        if not keys:
            return False

        if isinstance(desc, bool):
            desc_flags = [desc] * len(keys)
        elif isinstance(desc, list):
            if len(desc) != len(keys):
                raise ValueError(
                    f"desc list length ({len(desc)}) must match number of keys ({len(keys)})"
                )
            desc_flags = desc
        else:
            raise TypeError(f"desc must be bool or list, got {type(desc).__name__}")

        for i, key in enumerate(keys):
            if i >= len(self._sort_keys):
                return False
            col, is_desc = self._sort_keys[i]
            if col != key or is_desc != desc_flags[i]:
                return False

        return True

    def _capture_expr(self, fn: Callable) -> Dict[str, Any]:
        """
        Capture and serialize an expression tree from a lambda.

        This internal method intercepts Python lambdas and converts them to
        serializable expression dicts without executing Python logic.

        Args:
            fn: Lambda function, e.g., lambda r: r.age > 18

        Returns:
            Serialized expression dict ready for Rust deserialization

        Raises:
            TypeError: If lambda doesn't return an Expr
            AttributeError: If lambda references a non-existent column
            ValueError: If schema is not initialized

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> expr = t._capture_expr(lambda r: r.age > 18)
            >>> expr["type"]
            'BinOp'
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )
        return _lambda_to_expr(fn, self._schema)

    def link(
        self, target_table: "LTSeq", on: Callable, as_: str, join_type: str = "inner"
    ) -> "LinkedTable":
        """
        Link this table to another table using pointer-based foreign keys.

        Creates a virtual pointer column that references rows in the target table
        based on a join condition. Unlike join(), this uses index-based lookups
        (pointer semantics) rather than expensive hash joins.

        Args:
            target_table: The table to link to (products, categories, etc.)
            on: Lambda with two parameters that specifies the join condition.
                E.g., lambda orders, products: orders.product_id == products.id
            as_: Alias for the linked table reference.
                E.g., as_="prod" allows accessing linked columns via r.prod.name
            join_type: Type of join to perform. One of: "inner", "left", "right", "full"
                Default: "inner" (backward compatible)

        Returns:
            A LinkedTable that supports accessing target columns via the alias

        Raises:
            ValueError: If schema is not initialized or join_type is invalid
            TypeError: If join condition is invalid

        Example:
            >>> orders = LTSeq.read_csv("orders.csv")
            >>> products = LTSeq.read_csv("products.csv")
            >>> linked = orders.link(products,
            ...     on=lambda o, p: o.product_id == p.id,
            ...     as_="prod")
            >>> result = linked.select(lambda r: [r.id, r.prod.name, r.prod.price])
        """
        valid_join_types = {"inner", "left", "right", "full"}
        if join_type not in valid_join_types:
            raise ValueError(
                f"Invalid join_type '{join_type}'. Must be one of: {valid_join_types}"
            )

        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        if not target_table._schema:
            raise ValueError(
                "Target table schema not initialized. Call read_csv() first."
            )

        try:
            self_proxy = SchemaProxy(self._schema)
            target_proxy = SchemaProxy(target_table._schema)
            _ = on(self_proxy, target_proxy)
        except Exception as e:
            raise TypeError(f"Invalid join condition: {e}")

        from .linking import LinkedTable

        return LinkedTable(self, target_table, on, as_, join_type)

    def partition(self, *args, by: Callable | None = None) -> "PartitionedTable":
        """
        Partition the table into groups based on columns or a key function.

        Unlike group_ordered() which handles consecutive groups, partition()
        groups all rows with the same key value regardless of their position
        in the table.

        Smart dispatch:
        - String arguments use fast SQL path (column-based partitioning)
        - Callable argument uses flexible Python path (lambda-based partitioning)

        Args:
            *args: Either column name(s) as strings, or a single callable.
                   - Strings: Use SQL-based partitioning (faster)
                   - Callable: Use Python-based partitioning (more flexible)
            by: Alternative way to pass a callable for partitioning.
                (e.g., by=lambda r: r.region)

        Returns:
            A PartitionedTable that supports dict-like access and iteration.

        Raises:
            ValueError: If schema is not initialized
            AttributeError: If any column doesn't exist (for SQL path)
            TypeError: If arguments are invalid

        Example:
            >>> t = LTSeq.read_csv("sales.csv")
            >>> partitions = t.partition("region")
            >>> partitions = t.partition("year", "region")  # Multiple columns
            >>> partitions = t.partition(lambda r: r.region)
            >>> west_sales = partitions["West"]
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        from .partitioning import PartitionedTable, SQLPartitionedTable

        if by is not None:
            if not callable(by):
                raise TypeError(f"'by' must be callable, got {type(by).__name__}")
            return PartitionedTable(self, by)
        elif len(args) == 1 and callable(args[0]):
            return PartitionedTable(self, args[0])
        elif len(args) == 0:
            raise TypeError("partition() requires at least one argument")
        else:
            for col in args:
                if not isinstance(col, str):
                    raise TypeError(
                        f"Expected column name (str), got {type(col).__name__}. "
                        f"Use partition(by=func) for callable-based partitioning."
                    )
                if col not in self._schema:
                    raise AttributeError(
                        f"Column '{col}' not found in schema. "
                        f"Available columns: {list(self._schema.keys())}"
                    )
            return SQLPartitionedTable(self, args)
