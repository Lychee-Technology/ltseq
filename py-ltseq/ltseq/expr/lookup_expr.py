"""LookupExpr class for cross-table lookups."""

from typing import Any, Dict, Optional

from .base import Expr


class LookupExpr(Expr):
    """
    Represents a lookup operation to fetch values from another table.

    Used as a shortcut for join operations in derived columns.
    Syntax: r.product_id.lookup(products, "name")

    This translates to an internal join where the left key is this expression
    and the right key is the primary key of the target table.

    Attributes:
        on (Expr): The expression to lookup (e.g., r.product_id)
        target_table: The actual target table object (LTSeq instance)
        target_name (str): Name of the target table (for reference)
        target_columns (list): Columns to fetch from the target table
        join_key (str): Column name in target table to match against (default: primary key)

    Class Attributes:
        _table_registry: Class-level dict mapping target_name -> table object.
                        Used to retrieve table references after serialization.
    """

    # Class-level registry for lookup tables
    # Maps target_name -> actual table object
    _table_registry: Dict[str, Any] = {}

    def __init__(
        self,
        on: Expr,
        target_table: Any,
        target_name: str,
        target_columns: list,
        join_key: Optional[str] = None,
    ):
        """
        Initialize a LookupExpr.

        Args:
            on: The expression containing the lookup key(s)
            target_table: The actual target table object (stored for later retrieval)
            target_name: Name/reference to the target table
            target_columns: List of column names to fetch from target
            join_key: Column name in target to join on (optional, defaults to primary key)
        """
        self.on = on
        self.target_table = target_table
        self.target_name = target_name
        self.target_columns = target_columns
        self.join_key = join_key

        # Register table for later retrieval during derive()
        if target_table is not None:
            LookupExpr._table_registry[target_name] = target_table

    @classmethod
    def get_table(cls, name: str) -> Any:
        """
        Retrieve a registered table by name.

        Args:
            name: The target_name used when creating the LookupExpr

        Returns:
            The table object, or None if not found
        """
        return cls._table_registry.get(name)

    @classmethod
    def clear_registry(cls) -> None:
        """Clear the table registry. Call after derive() completes."""
        cls._table_registry.clear()

    def serialize(self) -> Dict[str, Any]:
        """
        Serialize to dict.

        Note: The actual table reference is stored in _table_registry
        and can be retrieved via LookupExpr.get_table(target_name).

        Returns:
            {
                "type": "Lookup",
                "on": serialized_on,
                "target_name": target_name,
                "target_columns": target_columns,
                "join_key": join_key or null
            }
        """
        return {
            "type": "Lookup",
            "on": self.on.serialize(),
            "target_name": self.target_name,
            "target_columns": self.target_columns,
            "join_key": self.join_key,
        }
