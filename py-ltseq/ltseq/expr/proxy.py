"""Schema proxy classes for expression capturing."""

from typing import Dict, Union

from .types import ColumnExpr


class SchemaProxy:
    """
    Represents the row proxy ('r') passed into lambda functions.

    When user accesses r.age, this returns a ColumnExpr("age").
    Validates that the column exists in the schema, raising AttributeError if not.

    This allows lambdas like: lambda r: r.age > 18 to build an Expr tree
    instead of executing Python code.

    Attributes:
        _schema (dict): Mapping of column name -> type string
    """

    def __init__(self, schema: Dict[str, str]):
        """
        Initialize a SchemaProxy.

        Args:
            schema: Dict mapping column name -> type string
                    E.g., {"age": "int64", "name": "string", "price": "float64"}
        """
        self._schema = schema

    def __getattr__(self, name: str) -> Union[ColumnExpr, "NestedSchemaProxy"]:
        """
        Return a ColumnExpr for the given column name, or NestedSchemaProxy for linked tables.

        Validates that the column exists in the schema.

        Args:
            name: Column name (e.g., "age") or linked table alias (e.g., "prod")

        Returns:
            ColumnExpr(name) if column exists as simple column
            NestedSchemaProxy if name matches a linked table prefix pattern (e.g., "prod" → all "prod_*" columns)

        Raises:
            AttributeError: If column not in schema or name is private
        """
        if name.startswith("_"):
            # Avoid issues with internal attributes like _schema
            raise AttributeError(f"No attribute {name}")

        # Check if this is a simple column
        if name in self._schema:
            return ColumnExpr(name)

        # Check if this is a linked table alias (look for prefix pattern)
        # For example, if schema has "prod_name", "prod_price", look for "prod" access
        matching_cols = {
            col: typ for col, typ in self._schema.items() if col.startswith(f"{name}_")
        }
        if matching_cols:
            # Return NestedSchemaProxy for the linked table
            return NestedSchemaProxy(name, matching_cols)

        raise AttributeError(
            f"Column '{name}' not found in schema. "
            f"Available columns: {list(self._schema.keys())}"
        )

    def get_schema(self) -> Dict[str, str]:
        """
        Return a copy of the underlying schema.

        Returns:
            A dict copy of {column_name -> type_string}
        """
        return self._schema.copy()


class NestedSchemaProxy:
    """
    Represents access to a linked table's columns via dot notation.

    For example, if a LinkedTable with alias "prod" has columns "prod_name", "prod_price",
    accessing r.prod returns a NestedSchemaProxy that allows r.prod.name and r.prod.price.

    This class maps the nested access to the prefixed column names in the parent schema.

    Attributes:
        _alias (str): The linked table alias (e.g., "prod")
        _prefixed_schema (dict): Map of unprefixed names to their prefixed column names
    """

    def __init__(self, alias: str, prefixed_columns: Dict[str, str]):
        """
        Initialize a NestedSchemaProxy for a linked table.

        Args:
            alias: The linked table alias (e.g., "prod")
            prefixed_columns: Dict of prefixed column names (e.g., {"prod_name": "string", "prod_price": "float64"})
        """
        self._alias = alias
        self._prefixed_columns = prefixed_columns
        # Create mapping from unprefixed name to full prefixed name
        # E.g., "name" -> "prod_name", "price" -> "prod_price"
        prefix = f"{alias}_"
        self._column_map = {
            col.replace(prefix, "", 1): col for col in prefixed_columns.keys()
        }

    def __getattr__(self, name: str) -> ColumnExpr:
        """
        Return a ColumnExpr for a column in the linked table.

        Maps unprefixed column names to their prefixed versions.
        For example: r.prod.name → ColumnExpr("prod_name")

        Args:
            name: Column name without prefix (e.g., "name")

        Returns:
            ColumnExpr with the full prefixed column name

        Raises:
            AttributeError: If column not found in linked table
        """
        if name.startswith("_"):
            raise AttributeError(f"No attribute {name}")

        if name not in self._column_map:
            raise AttributeError(
                f"Linked table '{self._alias}' has no column '{name}'. "
                f"Available columns: {list(self._column_map.keys())}"
            )

        full_col_name = self._column_map[name]
        return ColumnExpr(full_col_name)
