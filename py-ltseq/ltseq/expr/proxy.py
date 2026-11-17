"""Schema proxy classes for expression capturing."""

from typing import Any

from .types import CallExpr, ColumnExpr


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

    def __init__(self, schema: dict[str, str]):
        """
        Initialize a SchemaProxy.

        Args:
            schema: Dict mapping column name -> type string
                    E.g., {"age": "int64", "name": "string", "price": "float64"}
        """
        self._schema = schema

    def __getattr__(self, name: str) -> "ColumnExpr | NestedSchemaProxy":
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

        from ..exceptions import ColumnNotFoundError

        raise ColumnNotFoundError(
            f"Column '{name}' not found in schema. "
            f"Available columns: {list(self._schema.keys())}"
        )

    def get_schema(self) -> dict[str, str]:
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

    def __init__(self, alias: str, prefixed_columns: dict[str, str]):
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
            from ..exceptions import ColumnNotFoundError

            raise ColumnNotFoundError(
                f"Linked table '{self._alias}' has no column '{name}'. "
                f"Available columns: {list(self._column_map.keys())}"
            )

        full_col_name = self._column_map[name]
        return ColumnExpr(full_col_name)


class GroupAggProxy(SchemaProxy):
    """
    The group proxy ('g') passed to the lambdas of ``group_by().agg()`` and
    ``LTSeq.agg()`` (#314).

    Columns read as on :class:`SchemaProxy`, and their aggregates are methods
    of the column: ``g.v.sum()``, ``g.v.first()``, ``g.v.last()``. ``first``
    and ``last`` give the value of the group's first or last row in table
    order, NULL included; they need a declared order (``sort`` or
    ``assume_sorted``) and raise ``SortRequiredError`` without one.

    The group has one attribute of its own, as in contract §8.1:
    ``g.count()``, the number of rows in the group. ``g.first()`` and
    ``g.last()``, the row proxies of ``NestedTable.agg``, are not group
    attributes here: they read as columns and raise ``ColumnNotFoundError``
    naming ``g.<col>.first()``.

    Column access of :class:`SchemaProxy` is kept whole. A column named
    ``count``, ``first`` or ``last`` keeps its name, so ``g.count`` is then
    that column (``g.count.sum()``), and calling it, ``g.count()``, still
    counts the group's rows. A linked alias keeps its prefixed columns too: with a
    ``first_v`` column, ``g.first.v`` is that column, and with ``count_v``,
    ``g.count.v`` is that column while ``g.count()`` still counts rows.
    """

    def __getattr__(self, name: str) -> "ColumnExpr | NestedSchemaProxy | Any":
        if name == "count" and name in self._schema:
            return _GroupCountColumn(name)
        if not name.startswith("_") and name not in self._schema:
            if name == "count":
                return _GroupCount(self._linked(name))
            if name in ("first", "last") and self._linked(name) is None:
                from ..exceptions import ColumnNotFoundError

                raise ColumnNotFoundError(
                    f"Column '{name}' not found in schema. "
                    f"Available columns: {list(self._schema.keys())}. "
                    f"In agg(), the value of a group's {name} row is an "
                    f"aggregate of its column: g.<col>.{name}(), which reads "
                    f"the table's declared order (call .sort(...) first). "
                    f"g.{name}().<col> is the form of group_ordered(...).agg()."
                )
        return super().__getattr__(name)

    def _linked(self, alias: str) -> "NestedSchemaProxy | None":
        """The linked-table proxy :class:`SchemaProxy` gives ``alias`` (its
        ``<alias>_*`` columns), or None when the schema has none."""
        prefixed = {
            col: typ for col, typ in self._schema.items() if col.startswith(f"{alias}_")
        }
        return NestedSchemaProxy(alias, prefixed) if prefixed else None


class _GroupCount:
    """``g.count`` when the schema has no ``count`` column.

    Calling it, ``g.count()``, counts the group's rows. When the schema has
    ``count_*`` columns (a link with ``as_="count"``), its attributes are
    those columns, as on :class:`SchemaProxy`: ``g.count.v`` is ``count_v``.
    """

    def __init__(self, linked: "NestedSchemaProxy | None"):
        self._linked = linked

    def __call__(self) -> "CallExpr":
        return CallExpr("count", (), {}, on=None)

    def __getattr__(self, name: str) -> ColumnExpr:
        if name.startswith("_"):
            raise AttributeError(f"No attribute {name}")
        if self._linked is not None:
            return getattr(self._linked, name)
        from ..exceptions import ColumnNotFoundError

        raise ColumnNotFoundError(
            f"Column 'count' not found in schema, so g.count has no "
            f"attribute '{name}'. In agg(), g.count() counts the group's "
            f"rows and g.<col>.count() counts a column's non-NULL values."
        )


class _GroupCountColumn(ColumnExpr):
    """``g.count`` when the schema has a ``count`` column.

    It is that column, so ``g.count.sum()`` and ``g.count.max()`` aggregate
    it, and calling it, ``g.count()``, still counts the group's rows (#314).
    """

    def __call__(self) -> "CallExpr":
        return CallExpr("count", (), {}, on=None)
