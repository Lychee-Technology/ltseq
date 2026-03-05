"""
DeriveGroupProxy - Proxy class for capturing derive expressions.

This proxy is used in NestedTable.derive() to capture group operations
as expression trees without requiring source code inspection.
"""

from ..expr import (
    GroupExpr,
    GroupCountExpr,
    GroupAggExpr,
    DeriveRowProxy,
)


class DeriveColumnProxy:
    """
    Enables property-style aggregation capture on DeriveGroupProxy.

    Instead of ``g.avg('price')``, this allows ``g.price.avg()``.
    Returns GroupAggExpr objects for SQL transpilation.

    Example:
        >>> g.price.avg()   # property-style → GroupAggExpr("avg", "price")
        >>> g.avg('price')  # string-style (equivalent)
    """

    def __init__(self, column_name: str):
        self._column_name = column_name

    def max(self) -> GroupAggExpr:
        """Capture MAX(column) expression."""
        return GroupAggExpr("max", self._column_name)

    def min(self) -> GroupAggExpr:
        """Capture MIN(column) expression."""
        return GroupAggExpr("min", self._column_name)

    def sum(self) -> GroupAggExpr:
        """Capture SUM(column) expression."""
        return GroupAggExpr("sum", self._column_name)

    def avg(self) -> GroupAggExpr:
        """Capture AVG(column) expression."""
        return GroupAggExpr("avg", self._column_name)


class DeriveGroupProxy:
    """
    Proxy for capturing derive expressions without executing them.

    When passed to a derive lambda, this proxy captures operations like
    g.count(), g.first().column, g.max('col') as GroupExpr objects that
    can be serialized and converted to SQL.

    Supports both string-style and property-style access:
        g.max('price')   # string-style → GroupAggExpr("max", "price")
        g.price.max()    # property-style → GroupAggExpr("max", "price")

    Example:
        proxy = DeriveGroupProxy()
        result = derive_lambda(proxy)
        # result is a dict of {col_name: GroupExpr}
    """

    def __getattr__(self, name: str) -> DeriveColumnProxy:
        """Enable property-style column access for aggregation capture.

        Attribute access returns a DeriveColumnProxy which provides
        aggregation methods that return GroupAggExpr objects.

        Args:
            name: Column name to access

        Returns:
            DeriveColumnProxy for the requested column
        """
        if name.startswith("_"):
            raise AttributeError(name)
        return DeriveColumnProxy(name)

    def count(self) -> GroupCountExpr:
        """Capture g.count() expression."""
        return GroupCountExpr()

    def first(self) -> DeriveRowProxy:
        """Capture g.first() - returns proxy for column access."""
        return DeriveRowProxy("first")

    def last(self) -> DeriveRowProxy:
        """Capture g.last() - returns proxy for column access."""
        return DeriveRowProxy("last")

    def max(self, column: str) -> GroupAggExpr:
        """Capture g.max('column') expression."""
        return GroupAggExpr("max", column)

    def min(self, column: str) -> GroupAggExpr:
        """Capture g.min('column') expression."""
        return GroupAggExpr("min", column)

    def sum(self, column: str) -> GroupAggExpr:
        """Capture g.sum('column') expression."""
        return GroupAggExpr("sum", column)

    def avg(self, column: str) -> GroupAggExpr:
        """Capture g.avg('column') expression."""
        return GroupAggExpr("avg", column)
