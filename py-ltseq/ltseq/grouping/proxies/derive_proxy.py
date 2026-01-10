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


class DeriveGroupProxy:
    """
    Proxy for capturing derive expressions without executing them.

    When passed to a derive lambda, this proxy captures operations like
    g.count(), g.first().column, g.max('col') as GroupExpr objects that
    can be serialized and converted to SQL.

    Example:
        proxy = DeriveGroupProxy()
        result = derive_lambda(proxy)
        # result is a dict of {col_name: GroupExpr}
    """

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
