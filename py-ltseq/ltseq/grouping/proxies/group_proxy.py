"""GroupProxy class for group-level operations in NestedTable."""

from typing import TYPE_CHECKING, Any

from .row_proxy import RowProxy
from .aggregations import GroupAggregationMixin
from .conditionals import GroupConditionalMixin

if TYPE_CHECKING:
    from ..nested_table import NestedTable


class GroupProxy(GroupAggregationMixin, GroupConditionalMixin):
    """
    Proxy for evaluating predicates on a group during filter operations.

    Provides access to group properties like count(), first(), last()
    so that predicates can be evaluated.

    This class combines mixins for aggregation and conditional operations.
    """

    def __init__(self, group_data: Any, nested_table: "NestedTable"):
        """
        Initialize a group proxy.

        Args:
            group_data: DataFrame containing rows for this group (list of dicts or pandas DataFrame)
            nested_table: The NestedTable this group belongs to
        """
        self._group_data = group_data
        self._nested_table = nested_table

    def count(self) -> int:
        """Get the number of rows in this group."""
        if isinstance(self._group_data, list):
            return len(self._group_data)
        else:
            return len(self._group_data)

    def first(self) -> RowProxy:
        """Get the first row of this group as a row proxy."""
        if isinstance(self._group_data, list):
            first_row_data = self._group_data[0]
        else:
            first_row_data = self._group_data.iloc[0]
        return RowProxy(first_row_data)

    def last(self) -> RowProxy:
        """Get the last row of this group as a row proxy."""
        if isinstance(self._group_data, list):
            last_row_data = self._group_data[-1]
        else:
            last_row_data = self._group_data.iloc[-1]
        return RowProxy(last_row_data)
