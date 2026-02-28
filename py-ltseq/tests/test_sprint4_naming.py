"""Sprint 4 tests: Naming & Consolidation (P0-1, P1-3, P2-6)."""

import warnings

import pytest

from ltseq import LTSeq


# ---------------------------------------------------------------------------
# Helper: create a simple test table
# ---------------------------------------------------------------------------

def make_table(rows, schema):
    return LTSeq._from_rows(rows, schema)


def make_users():
    return make_table(
        [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
            {"id": 3, "name": "Charlie", "age": 35},
        ],
        {"id": "int64", "name": "string", "age": "int64"},
    )


def make_orders():
    return make_table(
        [
            {"order_id": 101, "user_id": 1, "amount": 50},
            {"order_id": 102, "user_id": 2, "amount": 75},
            {"order_id": 103, "user_id": 1, "amount": 100},
        ],
        {"order_id": "int64", "user_id": "int64", "amount": "int64"},
    )


# ===========================================================================
# P0-1: except_() + deprecated diff()
# ===========================================================================


class TestExceptAndDiff:
    """Test that except_() works and diff() emits DeprecationWarning."""

    def _make_tables(self):
        t1 = make_table(
            [{"x": 1}, {"x": 2}, {"x": 3}],
            {"x": "int64"},
        )
        t2 = make_table(
            [{"x": 2}, {"x": 3}],
            {"x": "int64"},
        )
        return t1, t2

    def test_except_returns_difference(self):
        t1, t2 = self._make_tables()
        result = t1.except_(t2)
        rows = result.collect()
        vals = sorted([r["x"] for r in rows])
        assert vals == [1]

    def test_diff_emits_deprecation_warning(self):
        t1, t2 = self._make_tables()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = t1.diff(t2)
            dep_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
            assert len(dep_warnings) >= 1
            assert "except_" in str(dep_warnings[0].message)

    def test_diff_returns_same_result_as_except(self):
        t1, t2 = self._make_tables()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            diff_rows = sorted(t1.diff(t2).collect(), key=lambda r: r["x"])
        except_rows = sorted(t1.except_(t2).collect(), key=lambda r: r["x"])
        assert diff_rows == except_rows


# ===========================================================================
# P1-3: Consolidated join() with strategy param
# ===========================================================================


class TestJoinStrategy:
    """Test join() with strategy parameter."""

    def test_join_default_strategy(self):
        """Default (no strategy) works as before."""
        users = make_users()
        orders = make_orders()
        result = users.join(orders, on=lambda u, o: u.id == o.user_id)
        assert result.count() == 3  # Alice has 2 orders, Bob has 1

    def test_join_hash_strategy(self):
        """Explicit strategy='hash' is same as default."""
        users = make_users()
        orders = make_orders()
        result = users.join(orders, on=lambda u, o: u.id == o.user_id, strategy="hash")
        assert result.count() == 3

    def test_join_merge_strategy_sorted(self):
        """strategy='merge' works with sorted data."""
        users = make_users().sort("id")
        orders = make_orders().sort("user_id")
        result = users.join(
            orders, on=lambda u, o: u.id == o.user_id, strategy="merge"
        )
        assert result.count() == 3

    def test_join_merge_strategy_unsorted_raises(self):
        """strategy='merge' raises ValueError when data not sorted."""
        users = make_users()  # not sorted
        orders = make_orders().sort("user_id")
        with pytest.raises(ValueError, match="not sorted"):
            users.join(
                orders, on=lambda u, o: u.id == o.user_id, strategy="merge"
            )

    def test_join_invalid_strategy_raises(self):
        """Invalid strategy raises ValueError."""
        users = make_users()
        orders = make_orders()
        with pytest.raises(ValueError, match="Invalid strategy"):
            users.join(
                orders, on=lambda u, o: u.id == o.user_id, strategy="nested_loop"
            )

    def test_join_left(self):
        """Left join with strategy param works."""
        users = make_users()
        orders = make_orders()
        result = users.join(orders, on=lambda u, o: u.id == o.user_id, how="left")
        # All 3 users, Charlie gets null order fields
        assert result.count() == 4  # Alice(2) + Bob(1) + Charlie(1 null)

    def test_join_merge_left(self):
        """Left merge join works."""
        users = make_users().sort("id")
        orders = make_orders().sort("user_id")
        result = users.join(
            orders, on=lambda u, o: u.id == o.user_id, how="left", strategy="merge"
        )
        assert result.count() == 4


class TestJoinMergeDeprecated:
    """Test that join_merge() emits DeprecationWarning."""

    def test_join_merge_deprecation_warning(self):
        users = make_users().sort("id")
        orders = make_orders().sort("user_id")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = users.join_merge(
                orders, on=lambda u, o: u.id == o.user_id
            )
            dep_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
            assert len(dep_warnings) >= 1
            assert "join_merge" in str(dep_warnings[0].message)

    def test_join_merge_still_works(self):
        users = make_users().sort("id")
        orders = make_orders().sort("user_id")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = users.join_merge(
                orders, on=lambda u, o: u.id == o.user_id
            )
        assert result.count() == 3


class TestJoinSortedDeprecated:
    """Test that join_sorted() emits DeprecationWarning."""

    def test_join_sorted_deprecation_warning(self):
        users = make_users().sort("id")
        orders = make_orders().sort("user_id")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = users.join_sorted(
                orders, on=lambda u, o: u.id == o.user_id
            )
            dep_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
            assert len(dep_warnings) >= 1
            assert "join_sorted" in str(dep_warnings[0].message)

    def test_join_sorted_still_works(self):
        users = make_users().sort("id")
        orders = make_orders().sort("user_id")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = users.join_sorted(
                orders, on=lambda u, o: u.id == o.user_id
            )
        assert result.count() == 3

    def test_join_sorted_unsorted_raises(self):
        """Even deprecated join_sorted() still validates sort order."""
        users = make_users()  # not sorted
        orders = make_orders().sort("user_id")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            with pytest.raises(ValueError, match="not sorted"):
                users.join_sorted(
                    orders, on=lambda u, o: u.id == o.user_id
                )


# ===========================================================================
# P2-6: group_consecutive() alias
# ===========================================================================


class TestGroupConsecutiveAlias:
    """Test that group_consecutive is an alias for group_ordered."""

    def _make_sequential_table(self):
        return make_table(
            [
                {"day": 1, "trend": "up", "val": 10},
                {"day": 2, "trend": "up", "val": 20},
                {"day": 3, "trend": "down", "val": 5},
                {"day": 4, "trend": "down", "val": 3},
                {"day": 5, "trend": "up", "val": 15},
            ],
            {"day": "int64", "trend": "string", "val": "int64"},
        )

    def test_group_consecutive_exists(self):
        t = self._make_sequential_table()
        assert hasattr(t, "group_consecutive")

    def test_group_consecutive_is_same_method(self):
        """group_consecutive should be the exact same method as group_ordered."""
        assert LTSeq.group_consecutive is LTSeq.group_ordered

    def test_group_consecutive_returns_nested_table(self):
        from ltseq.grouping import NestedTable

        t = self._make_sequential_table().sort("day")
        result = t.group_consecutive(lambda r: r.trend)
        assert isinstance(result, NestedTable)

    def test_group_consecutive_groups_correctly(self):
        t = self._make_sequential_table().sort("day")
        groups = t.group_consecutive(lambda r: r.trend)
        # flatten() adds __group_id__ and __group_count__ columns
        flat = groups.flatten()
        rows = flat.collect()
        # Should get 3 groups: up(day 1-2), down(day 3-4), up(day 5)
        group_ids = sorted(set(r["__group_id__"] for r in rows))
        assert group_ids == [1, 2, 3]
        # Group counts should be 2, 2, 1
        group_counts = [rows[0]["__group_count__"], rows[2]["__group_count__"], rows[4]["__group_count__"]]
        assert group_counts == [2, 2, 1]
