"""Tests for #107 (PR 5a): string on / left_on / right_on / suffix, Polars-aligned naming."""

import pytest

from ltseq import LTSeq


def make_table(rows, schema):
    return LTSeq._from_rows(rows, schema)


@pytest.fixture
def users():
    return make_table(
        [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}, {"id": 3, "name": "Cara"}],
        {"id": "int64", "name": "string"},
    )


@pytest.fixture
def orders():
    return make_table(
        [
            {"oid": 10, "user_id": 1, "amount": 50},
            {"oid": 11, "user_id": 2, "amount": 75},
            {"oid": 12, "user_id": 1, "amount": 30},
        ],
        {"oid": "int64", "user_id": "int64", "amount": "int64"},
    )


class TestStringOn:
    def test_on_same_name_key(self):
        left = make_table([{"id": 1, "v": "a"}, {"id": 2, "v": "b"}], {"id": "int64", "v": "string"})
        right = make_table([{"id": 1, "w": "x"}, {"id": 3, "w": "z"}], {"id": "int64", "w": "string"})
        rows = left.join(right, on="id", how="inner").to_dicts()
        assert len(rows) == 1
        # Right key "id" is dropped (inner coalesce); non-conflicting "w" kept
        assert set(rows[0].keys()) == {"id", "v", "w"}
        assert rows[0] == {"id": 1, "v": "a", "w": "x"}

    def test_left_on_right_on(self, users, orders):
        rows = users.join(orders, left_on="id", right_on="user_id", how="inner").to_dicts()
        assert len(rows) == 3
        # id conflicts (both tables) → orders.id? no, orders has oid. Only "id" from users.
        # user_id is the right key → dropped. amount/oid kept.
        cols = set(rows[0].keys())
        assert "id" in cols and "name" in cols and "amount" in cols and "oid" in cols
        assert "user_id" not in cols

    def test_conflicting_column_suffixed(self):
        left = make_table([{"id": 1, "val": 10}], {"id": "int64", "val": "int64"})
        right = make_table([{"id": 1, "val": 99}], {"id": "int64", "val": "int64"})
        rows = left.join(right, on="id", how="inner").to_dicts()
        # "val" conflicts → right becomes "val_right"; right key "id" dropped
        assert rows[0] == {"id": 1, "val": 10, "val_right": 99}

    def test_custom_suffix(self):
        left = make_table([{"id": 1, "val": 10}], {"id": "int64", "val": "int64"})
        right = make_table([{"id": 1, "val": 99}], {"id": "int64", "val": "int64"})
        rows = left.join(right, on="id", suffix="_r").to_dicts()
        assert "val_r" in rows[0]

    def test_list_on_composite(self):
        left = make_table(
            [{"a": 1, "b": 1, "x": "p"}, {"a": 1, "b": 2, "x": "q"}],
            {"a": "int64", "b": "int64", "x": "string"},
        )
        right = make_table(
            [{"a": 1, "b": 2, "y": "hit"}],
            {"a": "int64", "b": "int64", "y": "string"},
        )
        rows = left.join(right, on=["a", "b"], how="inner").to_dicts()
        assert len(rows) == 1
        assert rows[0]["x"] == "q" and rows[0]["y"] == "hit"


class TestJoinKeyDrop:
    def test_inner_drops_right_key(self):
        left = make_table([{"id": 1}], {"id": "int64"})
        right = make_table([{"id": 1, "extra": 5}], {"id": "int64", "extra": "int64"})
        rows = left.join(right, on="id", how="inner").to_dicts()
        assert set(rows[0].keys()) == {"id", "extra"}

    def test_left_join_drops_right_key(self):
        left = make_table([{"id": 1}, {"id": 2}], {"id": "int64"})
        right = make_table([{"id": 1, "extra": 5}], {"id": "int64", "extra": "int64"})
        rows = left.join(right, on="id", how="left").to_dicts()
        assert len(rows) == 2
        assert "id_right" not in rows[0] and set(rows[0].keys()) == {"id", "extra"}

    def test_full_join_keeps_both_keys(self):
        left = make_table([{"id": 1}], {"id": "int64"})
        right = make_table([{"id": 2, "extra": 5}], {"id": "int64", "extra": "int64"})
        rows = left.join(right, on="id", how="full").to_dicts()
        # right/full keep both keys; right "id" suffixed
        assert any("id_right" in r for r in rows)


class TestLambdaFormUnchanged:
    def test_lambda_still_works(self, users, orders):
        rows = users.join(orders, on=lambda u, o: u.id == o.user_id).to_dicts()
        assert len(rows) == 3
        assert "user_id" not in rows[0]  # right key dropped for inner


class TestSemiAntiStringOn:
    def test_semi_join_string_on(self):
        left = make_table([{"id": 1}, {"id": 2}, {"id": 3}], {"id": "int64"})
        right = make_table([{"id": 2}, {"id": 3}], {"id": "int64"})
        rows = left.semi_join(right, on="id").to_dicts()
        assert sorted(r["id"] for r in rows) == [2, 3]

    def test_anti_join_string_on(self):
        left = make_table([{"id": 1}, {"id": 2}, {"id": 3}], {"id": "int64"})
        right = make_table([{"id": 2}], {"id": "int64"})
        rows = left.anti_join(right, on="id").to_dicts()
        assert sorted(r["id"] for r in rows) == [1, 3]


class TestSetOpStringOn:
    def test_intersect_string_on(self):
        left = make_table([{"id": 1, "v": "a"}, {"id": 2, "v": "b"}], {"id": "int64", "v": "string"})
        right = make_table([{"id": 2, "v": "z"}], {"id": "int64", "v": "string"})
        rows = left.intersect(right, on="id").to_dicts()
        assert [r["id"] for r in rows] == [2]

    def test_except_string_on(self):
        left = make_table([{"id": 1}, {"id": 2}, {"id": 3}], {"id": "int64"})
        right = make_table([{"id": 2}], {"id": "int64"})
        rows = left.except_(right, on="id").to_dicts()
        assert sorted(r["id"] for r in rows) == [1, 3]


class TestErrors:
    def test_on_and_left_on_conflict(self, users, orders):
        with pytest.raises(ValueError, match="either on|not both"):
            users.join(orders, on="id", left_on="id", right_on="user_id")

    def test_missing_column(self, users, orders):
        with pytest.raises(ValueError, match="not found"):
            users.join(orders, on="nope")

    def test_suffix_collision(self):
        # left has x and x_right; right has x → x_right collides
        left = make_table([{"k": 1, "x": 1, "x_right": 2}], {"k": "int64", "x": "int64", "x_right": "int64"})
        right = make_table([{"k": 1, "x": 9}], {"k": "int64", "x": "int64"})
        with pytest.raises(Exception, match="collision|suffix"):
            left.join(right, on="k").to_dicts()


class TestMergeStrategyValidation:
    """strategy='merge' must validate sortedness for composite keys too, not
    just single-column keys (otherwise unsorted tables are silently accepted)."""

    def _tables(self):
        left = make_table(
            [{"a": 2, "b": 1, "v": "x"}, {"a": 1, "b": 1, "v": "y"}],
            {"a": "int64", "b": "int64", "v": "string"},
        )
        right = make_table(
            [{"a": 1, "b": 1, "w": "p"}, {"a": 2, "b": 1, "w": "q"}],
            {"a": "int64", "b": "int64", "w": "string"},
        )
        return left, right

    def test_composite_merge_rejects_unsorted_left(self):
        left, right = self._tables()  # left is NOT sorted by (a, b)
        with pytest.raises(ValueError, match="not sorted"):
            left.join(right.sort("a", "b"), on=["a", "b"], strategy="merge")

    def test_composite_merge_rejects_unsorted_right(self):
        left, right = self._tables()
        with pytest.raises(ValueError, match="not sorted"):
            left.sort("a", "b").join(right, on=["a", "b"], strategy="merge")

    def test_composite_merge_accepts_sorted(self):
        left, right = self._tables()
        out = (
            left.sort("a", "b")
            .join(right.sort("a", "b"), on=["a", "b"], strategy="merge")
            .to_dicts()
        )
        assert len(out) == 2

    def test_composite_lambda_merge_rejects_unsorted(self):
        left, right = self._tables()
        with pytest.raises(ValueError, match="not sorted"):
            left.join(
                right.sort("a", "b"),
                on=lambda l, r: (l.a == r.a) & (l.b == r.b),
                strategy="merge",
            )
