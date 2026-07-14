"""Tests for semi-join and anti-join operations."""

import os
import tempfile
import warnings

import pytest

from ltseq import LTSeq


@pytest.fixture
def users_csv():
    """Create a temporary CSV file with user data."""
    content = """id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
3,Charlie,charlie@example.com
4,Diana,diana@example.com
5,Eve,eve@example.com
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def orders_csv():
    """Create a temporary CSV file with order data (some users have orders)."""
    content = """order_id,user_id,amount
101,1,100.00
102,1,150.00
103,2,200.00
104,3,75.00
105,3,125.00
106,3,50.00
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def products_csv():
    """Create a temporary CSV file with product data."""
    content = """product_id,product_name,price
P1,Widget,10.00
P2,Gadget,25.00
P3,Gizmo,15.00
P4,Doohickey,30.00
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def reviews_csv():
    """Create a temporary CSV file with review data (some products have reviews)."""
    content = """review_id,product_id,rating
R1,P1,5
R2,P1,4
R3,P2,3
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def empty_csv():
    """Create an empty CSV file (header only)."""
    content = """id,value
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


class TestSemiJoinBasic:
    """Basic semi-join functionality tests."""

    def test_semi_join_returns_ltseq(self, users_csv, orders_csv):
        """Semi-join returns an LTSeq instance."""
        users = LTSeq.read_csv(users_csv)
        orders = LTSeq.read_csv(orders_csv)

        result = users.semi_join(orders, on=lambda u, o: u.id == o.user_id)

        assert isinstance(result, LTSeq)

    def test_semi_join_filters_to_matching_rows(self, users_csv, orders_csv):
        """Semi-join returns only rows with matches in right table."""
        users = LTSeq.read_csv(users_csv)
        orders = LTSeq.read_csv(orders_csv)

        # Users 1, 2, 3 have orders; users 4, 5 do not
        result = users.semi_join(orders, on=lambda u, o: u.id == o.user_id)

        df = result.to_pandas()
        assert len(df) == 3
        assert set(df["id"].tolist()) == {1, 2, 3}

    def test_semi_join_returns_left_schema_only(self, users_csv, orders_csv):
        """Semi-join returns only left table columns."""
        users = LTSeq.read_csv(users_csv)
        orders = LTSeq.read_csv(orders_csv)

        result = users.semi_join(orders, on=lambda u, o: u.id == o.user_id)

        df = result.to_pandas()
        # Should only have user columns, not order columns
        assert list(df.columns) == ["id", "name", "email"]

    def test_semi_join_deduplicates_multiple_matches(self, users_csv, orders_csv):
        """Semi-join returns distinct rows even when multiple matches exist."""
        users = LTSeq.read_csv(users_csv)
        orders = LTSeq.read_csv(orders_csv)

        # User 3 has 3 orders, but should appear only once
        result = users.semi_join(orders, on=lambda u, o: u.id == o.user_id)

        df = result.to_pandas()
        # Count of user 3 should be 1, not 3
        user_3_count = len(df[df["id"] == 3])
        assert user_3_count == 1


class TestAntiJoinBasic:
    """Basic anti-join functionality tests."""

    def test_anti_join_returns_ltseq(self, users_csv, orders_csv):
        """Anti-join returns an LTSeq instance."""
        users = LTSeq.read_csv(users_csv)
        orders = LTSeq.read_csv(orders_csv)

        result = users.anti_join(orders, on=lambda u, o: u.id == o.user_id)

        assert isinstance(result, LTSeq)

    def test_anti_join_filters_to_non_matching_rows(self, users_csv, orders_csv):
        """Anti-join returns only rows WITHOUT matches in right table."""
        users = LTSeq.read_csv(users_csv)
        orders = LTSeq.read_csv(orders_csv)

        # Users 4, 5 have no orders; users 1, 2, 3 do
        result = users.anti_join(orders, on=lambda u, o: u.id == o.user_id)

        df = result.to_pandas()
        assert len(df) == 2
        assert set(df["id"].tolist()) == {4, 5}

    def test_anti_join_returns_left_schema_only(self, users_csv, orders_csv):
        """Anti-join returns only left table columns."""
        users = LTSeq.read_csv(users_csv)
        orders = LTSeq.read_csv(orders_csv)

        result = users.anti_join(orders, on=lambda u, o: u.id == o.user_id)

        df = result.to_pandas()
        # Should only have user columns, not order columns
        assert list(df.columns) == ["id", "name", "email"]


class TestDifferentColumnNames:
    """Tests for joins with different column names between tables."""

    def test_semi_join_different_column_names(self, users_csv, orders_csv):
        """Semi-join works when join columns have different names."""
        users = LTSeq.read_csv(users_csv)
        orders = LTSeq.read_csv(orders_csv)

        # Join user.id to order.user_id (different column names)
        result = users.semi_join(orders, on=lambda u, o: u.id == o.user_id)

        df = result.to_pandas()
        assert len(df) == 3

    def test_anti_join_different_column_names(self, users_csv, orders_csv):
        """Anti-join works when join columns have different names."""
        users = LTSeq.read_csv(users_csv)
        orders = LTSeq.read_csv(orders_csv)

        # Join user.id to order.user_id (different column names)
        result = users.anti_join(orders, on=lambda u, o: u.id == o.user_id)

        df = result.to_pandas()
        assert len(df) == 2

    def test_semi_join_same_column_names(self, products_csv, reviews_csv):
        """Semi-join works when join columns have the same name."""
        products = LTSeq.read_csv(products_csv)
        reviews = LTSeq.read_csv(reviews_csv)

        # Both tables have product_id column
        result = products.semi_join(
            reviews, on=lambda p, r: p.product_id == r.product_id
        )

        df = result.to_pandas()
        # Products P1 and P2 have reviews
        assert len(df) == 2
        assert set(df["product_id"].tolist()) == {"P1", "P2"}

    def test_anti_join_same_column_names(self, products_csv, reviews_csv):
        """Anti-join works when join columns have the same name."""
        products = LTSeq.read_csv(products_csv)
        reviews = LTSeq.read_csv(reviews_csv)

        # Both tables have product_id column
        result = products.anti_join(
            reviews, on=lambda p, r: p.product_id == r.product_id
        )

        df = result.to_pandas()
        # Products P3 and P4 have no reviews
        assert len(df) == 2
        assert set(df["product_id"].tolist()) == {"P3", "P4"}


class TestEdgeCases:
    """Edge case tests."""

    def test_semi_join_empty_right_table(self, users_csv, empty_csv):
        """Semi-join with empty right table returns empty result."""
        users = LTSeq.read_csv(users_csv)
        empty = LTSeq.read_csv(empty_csv)

        result = users.semi_join(empty, on=lambda u, e: u.id == e.id)

        df = result.to_pandas()
        assert len(df) == 0

    def test_anti_join_empty_right_table(self, users_csv, empty_csv):
        """Anti-join with empty right table returns all left rows."""
        users = LTSeq.read_csv(users_csv)
        empty = LTSeq.read_csv(empty_csv)

        result = users.anti_join(empty, on=lambda u, e: u.id == e.id)

        df = result.to_pandas()
        assert len(df) == 5  # All users returned

    def test_semi_join_no_matches(self, products_csv, orders_csv):
        """Semi-join with no matches returns empty result."""
        products = LTSeq.read_csv(products_csv)
        orders = LTSeq.read_csv(orders_csv)

        # No product_id matches any order_id
        result = products.semi_join(orders, on=lambda p, o: p.product_id == o.order_id)

        df = result.to_pandas()
        assert len(df) == 0

    def test_semi_join_mixed_numeric_types_does_not_truncate(self):
        """Mixed numeric keys compare without truncating fractional values."""
        left = LTSeq.from_rows([{"id": 1}, {"id": 2}])
        right = LTSeq.from_rows([{"id": 1.5}, {"id": 2.0}])

        result = left.semi_join(right, on=lambda l, r: l.id == r.id)

        assert result.to_dicts() == [{"id": 2}]

    def test_anti_join_all_match(self, users_csv, orders_csv):
        """Anti-join where all left rows have matches returns empty."""
        users = LTSeq.read_csv(users_csv)
        orders = LTSeq.read_csv(orders_csv)

        # First filter users to only those with orders
        users_with_orders = users.semi_join(orders, on=lambda u, o: u.id == o.user_id)

        # Then anti-join should return empty
        result = users_with_orders.anti_join(orders, on=lambda u, o: u.id == o.user_id)

        df = result.to_pandas()
        assert len(df) == 0


class TestSortKeyPreservation:
    """Tests for sort key preservation."""

    def test_semi_join_preserves_sort_keys(self, users_csv, orders_csv):
        """Semi-join preserves sort keys from left table."""
        users = LTSeq.read_csv(users_csv).sort("name")
        orders = LTSeq.read_csv(orders_csv)

        result = users.semi_join(orders, on=lambda u, o: u.id == o.user_id)

        # Check sort keys are preserved
        assert result._sort_keys == users._sort_keys

    def test_anti_join_preserves_sort_keys(self, users_csv, orders_csv):
        """Anti-join preserves sort keys from left table."""
        users = LTSeq.read_csv(users_csv).sort("name", desc=True)
        orders = LTSeq.read_csv(orders_csv)

        result = users.anti_join(orders, on=lambda u, o: u.id == o.user_id)

        # Check sort keys are preserved
        assert result._sort_keys == users._sort_keys


class TestValidationErrors:
    """Tests for validation and error handling."""

    def test_semi_join_requires_schema(self, users_csv, orders_csv):
        """Semi-join raises error if left table has no schema."""
        users = LTSeq()  # No schema
        orders = LTSeq.read_csv(orders_csv)

        with pytest.raises(ValueError, match="Schema not initialized"):
            users.semi_join(orders, on=lambda u, o: u.id == o.user_id)

    def test_semi_join_requires_other_schema(self, users_csv, orders_csv):
        """Semi-join raises error if right table has no schema."""
        users = LTSeq.read_csv(users_csv)
        orders = LTSeq()  # No schema

        with pytest.raises(ValueError, match="Other table schema not initialized"):
            users.semi_join(orders, on=lambda u, o: u.id == o.user_id)

    def test_semi_join_requires_ltseq_argument(self, users_csv):
        """Semi-join raises error if other is not LTSeq."""
        users = LTSeq.read_csv(users_csv)

        with pytest.raises(TypeError, match="must be LTSeq"):
            users.semi_join("not an ltseq", on=lambda u, o: u.id == o.id)

    def test_anti_join_requires_schema(self, users_csv, orders_csv):
        """Anti-join raises error if left table has no schema."""
        users = LTSeq()  # No schema
        orders = LTSeq.read_csv(orders_csv)

        with pytest.raises(ValueError, match="Schema not initialized"):
            users.anti_join(orders, on=lambda u, o: u.id == o.user_id)

    def test_invalid_join_condition(self, users_csv, orders_csv):
        """Invalid join condition raises TypeError."""
        users = LTSeq.read_csv(users_csv)
        orders = LTSeq.read_csv(orders_csv)

        with pytest.raises(TypeError, match="Invalid join condition"):
            users.semi_join(orders, on=lambda u, o: u.nonexistent == o.user_id)


class TestSchemaOverlapWarning:
    """Tests for avoiding noisy schema overlap warnings."""

    def test_no_warning_for_valid_different_column_names(self, users_csv, orders_csv):
        """Valid foreign-key style joins should not warn just because names differ."""
        users = LTSeq.read_csv(users_csv)
        orders = LTSeq.read_csv(orders_csv)

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            users.semi_join(orders, on=lambda u, o: u.id == o.user_id)

        assert len(record) == 0

    def test_no_warning_for_explicit_non_overlapping_join(self, users_csv, products_csv):
        """Explicit join conditions should not warn just because schemas differ."""
        users = LTSeq.read_csv(users_csv)
        products = LTSeq.read_csv(products_csv)

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            users.semi_join(products, on=lambda u, p: u.id == p.product_id)

        assert len(record) == 0


class TestCompositeKeys:
    """Tests for composite (multi-column) join keys."""

    @pytest.fixture
    def orders_multi_key_csv(self):
        """Create orders with composite key."""
        content = """order_id,customer_id,region,amount
1,C1,US,100.00
2,C1,EU,150.00
3,C2,US,200.00
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            return f.name

    @pytest.fixture
    def customers_multi_key_csv(self):
        """Create customers with composite key."""
        content = """customer_id,region,name
C1,US,Alice US
C1,EU,Alice EU
C2,US,Bob US
C2,EU,Bob EU
C3,US,Charlie US
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            return f.name

    def test_semi_join_composite_key(
        self, customers_multi_key_csv, orders_multi_key_csv
    ):
        """Semi-join works with composite keys."""
        customers = LTSeq.read_csv(customers_multi_key_csv)
        orders = LTSeq.read_csv(orders_multi_key_csv)

        result = customers.semi_join(
            orders,
            on=lambda c, o: (c.customer_id == o.customer_id) & (c.region == o.region),
        )

        df = result.to_pandas()
        # Customers that have orders: (C1, US), (C1, EU), (C2, US)
        assert len(df) == 3
        names = set(df["name"].tolist())
        assert names == {"Alice US", "Alice EU", "Bob US"}

    def test_anti_join_composite_key(
        self, customers_multi_key_csv, orders_multi_key_csv
    ):
        """Anti-join works with composite keys."""
        customers = LTSeq.read_csv(customers_multi_key_csv)
        orders = LTSeq.read_csv(orders_multi_key_csv)

        result = customers.anti_join(
            orders,
            on=lambda c, o: (c.customer_id == o.customer_id) & (c.region == o.region),
        )

        df = result.to_pandas()
        # Customers without orders: (C2, EU), (C3, US)
        assert len(df) == 2
        names = set(df["name"].tolist())
        assert names == {"Bob EU", "Charlie US"}


class TestMismatchedKeyTypes:
    """String-vs-numeric join keys must compare numerically (DF51 semantics)."""

    @pytest.fixture
    def string_ids_csv(self):
        # "abc" forces Utf8 inference for sid; "01" keeps its leading zero.
        content = """sid,label
2,two
01,one
abc,junk
5,five
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
        yield f.name
        os.unlink(f.name)

    @pytest.fixture
    def numeric_ids_csv(self):
        content = """nid,score
1,10
2,20
3,30
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
        yield f.name
        os.unlink(f.name)

    def test_semi_join_string_left_numeric_right(self, string_ids_csv, numeric_ids_csv):
        """String keys match numeric keys by numeric value, not display format."""
        strings = LTSeq.read_csv(string_ids_csv)
        numbers = LTSeq.read_csv(numeric_ids_csv)

        result = strings.semi_join(numbers, on=lambda s, n: s.sid == n.nid)

        df = result.to_pandas()
        # "2" -> 2 and "01" -> 1 match; "abc" (unparseable) and "5" do not
        assert len(df) == 2
        assert set(df["sid"].tolist()) == {"2", "01"}

    def test_anti_join_string_left_numeric_right(self, string_ids_csv, numeric_ids_csv):
        """Unparseable string keys land on the anti side without erroring."""
        strings = LTSeq.read_csv(string_ids_csv)
        numbers = LTSeq.read_csv(numeric_ids_csv)

        result = strings.anti_join(numbers, on=lambda s, n: s.sid == n.nid)

        df = result.to_pandas()
        assert len(df) == 2
        assert set(df["sid"].tolist()) == {"abc", "5"}

    def test_semi_join_numeric_left_string_right(self, string_ids_csv, numeric_ids_csv):
        """Cast direction is symmetric: numeric left vs string right."""
        strings = LTSeq.read_csv(string_ids_csv)
        numbers = LTSeq.read_csv(numeric_ids_csv)

        result = numbers.semi_join(strings, on=lambda n, s: n.nid == s.sid)

        df = result.to_pandas()
        assert len(df) == 2
        assert set(df["nid"].tolist()) == {1, 2}

    def test_anti_join_numeric_left_string_right(self, string_ids_csv, numeric_ids_csv):
        strings = LTSeq.read_csv(string_ids_csv)
        numbers = LTSeq.read_csv(numeric_ids_csv)

        result = numbers.anti_join(strings, on=lambda n, s: n.nid == s.sid)

        df = result.to_pandas()
        assert len(df) == 1
        assert set(df["nid"].tolist()) == {3}

    def test_semi_join_string_key_matches_float_display_variants(self):
        """Regression: "2" must match 2.0 numerically, not via string rendering."""
        left = LTSeq.from_rows([{"k": "2"}, {"k": "2.0"}, {"k": "x"}])
        right = LTSeq.from_rows([{"k": 2.0}, {"k": 3.0}])

        result = left.semi_join(right, on=lambda l, r: l.k == r.k)

        assert result.to_dicts() == [{"k": "2"}, {"k": "2.0"}]

    def test_anti_join_unparseable_string_key_does_not_error(self):
        left = LTSeq.from_rows([{"k": "2"}, {"k": "2.0"}, {"k": "x"}])
        right = LTSeq.from_rows([{"k": 2.0}, {"k": 3.0}])

        result = left.anti_join(right, on=lambda l, r: l.k == r.k)

        assert result.to_dicts() == [{"k": "x"}]


class TestNullTypeKeys:
    """All-NULL (Arrow Null-typed) key columns: NULL never equals anything."""

    @pytest.fixture
    def null_key_csv(self):
        # An all-empty column infers as the Arrow Null type (same inference
        # path as the header-only CSV case).
        content = """id,k
1,
2,
3,
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
        yield f.name
        os.unlink(f.name)

    def test_null_key_column_is_null_typed(self, null_key_csv):
        """Fixture guard: the all-empty column must exercise the Null-type path."""
        t = LTSeq.read_csv(null_key_csv)
        assert t._schema["k"].lower() == "null"

    def test_semi_join_null_type_left_key_returns_empty(self, null_key_csv):
        nulls = LTSeq.read_csv(null_key_csv)
        right = LTSeq.from_rows([{"v": 1}, {"v": 2}])

        result = nulls.semi_join(right, on=lambda l, r: l.k == r.v)

        assert len(result.to_pandas()) == 0

    def test_anti_join_null_type_left_key_returns_all_left(self, null_key_csv):
        nulls = LTSeq.read_csv(null_key_csv)
        right = LTSeq.from_rows([{"v": 1}, {"v": 2}])

        result = nulls.anti_join(right, on=lambda l, r: l.k == r.v)

        df = result.to_pandas()
        assert len(df) == 3
        assert set(df["id"].tolist()) == {1, 2, 3}

    def test_semi_join_null_type_right_key_returns_empty(self, users_csv, null_key_csv):
        users = LTSeq.read_csv(users_csv)
        nulls = LTSeq.read_csv(null_key_csv)

        result = users.semi_join(nulls, on=lambda u, n: u.id == n.k)

        assert len(result.to_pandas()) == 0

    def test_anti_join_null_type_right_key_returns_all_left(self, users_csv, null_key_csv):
        users = LTSeq.read_csv(users_csv)
        nulls = LTSeq.read_csv(null_key_csv)

        result = users.anti_join(nulls, on=lambda u, n: u.id == n.k)

        assert len(result.to_pandas()) == 5


# Cleanup fixtures
@pytest.fixture(autouse=True)
def cleanup(request, users_csv, orders_csv, products_csv, reviews_csv, empty_csv):
    """Clean up temporary files after tests."""
    yield
    for csv_file in [users_csv, orders_csv, products_csv, reviews_csv, empty_csv]:
        if os.path.exists(csv_file):
            os.unlink(csv_file)
