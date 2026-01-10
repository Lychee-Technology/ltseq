"""Tests for NestedTable quantifier functions: all(), any(), none().

Tests the quantifier methods on GroupProxy that check if predicates
hold for all/any/none rows within each group.
"""

import pytest
import tempfile
import os
from ltseq import LTSeq


class TestQuantifierBasic:
    """Basic tests for all/any/none quantifier functions."""

    @pytest.fixture
    def grouped_data(self):
        """Create test data with groups containing different patterns."""
        # Group 1 (id=1): all positive amounts
        # Group 2 (id=2): mixed amounts (some positive, some negative)
        # Group 3 (id=3): all negative amounts
        data = """group_id,amount,status
1,100,ok
1,200,ok
1,50,ok
2,100,ok
2,-50,error
2,75,ok
3,-10,error
3,-20,error
3,-5,error
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write(data)
            t = LTSeq.read_csv(path)
            yield t

    def test_all_basic(self, grouped_data):
        """g.all() should return True only if predicate holds for ALL rows."""
        groups = grouped_data.sort("group_id").group_ordered(lambda r: r.group_id)

        # Filter groups where ALL amounts are positive
        result = groups.filter(lambda g: g.all(lambda r: r.amount > 0))
        df = result.flatten().to_pandas()

        # Only group 1 should remain (all positive)
        assert set(df["group_id"].unique()) == {1}

    def test_any_basic(self, grouped_data):
        """g.any() should return True if predicate holds for ANY row."""
        groups = grouped_data.sort("group_id").group_ordered(lambda r: r.group_id)

        # Filter groups where ANY row has status == "error"
        result = groups.filter(lambda g: g.any(lambda r: r.status == "error"))
        df = result.flatten().to_pandas()

        # Groups 2 and 3 have errors
        assert set(df["group_id"].unique()) == {2, 3}

    def test_none_basic(self, grouped_data):
        """g.none() should return True if predicate holds for NO rows."""
        groups = grouped_data.sort("group_id").group_ordered(lambda r: r.group_id)

        # Filter groups where NO rows have errors
        result = groups.filter(lambda g: g.none(lambda r: r.status == "error"))
        df = result.flatten().to_pandas()

        # Only group 1 has no errors
        assert set(df["group_id"].unique()) == {1}


class TestQuantifierEdgeCases:
    """Edge case tests for quantifier functions."""

    @pytest.fixture
    def single_row_groups(self):
        """Create data with single-row groups."""
        data = """group_id,value
1,10
2,20
3,30
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write(data)
            t = LTSeq.read_csv(path)
            yield t

    def test_single_row_group_all(self, single_row_groups):
        """g.all() should work correctly with single-row groups."""
        groups = single_row_groups.sort("group_id").group_ordered(lambda r: r.group_id)

        result = groups.filter(lambda g: g.all(lambda r: r.value > 15))
        df = result.flatten().to_pandas()

        # Groups 2 and 3 have value > 15
        assert set(df["group_id"].unique()) == {2, 3}

    def test_single_row_group_any(self, single_row_groups):
        """g.any() should work correctly with single-row groups."""
        groups = single_row_groups.sort("group_id").group_ordered(lambda r: r.group_id)

        result = groups.filter(lambda g: g.any(lambda r: r.value == 20))
        df = result.flatten().to_pandas()

        assert set(df["group_id"].unique()) == {2}

    def test_all_true_condition(self):
        """g.all() with always-true condition should keep all groups."""
        data = """group_id,value
1,10
1,20
2,30
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write(data)

            t = LTSeq.read_csv(path)
            groups = t.sort("group_id").group_ordered(lambda r: r.group_id)

            # All values are > 0
            result = groups.filter(lambda g: g.all(lambda r: r.value > 0))
            df = result.flatten().to_pandas()

            assert set(df["group_id"].unique()) == {1, 2}

    def test_any_false_condition(self):
        """g.any() with always-false condition should filter out all groups."""
        data = """group_id,value
1,10
1,20
2,30
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write(data)

            t = LTSeq.read_csv(path)
            groups = t.sort("group_id").group_ordered(lambda r: r.group_id)

            # No values are negative - result should be empty
            result = groups.filter(lambda g: g.any(lambda r: r.value < 0))
            # When all groups are filtered out, the result may be empty
            # Try to get length or catch the empty data error
            try:
                flattened = result.flatten()
                length = len(flattened)
                assert length == 0
            except RuntimeError as e:
                # Empty result is expected - no data loaded means no matching groups
                assert "No data loaded" in str(e)


class TestQuantifierCombinations:
    """Tests for combining quantifiers with other predicates."""

    @pytest.fixture
    def test_data(self):
        """Create test data."""
        data = """group_id,amount,status
1,100,ok
1,200,ok
1,50,ok
2,100,ok
2,-50,error
2,75,ok
3,-10,error
3,-20,error
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write(data)
            t = LTSeq.read_csv(path)
            yield t

    def test_all_with_count(self, test_data):
        """Combine g.all() with g.count()."""
        groups = test_data.sort("group_id").group_ordered(lambda r: r.group_id)

        # Groups with 3+ rows where all amounts are positive
        result = groups.filter(
            lambda g: (g.count() >= 3) & g.all(lambda r: r.amount > 0)
        )
        df = result.flatten().to_pandas()

        # Group 1 has 3 rows, all positive
        assert set(df["group_id"].unique()) == {1}

    def test_any_with_first(self, test_data):
        """Combine g.any() with g.first()."""
        groups = test_data.sort("group_id").group_ordered(lambda r: r.group_id)

        # Groups where first row has amount > 50 AND any row has error
        result = groups.filter(
            lambda g: (g.first().amount > 50) & g.any(lambda r: r.status == "error")
        )
        df = result.flatten().to_pandas()

        # Group 2 starts with 100 and has an error
        assert set(df["group_id"].unique()) == {2}

    def test_none_with_last(self, test_data):
        """Combine g.none() with g.last()."""
        groups = test_data.sort("group_id").group_ordered(lambda r: r.group_id)

        # Groups with no errors AND last amount > 40
        result = groups.filter(
            lambda g: g.none(lambda r: r.status == "error") & (g.last().amount > 40)
        )
        df = result.flatten().to_pandas()

        # Group 1 has no errors and last amount is 50
        assert set(df["group_id"].unique()) == {1}


class TestQuantifierPredicateTypes:
    """Tests for different predicate types in quantifiers."""

    @pytest.fixture
    def multi_type_data(self):
        """Create test data with various column types."""
        data = """group_id,value,name,is_active
1,100,Alice,true
1,200,Bob,true
2,50,Charlie,false
2,150,Diana,true
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write(data)
            t = LTSeq.read_csv(path)
            yield t

    def test_numeric_comparison(self, multi_type_data):
        """Test quantifiers with numeric comparisons."""
        groups = multi_type_data.sort("group_id").group_ordered(lambda r: r.group_id)

        # Groups where all values >= 100
        result = groups.filter(lambda g: g.all(lambda r: r.value >= 100))
        df = result.flatten().to_pandas()

        assert set(df["group_id"].unique()) == {1}

    def test_string_equality(self, multi_type_data):
        """Test quantifiers with string equality."""
        groups = multi_type_data.sort("group_id").group_ordered(lambda r: r.group_id)

        # Groups where any name is "Charlie"
        result = groups.filter(lambda g: g.any(lambda r: r.name == "Charlie"))
        df = result.flatten().to_pandas()

        assert set(df["group_id"].unique()) == {2}

    def test_boolean_comparison(self, multi_type_data):
        """Test quantifiers with boolean comparisons."""
        groups = multi_type_data.sort("group_id").group_ordered(lambda r: r.group_id)

        # DuckDB parses true/false in CSV as boolean type
        # Groups where all rows have is_active == True
        result = groups.filter(lambda g: g.all(lambda r: r.is_active == True))  # noqa: E712
        df = result.flatten().to_pandas()

        assert set(df["group_id"].unique()) == {1}


class TestQuantifierWithGroupSorted:
    """Tests for quantifiers with group_sorted (one-pass grouping)."""

    def test_all_with_group_sorted(self):
        """Test g.all() with group_sorted."""
        data = """key,value
A,10
A,20
B,30
B,-5
C,15
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write(data)

            t = LTSeq.read_csv(path)
            groups = t.sort("key").group_sorted(lambda r: r.key)

            result = groups.filter(lambda g: g.all(lambda r: r.value > 0))
            df = result.flatten().to_pandas()

            # Groups A and C have all positive values
            assert set(df["key"].unique()) == {"A", "C"}

    def test_any_with_group_sorted(self):
        """Test g.any() with group_sorted."""
        data = """key,value
A,10
A,20
B,30
B,-5
C,15
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write(data)

            t = LTSeq.read_csv(path)
            groups = t.sort("key").group_sorted(lambda r: r.key)

            result = groups.filter(lambda g: g.any(lambda r: r.value < 0))
            df = result.flatten().to_pandas()

            # Only group B has a negative value
            assert set(df["key"].unique()) == {"B"}


class TestQuantifierNoneSemantics:
    """Specific tests for g.none() semantics."""

    def test_none_equals_not_any(self):
        """g.none(pred) should be equivalent to not g.any(pred)."""
        data = """group_id,value
1,10
1,20
2,-5
2,15
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write(data)

            t = LTSeq.read_csv(path)
            groups = t.sort("group_id").group_ordered(lambda r: r.group_id)

            # g.none(r.value < 0) should filter out group 2
            result = groups.filter(lambda g: g.none(lambda r: r.value < 0))
            df = result.flatten().to_pandas()

            assert set(df["group_id"].unique()) == {1}

    def test_none_with_empty_match(self):
        """g.none() should return True when no rows match."""
        data = """group_id,value
1,10
1,20
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write(data)

            t = LTSeq.read_csv(path)
            groups = t.sort("group_id").group_ordered(lambda r: r.group_id)

            # No negative values
            result = groups.filter(lambda g: g.none(lambda r: r.value < 0))
            df = result.flatten().to_pandas()

            assert len(df) == 2  # Both rows preserved
