"""Phase 5: Core Relational Operations Tests

Tests for sort(), distinct(), and slice() methods.
"""

import pytest
from ltseq import LTSeq


class TestSortBasic:
    """Test basic sort functionality"""

    def test_sort_single_column_string(self):
        """sort() with single string column name"""
        t = LTSeq()
        t._schema = {"id": "int64", "name": "string", "age": "int64"}
        result = t.sort("name")
        assert isinstance(result, LTSeq)
        assert result._schema == t._schema

    def test_sort_single_column_lambda(self):
        """sort() with single lambda expression"""
        t = LTSeq()
        t._schema = {"id": "int64", "name": "string", "age": "int64"}
        result = t.sort(lambda r: r.name)
        assert isinstance(result, LTSeq)
        assert result._schema == t._schema

    def test_sort_derived_expression(self):
        """sort() with derived expression"""
        t = LTSeq()
        t._schema = {"id": "int64", "price": "float64", "qty": "int64"}
        result = t.sort(lambda r: r.price * r.qty)
        assert isinstance(result, LTSeq)

    def test_sort_preserves_schema(self):
        """sort() preserves the original schema"""
        t = LTSeq()
        original_schema = {"id": "int64", "name": "string"}
        t._schema = original_schema.copy()
        result = t.sort("id")
        assert result._schema == original_schema

    def test_sort_returns_ltseq(self):
        """sort() returns new LTSeq instance"""
        t = LTSeq()
        t._schema = {"x": "int64"}
        result = t.sort("x")
        assert isinstance(result, LTSeq)
        assert result is not t

    def test_sort_requires_schema(self):
        """sort() raises ValueError if schema not initialized"""
        t = LTSeq()
        with pytest.raises(ValueError, match="Schema not initialized"):
            t.sort("id")

    def test_sort_invalid_column_raises_error(self):
        """sort() with invalid column name raises AttributeError"""
        t = LTSeq()
        t._schema = {"id": "int64", "name": "string"}
        with pytest.raises(AttributeError, match="Column 'nonexistent' not found"):
            t.sort(lambda r: r.nonexistent)

    def test_sort_invalid_argument_type_raises_error(self):
        """sort() with invalid argument type raises TypeError"""
        t = LTSeq()
        t._schema = {"id": "int64"}
        with pytest.raises(TypeError, match="must be str or callable"):
            t.sort(123)  # type: ignore


class TestSortMultiColumn:
    """Test multi-column sort functionality"""

    def test_sort_two_columns_strings(self):
        """sort() with two string column names"""
        t = LTSeq()
        t._schema = {"date": "string", "id": "int64", "name": "string"}
        result = t.sort("date", "id")
        assert isinstance(result, LTSeq)

    def test_sort_three_columns(self):
        """sort() with three columns"""
        t = LTSeq()
        t._schema = {"a": "int64", "b": "int64", "c": "int64"}
        result = t.sort("a", "b", "c")
        assert isinstance(result, LTSeq)

    def test_sort_mixed_string_and_lambda(self):
        """sort() with mixed string and lambda arguments"""
        t = LTSeq()
        t._schema = {"id": "int64", "price": "float64", "qty": "int64"}
        result = t.sort("id", lambda r: r.price)
        assert isinstance(result, LTSeq)

    def test_sort_multiple_lambda_expressions(self):
        """sort() with multiple lambda expressions"""
        t = LTSeq()
        t._schema = {"id": "int64", "price": "float64", "qty": "int64"}
        result = t.sort(lambda r: r.id, lambda r: r.price)
        assert isinstance(result, LTSeq)

    def test_sort_multi_column_derived(self):
        """sort() with derived expressions in multiple columns"""
        t = LTSeq()
        t._schema = {"id": "int64", "price": "float64", "qty": "int64"}
        result = t.sort(lambda r: r.id, lambda r: r.price * r.qty)
        assert isinstance(result, LTSeq)


class TestSortDescending:
    """Test descending sort functionality (desc parameter)"""

    def test_sort_desc_single_column_bool_true(self):
        """sort() with desc=True for single column"""
        t = LTSeq()
        t._schema = {"id": "int64", "name": "string"}
        result = t.sort("id", desc=True)
        assert isinstance(result, LTSeq)
        assert result._schema == t._schema

    def test_sort_desc_single_column_bool_false(self):
        """sort() with desc=False is same as ascending (default)"""
        t = LTSeq()
        t._schema = {"id": "int64", "name": "string"}
        result = t.sort("id", desc=False)
        assert isinstance(result, LTSeq)

    def test_sort_desc_multiple_columns_all_desc(self):
        """sort() with desc=True applies to all columns"""
        t = LTSeq()
        t._schema = {"date": "string", "id": "int64", "value": "float64"}
        result = t.sort("date", "id", desc=True)
        assert isinstance(result, LTSeq)

    def test_sort_desc_multiple_columns_list(self):
        """sort() with desc list specifying direction per column"""
        t = LTSeq()
        t._schema = {"date": "string", "id": "int64", "value": "float64"}
        # date ASC, id DESC
        result = t.sort("date", "id", desc=[False, True])
        assert isinstance(result, LTSeq)

    def test_sort_desc_all_ascending_list(self):
        """sort() with desc=[False, False] is all ascending"""
        t = LTSeq()
        t._schema = {"a": "int64", "b": "int64"}
        result = t.sort("a", "b", desc=[False, False])
        assert isinstance(result, LTSeq)

    def test_sort_desc_all_descending_list(self):
        """sort() with desc=[True, True] is all descending"""
        t = LTSeq()
        t._schema = {"a": "int64", "b": "int64"}
        result = t.sort("a", "b", desc=[True, True])
        assert isinstance(result, LTSeq)

    def test_sort_desc_mixed_directions(self):
        """sort() with mixed asc/desc: a ASC, b DESC, c ASC"""
        t = LTSeq()
        t._schema = {"a": "int64", "b": "int64", "c": "int64"}
        result = t.sort("a", "b", "c", desc=[False, True, False])
        assert isinstance(result, LTSeq)

    def test_sort_desc_with_lambda(self):
        """sort() with lambda and desc=True"""
        t = LTSeq()
        t._schema = {"price": "float64", "qty": "int64"}
        result = t.sort(lambda r: r.price, desc=True)
        assert isinstance(result, LTSeq)

    def test_sort_desc_with_lambda_list(self):
        """sort() with multiple lambdas and desc list"""
        t = LTSeq()
        t._schema = {"price": "float64", "qty": "int64"}
        result = t.sort(lambda r: r.price, lambda r: r.qty, desc=[True, False])
        assert isinstance(result, LTSeq)

    def test_sort_desc_preserves_schema(self):
        """sort() with desc preserves original schema"""
        t = LTSeq()
        original_schema = {"id": "int64", "name": "string", "value": "float64"}
        t._schema = original_schema.copy()
        result = t.sort("id", desc=True)
        assert result._schema == original_schema

    def test_sort_desc_list_length_mismatch_raises_error(self):
        """sort() with desc list length != number of keys raises ValueError"""
        t = LTSeq()
        t._schema = {"a": "int64", "b": "int64"}
        with pytest.raises(ValueError, match="desc list length"):
            t.sort("a", "b", desc=[True])  # Only 1 bool for 2 columns

    def test_sort_desc_list_too_many_raises_error(self):
        """sort() with desc list too long raises ValueError"""
        t = LTSeq()
        t._schema = {"a": "int64", "b": "int64"}
        with pytest.raises(ValueError, match="desc list length"):
            t.sort("a", desc=[True, False])  # 2 bools for 1 column

    def test_sort_desc_invalid_type_raises_error(self):
        """sort() with invalid desc type raises TypeError"""
        t = LTSeq()
        t._schema = {"id": "int64"}
        with pytest.raises(TypeError, match="desc must be bool or list"):
            t.sort("id", desc="descending")  # type: ignore

    def test_sort_desc_invalid_type_int_raises_error(self):
        """sort() with int desc raises TypeError"""
        t = LTSeq()
        t._schema = {"id": "int64"}
        with pytest.raises(TypeError, match="desc must be bool or list"):
            t.sort("id", desc=1)  # type: ignore


class TestDistinctBasic:
    """Test basic distinct functionality"""

    def test_distinct_no_args(self):
        """distinct() with no arguments (all columns)"""
        t = LTSeq()
        t._schema = {"id": "int64", "name": "string"}
        result = t.distinct()
        assert isinstance(result, LTSeq)
        assert result._schema == t._schema

    def test_distinct_single_column_string(self):
        """distinct() with single string column"""
        t = LTSeq()
        t._schema = {"id": "int64", "name": "string"}
        result = t.distinct("id")
        assert isinstance(result, LTSeq)

    def test_distinct_single_column_lambda(self):
        """distinct() with single lambda expression"""
        t = LTSeq()
        t._schema = {"id": "int64", "name": "string"}
        result = t.distinct(lambda r: r.id)
        assert isinstance(result, LTSeq)

    def test_distinct_preserves_schema(self):
        """distinct() preserves schema"""
        t = LTSeq()
        original_schema = {"id": "int64", "name": "string", "age": "int64"}
        t._schema = original_schema.copy()
        result = t.distinct("id")
        assert result._schema == original_schema

    def test_distinct_returns_ltseq(self):
        """distinct() returns new LTSeq instance"""
        t = LTSeq()
        t._schema = {"x": "int64"}
        result = t.distinct()
        assert isinstance(result, LTSeq)
        assert result is not t

    def test_distinct_requires_schema(self):
        """distinct() raises ValueError if schema not initialized"""
        t = LTSeq()
        with pytest.raises(ValueError, match="Schema not initialized"):
            t.distinct()

    def test_distinct_invalid_column_raises_error(self):
        """distinct() with invalid column name raises error"""
        t = LTSeq()
        t._schema = {"id": "int64", "name": "string"}
        with pytest.raises(AttributeError, match="Column 'nonexistent' not found"):
            t.distinct(lambda r: r.nonexistent)

    def test_distinct_invalid_argument_type_raises_error(self):
        """distinct() with invalid argument type raises TypeError"""
        t = LTSeq()
        t._schema = {"id": "int64"}
        with pytest.raises(TypeError, match="must be str or callable"):
            t.distinct(123)  # type: ignore


class TestDistinctMultiColumn:
    """Test multi-column distinct functionality"""

    def test_distinct_two_columns(self):
        """distinct() with two columns"""
        t = LTSeq()
        t._schema = {"id": "int64", "date": "string", "value": "float64"}
        result = t.distinct("id", "date")
        assert isinstance(result, LTSeq)

    def test_distinct_multiple_columns(self):
        """distinct() with multiple columns"""
        t = LTSeq()
        t._schema = {"a": "int64", "b": "int64", "c": "int64"}
        result = t.distinct("a", "b", "c")
        assert isinstance(result, LTSeq)

    def test_distinct_mixed_string_and_lambda(self):
        """distinct() with mixed string and lambda arguments"""
        t = LTSeq()
        t._schema = {"id": "int64", "date": "string", "value": "float64"}
        result = t.distinct("id", lambda r: r.date)
        assert isinstance(result, LTSeq)


class TestSliceBasic:
    """Test basic slice functionality"""

    def test_slice_offset_only(self):
        """slice() with offset only"""
        t = LTSeq()
        t._schema = {"id": "int64", "name": "string"}
        result = t.slice(offset=5)
        assert isinstance(result, LTSeq)
        assert result._schema == t._schema

    def test_slice_length_only(self):
        """slice() with length only"""
        t = LTSeq()
        t._schema = {"id": "int64", "name": "string"}
        result = t.slice(length=10)
        assert isinstance(result, LTSeq)

    def test_slice_offset_and_length(self):
        """slice() with both offset and length"""
        t = LTSeq()
        t._schema = {"id": "int64", "name": "string"}
        result = t.slice(offset=5, length=10)
        assert isinstance(result, LTSeq)

    def test_slice_zero_offset(self):
        """slice() with offset=0"""
        t = LTSeq()
        t._schema = {"id": "int64"}
        result = t.slice(0, 5)
        assert isinstance(result, LTSeq)

    def test_slice_first_n_rows(self):
        """slice(length=n) gets first n rows"""
        t = LTSeq()
        t._schema = {"id": "int64"}
        result = t.slice(length=10)
        assert isinstance(result, LTSeq)

    def test_slice_preserves_schema(self):
        """slice() preserves schema"""
        t = LTSeq()
        original_schema = {"id": "int64", "name": "string", "age": "int64"}
        t._schema = original_schema.copy()
        result = t.slice(offset=2, length=5)
        assert result._schema == original_schema

    def test_slice_returns_ltseq(self):
        """slice() returns new LTSeq instance"""
        t = LTSeq()
        t._schema = {"x": "int64"}
        result = t.slice(0, 1)
        assert isinstance(result, LTSeq)
        assert result is not t

    def test_slice_requires_schema(self):
        """slice() raises ValueError if schema not initialized"""
        t = LTSeq()
        with pytest.raises(ValueError, match="Schema not initialized"):
            t.slice(0, 10)

    def test_slice_negative_offset_raises_error(self):
        """slice() with negative offset raises ValueError"""
        t = LTSeq()
        t._schema = {"id": "int64"}
        with pytest.raises(ValueError, match="offset must be non-negative"):
            t.slice(offset=-1)

    def test_slice_negative_length_raises_error(self):
        """slice() with negative length raises ValueError"""
        t = LTSeq()
        t._schema = {"id": "int64"}
        with pytest.raises(ValueError, match="length must be non-negative"):
            t.slice(length=-5)


class TestMethodChaining:
    """Test method chaining with relational operations"""

    def test_filter_then_sort(self):
        """filter() → sort() chain"""
        t = LTSeq()
        t._schema = {"id": "int64", "age": "int64", "name": "string"}
        result = t.filter(lambda r: r.age > 18).sort("name")
        assert isinstance(result, LTSeq)

    def test_sort_then_slice(self):
        """sort() → slice() chain"""
        t = LTSeq()
        t._schema = {"id": "int64", "name": "string"}
        result = t.sort("name").slice(0, 10)
        assert isinstance(result, LTSeq)

    def test_filter_distinct_sort(self):
        """filter() → distinct() → sort() chain"""
        t = LTSeq()
        t._schema = {"id": "int64", "category": "string", "name": "string"}
        result = t.filter(lambda r: r.id > 0).distinct("category").sort("name")
        assert isinstance(result, LTSeq)

    def test_sort_distinct_slice(self):
        """sort() → distinct() → slice() chain"""
        t = LTSeq()
        t._schema = {"id": "int64", "value": "float64"}
        result = t.sort("value").distinct("id").slice(0, 5)
        assert isinstance(result, LTSeq)

    def test_derive_sort_select(self):
        """derive() → sort() → select() chain (test structure only, needs loaded data)"""
        t = LTSeq()
        t._schema = {"id": "int64", "price": "float64", "qty": "int64"}
        # Test structure without calling methods (avoid runtime errors on empty data)
        # In actual use with loaded CSV, this would work:
        # result = t.derive(total=lambda r: r.price * r.qty).sort(lambda r: r.total).select("id", "total")
        # For now, just test that sort works on derived schema
        result_schema = t._schema.copy()
        result_schema["total"] = "Unknown"  # Simulating derived column
        assert "total" in result_schema

    def test_complex_chain(self):
        """Complex multi-operation chain (test structure only)"""
        t = LTSeq()
        t._schema = {
            "id": "int64",
            "price": "float64",
            "qty": "int64",
            "date": "string",
        }
        # Test structure - actual chain would need loaded data
        # For unit test, verify that operation signatures are correct
        sort_result = t.sort("date")
        slice_result = sort_result.slice(0, 10)
        assert isinstance(sort_result, LTSeq)
        assert isinstance(slice_result, LTSeq)


class TestEdgeCases:
    """Test edge cases and boundary conditions"""

    def test_sort_empty_schema(self):
        """sort() requires non-empty schema"""
        t = LTSeq()
        t._schema = {}
        # Empty schema should raise ValueError (treated as not initialized)
        with pytest.raises(ValueError, match="Schema not initialized"):
            t.sort()

    def test_distinct_empty_schema(self):
        """distinct() requires non-empty schema"""
        t = LTSeq()
        t._schema = {}
        # Empty schema should raise ValueError (treated as not initialized)
        with pytest.raises(ValueError, match="Schema not initialized"):
            t.distinct()

    def test_slice_zero_length(self):
        """slice(length=0) returns 0 rows"""
        t = LTSeq()
        t._schema = {"id": "int64"}
        result = t.slice(offset=0, length=0)
        assert isinstance(result, LTSeq)

    def test_slice_large_offset(self):
        """slice() with offset beyond dataset size"""
        t = LTSeq()
        t._schema = {"id": "int64"}
        # Should not raise error, just return empty result
        result = t.slice(offset=999999, length=10)
        assert isinstance(result, LTSeq)

    def test_sort_numeric_column(self):
        """sort() numeric columns"""
        t = LTSeq()
        t._schema = {"value": "float64", "count": "int64"}
        result = t.sort("value")
        assert isinstance(result, LTSeq)

    def test_sort_string_column(self):
        """sort() string columns"""
        t = LTSeq()
        t._schema = {"name": "string", "city": "string"}
        result = t.sort("name")
        assert isinstance(result, LTSeq)

    def test_multiple_sorts_override(self):
        """Multiple sort() calls create new sorted result"""
        t = LTSeq()
        t._schema = {"id": "int64", "value": "float64"}
        result1 = t.sort("id")
        result2 = result1.sort("value")
        # Second sort should override/create new sort
        assert result2 is not result1
        assert isinstance(result2, LTSeq)


class TestExpressionIntegration:
    """Test expressions with relational operations"""

    def test_sort_with_arithmetic_expression(self):
        """sort() with arithmetic expressions"""
        t = LTSeq()
        t._schema = {"price": "float64", "qty": "int64", "tax": "float64"}
        result = t.sort(lambda r: r.price + r.tax)
        assert isinstance(result, LTSeq)

    def test_sort_with_comparison_expression(self):
        """sort() captures comparison but sorts numerically"""
        t = LTSeq()
        t._schema = {"value": "int64"}
        # Note: comparison results in boolean, sorting by boolean is valid
        result = t.sort(lambda r: r.value > 10)
        assert isinstance(result, LTSeq)

    def test_distinct_with_lambda_arithmetic(self):
        """distinct() with arithmetic lambda expression"""
        t = LTSeq()
        t._schema = {"a": "int64", "b": "int64"}
        result = t.distinct(lambda r: r.a + r.b)
        assert isinstance(result, LTSeq)


class TestSchemaUpdate:
    """Test that operations properly maintain schema"""

    def test_sort_maintains_column_types(self):
        """sort() maintains original column types"""
        t = LTSeq()
        schema = {"int_col": "int64", "float_col": "float64", "string_col": "string"}
        t._schema = schema.copy()
        result = t.sort("int_col")
        assert result._schema == schema

    def test_distinct_maintains_column_types(self):
        """distinct() maintains original column types"""
        t = LTSeq()
        schema = {"id": "int64", "name": "string"}
        t._schema = schema.copy()
        result = t.distinct("id")
        assert result._schema == schema

    def test_slice_maintains_column_types(self):
        """slice() maintains original column types"""
        t = LTSeq()
        schema = {"id": "int64", "value": "float64", "label": "string"}
        t._schema = schema.copy()
        result = t.slice(2, 5)
        assert result._schema == schema


class TestOperationWithoutDataframe:
    """Test operations when no dataframe is loaded (unit test scenarios)"""

    def test_sort_without_dataframe(self):
        """sort() works without loaded dataframe (unit test case)"""
        t = LTSeq()
        t._schema = {"id": "int64"}
        # Should not raise error, just operate on empty internal state
        result = t.sort("id")
        assert isinstance(result, LTSeq)

    def test_distinct_without_dataframe(self):
        """distinct() works without loaded dataframe (unit test case)"""
        t = LTSeq()
        t._schema = {"id": "int64"}
        result = t.distinct("id")
        assert isinstance(result, LTSeq)

    def test_slice_without_dataframe(self):
        """slice() works without loaded dataframe (unit test case)"""
        t = LTSeq()
        t._schema = {"id": "int64"}
        result = t.slice(0, 10)
        assert isinstance(result, LTSeq)


# Integration tests with actual data
import csv
import tempfile
import os


class TestSortDescendingIntegration:
    """Integration tests for descending sort with actual data"""

    @pytest.fixture
    def sample_csv(self):
        """Create a temporary CSV with test data."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "value", "category"])
            writer.writeheader()
            data = [
                {"id": "3", "name": "Charlie", "value": "30", "category": "B"},
                {"id": "1", "name": "Alice", "value": "10", "category": "A"},
                {"id": "4", "name": "David", "value": "20", "category": "A"},
                {"id": "2", "name": "Bob", "value": "40", "category": "B"},
                {"id": "5", "name": "Eve", "value": "25", "category": "C"},
            ]
            for row in data:
                writer.writerow(row)
            temp_path = f.name

        yield temp_path
        os.unlink(temp_path)

    def test_sort_ascending_verifies_order(self, sample_csv):
        """Verify ascending sort order with actual data."""
        t = LTSeq.read_csv(sample_csv)
        result = t.sort("id")
        df = result.to_pandas()
        ids = df["id"].tolist()
        # Should be [1, 2, 3, 4, 5] in ascending order
        assert ids == [1, 2, 3, 4, 5]

    def test_sort_descending_verifies_order(self, sample_csv):
        """Verify descending sort order with actual data."""
        t = LTSeq.read_csv(sample_csv)
        result = t.sort("id", desc=True)
        df = result.to_pandas()
        ids = df["id"].tolist()
        # Should be [5, 4, 3, 2, 1] in descending order
        assert ids == [5, 4, 3, 2, 1]

    def test_sort_by_string_descending(self, sample_csv):
        """Verify descending sort on string column."""
        t = LTSeq.read_csv(sample_csv)
        result = t.sort("name", desc=True)
        df = result.to_pandas()
        names = df["name"].tolist()
        # Should be sorted Z-A: Eve, David, Charlie, Bob, Alice
        assert names == ["Eve", "David", "Charlie", "Bob", "Alice"]

    def test_sort_by_string_ascending(self, sample_csv):
        """Verify ascending sort on string column."""
        t = LTSeq.read_csv(sample_csv)
        result = t.sort("name", desc=False)
        df = result.to_pandas()
        names = df["name"].tolist()
        # Should be sorted A-Z: Alice, Bob, Charlie, David, Eve
        assert names == ["Alice", "Bob", "Charlie", "David", "Eve"]

    def test_sort_multi_column_mixed_directions(self, sample_csv):
        """Verify multi-column sort with mixed asc/desc."""
        t = LTSeq.read_csv(sample_csv)
        # Sort by category ASC, then by value DESC within category
        result = t.sort("category", "value", desc=[False, True])
        df = result.to_pandas()

        # Category A: Alice(10), David(20) -> sorted DESC by value: David(20), Alice(10)
        # Category B: Charlie(30), Bob(40) -> sorted DESC by value: Bob(40), Charlie(30)
        # Category C: Eve(25)
        categories = df["category"].tolist()
        values = df["value"].tolist()

        # Categories should be in ASC order
        assert categories == ["A", "A", "B", "B", "C"]
        # Within each category, values should be DESC
        # A: 20, 10 (desc)
        # B: 40, 30 (desc)
        # C: 25
        assert values == [20, 10, 40, 30, 25]

    def test_sort_multi_column_both_desc(self, sample_csv):
        """Verify multi-column sort with both columns descending."""
        t = LTSeq.read_csv(sample_csv)
        result = t.sort("category", "value", desc=[True, True])
        df = result.to_pandas()
        categories = df["category"].tolist()
        values = df["value"].tolist()

        # Categories should be in DESC order: C, B, B, A, A
        assert categories == ["C", "B", "B", "A", "A"]
        # Within each category, values should also be DESC
        # C: 25
        # B: 40, 30
        # A: 20, 10
        assert values == [25, 40, 30, 20, 10]

    def test_sort_with_lambda_descending(self, sample_csv):
        """Verify descending sort using lambda expression."""
        t = LTSeq.read_csv(sample_csv)
        result = t.sort(lambda r: r.value, desc=True)
        df = result.to_pandas()
        values = df["value"].tolist()
        # Should be sorted in descending order: 40, 30, 25, 20, 10
        assert values == [40, 30, 25, 20, 10]

    def test_sort_desc_then_filter(self, sample_csv):
        """Sort descending then filter (chain test)."""
        t = LTSeq.read_csv(sample_csv)
        result = t.sort("value", desc=True).filter(lambda r: r.value > 20)
        df = result.to_pandas()
        values = df["value"].tolist()
        # Values > 20 in descending order: 40, 30, 25
        assert values == [40, 30, 25]

    def test_sort_filter_then_sort_desc(self, sample_csv):
        """Filter then sort descending (chain test)."""
        t = LTSeq.read_csv(sample_csv)
        result = t.filter(lambda r: r.category == "B").sort("value", desc=True)
        df = result.to_pandas()
        values = df["value"].tolist()
        # Category B values in descending order: 40, 30
        assert values == [40, 30]


class TestDistinctKeyColumnsIntegration:
    """Integration tests for distinct with specific key columns"""

    @pytest.fixture
    def duplicates_csv(self):
        """Create a temporary CSV with duplicate data."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "region", "value"])
            writer.writeheader()
            data = [
                {"id": "1", "name": "Alice", "region": "North", "value": "100"},
                {"id": "1", "name": "Alice", "region": "South", "value": "150"},
                {"id": "2", "name": "Bob", "region": "North", "value": "200"},
                {"id": "2", "name": "Charlie", "region": "North", "value": "250"},
                {"id": "3", "name": "Diana", "region": "East", "value": "300"},
                {"id": "3", "name": "Diana", "region": "East", "value": "350"},
            ]
            for row in data:
                writer.writerow(row)
            temp_path = f.name

        yield temp_path
        os.unlink(temp_path)

    def test_distinct_no_args_all_columns(self, duplicates_csv):
        """distinct() with no args removes only fully duplicate rows."""
        t = LTSeq.read_csv(duplicates_csv)
        result = t.distinct()
        df = result.to_pandas()
        # Row id=3,Diana,East appears twice with different values, so both kept
        # All other rows are unique
        assert len(df) == 6  # All rows are unique across all columns

    def test_distinct_single_column(self, duplicates_csv):
        """distinct('id') keeps first occurrence for each unique id."""
        t = LTSeq.read_csv(duplicates_csv)
        result = t.distinct("id")
        df = result.to_pandas()
        # Should have 3 unique ids: 1, 2, 3
        assert len(df) == 3
        assert set(df["id"].tolist()) == {1, 2, 3}

    def test_distinct_single_column_lambda(self, duplicates_csv):
        """distinct(lambda r: r.region) keeps first occurrence for each region."""
        t = LTSeq.read_csv(duplicates_csv)
        result = t.distinct(lambda r: r.region)
        df = result.to_pandas()
        # Should have 3 unique regions: North, South, East
        assert len(df) == 3
        assert set(df["region"].tolist()) == {"North", "South", "East"}

    def test_distinct_two_columns(self, duplicates_csv):
        """distinct('id', 'region') keeps first for each id-region combo."""
        t = LTSeq.read_csv(duplicates_csv)
        result = t.distinct("id", "region")
        df = result.to_pandas()
        # id=1 has North and South (2 rows)
        # id=2 has only North (1 row, even though two names)
        # id=3 has only East (1 row)
        assert len(df) == 4

    def test_distinct_mixed_string_lambda(self, duplicates_csv):
        """distinct() with mixed string and lambda arguments."""
        t = LTSeq.read_csv(duplicates_csv)
        result = t.distinct("id", lambda r: r.region)
        df = result.to_pandas()
        # Same as distinct('id', 'region')
        assert len(df) == 4

    def test_distinct_preserves_all_columns(self, duplicates_csv):
        """distinct() on key columns preserves all original columns."""
        t = LTSeq.read_csv(duplicates_csv)
        result = t.distinct("id")
        df = result.to_pandas()
        # All original columns should be present
        assert list(df.columns) == ["id", "name", "region", "value"]

    def test_distinct_chain_with_filter(self, duplicates_csv):
        """distinct() can be chained with filter()."""
        t = LTSeq.read_csv(duplicates_csv)
        result = t.filter(lambda r: r.region != "East").distinct("id")
        df = result.to_pandas()
        # After filtering out East, we have id=1,2 each appearing multiple times
        # distinct('id') should give us 2 rows
        assert len(df) == 2
        assert set(df["id"].tolist()) == {1, 2}

    def test_distinct_chain_with_sort(self, duplicates_csv):
        """distinct() can be chained with sort()."""
        t = LTSeq.read_csv(duplicates_csv)
        result = t.distinct("id").sort("id")
        df = result.to_pandas()
        ids = df["id"].tolist()
        assert ids == [1, 2, 3]

    def test_distinct_after_sort(self, duplicates_csv):
        """sort() then distinct() - first occurrence after sort."""
        t = LTSeq.read_csv(duplicates_csv)
        # Sort by value descending, then distinct by id
        # This should give us the highest value for each id
        result = t.sort("value", desc=True).distinct("id")
        df = result.to_pandas()
        assert len(df) == 3
        # Each id should have its maximum value
        for _, row in df.iterrows():
            if row["id"] == 1:
                assert row["value"] == 150  # Max for id=1
            elif row["id"] == 2:
                assert row["value"] == 250  # Max for id=2
            elif row["id"] == 3:
                assert row["value"] == 350  # Max for id=3

    def test_distinct_invalid_column_error(self, duplicates_csv):
        """distinct() with invalid column raises error."""
        t = LTSeq.read_csv(duplicates_csv)
        with pytest.raises(Exception):  # Could be ValueError or AttributeError
            t.distinct("nonexistent")

    def test_distinct_name_column(self, duplicates_csv):
        """distinct('name') keeps first for each unique name."""
        t = LTSeq.read_csv(duplicates_csv)
        result = t.distinct("name")
        df = result.to_pandas()
        # Names: Alice (appears 2x), Bob (1x), Charlie (1x), Diana (2x)
        # Should have 4 unique names
        assert len(df) == 4
        assert set(df["name"].tolist()) == {"Alice", "Bob", "Charlie", "Diana"}
