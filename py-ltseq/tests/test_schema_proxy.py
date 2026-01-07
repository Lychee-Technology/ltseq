"""
Tests for SchemaProxy class.

Tests the row proxy mechanism used in lambda expressions:
- Attribute access returns ColumnExpr objects
- Schema validation (columns exist)
- Private attribute handling
- get_schema() method
"""

import pytest
from ltseq.expr import SchemaProxy, ColumnExpr, _lambda_to_expr


class TestSchemaProxyBasic:
    """Test basic SchemaProxy functionality."""

    def test_schema_proxy_init(self):
        """SchemaProxy initializes with a schema dict."""
        schema = {"age": "int64", "name": "string"}
        proxy = SchemaProxy(schema)
        assert proxy.get_schema() == schema

    def test_schema_proxy_column_access(self):
        """Accessing a valid column returns ColumnExpr."""
        schema = {"age": "int64", "name": "string"}
        proxy = SchemaProxy(schema)

        col = proxy.age
        assert isinstance(col, ColumnExpr)
        assert col.name == "age"

    def test_schema_proxy_multiple_columns(self):
        """Can access multiple columns from the same proxy."""
        schema = {"age": "int64", "name": "string", "salary": "float64"}
        proxy = SchemaProxy(schema)

        col_age = proxy.age
        col_name = proxy.name
        col_salary = proxy.salary

        assert col_age.name == "age"
        assert col_name.name == "name"
        assert col_salary.name == "salary"


class TestSchemaProxyValidation:
    """Test schema validation in SchemaProxy."""

    def test_schema_proxy_invalid_column(self):
        """Accessing a non-existent column raises AttributeError."""
        schema = {"age": "int64", "name": "string"}
        proxy = SchemaProxy(schema)

        with pytest.raises(AttributeError) as exc_info:
            proxy.invalid_column

        assert "Column 'invalid_column' not found in schema" in str(exc_info.value)
        assert "Available columns" in str(exc_info.value)

    def test_schema_proxy_private_attribute(self):
        """Accessing private attributes raises AttributeError."""
        schema = {"age": "int64"}
        proxy = SchemaProxy(schema)

        with pytest.raises(AttributeError) as exc_info:
            proxy._private

        assert "No attribute _private" in str(exc_info.value)

    def test_schema_proxy_underscore_error_message(self):
        """Error message for private attributes is informative."""
        schema = {"age": "int64"}
        proxy = SchemaProxy(schema)

        with pytest.raises(AttributeError):
            proxy._anything


class TestSchemaProxyGetSchema:
    """Test SchemaProxy.get_schema() method."""

    def test_get_schema_returns_dict(self):
        """get_schema() returns a dict copy."""
        schema = {"age": "int64", "name": "string"}
        proxy = SchemaProxy(schema)

        result = proxy.get_schema()
        assert isinstance(result, dict)
        assert result == schema

    def test_get_schema_returns_copy(self):
        """get_schema() returns a copy, not the original."""
        schema = {"age": "int64"}
        proxy = SchemaProxy(schema)

        result = proxy.get_schema()
        result["age"] = "float64"  # Modify the copy

        # Original schema should be unchanged
        assert proxy.get_schema()["age"] == "int64"

    def test_get_schema_empty(self):
        """get_schema() works with an empty schema."""
        proxy = SchemaProxy({})
        assert proxy.get_schema() == {}

    def test_get_schema_multiple_types(self):
        """get_schema() returns all column types correctly."""
        schema = {
            "age": "int64",
            "name": "string",
            "salary": "float64",
            "active": "bool",
        }
        proxy = SchemaProxy(schema)

        result = proxy.get_schema()
        assert result == schema


class TestSchemaProxyIntegration:
    """Test SchemaProxy integration with expressions."""

    def test_schema_proxy_in_comparison(self):
        """SchemaProxy column can be used in comparisons."""
        schema = {"age": "int64"}
        proxy = SchemaProxy(schema)

        expr = proxy.age > 18
        assert expr is not None
        assert expr.serialize()["type"] == "BinOp"

    def test_schema_proxy_in_arithmetic(self):
        """SchemaProxy column can be used in arithmetic."""
        schema = {"price": "float64", "quantity": "int64"}
        proxy = SchemaProxy(schema)

        expr = proxy.price * proxy.quantity
        assert expr is not None
        assert expr.serialize()["type"] == "BinOp"

    def test_schema_proxy_in_method_call(self):
        """SchemaProxy column can have methods called on it."""
        schema = {"values": "int64"}
        proxy = SchemaProxy(schema)

        expr = proxy.values.shift(1)
        assert expr is not None
        assert expr.serialize()["type"] == "Call"

    def test_schema_proxy_complex_expression(self):
        """SchemaProxy supports complex expressions."""
        schema = {"age": "int64", "salary": "float64"}
        proxy = SchemaProxy(schema)

        expr = (proxy.age > 30) & (proxy.salary > 50000.0)
        assert expr is not None
        serialized = expr.serialize()
        assert serialized["type"] == "BinOp"
        assert serialized["op"] == "And"


class TestLambdaToExpr:
    """Test _lambda_to_expr function with SchemaProxy."""

    def test_lambda_to_expr_simple_comparison(self):
        """_lambda_to_expr captures a simple comparison."""
        schema = {"age": "int64"}
        expr_dict = _lambda_to_expr(lambda r: r.age > 18, schema)

        assert expr_dict["type"] == "BinOp"
        assert expr_dict["op"] == "Gt"
        assert expr_dict["left"]["type"] == "Column"
        assert expr_dict["left"]["name"] == "age"
        assert expr_dict["right"]["type"] == "Literal"
        assert expr_dict["right"]["value"] == 18

    def test_lambda_to_expr_string_comparison(self):
        """_lambda_to_expr works with string columns."""
        schema = {"name": "string"}
        expr_dict = _lambda_to_expr(lambda r: r.name == "Alice", schema)

        assert expr_dict["type"] == "BinOp"
        assert expr_dict["op"] == "Eq"
        assert expr_dict["right"]["value"] == "Alice"

    def test_lambda_to_expr_logical_and(self):
        """_lambda_to_expr captures logical AND."""
        schema = {"age": "int64", "salary": "float64"}
        expr_dict = _lambda_to_expr(
            lambda r: (r.age > 30) & (r.salary > 50000.0), schema
        )

        assert expr_dict["type"] == "BinOp"
        assert expr_dict["op"] == "And"

    def test_lambda_to_expr_logical_or(self):
        """_lambda_to_expr captures logical OR."""
        schema = {"active": "bool", "premium": "bool"}
        expr_dict = _lambda_to_expr(lambda r: r.active | r.premium, schema)

        assert expr_dict["type"] == "BinOp"
        assert expr_dict["op"] == "Or"

    def test_lambda_to_expr_method_call(self):
        """_lambda_to_expr captures method calls."""
        schema = {"values": "int64"}
        expr_dict = _lambda_to_expr(lambda r: r.values.shift(1), schema)

        assert expr_dict["type"] == "Call"
        assert expr_dict["func"] == "shift"
        assert len(expr_dict["args"]) == 1
        assert expr_dict["args"][0]["type"] == "Literal"
        assert expr_dict["args"][0]["value"] == 1

    def test_lambda_to_expr_chained_calls(self):
        """_lambda_to_expr captures chained method calls."""
        schema = {"values": "int64"}
        expr_dict = _lambda_to_expr(lambda r: r.values.rolling(3).mean(), schema)

        assert expr_dict["type"] == "Call"
        assert expr_dict["func"] == "mean"
        assert expr_dict["on"] is not None
        assert expr_dict["on"]["type"] == "Call"
        assert expr_dict["on"]["func"] == "rolling"

    def test_lambda_to_expr_invalid_column(self):
        """_lambda_to_expr raises AttributeError for invalid column."""
        schema = {"age": "int64"}

        with pytest.raises(AttributeError) as exc_info:
            _lambda_to_expr(lambda r: r.invalid_col > 18, schema)

        assert "Column 'invalid_col' not found" in str(exc_info.value)

    def test_lambda_to_expr_non_expr_return(self):
        """_lambda_to_expr raises TypeError if lambda doesn't return Expr."""
        schema = {"age": "int64"}

        with pytest.raises(TypeError) as exc_info:
            _lambda_to_expr(lambda r: 42, schema)

        assert "Lambda must return an Expr" in str(exc_info.value)
        assert "int" in str(exc_info.value)

    def test_lambda_to_expr_arithmetic(self):
        """_lambda_to_expr captures arithmetic operations."""
        schema = {"price": "float64", "quantity": "int64"}
        expr_dict = _lambda_to_expr(lambda r: r.price * r.quantity, schema)

        assert expr_dict["type"] == "BinOp"
        assert expr_dict["op"] == "Mul"

    def test_lambda_to_expr_mixed_operations(self):
        """_lambda_to_expr handles complex mixed expressions."""
        schema = {"a": "int64", "b": "float64", "c": "int64"}
        expr_dict = _lambda_to_expr(lambda r: (r.a + r.b) > r.c, schema)

        assert expr_dict["type"] == "BinOp"
        assert expr_dict["op"] == "Gt"
        assert expr_dict["left"]["type"] == "BinOp"
        assert expr_dict["left"]["op"] == "Add"


class TestSchemaProxyEdgeCases:
    """Test edge cases and special scenarios."""

    def test_schema_proxy_numeric_column_names(self):
        """Numeric column names (as strings) should not work via dot notation."""
        # This is Python syntax limitation - can't do proxy.123
        # But we can test that the schema accepts them
        schema = {"col1": "int64", "col2": "string"}
        proxy = SchemaProxy(schema)

        col = proxy.col1
        assert col.name == "col1"

    def test_schema_proxy_special_method_names(self):
        """Accessing private attributes with underscore should raise AttributeError."""
        schema = {"age": "int64"}
        proxy = SchemaProxy(schema)

        # Private attributes like _schema raise AttributeError
        with pytest.raises(AttributeError):
            proxy._schema_access  # This goes through __getattr__

    def test_schema_proxy_none_schema(self):
        """SchemaProxy with empty schema handles properly."""
        proxy = SchemaProxy({})

        with pytest.raises(AttributeError) as exc_info:
            proxy.any_column

        assert "not found in schema" in str(exc_info.value)

    def test_lambda_to_expr_preserves_schema(self):
        """_lambda_to_expr doesn't modify the original schema."""
        schema = {"age": "int64"}
        schema_copy = schema.copy()

        _lambda_to_expr(lambda r: r.age > 18, schema)

        assert schema == schema_copy

    def test_schema_proxy_multiple_accesses_same_column(self):
        """Accessing the same column multiple times returns new ColumnExpr instances."""
        schema = {"age": "int64"}
        proxy = SchemaProxy(schema)

        col1 = proxy.age
        col2 = proxy.age

        # Both should be ColumnExpr with same name, but different objects
        assert col1.name == col2.name
        # They should serialize identically
        assert col1.serialize() == col2.serialize()
