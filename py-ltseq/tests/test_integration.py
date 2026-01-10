"""
Integration tests for lambda capture and LTSeq expression system.

These tests verify that:
1. LTSeq can capture expression trees from lambdas
2. Expressions are properly serialized for Rust consumption
3. Integration points between Python expr system and LTSeq work correctly
"""

import pytest
from ltseq.expr import _lambda_to_expr, SchemaProxy
from ltseq import LTSeq


class TestLambdaCaptureBasic:
    """Test basic lambda capture functionality."""

    def test_capture_simple_filter(self):
        """Lambda capture works for simple filter expressions."""
        schema = {"age": "int64"}
        expr_dict = _lambda_to_expr(lambda r: r.age > 18, schema)

        # Should produce a serializable dict
        assert isinstance(expr_dict, dict)
        assert expr_dict["type"] == "BinOp"
        assert expr_dict["op"] == "Gt"

    def test_capture_complex_filter(self):
        """Lambda capture works for complex filter expressions."""
        schema = {"age": "int64", "salary": "float64", "active": "bool"}
        expr_dict = _lambda_to_expr(
            lambda r: (r.age > 30) & (r.salary > 50000.0) & r.active, schema
        )

        # Should have nested BinOp with And operations
        assert expr_dict["type"] == "BinOp"
        assert expr_dict["op"] == "And"

    def test_capture_arithmetic_expression(self):
        """Lambda capture works for arithmetic expressions."""
        schema = {"price": "float64", "quantity": "int64"}
        expr_dict = _lambda_to_expr(lambda r: r.price * r.quantity, schema)

        assert expr_dict["type"] == "BinOp"
        assert expr_dict["op"] == "Mul"

    def test_capture_method_chain(self):
        """Lambda capture works for method chains."""
        schema = {"values": "int64"}
        expr_dict = _lambda_to_expr(lambda r: r.values.rolling(3).mean(), schema)

        assert expr_dict["type"] == "Call"
        assert expr_dict["func"] == "mean"
        assert expr_dict["on"]["type"] == "Call"
        assert expr_dict["on"]["func"] == "rolling"


class TestExpressionSerialization:
    """Test that captured expressions serialize correctly."""

    def test_serialized_expr_has_required_fields(self):
        """Serialized expr has all required fields for Rust deserialization."""
        schema = {"age": "int64"}
        expr_dict = _lambda_to_expr(lambda r: r.age > 18, schema)

        # BinOp should have type, op, left, right
        assert "type" in expr_dict
        assert "op" in expr_dict
        assert "left" in expr_dict
        assert "right" in expr_dict

    def test_serialized_column_expr(self):
        """ColumnExpr serializes with correct structure."""
        schema = {"age": "int64"}
        expr_dict = _lambda_to_expr(lambda r: r.age, schema)

        assert expr_dict["type"] == "Column"
        assert expr_dict["name"] == "age"

    def test_serialized_literal_expr(self):
        """LiteralExpr serializes with correct structure in binary expression."""
        schema = {"age": "int64"}
        # To get a LiteralExpr, we need it to be part of an expression tree
        expr_dict = _lambda_to_expr(lambda r: r.age == 18, schema)

        # The right side should be a serialized LiteralExpr
        assert expr_dict["right"]["type"] == "Literal"
        assert expr_dict["right"]["value"] == 18

    def test_serialized_call_expr_with_args(self):
        """CallExpr with arguments serializes correctly."""
        schema = {"values": "int64"}
        expr_dict = _lambda_to_expr(lambda r: r.values.shift(1), schema)

        assert expr_dict["type"] == "Call"
        assert expr_dict["func"] == "shift"
        assert len(expr_dict["args"]) == 1
        # Args should be serialized
        assert isinstance(expr_dict["args"][0], dict)
        assert expr_dict["args"][0]["type"] == "Literal"

    def test_serialized_nested_operations(self):
        """Nested operations serialize correctly."""
        schema = {"a": "int64", "b": "int64", "c": "int64"}
        expr_dict = _lambda_to_expr(lambda r: (r.a + r.b) > r.c, schema)

        # Root should be BinOp (Gt)
        assert expr_dict["type"] == "BinOp"
        assert expr_dict["op"] == "Gt"
        # Left should be BinOp (Add)
        assert expr_dict["left"]["type"] == "BinOp"
        assert expr_dict["left"]["op"] == "Add"
        # Right should be Column
        assert expr_dict["right"]["type"] == "Column"


class TestExpressionValidation:
    """Test validation during expression capture."""

    def test_invalid_column_raises_error(self):
        """Referencing non-existent column raises AttributeError."""
        schema = {"age": "int64"}

        with pytest.raises(AttributeError) as exc_info:
            _lambda_to_expr(lambda r: r.non_existent > 18, schema)

        assert "non_existent" in str(exc_info.value)

    def test_non_expr_return_raises_error(self):
        """Lambda returning non-Expr value raises TypeError."""
        schema = {"age": "int64"}

        with pytest.raises(TypeError) as exc_info:
            _lambda_to_expr(lambda r: 42, schema)

        assert "Lambda must return an Expr" in str(exc_info.value)

    def test_accessing_wrong_parameter_raises_error(self):
        """Using undefined name in lambda raises NameError."""
        schema = {"age": "int64"}

        # Create a lambda that references an undefined variable
        # This will raise NameError when executed
        bad_lambda = lambda r: eval("x.age > 18")

        with pytest.raises(NameError):
            _lambda_to_expr(bad_lambda, schema)

    def test_empty_schema_rejects_any_column(self):
        """Empty schema means no columns are valid."""
        schema = {}

        with pytest.raises(AttributeError) as exc_info:
            _lambda_to_expr(lambda r: r.any_col > 0, schema)

        assert "not found in schema" in str(exc_info.value)


class TestMultipleColumnExpressions:
    """Test expressions using multiple columns."""

    def test_two_column_comparison(self):
        """Comparing two columns works."""
        schema = {"age": "int64", "min_age": "int64"}
        expr_dict = _lambda_to_expr(lambda r: r.age > r.min_age, schema)

        assert expr_dict["left"]["type"] == "Column"
        assert expr_dict["left"]["name"] == "age"
        assert expr_dict["right"]["type"] == "Column"
        assert expr_dict["right"]["name"] == "min_age"

    def test_three_way_comparison_chain(self):
        """Multiple comparisons connected with logical operators."""
        schema = {"a": "int64", "b": "int64", "c": "int64"}
        expr_dict = _lambda_to_expr(lambda r: (r.a > r.b) & (r.b > r.c), schema)

        assert expr_dict["type"] == "BinOp"
        assert expr_dict["op"] == "And"

    def test_arithmetic_with_multiple_columns(self):
        """Arithmetic operations on multiple columns."""
        schema = {"x": "float64", "y": "float64", "z": "float64"}
        expr_dict = _lambda_to_expr(lambda r: (r.x + r.y) * r.z, schema)

        assert expr_dict["type"] == "BinOp"
        assert expr_dict["op"] == "Mul"

    def test_method_calls_on_different_columns(self):
        """Methods can be called on different columns."""
        schema = {"series1": "int64", "series2": "int64"}
        expr_dict = _lambda_to_expr(lambda r: r.series1.shift(1), schema)

        # Should work fine
        assert expr_dict["type"] == "Call"
        assert expr_dict["func"] == "shift"


class TestComplexDataTypes:
    """Test lambda capture with various data types."""

    def test_float_literal(self):
        """Float literals are captured correctly."""
        schema = {"price": "float64"}
        expr_dict = _lambda_to_expr(lambda r: r.price > 99.99, schema)

        assert expr_dict["right"]["type"] == "Literal"
        assert expr_dict["right"]["value"] == 99.99
        assert expr_dict["right"]["dtype"] == "Float64"

    def test_string_literal(self):
        """String literals are captured correctly."""
        schema = {"name": "string"}
        expr_dict = _lambda_to_expr(lambda r: r.name == "Alice", schema)

        assert expr_dict["right"]["type"] == "Literal"
        assert expr_dict["right"]["value"] == "Alice"
        assert expr_dict["right"]["dtype"] == "String"

    def test_boolean_literal(self):
        """Boolean literals are captured correctly."""
        schema = {"active": "bool"}
        expr_dict = _lambda_to_expr(lambda r: r.active == True, schema)

        assert expr_dict["right"]["type"] == "Literal"
        assert expr_dict["right"]["value"] is True

    def test_negative_number(self):
        """Negative numbers are captured correctly."""
        schema = {"balance": "int64"}
        expr_dict = _lambda_to_expr(lambda r: r.balance > -100, schema)

        assert expr_dict["right"]["type"] == "Literal"
        assert expr_dict["right"]["value"] == -100


class TestLambdaParameterHandling:
    """Test how lambda parameters are handled."""

    def test_lambda_with_r_parameter(self):
        """Standard 'r' parameter works."""
        schema = {"age": "int64"}
        expr_dict = _lambda_to_expr(lambda r: r.age > 18, schema)
        assert expr_dict["type"] == "BinOp"

    def test_lambda_with_different_parameter_name(self):
        """Parameter name 'row' also works."""
        schema = {"age": "int64"}
        expr_dict = _lambda_to_expr(lambda row: row.age > 18, schema)
        assert expr_dict["type"] == "BinOp"

    def test_lambda_with_any_parameter_name(self):
        """Any parameter name works (underscore-prefixed names excluded)."""
        schema = {"age": "int64"}
        expr_dict = _lambda_to_expr(lambda x: x.age > 18, schema)
        assert expr_dict["type"] == "BinOp"


class TestNegationAndLogical:
    """Test negation and logical operations."""

    def test_not_operator(self):
        """NOT operator is captured correctly."""
        schema = {"active": "bool"}
        expr_dict = _lambda_to_expr(lambda r: ~r.active, schema)

        assert expr_dict["type"] == "UnaryOp"
        assert expr_dict["op"] == "Not"

    def test_logical_and(self):
        """Logical AND with & operator."""
        schema = {"a": "bool", "b": "bool"}
        expr_dict = _lambda_to_expr(lambda r: r.a & r.b, schema)

        assert expr_dict["type"] == "BinOp"
        assert expr_dict["op"] == "And"

    def test_logical_or(self):
        """Logical OR with | operator."""
        schema = {"a": "bool", "b": "bool"}
        expr_dict = _lambda_to_expr(lambda r: r.a | r.b, schema)

        assert expr_dict["type"] == "BinOp"
        assert expr_dict["op"] == "Or"

    def test_not_of_comparison(self):
        """NOT of a comparison expression."""
        schema = {"age": "int64"}
        expr_dict = _lambda_to_expr(lambda r: ~(r.age > 18), schema)

        assert expr_dict["type"] == "UnaryOp"
        assert expr_dict["op"] == "Not"


class TestMethodChaining:
    """Test method chaining in expressions."""

    def test_single_method_call(self):
        """Single method call on column."""
        schema = {"values": "int64"}
        expr_dict = _lambda_to_expr(lambda r: r.values.shift(1), schema)

        assert expr_dict["type"] == "Call"
        assert expr_dict["func"] == "shift"
        assert expr_dict["on"]["type"] == "Column"

    def test_two_method_chain(self):
        """Chaining two methods: rolling(3).mean()."""
        schema = {"values": "int64"}
        expr_dict = _lambda_to_expr(lambda r: r.values.rolling(3).mean(), schema)

        # Outer call should be mean
        assert expr_dict["type"] == "Call"
        assert expr_dict["func"] == "mean"
        # Inner call should be rolling
        assert expr_dict["on"]["type"] == "Call"
        assert expr_dict["on"]["func"] == "rolling"
        # Base should be column
        assert expr_dict["on"]["on"]["type"] == "Column"

    def test_three_method_chain(self):
        """Chaining three methods."""
        schema = {"values": "int64"}
        expr_dict = _lambda_to_expr(
            lambda r: r.values.shift(1).rolling(3).sum(), schema
        )

        # Verify the chain: values.shift(1).rolling(3).sum()
        assert expr_dict["func"] == "sum"
        assert expr_dict["on"]["func"] == "rolling"
        assert expr_dict["on"]["on"]["func"] == "shift"
        assert expr_dict["on"]["on"]["on"]["type"] == "Column"

    def test_method_with_kwargs(self):
        """Methods with keyword arguments."""
        schema = {"values": "int64"}
        expr_dict = _lambda_to_expr(lambda r: r.values.rolling(3, center=True), schema)

        assert expr_dict["type"] == "Call"
        assert expr_dict["func"] == "rolling"
        assert len(expr_dict["args"]) == 1
        assert "center" in expr_dict["kwargs"]
        assert expr_dict["kwargs"]["center"]["value"] is True


class TestExpressionReproducibility:
    """Test that expressions are reproducible and deterministic."""

    def test_same_lambda_same_result(self):
        """Same lambda produces same serialized expression."""
        schema = {"age": "int64"}

        expr1 = _lambda_to_expr(lambda r: r.age > 18, schema)
        expr2 = _lambda_to_expr(lambda r: r.age > 18, schema)

        assert expr1 == expr2

    def test_different_lambdas_different_result(self):
        """Different lambdas produce different expressions."""
        schema = {"age": "int64"}

        expr1 = _lambda_to_expr(lambda r: r.age > 18, schema)
        expr2 = _lambda_to_expr(lambda r: r.age > 21, schema)

        assert expr1 != expr2

    def test_order_preserved_in_operations(self):
        """Operation order matters in expression."""
        schema = {"a": "int64", "b": "int64"}

        expr1 = _lambda_to_expr(lambda r: r.a - r.b, schema)
        expr2 = _lambda_to_expr(lambda r: r.b - r.a, schema)

        # These should be different
        assert expr1["left"]["name"] == "a"
        assert expr2["left"]["name"] == "b"
