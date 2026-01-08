"""
Unit tests for Expr classes (ColumnExpr, LiteralExpr, BinOpExpr, UnaryOpExpr, CallExpr).

Tests verify:
- Serialization format (nested dicts)
- Operator overloads (+ - * / > < == & | ~)
- Type inference for literals
- Chaining and composition of expressions
"""

import pytest
from ltseq.expr import (
    Expr,
    ColumnExpr,
    LiteralExpr,
    BinOpExpr,
    UnaryOpExpr,
    CallExpr,
)


class TestColumnExpr:
    """Tests for ColumnExpr"""

    def test_column_expr_serialize(self):
        """ColumnExpr serializes to dict with type and name"""
        expr = ColumnExpr("age")
        serialized = expr.serialize()
        assert serialized == {"type": "Column", "name": "age"}

    def test_column_expr_with_operators(self):
        """ColumnExpr works with operators"""
        col = ColumnExpr("age")
        expr = col + 5
        assert isinstance(expr, BinOpExpr)
        assert expr.op == "Add"


class TestLiteralExpr:
    """Tests for LiteralExpr"""

    def test_literal_int(self):
        """LiteralExpr for int infers Int64 type"""
        expr = LiteralExpr(42)
        serialized = expr.serialize()
        assert serialized["type"] == "Literal"
        assert serialized["value"] == 42
        assert serialized["dtype"] == "Int64"

    def test_literal_float(self):
        """LiteralExpr for float infers Float64 type"""
        expr = LiteralExpr(3.14)
        serialized = expr.serialize()
        assert serialized["dtype"] == "Float64"

    def test_literal_string(self):
        """LiteralExpr for string infers String type"""
        expr = LiteralExpr("hello")
        serialized = expr.serialize()
        assert serialized["dtype"] == "String"

    def test_literal_bool(self):
        """LiteralExpr for bool infers Boolean type"""
        expr = LiteralExpr(True)
        serialized = expr.serialize()
        assert serialized["dtype"] == "Boolean"

    def test_literal_none(self):
        """LiteralExpr for None infers Null type"""
        expr = LiteralExpr(None)
        serialized = expr.serialize()
        assert serialized["dtype"] == "Null"


class TestBinOpExpr:
    """Tests for BinOpExpr"""

    def test_binop_add(self):
        """ColumnExpr + literal creates BinOpExpr with Add"""
        col = ColumnExpr("age")
        expr = col + 5
        serialized = expr.serialize()
        assert serialized["type"] == "BinOp"
        assert serialized["op"] == "Add"
        assert serialized["left"]["type"] == "Column"
        assert serialized["right"]["type"] == "Literal"

    def test_binop_sub(self):
        """ColumnExpr - literal creates BinOpExpr with Sub"""
        col = ColumnExpr("price")
        expr = col - 10
        assert expr.serialize()["op"] == "Sub"

    def test_binop_mul(self):
        """ColumnExpr * literal creates BinOpExpr with Mul"""
        col = ColumnExpr("qty")
        expr = col * 2
        assert expr.serialize()["op"] == "Mul"

    def test_binop_div(self):
        """ColumnExpr / literal creates BinOpExpr with Div"""
        col = ColumnExpr("total")
        expr = col / 3
        assert expr.serialize()["op"] == "Div"

    def test_binop_floordiv(self):
        """ColumnExpr // literal creates BinOpExpr with FloorDiv"""
        col = ColumnExpr("items")
        expr = col // 4
        assert expr.serialize()["op"] == "FloorDiv"

    def test_binop_mod(self):
        """ColumnExpr % literal creates BinOpExpr with Mod"""
        col = ColumnExpr("num")
        expr = col % 5
        assert expr.serialize()["op"] == "Mod"

    def test_binop_eq(self):
        """ColumnExpr == literal creates BinOpExpr with Eq"""
        col = ColumnExpr("status")
        expr = col == "active"
        assert expr.serialize()["op"] == "Eq"

    def test_binop_ne(self):
        """ColumnExpr != literal creates BinOpExpr with Ne"""
        col = ColumnExpr("status")
        expr = col != "inactive"
        assert expr.serialize()["op"] == "Ne"

    def test_binop_lt(self):
        """ColumnExpr < literal creates BinOpExpr with Lt"""
        col = ColumnExpr("age")
        expr = col < 18
        assert expr.serialize()["op"] == "Lt"

    def test_binop_le(self):
        """ColumnExpr <= literal creates BinOpExpr with Le"""
        col = ColumnExpr("age")
        expr = col <= 65
        assert expr.serialize()["op"] == "Le"

    def test_binop_gt(self):
        """ColumnExpr > literal creates BinOpExpr with Gt"""
        col = ColumnExpr("age")
        expr = col > 18
        assert expr.serialize()["op"] == "Gt"

    def test_binop_ge(self):
        """ColumnExpr >= literal creates BinOpExpr with Ge"""
        col = ColumnExpr("age")
        expr = col >= 18
        assert expr.serialize()["op"] == "Ge"

    def test_binop_and(self):
        """ColumnExpr & ColumnExpr creates BinOpExpr with And"""
        col1 = ColumnExpr("is_active")
        col2 = ColumnExpr("is_verified")
        expr = col1 & col2
        assert expr.serialize()["op"] == "And"

    def test_binop_or(self):
        """ColumnExpr | ColumnExpr creates BinOpExpr with Or"""
        col1 = ColumnExpr("is_admin")
        col2 = ColumnExpr("is_moderator")
        expr = col1 | col2
        assert expr.serialize()["op"] == "Or"

    def test_binop_reverse_add(self):
        """literal + ColumnExpr creates BinOpExpr (reversed)"""
        col = ColumnExpr("x")
        expr = 5 + col
        serialized = expr.serialize()
        assert serialized["op"] == "Add"
        # Left should be Literal, right should be Column
        assert serialized["left"]["type"] == "Literal"
        assert serialized["right"]["type"] == "Column"

    def test_binop_reverse_sub(self):
        """literal - ColumnExpr creates BinOpExpr (reversed)"""
        col = ColumnExpr("y")
        expr = 100 - col
        assert expr.serialize()["op"] == "Sub"

    def test_binop_nested(self):
        """(a + b) > c creates nested BinOpExpr"""
        col1 = ColumnExpr("age")
        col2 = ColumnExpr("min_age")
        expr = (col1 + 5) > col2
        serialized = expr.serialize()
        assert serialized["op"] == "Gt"
        assert serialized["left"]["op"] == "Add"


class TestUnaryOpExpr:
    """Tests for UnaryOpExpr"""

    def test_unary_not(self):
        """~ColumnExpr creates UnaryOpExpr with Not"""
        col = ColumnExpr("is_deleted")
        expr = ~col
        serialized = expr.serialize()
        assert serialized["type"] == "UnaryOp"
        assert serialized["op"] == "Not"
        assert serialized["operand"]["type"] == "Column"

    def test_unary_not_comparison(self):
        """~(col > 5) creates UnaryOpExpr(Not(BinOpExpr))"""
        col = ColumnExpr("value")
        expr = ~(col > 5)
        serialized = expr.serialize()
        assert serialized["op"] == "Not"
        assert serialized["operand"]["op"] == "Gt"


class TestCallExpr:
    """Tests for CallExpr"""

    def test_call_basic_method(self):
        """col.shift(1) creates CallExpr"""
        col = ColumnExpr("price")
        expr = col.shift(1)
        serialized = expr.serialize()
        assert serialized["type"] == "Call"
        assert serialized["func"] == "shift"
        assert len(serialized["args"]) == 1
        assert serialized["args"][0]["type"] == "Literal"
        assert serialized["args"][0]["value"] == 1
        assert serialized["on"]["type"] == "Column"

    def test_call_with_kwargs(self):
        """col.rolling(window=3) creates CallExpr with kwargs"""
        col = ColumnExpr("price")
        expr = col.rolling(window=3)
        serialized = expr.serialize()
        assert serialized["func"] == "rolling"
        assert "window" in serialized["kwargs"]

    def test_call_chained_rolling_mean(self):
        """col.rolling(3).mean() creates nested CallExpr"""
        col = ColumnExpr("price")
        expr = col.rolling(3).mean()
        serialized = expr.serialize()
        assert serialized["type"] == "Call"
        assert serialized["func"] == "mean"
        # The 'on' should be another CallExpr
        on_serialized = serialized["on"]
        assert on_serialized["type"] == "Call"
        assert on_serialized["func"] == "rolling"
        # And that CallExpr's 'on' should be a Column
        assert on_serialized["on"]["type"] == "Column"

    def test_call_no_args(self):
        """col.sum() creates CallExpr with no args"""
        col = ColumnExpr("amount")
        expr = col.sum()
        serialized = expr.serialize()
        assert serialized["func"] == "sum"
        assert serialized["args"] == []

    def test_call_multiple_args(self):
        """col.func(a, b, c) creates CallExpr with multiple args"""
        col = ColumnExpr("value")
        expr = col.clip(0, 100)
        serialized = expr.serialize()
        assert len(serialized["args"]) == 2


class TestComplexExpressions:
    """Tests for complex nested expressions"""

    def test_complex_logical_and_arithmetic(self):
        """(age + 5 > 18) & (salary < 100000)"""
        age = ColumnExpr("age")
        salary = ColumnExpr("salary")
        expr = ((age + 5) > 18) & (salary < 100000)
        serialized = expr.serialize()
        assert serialized["op"] == "And"
        assert serialized["left"]["op"] == "Gt"
        assert serialized["left"]["left"]["op"] == "Add"
        assert serialized["right"]["op"] == "Lt"

    def test_multiple_ands_ors(self):
        """(a & b) | (c & d)"""
        a = ColumnExpr("a")
        b = ColumnExpr("b")
        c = ColumnExpr("c")
        d = ColumnExpr("d")
        expr = (a & b) | (c & d)
        serialized = expr.serialize()
        assert serialized["op"] == "Or"
        assert serialized["left"]["op"] == "And"
        assert serialized["right"]["op"] == "And"

    def test_method_and_operators(self):
        """col.shift(1) > col.shift(2)"""
        col = ColumnExpr("price")
        expr = col.shift(1) > col.shift(2)
        serialized = expr.serialize()
        assert serialized["op"] == "Gt"
        assert serialized["left"]["func"] == "shift"
        assert serialized["right"]["func"] == "shift"

    def test_arithmetic_chain(self):
        """(x + y) * (z - 1)"""
        x = ColumnExpr("x")
        y = ColumnExpr("y")
        z = ColumnExpr("z")
        expr = (x + y) * (z - 1)
        serialized = expr.serialize()
        assert serialized["op"] == "Mul"
        assert serialized["left"]["op"] == "Add"
        assert serialized["right"]["op"] == "Sub"


class TestNullHandling:
    """Tests for null handling expressions"""

    def test_is_null(self):
        """col.is_null() creates CallExpr with is_null"""
        col = ColumnExpr("email")
        expr = col.is_null()
        serialized = expr.serialize()
        assert serialized["type"] == "Call"
        assert serialized["func"] == "is_null"
        assert serialized["on"]["type"] == "Column"
        assert serialized["args"] == []

    def test_is_not_null(self):
        """col.is_not_null() creates CallExpr with is_not_null"""
        col = ColumnExpr("email")
        expr = col.is_not_null()
        serialized = expr.serialize()
        assert serialized["type"] == "Call"
        assert serialized["func"] == "is_not_null"
        assert serialized["on"]["type"] == "Column"

    def test_fill_null(self):
        """col.fill_null(default) creates CallExpr with fill_null"""
        col = ColumnExpr("name")
        expr = col.fill_null("unknown")
        serialized = expr.serialize()
        assert serialized["func"] == "fill_null"
        assert serialized["args"][0]["value"] == "unknown"


class TestStringAccessor:
    """Tests for StringAccessor methods"""

    def test_regex_match(self):
        """col.s.regex_match(pattern) creates CallExpr"""
        col = ColumnExpr("email")
        expr = col.s.regex_match(r"^[a-z]+@")
        serialized = expr.serialize()
        assert serialized["type"] == "Call"
        assert serialized["func"] == "str_regex_match"
        assert serialized["args"][0]["value"] == r"^[a-z]+@"
        assert serialized["on"]["type"] == "Column"

    def test_contains(self):
        """col.s.contains(pattern) creates CallExpr"""
        col = ColumnExpr("name")
        expr = col.s.contains("test")
        serialized = expr.serialize()
        assert serialized["func"] == "str_contains"

    def test_starts_with(self):
        """col.s.starts_with(prefix) creates CallExpr"""
        col = ColumnExpr("url")
        expr = col.s.starts_with("https://")
        serialized = expr.serialize()
        assert serialized["func"] == "str_starts_with"

    def test_ends_with(self):
        """col.s.ends_with(suffix) creates CallExpr"""
        col = ColumnExpr("file")
        expr = col.s.ends_with(".csv")
        serialized = expr.serialize()
        assert serialized["func"] == "str_ends_with"

    def test_lower(self):
        """col.s.lower() creates CallExpr"""
        col = ColumnExpr("name")
        expr = col.s.lower()
        serialized = expr.serialize()
        assert serialized["func"] == "str_lower"

    def test_upper(self):
        """col.s.upper() creates CallExpr"""
        col = ColumnExpr("name")
        expr = col.s.upper()
        serialized = expr.serialize()
        assert serialized["func"] == "str_upper"

    def test_strip(self):
        """col.s.strip() creates CallExpr"""
        col = ColumnExpr("text")
        expr = col.s.strip()
        serialized = expr.serialize()
        assert serialized["func"] == "str_strip"

    def test_len(self):
        """col.s.len() creates CallExpr"""
        col = ColumnExpr("text")
        expr = col.s.len()
        serialized = expr.serialize()
        assert serialized["func"] == "str_len"

    def test_slice(self):
        """col.s.slice(start, length) creates CallExpr"""
        col = ColumnExpr("text")
        expr = col.s.slice(0, 5)
        serialized = expr.serialize()
        assert serialized["func"] == "str_slice"
        assert serialized["args"][0]["value"] == 0
        assert serialized["args"][1]["value"] == 5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
