"""
Unit tests for Expr classes (ColumnExpr, LiteralExpr, BinOpExpr, UnaryOpExpr, CallExpr).

Tests verify:
- Serialization format (nested dicts)
- Operator overloads (+ - * / > < == & | ~)
- Type inference for literals
- Chaining and composition of expressions
"""

import pytest
from ltseq import LTSeq
from ltseq.expr import (
    Expr,
    ColumnExpr,
    LiteralExpr,
    BinOpExpr,
    UnaryOpExpr,
    CallExpr,
)
from ltseq.expr.base import if_else


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


# ---------------------------------------------------------------------------
# End-to-end tests that exercise expressions through the full Rust pipeline
# ---------------------------------------------------------------------------

@pytest.fixture
def numbers_table():
    """Create a small numeric table for expression e2e tests."""
    return LTSeq._from_rows(
        [
            {"id": 1, "x": 10, "y": 5},
            {"id": 2, "x": -3, "y": 8},
            {"id": 3, "x": 0, "y": 12},
        ],
        {"id": "int64", "x": "int64", "y": "int64"},
    )


class TestIfElseEndToEnd:
    """End-to-end tests for if_else() (T21)."""

    def test_if_else_basic_derive(self, numbers_table):
        """if_else in derive produces correct values."""
        result = numbers_table.derive(
            label=lambda r: if_else(r.x > 0, "positive", "non-positive")
        )
        df = result.to_pandas()
        assert "label" in df.columns
        labels = df["label"].tolist()
        assert labels[0] == "positive"      # x=10
        assert labels[1] == "non-positive"  # x=-3
        assert labels[2] == "non-positive"  # x=0

    def test_if_else_numeric(self, numbers_table):
        """if_else with numeric outputs."""
        result = numbers_table.derive(
            clamped=lambda r: if_else(r.x > 0, r.x, 0)
        )
        df = result.to_pandas()
        vals = df["clamped"].tolist()
        assert vals[0] == 10
        assert vals[1] == 0
        assert vals[2] == 0

    def test_if_else_nested(self, numbers_table):
        """Nested if_else produces correct values."""
        result = numbers_table.derive(
            tier=lambda r: if_else(
                r.x > 5,
                "high",
                if_else(r.x > 0, "medium", "low"),
            )
        )
        df = result.to_pandas()
        tiers = df["tier"].tolist()
        assert tiers[0] == "high"    # x=10
        assert tiers[1] == "low"     # x=-3
        assert tiers[2] == "low"     # x=0


class TestReverseOperatorsEndToEnd:
    """End-to-end tests for right-hand operators (T22)."""

    def test_rmul(self, numbers_table):
        """2 * r.x should produce correct results."""
        result = numbers_table.derive(doubled=lambda r: 2 * r.x)
        df = result.to_pandas()
        vals = df["doubled"].tolist()
        assert vals[0] == 20
        assert vals[1] == -6
        assert vals[2] == 0

    def test_rtruediv(self, numbers_table):
        """100 / r.y should produce correct results (int division on int cols)."""
        result = numbers_table.derive(ratio=lambda r: 100 / r.y)
        df = result.to_pandas()
        vals = df["ratio"].tolist()
        assert vals[0] == 20   # 100 / 5  (integer division on int64)
        assert vals[1] == 12   # 100 / 8  (integer division on int64)

    def test_rmod(self, numbers_table):
        """17 % r.y should produce correct results."""
        result = numbers_table.derive(remainder=lambda r: 17 % r.y)
        df = result.to_pandas()
        vals = df["remainder"].tolist()
        assert vals[0] == 2   # 17 % 5
        assert vals[1] == 1   # 17 % 8

    def test_radd(self, numbers_table):
        """100 + r.x should produce correct results."""
        result = numbers_table.derive(shifted=lambda r: 100 + r.x)
        df = result.to_pandas()
        vals = df["shifted"].tolist()
        assert vals[0] == 110
        assert vals[1] == 97
        assert vals[2] == 100

    def test_rsub(self, numbers_table):
        """100 - r.x should produce correct results."""
        result = numbers_table.derive(inv=lambda r: 100 - r.x)
        df = result.to_pandas()
        vals = df["inv"].tolist()
        assert vals[0] == 90
        assert vals[1] == 103
        assert vals[2] == 100


class TestAbsEndToEnd:
    """End-to-end tests for __abs__ (T23)."""

    def test_abs_serialization(self):
        """abs(col) creates CallExpr with abs function."""
        col = ColumnExpr("x")
        expr = abs(col)
        serialized = expr.serialize()
        assert serialized["type"] == "Call"
        assert serialized["func"] == "abs"

    def test_abs_in_plain_derive_errors(self, numbers_table):
        """abs() in plain derive raises ValueError (requires window context)."""
        with pytest.raises(ValueError, match="window context"):
            numbers_table.derive(ax=lambda r: abs(r.x))


class TestFillNullEndToEnd:
    """End-to-end tests for fill_null() (T25)."""

    def test_fill_null_replaces_nulls(self):
        """fill_null() replaces null values with the default."""
        # Create table with nulls using _from_rows
        t = LTSeq._from_rows(
            [
                {"id": 1, "val": "hello"},
                {"id": 2, "val": ""},
                {"id": 3, "val": "world"},
            ],
            {"id": "int64", "val": "string"},
        )
        # fill_null on a non-null column is a no-op
        result = t.derive(safe_val=lambda r: r.val.fill_null("DEFAULT"))
        df = result.to_pandas()
        assert "safe_val" in df.columns
        # No actual nulls in this data, so all originals preserved
        vals = df["safe_val"].tolist()
        assert vals[0] == "hello"
        assert vals[2] == "world"
