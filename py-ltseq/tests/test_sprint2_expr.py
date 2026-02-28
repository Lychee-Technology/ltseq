"""
Sprint 2 — Expression Completeness: End-to-end tests.

Tests for new expression methods and functions added in Sprint 2:
- is_in() — membership test
- between() — range check (pure Python sugar)
- cast() — type coercion
- abs() — absolute value (method form)
- round() — rounding to N decimal places
- floor() — floor rounding
- ceil() — ceiling rounding
- pct_change() — percentage change from previous row
- coalesce() — first non-null value (standalone function)
"""

import math
import pytest
from ltseq import LTSeq, coalesce
from ltseq.expr import ColumnExpr, LiteralExpr


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def numbers_table():
    """Table with mixed positive/negative/zero integers and floats."""
    return LTSeq._from_rows(
        [
            {"id": 1, "x": 10, "y": 5, "val": 3.14},
            {"id": 2, "x": -3, "y": 8, "val": 2.718},
            {"id": 3, "x": 0, "y": 12, "val": -1.5},
            {"id": 4, "x": 7, "y": -2, "val": 9.999},
        ],
        {"id": "int64", "x": "int64", "y": "int64", "val": "float64"},
    )


@pytest.fixture
def string_table():
    """Table with string columns for is_in and cast tests."""
    return LTSeq._from_rows(
        [
            {"id": 1, "status": "active", "score": 85},
            {"id": 2, "status": "inactive", "score": 42},
            {"id": 3, "status": "pending", "score": 73},
            {"id": 4, "status": "active", "score": 91},
            {"id": 5, "status": "review", "score": 60},
        ],
        {"id": "int64", "status": "string", "score": "int64"},
    )


@pytest.fixture
def time_series_table():
    """Table for pct_change tests — ordered data with known values."""
    return LTSeq._from_rows(
        [
            {"day": 1, "price": 100},
            {"day": 2, "price": 110},
            {"day": 3, "price": 99},
            {"day": 4, "price": 120},
        ],
        {"day": "int64", "price": "int64"},
    )


# ============================================================================
# is_in
# ============================================================================


class TestIsIn:
    """Tests for Expr.is_in(values)."""

    def test_is_in_serialization(self):
        """is_in serializes to a Call with correct structure."""
        col = ColumnExpr("status")
        expr = col.is_in(["a", "b"])
        s = expr.serialize()
        assert s["type"] == "Call"
        assert s["func"] == "is_in"
        assert len(s["args"]) == 2

    def test_is_in_filter_strings(self, string_table):
        """Filter rows where status is in a set of values."""
        result = string_table.filter(
            lambda r: r.status.is_in(["active", "pending"])
        )
        df = result.to_pandas()
        assert len(df) == 3  # id 1, 3, 4
        assert set(df["status"].tolist()) == {"active", "pending"}

    def test_is_in_filter_integers(self, numbers_table):
        """Filter rows where integer column is in a set."""
        result = numbers_table.filter(lambda r: r.x.is_in([10, 0]))
        df = result.to_pandas()
        assert len(df) == 2
        assert set(df["x"].tolist()) == {10, 0}

    def test_is_in_no_matches(self, string_table):
        """is_in returns no rows when no values match."""
        result = string_table.filter(
            lambda r: r.status.is_in(["deleted", "archived"])
        )
        df = result.to_pandas()
        assert len(df) == 0

    def test_is_in_all_match(self, numbers_table):
        """is_in returns all rows when all values are listed."""
        result = numbers_table.filter(
            lambda r: r.x.is_in([10, -3, 0, 7])
        )
        df = result.to_pandas()
        assert len(df) == 4

    def test_is_in_derive_boolean(self, string_table):
        """is_in in derive produces a boolean column."""
        result = string_table.derive(
            is_actionable=lambda r: r.status.is_in(["active", "review"])
        )
        df = result.to_pandas()
        assert "is_actionable" in df.columns
        expected = [True, False, False, True, True]
        assert df["is_actionable"].tolist() == expected


# ============================================================================
# between
# ============================================================================


class TestBetween:
    """Tests for Expr.between(low, high) — pure Python sugar."""

    def test_between_serialization(self):
        """between() expands to (>= low) & (<= high) — no special Call."""
        col = ColumnExpr("x")
        expr = col.between(5, 10)
        s = expr.serialize()
        # Should be BinOp(And, ...)
        assert s["type"] == "BinOp"
        assert s["op"] == "And"

    def test_between_filter(self, numbers_table):
        """Filter rows where x is between 0 and 10 (inclusive)."""
        result = numbers_table.filter(lambda r: r.x.between(0, 10))
        df = result.to_pandas()
        assert len(df) == 3  # x=10, x=0, x=7
        assert set(df["x"].tolist()) == {10, 0, 7}

    def test_between_excludes_outside(self, string_table):
        """Values outside range are excluded."""
        result = string_table.filter(lambda r: r.score.between(50, 80))
        df = result.to_pandas()
        assert len(df) == 2  # score 73 and 60
        for s in df["score"].tolist():
            assert 50 <= s <= 80

    def test_between_inclusive_boundaries(self, numbers_table):
        """Both boundaries are inclusive."""
        result = numbers_table.filter(lambda r: r.x.between(-3, 7))
        df = result.to_pandas()
        # x=-3, x=0, x=7 are all included
        assert len(df) == 3
        assert -3 in df["x"].tolist()
        assert 7 in df["x"].tolist()


# ============================================================================
# cast
# ============================================================================


class TestCast:
    """Tests for Expr.cast(dtype)."""

    def test_cast_serialization(self):
        """cast() serializes to a Call with dtype string arg."""
        col = ColumnExpr("x")
        expr = col.cast("float64")
        s = expr.serialize()
        assert s["type"] == "Call"
        assert s["func"] == "cast"
        assert s["args"][0]["value"] == "float64"

    def test_cast_int_to_float(self, numbers_table):
        """Casting integer to float64 produces floats."""
        result = numbers_table.derive(x_float=lambda r: r.x.cast("float64"))
        df = result.to_pandas()
        assert "x_float" in df.columns
        # Values should be the same numerically
        assert df["x_float"].tolist() == [10.0, -3.0, 0.0, 7.0]

    def test_cast_float_to_int(self, numbers_table):
        """Casting float to int64 truncates toward zero."""
        result = numbers_table.derive(val_int=lambda r: r.val.cast("int64"))
        df = result.to_pandas()
        assert "val_int" in df.columns
        # 3.14 -> 3, 2.718 -> 2, -1.5 -> -1, 9.999 -> 9
        vals = df["val_int"].tolist()
        assert vals[0] == 3
        assert vals[1] == 2
        assert vals[2] == -1
        assert vals[3] == 9

    def test_cast_int_to_string(self, numbers_table):
        """Casting integer to utf8/string produces string representations."""
        result = numbers_table.derive(id_str=lambda r: r.id.cast("utf8"))
        df = result.to_pandas()
        assert df["id_str"].tolist() == ["1", "2", "3", "4"]

    def test_cast_int_to_bool(self, numbers_table):
        """Casting integer to boolean: 0 is False, non-zero is True."""
        result = numbers_table.derive(x_bool=lambda r: r.x.cast("bool"))
        df = result.to_pandas()
        vals = df["x_bool"].tolist()
        assert vals[0] is True   # x=10
        assert vals[1] is True   # x=-3
        assert vals[2] is False  # x=0
        assert vals[3] is True   # x=7


# ============================================================================
# abs (method form)
# ============================================================================


class TestAbsMethod:
    """Tests for Expr.abs() method (in addition to __abs__ builtin)."""

    def test_abs_method_serialization(self):
        """expr.abs() serializes with func=abs and on=expr."""
        col = ColumnExpr("x")
        expr = col.abs()
        s = expr.serialize()
        assert s["type"] == "Call"
        assert s["func"] == "abs"
        assert s["on"]["type"] == "Column"
        assert s["on"]["name"] == "x"

    def test_abs_method_derive(self, numbers_table):
        """expr.abs() in derive produces absolute values."""
        result = numbers_table.derive(abs_x=lambda r: r.x.abs())
        df = result.to_pandas()
        assert df["abs_x"].tolist() == [10, 3, 0, 7]

    def test_abs_on_expression(self, numbers_table):
        """abs() on computed expression: abs(x - y)."""
        result = numbers_table.derive(
            diff_abs=lambda r: (r.x - r.y).abs()
        )
        df = result.to_pandas()
        # x-y: 10-5=5, -3-8=-11, 0-12=-12, 7-(-2)=9
        assert df["diff_abs"].tolist() == [5, 11, 12, 9]

    def test_abs_builtin_still_works(self, numbers_table):
        """Built-in abs() still works via __abs__."""
        result = numbers_table.derive(ax=lambda r: abs(r.x))
        df = result.to_pandas()
        assert df["ax"].tolist() == [10, 3, 0, 7]


# ============================================================================
# round
# ============================================================================


class TestRound:
    """Tests for Expr.round(decimals)."""

    def test_round_serialization(self):
        """round() serializes correctly."""
        col = ColumnExpr("val")
        expr = col.round(2)
        s = expr.serialize()
        assert s["type"] == "Call"
        assert s["func"] == "round"

    def test_round_default_zero_decimals(self, numbers_table):
        """round() with no args rounds to integers."""
        result = numbers_table.derive(rounded=lambda r: r.val.round())
        df = result.to_pandas()
        vals = df["rounded"].tolist()
        assert vals[0] == 3.0   # 3.14 -> 3
        assert vals[1] == 3.0   # 2.718 -> 3
        assert vals[2] == -2.0  # -1.5 -> -2 (round half away from zero varies)
        assert vals[3] == 10.0  # 9.999 -> 10

    def test_round_two_decimals(self, numbers_table):
        """round(2) keeps 2 decimal places."""
        result = numbers_table.derive(rounded=lambda r: r.val.round(2))
        df = result.to_pandas()
        vals = df["rounded"].tolist()
        assert vals[0] == 3.14
        assert vals[1] == 2.72
        assert vals[2] == -1.5
        assert vals[3] == 10.0

    def test_round_one_decimal(self, numbers_table):
        """round(1) keeps 1 decimal place."""
        result = numbers_table.derive(rounded=lambda r: r.val.round(1))
        df = result.to_pandas()
        vals = df["rounded"].tolist()
        assert vals[0] == 3.1   # 3.14 -> 3.1
        assert vals[3] == 10.0  # 9.999 -> 10.0


# ============================================================================
# floor
# ============================================================================


class TestFloor:
    """Tests for Expr.floor()."""

    def test_floor_serialization(self):
        """floor() serializes correctly."""
        col = ColumnExpr("val")
        expr = col.floor()
        s = expr.serialize()
        assert s["type"] == "Call"
        assert s["func"] == "floor"

    def test_floor_positive(self, numbers_table):
        """floor() on positive floats rounds down."""
        result = numbers_table.derive(floored=lambda r: r.val.floor())
        df = result.to_pandas()
        vals = df["floored"].tolist()
        assert vals[0] == 3.0   # floor(3.14) = 3
        assert vals[1] == 2.0   # floor(2.718) = 2
        assert vals[3] == 9.0   # floor(9.999) = 9

    def test_floor_negative(self, numbers_table):
        """floor() on negative floats rounds toward negative infinity."""
        result = numbers_table.derive(floored=lambda r: r.val.floor())
        df = result.to_pandas()
        assert df["floored"].tolist()[2] == -2.0  # floor(-1.5) = -2


# ============================================================================
# ceil
# ============================================================================


class TestCeil:
    """Tests for Expr.ceil()."""

    def test_ceil_serialization(self):
        """ceil() serializes correctly."""
        col = ColumnExpr("val")
        expr = col.ceil()
        s = expr.serialize()
        assert s["type"] == "Call"
        assert s["func"] == "ceil"

    def test_ceil_positive(self, numbers_table):
        """ceil() on positive floats rounds up."""
        result = numbers_table.derive(ceiled=lambda r: r.val.ceil())
        df = result.to_pandas()
        vals = df["ceiled"].tolist()
        assert vals[0] == 4.0   # ceil(3.14) = 4
        assert vals[1] == 3.0   # ceil(2.718) = 3
        assert vals[3] == 10.0  # ceil(9.999) = 10

    def test_ceil_negative(self, numbers_table):
        """ceil() on negative floats rounds toward zero."""
        result = numbers_table.derive(ceiled=lambda r: r.val.ceil())
        df = result.to_pandas()
        assert df["ceiled"].tolist()[2] == -1.0  # ceil(-1.5) = -1


# ============================================================================
# coalesce (standalone function)
# ============================================================================


class TestCoalesce:
    """Tests for coalesce() standalone function."""

    def test_coalesce_serialization(self):
        """coalesce() serializes to Call with func=coalesce."""
        from ltseq.expr import ColumnExpr
        expr = coalesce(ColumnExpr("a"), ColumnExpr("b"), "fallback")
        s = expr.serialize()
        assert s["type"] == "Call"
        assert s["func"] == "coalesce"
        assert len(s["args"]) == 3

    def test_coalesce_with_non_null_values(self, numbers_table):
        """coalesce on non-null columns returns the first value."""
        result = numbers_table.derive(
            first=lambda r: coalesce(r.x, r.y)
        )
        df = result.to_pandas()
        # When all values are non-null, coalesce returns the first arg
        assert df["first"].tolist() == [10, -3, 0, 7]

    def test_coalesce_with_literal_fallback(self):
        """coalesce falls back to literal when column is null."""
        t = LTSeq._from_rows(
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": None},
                {"id": 3, "name": "Charlie"},
            ],
            {"id": "int64", "name": "string"},
        )
        # coalesce replaces null with the fallback literal
        result = t.derive(safe=lambda r: coalesce(r.name, "Unknown"))
        df = result.to_pandas()
        assert df["safe"].tolist() == ["Alice", "Unknown", "Charlie"]

    def test_coalesce_importable_from_ltseq(self):
        """coalesce is importable from the top-level ltseq module."""
        from ltseq import coalesce as c
        assert callable(c)


# ============================================================================
# pct_change (pure Python sugar over shift)
# ============================================================================


class TestPctChange:
    """Tests for Expr.pct_change() — requires sorted data."""

    def test_pct_change_serialization(self):
        """pct_change() expands to (self - self.shift(1)) / self.shift(1)."""
        col = ColumnExpr("price")
        expr = col.pct_change()
        s = expr.serialize()
        # Should be BinOp(Div, ...)
        assert s["type"] == "BinOp"
        assert s["op"] == "Div"

    def test_pct_change_values(self, time_series_table):
        """pct_change produces correct percentage changes."""
        result = (
            time_series_table
            .sort("day")
            .derive(pct=lambda r: r.price.cast("float64").pct_change())
        )
        df = result.to_pandas()
        assert "pct" in df.columns
        vals = df["pct"].tolist()
        # First row should be null (no previous value)
        assert vals[0] is None or math.isnan(vals[0])
        # 100 -> 110: +10%
        assert abs(vals[1] - 0.1) < 0.001
        # 110 -> 99: -10%
        assert abs(vals[2] - (-0.1)) < 0.001
        # 99 -> 120: ~+21.2%
        assert abs(vals[3] - (21.0/99.0)) < 0.001


# ============================================================================
# Combined / chaining tests
# ============================================================================


class TestExpressionCombinations:
    """Test combining new expressions with existing ones."""

    def test_filter_between_then_derive_round(self, numbers_table):
        """Chain between filter with round derive."""
        result = (
            numbers_table
            .filter(lambda r: r.x.between(-5, 10))
            .derive(rounded_val=lambda r: r.val.round(1))
        )
        df = result.to_pandas()
        assert len(df) == 4  # all rows match between(-5, 10)
        assert "rounded_val" in df.columns

    def test_is_in_and_abs_combined(self, numbers_table):
        """Use is_in in filter, then abs in derive."""
        result = (
            numbers_table
            .filter(lambda r: r.x.is_in([-3, 7]))
            .derive(abs_x=lambda r: r.x.abs())
        )
        df = result.to_pandas()
        assert len(df) == 2
        assert set(df["abs_x"].tolist()) == {3, 7}

    def test_cast_then_ceil(self, numbers_table):
        """Cast int to float then apply ceil."""
        result = numbers_table.derive(
            result=lambda r: r.x.cast("float64").ceil()
        )
        df = result.to_pandas()
        # ceil on already-integer values cast to float should be the same
        assert df["result"].tolist() == [10.0, -3.0, 0.0, 7.0]

    def test_floor_ceil_bracket(self, numbers_table):
        """floor and ceil bracket the original value."""
        result = numbers_table.derive(
            f=lambda r: r.val.floor(),
            c=lambda r: r.val.ceil(),
        )
        df = result.to_pandas()
        for _, row in df.iterrows():
            assert row["f"] <= row["val"] <= row["c"]


# ============================================================================
# Sprint 1 regressions — ensure rename/drop/pipe/show/repr still work
# ============================================================================


class TestSprint1Regression:
    """Ensure Sprint 1 features still work after Sprint 2 changes."""

    def test_rename_still_works(self, numbers_table):
        """rename() from Sprint 1 still works."""
        result = numbers_table.rename(x="renamed_x")
        assert "renamed_x" in result.columns
        assert "x" not in result.columns

    def test_drop_still_works(self, numbers_table):
        """drop() from Sprint 1 still works."""
        result = numbers_table.drop("y")
        assert "y" not in result.columns
        assert "x" in result.columns

    def test_pipe_still_works(self, numbers_table):
        """pipe() from Sprint 1 still works."""
        def add_constant(t, col, val):
            return t.derive(**{f"{col}_plus": lambda r: getattr(r, col) + val})
        result = numbers_table.pipe(add_constant, "x", 100)
        df = result.to_pandas()
        assert "x_plus" in df.columns

    def test_repr_still_works(self, numbers_table):
        """__repr__ from Sprint 1 still works."""
        r = repr(numbers_table)
        assert "LTSeq" in r

    def test_dtypes_still_works(self, numbers_table):
        """dtypes property from Sprint 1 still works."""
        dt = numbers_table.dtypes
        # dtypes returns List[Tuple[str, str]], not a dict
        assert isinstance(dt, list)
        col_names = [name for name, _ in dt]
        assert "x" in col_names

    def test_with_columns_alias(self, numbers_table):
        """with_columns (alias for derive) still works."""
        result = numbers_table.with_columns(double_x=lambda r: r.x * 2)
        df = result.to_pandas()
        assert "double_x" in df.columns
        assert df["double_x"].tolist() == [20, -6, 0, 14]
