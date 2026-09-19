"""
Phase 5.2 Tests: NestedTable.derive() Proxy-Based Capture

Tests that NestedTable.derive() works in pytest context (where inspect.getsource() fails)
by using proxy-based expression capture instead of source code parsing.
"""

import pytest
from ltseq import LTSeq
import csv
import tempfile
import os


@pytest.fixture
def sample_csv():
    """Create a sample CSV for group operations."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        writer = csv.DictWriter(f, fieldnames=["date", "price", "is_up", "volume"])
        writer.writeheader()
        data = [
            # Group 1: is_up = True (3 rows)
            {"date": "2024-01-01", "price": "100", "is_up": "true", "volume": "10"},
            {"date": "2024-01-02", "price": "105", "is_up": "true", "volume": "15"},
            {"date": "2024-01-03", "price": "110", "is_up": "true", "volume": "20"},
            # Group 2: is_up = False (2 rows)
            {"date": "2024-01-04", "price": "108", "is_up": "false", "volume": "25"},
            {"date": "2024-01-05", "price": "102", "is_up": "false", "volume": "30"},
            # Group 3: is_up = True (2 rows)
            {"date": "2024-01-06", "price": "104", "is_up": "true", "volume": "12"},
            {"date": "2024-01-07", "price": "109", "is_up": "true", "volume": "18"},
        ]
        for row in data:
            writer.writerow(row)
        temp_path = f.name

    yield temp_path
    os.unlink(temp_path)


class TestDeriveGroupCount:
    """Test g.count() in derive()."""

    def test_derive_count_basic(self, sample_csv):
        """g.count() should work in pytest context."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        result = t.group_ordered(lambda r: r.is_up).derive(
            lambda g: {"group_size": g.count()}
        )

        assert "group_size" in result._schema
        df = result.to_pandas()

        # First group (is_up=True) has 3 rows
        # Note: boolean columns may be True/False, not "true"/"false"
        first_group_sizes = df[df["is_up"] == True].head(3)["group_size"].unique()
        assert 3 in first_group_sizes

    def test_derive_count_multiple_groups(self, sample_csv):
        """Count should be correct for each group."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        result = t.group_ordered(lambda r: r.is_up).derive(
            lambda g: {"size": g.count()}
        )

        df = result.to_pandas()
        # Check that different groups can have different sizes
        sizes = df["size"].unique()
        assert len(sizes) >= 2  # At least two different group sizes


class TestDeriveFirstLast:
    """Test g.first().col and g.last().col in derive()."""

    def test_derive_first_column(self, sample_csv):
        """g.first().date should capture first row's column value."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        result = t.group_ordered(lambda r: r.is_up).derive(
            lambda g: {"start_date": g.first().date}
        )

        assert "start_date" in result._schema
        df = result.to_pandas()

        # First group starts at 2024-01-01
        first_rows = df.head(3)
        assert all(first_rows["start_date"].astype(str) == "2024-01-01")

    def test_derive_last_column(self, sample_csv):
        """g.last().date should capture last row's column value."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        result = t.group_ordered(lambda r: r.is_up).derive(
            lambda g: {"end_date": g.last().date}
        )

        assert "end_date" in result._schema
        df = result.to_pandas()

        # First group ends at 2024-01-03
        first_rows = df.head(3)
        assert all(first_rows["end_date"].astype(str) == "2024-01-03")

    def test_derive_both_first_and_last(self, sample_csv):
        """Can use both g.first() and g.last() in same derive."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        result = t.group_ordered(lambda r: r.is_up).derive(
            lambda g: {
                "start": g.first().date,
                "end": g.last().date,
            }
        )

        assert "start" in result._schema
        assert "end" in result._schema


class TestDeriveArithmetic:
    """Test arithmetic operations between group expressions."""

    def test_derive_price_change(self, sample_csv):
        """g.last().price - g.first().price should compute price change."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        result = t.group_ordered(lambda r: r.is_up).derive(
            lambda g: {"price_change": g.last().price - g.first().price}
        )

        assert "price_change" in result._schema
        df = result.to_pandas()

        # First group: 110 - 100 = 10
        first_group = df.head(3)
        # All rows in same group should have same price_change
        assert len(first_group["price_change"].unique()) == 1

    def test_derive_division(self, sample_csv):
        """Division operations should work."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        result = t.group_ordered(lambda r: r.is_up).derive(
            lambda g: {"gain_ratio": g.last().price / g.first().price}
        )

        assert "gain_ratio" in result._schema


class TestDeriveAggregations:
    """Test group aggregation functions."""

    def test_derive_max(self, sample_csv):
        """g.max('col') should work."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        result = t.group_ordered(lambda r: r.is_up).derive(
            lambda g: {"max_price": g.max("price")}
        )

        assert "max_price" in result._schema
        df = result.to_pandas()

        # First group max is 110
        first_group = df.head(3)
        assert all(first_group["max_price"] == 110)

    def test_derive_min(self, sample_csv):
        """g.min('col') should work."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        result = t.group_ordered(lambda r: r.is_up).derive(
            lambda g: {"min_volume": g.min("volume")}
        )

        assert "min_volume" in result._schema

    def test_derive_sum(self, sample_csv):
        """g.sum('col') should work."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        result = t.group_ordered(lambda r: r.is_up).derive(
            lambda g: {"total_volume": g.sum("volume")}
        )

        assert "total_volume" in result._schema
        df = result.to_pandas()

        # First group sum: 10 + 15 + 20 = 45
        first_group = df.head(3)
        assert all(first_group["total_volume"] == 45)

    def test_derive_avg(self, sample_csv):
        """g.avg('col') should work."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        result = t.group_ordered(lambda r: r.is_up).derive(
            lambda g: {"avg_volume": g.avg("volume")}
        )

        assert "avg_volume" in result._schema


class TestDeriveMultipleColumns:
    """Test deriving multiple columns at once."""

    def test_derive_multiple_expressions(self, sample_csv):
        """Can derive multiple columns in single call."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        result = t.group_ordered(lambda r: r.is_up).derive(
            lambda g: {
                "group_size": g.count(),
                "start_price": g.first().price,
                "end_price": g.last().price,
                "max_vol": g.max("volume"),
            }
        )

        assert "group_size" in result._schema
        assert "start_price" in result._schema
        assert "end_price" in result._schema
        assert "max_vol" in result._schema


class TestDeriveChainedWithFilter:
    """Test derive after filter (using stored group assignments)."""

    def test_filter_then_derive(self, sample_csv):
        """filter().derive() chain should work."""
        t = LTSeq.read_csv(sample_csv).sort("date")
        result = (
            t.group_ordered(lambda r: r.is_up)
            .filter(lambda g: g.count() >= 2)
            .derive(
                lambda g: {
                    "size": g.count(),
                    "start": g.first().date,
                }
            )
        )

        assert "size" in result._schema
        assert "start" in result._schema
        df = result.to_pandas()
        # All remaining groups should have size >= 2
        assert all(df["size"] >= 2)


class TestGroupExprSerialization:
    """Test that GroupExpr classes serialize correctly."""

    def test_count_serialization(self):
        """GroupCountExpr should serialize correctly."""
        from ltseq.grouping import GroupCountExpr

        expr = GroupCountExpr()
        serialized = expr.serialize()

        assert serialized["type"] == "GroupCount"

    def test_agg_serialization(self):
        """GroupAggExpr should serialize correctly."""
        from ltseq.grouping import GroupAggExpr

        expr = GroupAggExpr("max", "price")
        serialized = expr.serialize()

        assert serialized["type"] == "GroupAgg"
        assert serialized["func"] == "max"
        assert serialized["column"] == "price"

    def test_row_column_serialization(self):
        """GroupRowColumnExpr should serialize correctly."""
        from ltseq.grouping import GroupRowColumnExpr

        expr = GroupRowColumnExpr("first", "date")
        serialized = expr.serialize()

        assert serialized["type"] == "GroupRowColumn"
        assert serialized["row"] == "first"
        assert serialized["column"] == "date"

    def test_binop_serialization(self):
        """BinOpGroupExpr should serialize correctly."""
        from ltseq.grouping import GroupRowColumnExpr, BinOpGroupExpr

        left = GroupRowColumnExpr("last", "price")
        right = GroupRowColumnExpr("first", "price")
        expr = BinOpGroupExpr(left, "-", right)
        serialized = expr.serialize()

        # Ops serialize under the row-dialect names so Rust shares one
        # operator table across both dialects (issue #91 PR 4).
        assert serialized["type"] == "BinOp"
        assert serialized["op"] == "Sub"
        assert serialized["left"]["type"] == "GroupRowColumn"
        assert serialized["right"]["type"] == "GroupRowColumn"

    def test_binop_literal_carries_dtype(self):
        """Scalar operands share the row dialect's Literal shape (with dtype)."""
        from ltseq.grouping import GroupCountExpr, BinOpGroupExpr

        serialized = BinOpGroupExpr(GroupCountExpr(), "+", 1).serialize()
        assert serialized["op"] == "Add"
        assert serialized["right"] == {"type": "Literal", "value": 1, "dtype": "Int64"}


class TestFilterExprSerialization:
    """FilterExpr serializes comparisons/combinators/quantifiers to the
    group dialect (replaces the deleted SQL-string generation tests)."""

    def test_comparison_serialization(self):
        from ltseq.grouping.proxies.filter_proxy import FilterGroupProxy

        g = FilterGroupProxy({})
        expr = g.count() > 2
        assert expr.serialize() == {
            "type": "BinOp",
            "op": "Gt",
            "left": {"type": "GroupCount"},
            "right": {"type": "Literal", "value": 2, "dtype": "Int64"},
        }

    def test_and_or_not_serialization(self):
        from ltseq.grouping.proxies.filter_proxy import FilterGroupProxy

        g = FilterGroupProxy({})
        combo = (g.count() > 1) & (g.max("price") < 10.5)
        d = combo.serialize()
        assert d["op"] == "And"
        assert d["left"]["op"] == "Gt"
        assert d["right"]["right"] == {
            "type": "Literal",
            "value": 10.5,
            "dtype": "Float64",
        }

        negated = ~(g.count() > 1)
        nd = negated.serialize()
        assert nd == {
            "type": "UnaryOp",
            "op": "Not",
            "operand": {
                "type": "BinOp",
                "op": "Gt",
                "left": {"type": "GroupCount"},
                "right": {"type": "Literal", "value": 1, "dtype": "Int64"},
            },
        }

    def test_quantifier_serialization_embeds_row_dialect(self):
        from ltseq.grouping.proxies.filter_proxy import FilterGroupProxy

        g = FilterGroupProxy({"amount": "int64"})
        expr = g.all(lambda r: r.amount > 0)
        d = expr.serialize()
        assert d["type"] == "GroupQuantifier"
        assert d["quant"] == "all"
        # Inner predicate is a full row-dialect expression dict
        assert d["pred"]["type"] == "BinOp"
        assert d["pred"]["left"] == {"type": "Column", "name": "amount"}
        assert d["pred"]["right"]["dtype"] == "Int64"

    def test_window_vs_window_comparison(self):
        from ltseq.grouping.proxies.filter_proxy import FilterGroupProxy

        g = FilterGroupProxy({})
        expr = g.first().price < g.last().price
        d = expr.serialize()
        assert d["op"] == "Lt"
        assert d["left"] == {"type": "GroupRowColumn", "row": "first", "column": "price"}
        assert d["right"] == {"type": "GroupRowColumn", "row": "last", "column": "price"}


class TestGroupExprBoolContext:
    """Group-dialect expressions must raise in boolean contexts (issue #163).

    Mirrors test_expr.py::TestExprBoolContext for the row dialect (#140): without
    __bool__, Python's default truthiness (always True) silently drops conditions
    in `and`/`or`/`not`/`in`/ternary/chained comparisons of group predicates.
    """

    @pytest.fixture
    def g(self):
        from ltseq.grouping.proxies.filter_proxy import FilterGroupProxy

        return FilterGroupProxy({"x": "int64"})

    def test_bool_raises_type_error_with_guidance(self, g):
        """bool(filter_expr) raises TypeError naming the misuse forms and & | ~."""
        with pytest.raises(TypeError, match=r"ternary") as exc_info:
            bool(g.count() > 2)
        msg = str(exc_info.value)
        assert "boolean context" in msg
        assert "&" in msg and "|" in msg and "~" in msg
        # Guidance is phrased for the group proxy, not the row proxy.
        assert "g.count()" in msg

    def test_and_raises(self, g):
        """`cond1 and cond2` raises instead of silently dropping cond1."""
        with pytest.raises(TypeError):
            (g.count() > 2) and (g.sum("x") > 0)

    def test_or_raises(self, g):
        """`cond1 or cond2` raises instead of silently returning cond1."""
        with pytest.raises(TypeError):
            (g.count() > 2) or (g.sum("x") > 0)

    def test_not_raises(self, g):
        """`not cond` raises instead of returning a Python bool."""
        with pytest.raises(TypeError):
            not (g.count() > 2)

    def test_in_raises(self, g):
        """`g.count() in [...]` raises: list.__contains__ calls __eq__ then bool()."""
        with pytest.raises(TypeError):
            g.count() in [1, 2, 3]

    def test_ternary_raises(self, g):
        """`x if cond else y` raises instead of swallowing the condition."""
        with pytest.raises(TypeError):
            g.sum("x") if g.count() > 2 else g.max("x")

    def test_chained_comparison_raises(self, g):
        """`1 < g.count() < 5` raises instead of dropping the first comparison."""
        with pytest.raises(TypeError):
            1 < g.count() < 5

    def test_if_expr_raises(self, g):
        """`if cond:` raises instead of always taking the branch."""
        with pytest.raises(TypeError):
            if g.first().x == 1:
                pass

    def test_quantifier_and_raises(self, g):
        """QuantifierFilterExpr inherits the guard: `g.all(...) and g.any(...)` raises."""
        with pytest.raises(TypeError):
            g.all(lambda r: r.x > 0) and g.any(lambda r: r.x > 10)

    def test_bare_group_expr_raises(self, g):
        """Aggregates and arithmetic (GroupExpr, not just FilterExpr) refuse truthiness."""
        with pytest.raises(TypeError, match=r"boolean context"):
            bool(g.count())
        with pytest.raises(TypeError, match=r"boolean context"):
            bool(g.max("x") + 1)
        with pytest.raises(TypeError, match=r"boolean context"):
            bool(g.first().x)

    def test_derive_group_expr_raises(self):
        """The derive proxy's GroupExpr objects carry the same guard."""
        from ltseq.grouping.proxies.derive_proxy import DeriveGroupProxy

        g = DeriveGroupProxy()
        with pytest.raises(TypeError, match=r"boolean context"):
            g.count() and g.sum("x")

    def test_bitwise_combinators_still_work(self, g):
        """Regression guard: & | ~ never touch bool() and keep serializing."""
        d = ((g.count() > 2) & (g.sum("x") > 0) | ~(g.max("x") < 1)).serialize()
        assert d["op"] == "Or"
        assert d["left"]["op"] == "And"
        assert d["right"]["type"] == "UnaryOp"

    def test_nested_filter_with_and_raises_with_guidance(self, sample_csv):
        """nested.filter(lambda g: cond1 and cond2) surfaces a TypeError with & guidance."""
        t = LTSeq.read_csv(sample_csv).assume_sorted("date")
        grouped = t.group_ordered(lambda r: r.is_up)
        with pytest.raises(TypeError, match=r"&"):
            grouped.filter(lambda g: (g.count() > 2) and (g.sum("volume") > 0))

    def test_nested_filter_with_ampersand_keeps_both_conditions(self, sample_csv):
        """The correct spelling keeps both conditions: only the 3-row group has count > 2."""
        t = LTSeq.read_csv(sample_csv).assume_sorted("date")
        grouped = t.group_ordered(lambda r: r.is_up)
        result = grouped.filter(lambda g: (g.count() > 2) & (g.sum("volume") > 0))
        assert result.flatten().count() == 3


class TestDeriveInREPLContext:
    """Test that derive() works when inspect.getsource() is unavailable (REPL/exec)."""

    def test_derive_count_via_exec(self, sample_csv):
        """derive(lambda g: {'n': g.count()}) works in exec() context."""
        result_holder = {}
        code = f"""
from ltseq import LTSeq
t = LTSeq.read_csv({repr(sample_csv)}).assume_sorted("date")
grouped = t.group_ordered(lambda r: r.is_up)
result = grouped.derive(lambda g: {{"n": g.count()}})
result_holder['result'] = result
"""
        exec(code, {"result_holder": result_holder})
        result = result_holder["result"]
        assert result.count() == 7
        assert "n" in result.schema

    def test_derive_first_via_exec(self, sample_csv):
        """derive(lambda g: {'start': g.first().date}) works in exec() context."""
        result_holder = {}
        code = f"""
from ltseq import LTSeq
t = LTSeq.read_csv({repr(sample_csv)}).assume_sorted("date")
grouped = t.group_ordered(lambda r: r.is_up)
result = grouped.derive(lambda g: {{"start": g.first().date}})
result_holder['result'] = result
"""
        exec(code, {"result_holder": result_holder})
        result = result_holder["result"]
        assert result.count() == 7
        assert "start" in result.schema

    def test_filter_then_derive_via_exec(self, sample_csv):
        """filter().derive() chain works in exec() context."""
        result_holder = {}
        code = f"""
from ltseq import LTSeq
t = LTSeq.read_csv({repr(sample_csv)}).assume_sorted("date")
grouped = t.group_ordered(lambda r: r.is_up)
filtered = grouped.filter(lambda g: g.count() > 2)
result = filtered.derive(lambda g: {{"n": g.count()}})
result_holder['result'] = result
"""
        exec(code, {"result_holder": result_holder})
        result = result_holder["result"]
        # Only group 1 (3 rows) passes the count > 2 filter
        assert result.count() == 3
        assert "n" in result.schema
