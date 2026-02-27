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

        assert serialized["type"] == "BinOp"
        assert serialized["op"] == "-"
        assert serialized["left"]["type"] == "GroupRowColumn"
        assert serialized["right"]["type"] == "GroupRowColumn"


class TestGroupExprToSQL:
    """Test SQL generation from GroupExpr."""

    def test_count_to_sql(self):
        """GroupCount should generate COUNT(*) OVER (...)."""
        from ltseq.grouping import group_expr_to_sql

        sql = group_expr_to_sql({"type": "GroupCount"})
        assert "COUNT(*)" in sql
        assert "PARTITION BY __group_id__" in sql

    def test_agg_to_sql(self):
        """GroupAgg should generate AGG(col) OVER (...)."""
        from ltseq.grouping import group_expr_to_sql

        sql = group_expr_to_sql(
            {
                "type": "GroupAgg",
                "func": "max",
                "column": "price",
            }
        )
        assert "MAX" in sql
        assert "price" in sql
        assert "PARTITION BY __group_id__" in sql

    def test_first_column_to_sql(self):
        """GroupRowColumn (first) should generate FIRST_VALUE(...)."""
        from ltseq.grouping import group_expr_to_sql

        sql = group_expr_to_sql(
            {
                "type": "GroupRowColumn",
                "row": "first",
                "column": "date",
            }
        )
        assert "FIRST_VALUE" in sql
        assert "date" in sql

    def test_last_column_to_sql(self):
        """GroupRowColumn (last) should generate LAST_VALUE(...)."""
        from ltseq.grouping import group_expr_to_sql

        sql = group_expr_to_sql(
            {
                "type": "GroupRowColumn",
                "row": "last",
                "column": "price",
            }
        )
        assert "LAST_VALUE" in sql
        assert "price" in sql
        assert "UNBOUNDED" in sql  # Frame clause needed for LAST_VALUE

    def test_binop_to_sql(self):
        """BinOp should generate (left OP right)."""
        from ltseq.grouping import group_expr_to_sql

        sql = group_expr_to_sql(
            {
                "type": "BinOp",
                "left": {"type": "GroupRowColumn", "row": "last", "column": "price"},
                "op": "-",
                "right": {"type": "GroupRowColumn", "row": "first", "column": "price"},
            }
        )
        assert "-" in sql
        assert "LAST_VALUE" in sql
        assert "FIRST_VALUE" in sql
