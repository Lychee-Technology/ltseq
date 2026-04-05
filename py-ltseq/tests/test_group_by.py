"""Tests for LTSeq.group_by().agg() chain API (issue #9)."""

import csv
import os
import tempfile

import pytest

pd = pytest.importorskip("pandas")

from ltseq import LTSeq, GroupBy


@pytest.fixture
def sales_csv():
    """CSV with region/product/amount columns."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        writer = csv.DictWriter(f, fieldnames=["region", "product", "amount"])
        writer.writeheader()
        writer.writerows([
            {"region": "North", "product": "Widget", "amount": "100"},
            {"region": "South", "product": "Widget", "amount": "150"},
            {"region": "North", "product": "Gadget", "amount": "200"},
            {"region": "South", "product": "Gadget", "amount": "250"},
            {"region": "North", "product": "Widget", "amount": "120"},
        ])
        path = f.name
    yield path
    os.unlink(path)


class TestGroupByString:
    """group_by(str).agg() using a column name."""

    def test_returns_groupby_object(self, sales_csv):
        t = LTSeq.read_csv(sales_csv)
        gb = t.group_by("region")
        assert isinstance(gb, GroupBy)

    def test_group_by_string_sum(self, sales_csv):
        t = LTSeq.read_csv(sales_csv)
        result = t.group_by("region").agg(total=lambda r: r.amount.sum())
        df = result.to_pandas().sort_values("region").reset_index(drop=True)

        assert list(df["region"]) == ["North", "South"]
        assert df.loc[df["region"] == "North", "total"].iloc[0] == 420
        assert df.loc[df["region"] == "South", "total"].iloc[0] == 400

    def test_group_by_string_count(self, sales_csv):
        t = LTSeq.read_csv(sales_csv)
        result = t.group_by("region").agg(n=lambda r: r.amount.count())
        df = result.to_pandas()

        assert df.loc[df["region"] == "North", "n"].iloc[0] == 3
        assert df.loc[df["region"] == "South", "n"].iloc[0] == 2

    def test_group_by_string_multiple_aggs(self, sales_csv):
        t = LTSeq.read_csv(sales_csv)
        result = t.group_by("region").agg(
            total=lambda r: r.amount.sum(),
            n=lambda r: r.amount.count(),
        )
        assert "region" in result.schema
        assert "total" in result.schema
        assert "n" in result.schema

    def test_group_by_result_row_count(self, sales_csv):
        t = LTSeq.read_csv(sales_csv)
        result = t.group_by("region").agg(total=lambda r: r.amount.sum())
        assert result.count() == 2  # North and South


class TestGroupByLambda:
    """group_by(lambda).agg() using a callable key."""

    def test_group_by_lambda_sum(self, sales_csv):
        t = LTSeq.read_csv(sales_csv)
        result = t.group_by(lambda r: r.region).agg(total=lambda r: r.amount.sum())
        assert result.count() == 2

    def test_group_by_lambda_result_columns(self, sales_csv):
        t = LTSeq.read_csv(sales_csv)
        result = t.group_by(lambda r: r.region).agg(total=lambda r: r.amount.sum())
        df = result.to_pandas()
        assert "total" in df.columns

    def test_group_by_lambda_matches_string(self, sales_csv):
        """group_by(str) and group_by(lambda) should produce the same results."""
        t = LTSeq.read_csv(sales_csv)
        r1 = t.group_by("region").agg(total=lambda r: r.amount.sum())
        r2 = t.group_by(lambda r: r.region).agg(total=lambda r: r.amount.sum())

        df1 = r1.to_pandas().sort_values("total").reset_index(drop=True)
        df2 = r2.to_pandas().sort_values("total").reset_index(drop=True)
        assert list(df1["total"]) == list(df2["total"])


class TestGroupByErrors:
    """Error handling for group_by()."""

    def test_group_by_missing_column_raises(self, sales_csv):
        t = LTSeq.read_csv(sales_csv)
        with pytest.raises(AttributeError, match="nonexistent"):
            t.group_by("nonexistent").agg(n=lambda r: r.amount.count())

    def test_group_by_no_schema_raises(self):
        t = LTSeq()
        with pytest.raises(ValueError, match="Schema not initialized"):
            t.group_by("region")

    def test_group_by_invalid_type_raises(self, sales_csv):
        t = LTSeq.read_csv(sales_csv)
        with pytest.raises(TypeError):
            t.group_by(42)

    def test_agg_no_expressions_raises(self, sales_csv):
        t = LTSeq.read_csv(sales_csv)
        with pytest.raises(ValueError, match="requires at least one"):
            t.group_by("region").agg()

    def test_agg_non_callable_raises(self, sales_csv):
        t = LTSeq.read_csv(sales_csv)
        with pytest.raises(TypeError):
            t.group_by("region").agg(total=42)


class TestGroupByVsAgg:
    """group_by().agg() and agg(by=) should be equivalent."""

    def test_equivalent_to_agg_by(self, sales_csv):
        t = LTSeq.read_csv(sales_csv)
        r1 = t.group_by("region").agg(total=lambda r: r.amount.sum())
        r2 = t.agg(by=lambda r: r.region, total=lambda r: r.amount.sum())

        df1 = r1.to_pandas().sort_values("total").reset_index(drop=True)
        df2 = r2.to_pandas().sort_values("total").reset_index(drop=True)
        assert list(df1["total"]) == list(df2["total"])
