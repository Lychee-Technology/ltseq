"""Tests for multi-key GROUP BY in agg()."""

import os
import tempfile

import pytest

from ltseq import LTSeq


@pytest.fixture
def sales_data():
    """Create sample sales data with multiple grouping dimensions."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "sales.csv")
        with open(path, "w") as f:
            f.write("region,product,year,sales,quantity\n")
            f.write("West,Widget,2023,100,10\n")
            f.write("East,Gadget,2023,200,20\n")
            f.write("West,Gadget,2023,150,15\n")
            f.write("East,Widget,2024,300,30\n")
            f.write("West,Widget,2024,125,12\n")
            f.write("East,Gadget,2024,250,25\n")
            f.write("West,Widget,2023,80,8\n")
            f.write("East,Widget,2023,175,17\n")
        yield LTSeq.read_csv(path)


class TestMultiKeyLambdaTuple:
    """Test multi-key GROUP BY via tuple-returning lambda."""

    def test_two_key_groupby_tuple_lambda(self, sales_data):
        """by=lambda r: (r.region, r.product) groups by both columns."""
        result = sales_data.agg(
            by=lambda r: (r.region, r.product),
            total=lambda g: g.sales.sum(),
        )
        df = result.to_pandas()

        # 4 unique (region, product) combos: West/Widget, West/Gadget, East/Widget, East/Gadget
        assert len(df) == 4

        west_widget = df[(df["region"] == "West") & (df["product"] == "Widget")]
        assert len(west_widget) == 1
        assert west_widget["total"].iloc[0] == 305  # 100 + 125 + 80

        east_gadget = df[(df["region"] == "East") & (df["product"] == "Gadget")]
        assert len(east_gadget) == 1
        assert east_gadget["total"].iloc[0] == 450  # 200 + 250

    def test_two_key_groupby_count(self, sales_data):
        """Multi-key GROUP BY with count aggregate."""
        result = sales_data.agg(
            by=lambda r: (r.region, r.year),
            cnt=lambda g: g.sales.count(),
        )
        df = result.to_pandas()

        # 4 combos: West/2023, West/2024, East/2023, East/2024
        assert len(df) == 4

        west_2023 = df[(df["region"] == "West") & (df["year"] == 2023)]
        assert west_2023["cnt"].iloc[0] == 3  # Widget, Gadget, Widget

        east_2024 = df[(df["region"] == "East") & (df["year"] == 2024)]
        assert east_2024["cnt"].iloc[0] == 2  # Widget + Gadget

    def test_three_key_groupby(self, sales_data):
        """Three-key GROUP BY via tuple lambda."""
        result = sales_data.agg(
            by=lambda r: (r.region, r.product, r.year),
            total=lambda g: g.sales.sum(),
        )
        df = result.to_pandas()

        # Each unique (region, product, year) combo
        west_widget_2023 = df[
            (df["region"] == "West")
            & (df["product"] == "Widget")
            & (df["year"] == 2023)
        ]
        assert len(west_widget_2023) == 1
        assert west_widget_2023["total"].iloc[0] == 180  # 100 + 80

    def test_multiple_aggregates_with_multikey(self, sales_data):
        """Multi-key GROUP BY with multiple aggregate functions."""
        result = sales_data.agg(
            by=lambda r: (r.region, r.product),
            total_sales=lambda g: g.sales.sum(),
            avg_qty=lambda g: g.quantity.avg(),
            max_sale=lambda g: g.sales.max(),
        )
        df = result.to_pandas()

        assert len(df) == 4
        assert "total_sales" in df.columns
        assert "avg_qty" in df.columns
        assert "max_sale" in df.columns

        west_widget = df[(df["region"] == "West") & (df["product"] == "Widget")]
        assert west_widget["total_sales"].iloc[0] == 305
        assert west_widget["max_sale"].iloc[0] == 125


class TestMultiKeyStringList:
    """Test multi-key GROUP BY via list of column name strings."""

    def test_two_key_string_list(self, sales_data):
        """by=["region", "product"] groups by both columns."""
        result = sales_data.agg(
            by=["region", "product"],
            total=lambda g: g.sales.sum(),
        )
        df = result.to_pandas()

        assert len(df) == 4
        west_gadget = df[(df["region"] == "West") & (df["product"] == "Gadget")]
        assert len(west_gadget) == 1
        assert west_gadget["total"].iloc[0] == 150

    def test_three_key_string_list(self, sales_data):
        """by=["region", "product", "year"] groups by three columns."""
        result = sales_data.agg(
            by=["region", "product", "year"],
            total=lambda g: g.sales.sum(),
        )
        df = result.to_pandas()

        east_widget_2024 = df[
            (df["region"] == "East")
            & (df["product"] == "Widget")
            & (df["year"] == 2024)
        ]
        assert len(east_widget_2024) == 1
        assert east_widget_2024["total"].iloc[0] == 300

    def test_single_key_string_list(self, sales_data):
        """by=["region"] should work the same as by=lambda r: r.region."""
        result = sales_data.agg(
            by=["region"],
            total=lambda g: g.sales.sum(),
        )
        df = result.to_pandas()

        assert len(df) == 2
        west = df[df["region"] == "West"]
        assert west["total"].iloc[0] == 455  # 100 + 150 + 125 + 80


class TestMultiKeyLambdaList:
    """Test multi-key GROUP BY via list of lambdas."""

    def test_two_key_lambda_list(self, sales_data):
        """by=[lambda r: r.region, lambda r: r.product] groups by both."""
        result = sales_data.agg(
            by=[lambda r: r.region, lambda r: r.product],
            total=lambda g: g.sales.sum(),
        )
        df = result.to_pandas()

        assert len(df) == 4
        east_gadget = df[(df["region"] == "East") & (df["product"] == "Gadget")]
        assert east_gadget["total"].iloc[0] == 450


class TestMultiKeyBackwardCompat:
    """Ensure single-key and no-key agg() still works unchanged."""

    def test_single_key_lambda_unchanged(self, sales_data):
        """by=lambda r: r.region still works as before."""
        result = sales_data.agg(
            by=lambda r: r.region,
            total=lambda g: g.sales.sum(),
        )
        df = result.to_pandas()

        assert len(df) == 2
        assert set(df["region"]) == {"West", "East"}

    def test_no_groupby_unchanged(self, sales_data):
        """Full-table aggregation with by=None still works."""
        result = sales_data.agg(
            total=lambda g: g.sales.sum(),
            cnt=lambda g: g.sales.count(),
        )
        df = result.to_pandas()

        assert len(df) == 1
        assert df["total"].iloc[0] == 1380  # sum of all sales
        assert df["cnt"].iloc[0] == 8


class TestMultiKeyEdgeCases:
    """Edge cases for multi-key GROUP BY."""

    def test_all_same_group(self):
        """All rows have the same key values — single result row."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "same.csv")
            with open(path, "w") as f:
                f.write("a,b,val\n")
                f.write("X,Y,10\n")
                f.write("X,Y,20\n")
                f.write("X,Y,30\n")
            t = LTSeq.read_csv(path)

            result = t.agg(by=["a", "b"], total=lambda g: g.val.sum())
            df = result.to_pandas()

            assert len(df) == 1
            assert df["total"].iloc[0] == 60

    def test_each_row_unique_group(self):
        """Every row has a unique key combination — N result rows."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "unique.csv")
            with open(path, "w") as f:
                f.write("a,b,val\n")
                f.write("X,1,10\n")
                f.write("X,2,20\n")
                f.write("Y,1,30\n")
                f.write("Y,2,40\n")
            t = LTSeq.read_csv(path)

            result = t.agg(by=["a", "b"], cnt=lambda g: g.val.count())
            df = result.to_pandas()

            assert len(df) == 4
            assert all(df["cnt"] == 1)

    def test_multikey_with_min_max(self):
        """Multi-key GROUP BY with min and max aggregates."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write("dept,role,salary\n")
                f.write("Eng,Senior,120\n")
                f.write("Eng,Junior,80\n")
                f.write("Eng,Senior,130\n")
                f.write("Sales,Junior,70\n")
                f.write("Sales,Senior,100\n")
            t = LTSeq.read_csv(path)

            result = t.agg(
                by=lambda r: (r.dept, r.role),
                min_sal=lambda g: g.salary.min(),
                max_sal=lambda g: g.salary.max(),
            )
            df = result.to_pandas()

            eng_senior = df[(df["dept"] == "Eng") & (df["role"] == "Senior")]
            assert eng_senior["min_sal"].iloc[0] == 120
            assert eng_senior["max_sal"].iloc[0] == 130

    def test_multikey_schema_has_all_group_columns(self, sales_data):
        """Result schema should include all group-by columns."""
        result = sales_data.agg(
            by=lambda r: (r.region, r.product),
            total=lambda g: g.sales.sum(),
        )

        assert "region" in result.columns
        assert "product" in result.columns
        assert "total" in result.columns

    def test_multikey_result_is_chainable(self, sales_data):
        """Multi-key agg result can be further filtered/derived."""
        result = sales_data.agg(
            by=["region", "product"],
            total=lambda g: g.sales.sum(),
        )

        # Filter the aggregated result
        filtered = result.filter(lambda r: r.total > 200)
        df = filtered.to_pandas()

        # Only groups with total > 200
        assert all(df["total"] > 200)


class TestMultiKeyErrors:
    """Error handling for multi-key GROUP BY."""

    def test_invalid_by_type_raises(self, sales_data):
        """by=123 should raise TypeError."""
        with pytest.raises(TypeError):
            sales_data.agg(by=123, total=lambda g: g.sales.sum())

    def test_invalid_list_element_raises(self, sales_data):
        """by=[123] should raise TypeError."""
        with pytest.raises(TypeError):
            sales_data.agg(by=[123], total=lambda g: g.sales.sum())

    def test_tuple_with_non_expr_raises(self, sales_data):
        """by=lambda r: (r.region, 123) should raise TypeError."""
        with pytest.raises(TypeError):
            sales_data.agg(
                by=lambda r: (r.region, 123),
                total=lambda g: g.sales.sum(),
            )
