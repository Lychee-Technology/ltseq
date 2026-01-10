"""Tests for lookup() expression functionality."""

import pytest
from ltseq import LTSeq
from ltseq.expr.types import ColumnExpr, CallExpr, LookupExpr


class TestLookupExprCreation:
    """Test that lookup() creates LookupExpr correctly."""

    def test_column_lookup_returns_lookup_expr(self):
        """ColumnExpr.lookup() should return a LookupExpr."""
        col = ColumnExpr("product_id")
        mock_table = type("MockTable", (), {"_name": "products"})()

        result = col.lookup(mock_table, "name")

        assert isinstance(result, LookupExpr)

    def test_call_lookup_returns_lookup_expr(self):
        """CallExpr.lookup() should return a LookupExpr (via inheritance)."""
        col = ColumnExpr("product_id")
        call = col.lower()  # Creates a CallExpr
        mock_table = type("MockTable", (), {"_name": "products"})()

        result = call.lookup(mock_table, "name")

        assert isinstance(result, LookupExpr)

    def test_lookup_stores_target_name(self):
        """lookup() should store the target table name."""
        col = ColumnExpr("product_id")
        mock_table = type("MockTable", (), {"_name": "products"})()

        result = col.lookup(mock_table, "name")

        assert result.target_name == "products"

    def test_lookup_stores_column(self):
        """lookup() should store the target column name."""
        col = ColumnExpr("product_id")
        mock_table = type("MockTable", (), {"_name": "products"})()

        result = col.lookup(mock_table, "price")

        assert result.target_columns == ["price"]

    def test_lookup_stores_join_key(self):
        """lookup() should store optional join_key."""
        col = ColumnExpr("product_id")
        mock_table = type("MockTable", (), {"_name": "products"})()

        result = col.lookup(mock_table, "name", join_key="id")

        assert result.join_key == "id"

    def test_lookup_stores_target_table(self):
        """lookup() should store actual table reference."""
        col = ColumnExpr("product_id")
        mock_table = type("MockTable", (), {"_name": "products"})()

        result = col.lookup(mock_table, "name")

        assert result.target_table is mock_table

    def test_lookup_registers_table(self):
        """lookup() should register table in class registry."""
        LookupExpr.clear_registry()  # Clear any existing entries
        col = ColumnExpr("product_id")
        mock_table = type("MockTable", (), {"_name": "products"})()

        col.lookup(mock_table, "name")

        assert LookupExpr.get_table("products") is mock_table
        LookupExpr.clear_registry()

    def test_lookup_default_target_name(self):
        """lookup() should generate a unique name if table has no _name."""
        col = ColumnExpr("product_id")
        mock_table = object()  # No _name attribute

        result = col.lookup(mock_table, "name")

        # Should generate a unique name based on object id
        assert result.target_name.startswith("lookup_table_")


class TestLookupExprSerialization:
    """Test LookupExpr serialization."""

    def test_lookup_serializes_correctly(self):
        """LookupExpr should serialize with all fields."""
        col = ColumnExpr("product_id")
        mock_table = type("MockTable", (), {"_name": "products"})()

        result = col.lookup(mock_table, "name", join_key="id")
        serialized = result.serialize()

        assert serialized["type"] == "Lookup"
        assert serialized["target_name"] == "products"
        assert serialized["target_columns"] == ["name"]
        assert serialized["join_key"] == "id"
        # Check that 'on' is serialized
        assert "on" in serialized
        assert serialized["on"]["type"] == "Column"
        assert serialized["on"]["name"] == "product_id"


class TestLookupWithLTSeq:
    """Test lookup() with actual LTSeq tables."""

    @pytest.fixture
    def orders_csv(self, tmp_path):
        """Create an orders CSV file."""
        csv_file = tmp_path / "orders.csv"
        csv_file.write_text(
            "order_id,product_id,quantity\n1,P001,2\n2,P002,1\n3,P001,3\n"
        )
        return str(csv_file)

    @pytest.fixture
    def products_csv(self, tmp_path):
        """Create a products CSV file."""
        csv_file = tmp_path / "products.csv"
        csv_file.write_text("id,name,price\nP001,Widget,10.00\nP002,Gadget,25.00\n")
        return str(csv_file)

    def test_lookup_with_ltseq_table(self, orders_csv, products_csv):
        """lookup() should work with actual LTSeq tables."""
        orders = LTSeq.read_csv(orders_csv)
        products = LTSeq.read_csv(products_csv)

        # Create a lookup expression
        col = ColumnExpr("product_id")
        result = col.lookup(products, "name")

        assert isinstance(result, LookupExpr)
        # LTSeq tables have a _name attribute set from file path
        assert result.target_name is not None


class TestLookupChaining:
    """Test lookup() chaining with other expressions."""

    def test_lookup_on_chained_call(self):
        """lookup() should work on chained CallExpr results."""
        col = ColumnExpr("product_code")
        # Chain: col.upper().strip().lookup(...)
        chained = col.upper().strip()
        mock_table = type("MockTable", (), {"_name": "products"})()

        result = chained.lookup(mock_table, "name")

        assert isinstance(result, LookupExpr)
        # The 'on' should be the chained CallExpr
        assert isinstance(result.on, CallExpr)

    def test_lookup_result_can_be_used_in_operations(self):
        """LookupExpr should support operations like other Exprs."""
        col = ColumnExpr("product_id")
        mock_table = type("MockTable", (), {"_name": "products"})()

        lookup_result = col.lookup(mock_table, "price")

        # LookupExpr inherits from Expr, so should support operations
        expr = lookup_result * 2
        assert expr is not None  # Should create a BinOpExpr


class TestLookupExecution:
    """Test that lookup() actually fetches data in derive()."""

    @pytest.fixture
    def orders_csv(self, tmp_path):
        """Create an orders CSV file."""
        csv_file = tmp_path / "orders.csv"
        csv_file.write_text(
            "order_id,product_id,quantity\n1,P001,2\n2,P002,1\n3,P001,3\n"
        )
        return str(csv_file)

    @pytest.fixture
    def products_csv(self, tmp_path):
        """Create a products CSV file."""
        csv_file = tmp_path / "products.csv"
        csv_file.write_text("id,name,price\nP001,Widget,10.00\nP002,Gadget,25.00\n")
        return str(csv_file)

    @pytest.fixture
    def orders_csv_with_missing(self, tmp_path):
        """Orders CSV with a product_id that doesn't exist in products."""
        csv_file = tmp_path / "orders_missing.csv"
        csv_file.write_text(
            "order_id,product_id,quantity\n1,P001,2\n2,P003,1\n3,P002,3\n"
        )
        return str(csv_file)

    def test_simple_lookup_in_derive(self, orders_csv, products_csv):
        """derive() with lookup should fetch values from target table."""
        from ltseq.expr.types import LookupExpr

        LookupExpr.clear_registry()

        orders = LTSeq.read_csv(orders_csv)
        products = LTSeq.read_csv(products_csv)

        enriched = orders.derive(
            product_name=lambda r: r.product_id.lookup(products, "name", join_key="id")
        )

        df = enriched.to_pandas()
        # Should contain product names from lookup
        assert "product_name" in df.columns
        product_names = df["product_name"].tolist()
        assert "Widget" in product_names
        assert "Gadget" in product_names

    def test_lookup_preserves_original_columns(self, orders_csv, products_csv):
        """derive() with lookup should keep original columns."""
        from ltseq.expr.types import LookupExpr

        LookupExpr.clear_registry()

        orders = LTSeq.read_csv(orders_csv)
        products = LTSeq.read_csv(products_csv)

        enriched = orders.derive(
            product_name=lambda r: r.product_id.lookup(products, "name", join_key="id")
        )

        df = enriched.to_pandas()
        # Original columns should still be present
        assert "order_id" in df.columns
        assert "product_id" in df.columns
        assert "quantity" in df.columns

    def test_lookup_unmatched_returns_null(self, orders_csv_with_missing, products_csv):
        """Unmatched lookup keys should return NULL values."""
        from ltseq.expr.types import LookupExpr

        LookupExpr.clear_registry()

        orders = LTSeq.read_csv(orders_csv_with_missing)
        products = LTSeq.read_csv(products_csv)

        enriched = orders.derive(
            product_name=lambda r: r.product_id.lookup(products, "name", join_key="id")
        )

        df = enriched.to_pandas()
        # P003 doesn't exist in products, should get null
        # Widget and Gadget should still appear for P001 and P002
        product_names = df["product_name"].tolist()
        assert "Widget" in product_names
        assert "Gadget" in product_names
        # Check that unmatched row has None/null
        assert None in product_names or any(
            str(v).lower() in ("none", "nan", "") for v in product_names
        )

    def test_lookup_fetches_correct_values(self, orders_csv, products_csv):
        """Lookup should fetch the correct value for each row."""
        from ltseq.expr.types import LookupExpr

        LookupExpr.clear_registry()

        orders = LTSeq.read_csv(orders_csv)
        products = LTSeq.read_csv(products_csv)

        enriched = orders.derive(
            price=lambda r: r.product_id.lookup(products, "price", join_key="id")
        )

        df = enriched.to_pandas()
        # P001 -> 10.00, P002 -> 25.00
        assert "price" in df.columns
        prices = df["price"].tolist()
        # Check both prices are present (may be strings or numbers)
        price_strs = [str(p) for p in prices]
        assert any("10" in p for p in price_strs)  # Price for Widget
        assert any("25" in p for p in price_strs)  # Price for Gadget

    def test_lookup_correct_row_association(self, orders_csv, products_csv):
        """Lookup should associate correct values with each row."""
        from ltseq.expr.types import LookupExpr

        LookupExpr.clear_registry()

        orders = LTSeq.read_csv(orders_csv)
        products = LTSeq.read_csv(products_csv)

        enriched = orders.derive(
            product_name=lambda r: r.product_id.lookup(products, "name", join_key="id")
        )

        df = enriched.to_pandas()
        # Verify correct association: P001 -> Widget, P002 -> Gadget
        for _, row in df.iterrows():
            if row["product_id"] == "P001":
                assert row["product_name"] == "Widget"
            elif row["product_id"] == "P002":
                assert row["product_name"] == "Gadget"
