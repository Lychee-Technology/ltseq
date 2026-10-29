"""Tests for lookup() expression functionality."""

import random
import re
from decimal import Decimal

import pyarrow as pa
import pytest
from ltseq import LTSeq, rand
from ltseq.expr.types import ColumnExpr, CallExpr, LookupExpr

from .layouts import CPUS


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

        assert result.target_name == f"products_{id(mock_table)}"

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

        result = col.lookup(mock_table, "name")

        assert LookupExpr.get_table(result.target_name) is mock_table
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
        assert serialized["target_name"] == f"products_{id(mock_table)}"
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


def _orders():
    return LTSeq.from_arrow(
        pa.table(
            {
                "oid": [1, 2, 3],
                "pid": pa.array([10, 20, 99], pa.int64()),
                "code": ["TEN", "twenty", "x"],
                "qty": [3, 5, 1],
            }
        )
    )


def _products():
    return LTSeq.from_arrow(
        pa.table(
            {
                "pid": pa.array([10, 20], pa.int64()),
                "code": ["ten", "twenty"],
                "name": ["ten", "twenty"],
                "price": [2.0, 4.0],
            }
        )
    )


def _by_oid(table):
    # A derive with a lookup declares no order, so compare rows by key.
    return sorted(table.to_dicts(), key=lambda row: row["oid"])


class TestLookupShape:
    """derive() evaluates a lookup only as a derived column's whole expression,
    keyed by a column; any other shape raises at the call (#223)."""

    @pytest.mark.parametrize(
        "build",
        [
            lambda p: lambda r: r.pid.lookup(p, "name") == "ten",
            lambda p: lambda r: r.pid.lookup(p, "name").is_null(),
            lambda p: lambda r: r.pid.lookup(p, "price") * r.qty,
            lambda p: lambda r: r.qty * r.pid.lookup(p, "price"),
            lambda p: lambda r: r.pid.lookup(p, "price").fill_null(0.0),
            lambda p: lambda r: r.qty.fill_null(r.pid.lookup(p, "price")),
            lambda p: lambda r: r.pid.lookup(p, "price").cast("int64"),
            lambda p: lambda r: ~(r.pid.lookup(p, "name") == "ten"),
        ],
        ids=["eq", "is_null", "mul", "rmul", "fill_null", "fill_null_arg", "cast", "invert"],
    )
    def test_lookup_inside_an_expression_raises(self, build):
        # These used to return the bare looked-up value.
        orders, products = _orders(), _products()

        with pytest.raises(ValueError, match=r"Column 'x': lookup\(\) .* larger expression"):
            orders.derive(x=build(products))

    def test_lookup_inside_an_expression_raises_in_dict_form(self):
        orders, products = _orders(), _products()

        with pytest.raises(ValueError, match=r"Column 'x': lookup\(\) .* larger expression"):
            orders.derive(lambda r: {"x": r.pid.lookup(products, "name") == "ten"})

    @pytest.mark.parametrize(
        "build",
        [
            lambda p: lambda r: r.code.s.lower().lookup(p, "name", join_key="code"),
            lambda p: lambda r: (r.pid + 0).lookup(p, "name"),
            lambda p: lambda r: r.pid.lookup(p, "pid").lookup(p, "name"),
        ],
        ids=["method", "arithmetic", "lookup"],
    )
    def test_lookup_on_a_computed_key_raises(self, build):
        # The key used to be the column under the computed value, so
        # r.code.s.lower().lookup(...) compared "TEN" with "ten" and found
        # no match.
        orders, products = _orders(), _products()

        with pytest.raises(ValueError, match=r"Column 'x': lookup\(\) needs a column as its key"):
            orders.derive(x=build(products))

    def test_a_later_derive_builds_on_the_looked_up_column(self):
        # The workaround the errors name.
        orders, products = _orders(), _products()

        result = (
            orders.derive(price=lambda r: r.pid.lookup(products, "price"))
            .derive(
                total=lambda r: r.price * r.qty,
                missing=lambda r: r.price.is_null(),
            )
        )

        rows = _by_oid(result)
        assert [row["total"] for row in rows] == [6.0, 20.0, None]
        assert [row["missing"] for row in rows] == [False, False, True]

    def test_a_computed_key_derived_first_is_looked_up(self):
        orders, products = _orders(), _products()

        result = orders.derive(key=lambda r: r.code.s.lower()).derive(
            name=lambda r: r.key.lookup(products, "name", join_key="code")
        )

        assert [row["name"] for row in _by_oid(result)] == ["ten", "twenty", None]


class TestLookupResult:
    """derive() with a lookup adds the derived columns and nothing else (#223)."""

    def test_bare_lookup_returns_the_looked_up_value(self):
        orders, products = _orders(), _products()

        result = orders.derive(x=lambda r: r.pid.lookup(products, "name"))

        assert result.columns == ["oid", "pid", "code", "qty", "x"]
        rows = _by_oid(result)
        assert [row["x"] for row in rows] == ["ten", "twenty", None]
        assert all(list(row) == result.columns for row in rows)

    def test_lookup_replacing_its_key_column(self):
        orders, products = _orders(), _products()

        result = orders.derive(pid=lambda r: r.pid.lookup(products, "name"))

        assert result.columns == ["oid", "pid", "code", "qty"]
        assert [row["pid"] for row in _by_oid(result)] == ["ten", "twenty", None]

    def test_lookups_beside_other_derived_columns(self):
        orders, products = _orders(), _products()

        result = orders.derive(
            name=lambda r: r.pid.lookup(products, "name"),
            double=lambda r: r.qty * 2,
            price=lambda r: r.pid.lookup(products, "price"),
        )

        assert result.columns == ["oid", "pid", "code", "qty", "name", "double", "price"]
        rows = _by_oid(result)
        assert [(row["name"], row["double"], row["price"]) for row in rows] == [
            ("ten", 6, 2.0),
            ("twenty", 10, 4.0),
            (None, 2, None),
        ]

    def test_lookups_on_different_keys_of_one_target(self):
        # The second lookup used to reuse the join on the first one's key.
        pairs = LTSeq.from_arrow(pa.table({"oid": [1, 2], "a": [1, 2], "b": [2, 1]}))
        names = LTSeq.from_arrow(pa.table({"k": [1, 2], "v": ["one", "two"]}))

        result = pairs.derive(
            va=lambda r: r.a.lookup(names, "v", join_key="k"),
            vb=lambda r: r.b.lookup(names, "v", join_key="k"),
        )

        assert result.columns == ["oid", "a", "b", "va", "vb"]
        assert [(row["va"], row["vb"]) for row in _by_oid(result)] == [
            ("one", "two"),
            ("two", "one"),
        ]

    @pytest.mark.parametrize("name", ["_lkp0_name", "_lkp0_pid"])
    def test_input_column_named_like_a_join_column_is_kept(self, name):
        # The first join's columns were _lkp0_<column> whatever the input
        # held, so this column made the join fail as ambiguous.
        orders = LTSeq.from_arrow(
            pa.table({"oid": [1, 2], "pid": [10, 99], name: ["a", "b"]})
        )
        products = _products()

        result = orders.derive(x=lambda r: r.pid.lookup(products, "name"))

        assert result.columns == ["oid", "pid", name, "x"]
        assert [(row[name], row["x"]) for row in _by_oid(result)] == [
            ("a", "ten"),
            ("b", None),
        ]

    def test_derived_column_named_like_a_join_column_is_appended(self):
        # It used to take the join column's place, ahead of the lookup.
        orders, products = _orders(), _products()

        result = orders.derive(
            x=lambda r: r.pid.lookup(products, "name"),
            _lkp0_name=lambda r: r.qty + 1,
        )

        assert result.columns == ["oid", "pid", "code", "qty", "x", "_lkp0_name"]
        assert [(row["x"], row["_lkp0_name"]) for row in _by_oid(result)] == [
            ("ten", 4),
            ("twenty", 6),
            (None, 2),
        ]

    def test_joins_skip_names_taken_by_input_and_derived_columns(self):
        # The input takes _lkp0 and a derived column _lkp1, so the joins
        # are _lkp2 and _lkp3.
        pairs = LTSeq.from_arrow(
            pa.table({"oid": [1, 2], "a": [1, 2], "b": [2, 1], "_lkp0_v": ["p", "q"]})
        )
        names = LTSeq.from_arrow(pa.table({"k": [1, 2], "v": ["one", "two"]}))

        result = pairs.derive(
            va=lambda r: r.a.lookup(names, "v", join_key="k"),
            _lkp1_k=lambda r: r.a * 10,
            vb=lambda r: r.b.lookup(names, "v", join_key="k"),
        )

        assert result.columns == ["oid", "a", "b", "_lkp0_v", "va", "_lkp1_k", "vb"]
        assert [
            (row["_lkp0_v"], row["va"], row["_lkp1_k"], row["vb"])
            for row in _by_oid(result)
        ] == [("p", "one", 10, "two"), ("q", "two", 20, "one")]

    def test_lookup_of_a_missing_target_column_names_it(self):
        orders, products = _orders(), _products()

        with pytest.raises(ValueError, match="'nope'") as err:
            orders.derive(x=lambda r: r.pid.lookup(products, "nope"))

        assert "_lkp" not in str(err.value)

    def test_targets_read_from_files_of_one_name(self, tmp_path):
        # Both tables are named "products"; both lookups used to read the
        # second one.
        (tmp_path / "a").mkdir()
        (tmp_path / "b").mkdir()
        (tmp_path / "a" / "products.csv").write_text("pid,name\n10,from_a\n")
        (tmp_path / "b" / "products.csv").write_text("pid,name\n10,from_b\n")
        a = LTSeq.read_csv(str(tmp_path / "a" / "products.csv"))
        b = LTSeq.read_csv(str(tmp_path / "b" / "products.csv"))
        orders = LTSeq.from_arrow(pa.table({"oid": [1], "pid": [10]}))

        result = orders.derive(
            x=lambda r: r.pid.lookup(a, "name"),
            y=lambda r: r.pid.lookup(b, "name"),
        )

        assert result.to_dicts() == [{"oid": 1, "pid": 10, "x": "from_a", "y": "from_b"}]

    @pytest.mark.parametrize("file_name", ["Products.csv", "products.v2.csv"])
    def test_target_file_name_is_not_a_column_name(self, tmp_path, file_name):
        # The join columns were named after the file, and DataFusion read an
        # uppercase or dotted name as another column.
        path = tmp_path / file_name
        path.write_text("pid,name\n10,ten\n")
        products = LTSeq.read_csv(str(path))
        orders = LTSeq.from_arrow(pa.table({"oid": [1], "pid": [10]}))

        result = orders.derive(x=lambda r: r.pid.lookup(products, "name"))

        assert result.to_dicts() == [{"oid": 1, "pid": 10, "x": "ten"}]


def _repeated(column, key):
    # The error names lookup(), the target's key column and a repeated key.
    return rf"lookup\(\): the target's key column '{column}' holds {key} in more than one row"


class TestLookupRepeatedKey:
    """A lookup whose target holds a key in more than one row raises when
    the result executes, instead of repeating each input row that matches
    the key (#351)."""

    @pytest.mark.parametrize(
        "terminal",
        [
            lambda t: t.to_dicts(),
            lambda t: t.count(),
            lambda t: t.select("oid").to_arrow(),
        ],
        ids=["to_dicts", "count", "select"],
    )
    def test_a_repeated_key_raises_when_the_result_executes(self, engine_layout, terminal):
        # The two rows of key 1 used to make one input row two: 2 rows
        # derived 3.
        prices = LTSeq.from_arrow(pa.table({"k": [1, 2, 1], "v": ["x", "z", "y"]}))
        orders = LTSeq.from_arrow(pa.table({"oid": [1, 2], "a": [1, 2]}))

        result = orders.derive(v=lambda r: r.a.lookup(prices, "v", join_key="k"))

        with pytest.raises(ValueError, match=_repeated("k", "1")):
            terminal(result)

    def test_head_returns_no_repeated_row(self, engine_layout):
        # head() may stop before the check reaches the second row of key 1,
        # but that row never reaches the join: order 1 comes back once. The
        # two rows of key 1 come first, so they used to be the first two.
        prices = LTSeq.from_arrow(pa.table({"k": [1, 1, 2], "v": ["x", "y", "z"]}))
        orders = LTSeq.from_arrow(pa.table({"oid": [1, 2], "a": [1, 2]}))

        result = orders.derive(v=lambda r: r.a.lookup(prices, "v", join_key="k"))

        try:
            rows = result.head(2).to_dicts()
        except ValueError as e:
            assert re.search(_repeated("k", "1"), str(e))
        else:
            assert sorted(row["oid"] for row in rows) == [1, 2]

    def test_a_volatile_key_is_checked_as_the_join_reads_it(self):
        # Each execution draws the target's keys afresh. A check that read
        # the target's plan a second time drew other keys than the join, so
        # it could pass while the join saw True twice and returned 3 rows.
        source = LTSeq.from_arrow(pa.table({"id": [1, 2]}))
        target = source.derive(k=lambda r: rand() > 0.5)
        orders = LTSeq.from_arrow(pa.table({"k": [True, False]}))

        result = orders.derive(v=lambda r: r.k.lookup(target, "id", join_key="k"))

        for _ in range(64):
            try:
                rows = result.to_dicts()
            except ValueError as e:
                assert re.search(_repeated("k", "(true|false)"), str(e))
            else:
                # One key drew True and the other False.
                assert len(rows) == 2
                assert {row["v"] for row in rows} == {1, 2}

    def test_a_repeated_key_raises_whichever_input_rows_match(self):
        # A query that reads the target to its end checks every key: no
        # input row has key 1.
        prices = LTSeq.from_arrow(pa.table({"k": [1, 2, 1], "v": ["x", "z", "y"]}))
        orders = LTSeq.from_arrow(pa.table({"oid": [1, 2], "a": [2, 3]}))

        result = orders.derive(v=lambda r: r.a.lookup(prices, "v", join_key="k"))

        with pytest.raises(ValueError, match=_repeated("k", "1")):
            result.to_dicts()

    def test_the_error_names_one_of_several_repeated_keys(self, engine_layout):
        prices = LTSeq.from_arrow(pa.table({"k": [3, 1, 3, 2, 1, 2, 4], "v": list("abcdefg")}))
        orders = LTSeq.from_arrow(pa.table({"oid": [1], "a": [4]}))

        result = orders.derive(v=lambda r: r.a.lookup(prices, "v", join_key="k"))

        with pytest.raises(ValueError, match=_repeated("k", "[123]")):
            result.to_dicts()

    def test_repeated_null_keys_do_not_raise(self):
        # A NULL key matches no input row, so it repeats none.
        prices = LTSeq.from_arrow(
            pa.table({"k": pa.array([None, 2, None], pa.int64()), "v": ["x", "z", "y"]})
        )
        orders = LTSeq.from_arrow(
            pa.table({"oid": [1, 2], "a": pa.array([None, 2], pa.int64())})
        )

        result = orders.derive(v=lambda r: r.a.lookup(prices, "v", join_key="k"))

        assert _by_oid(result) == [
            {"oid": 1, "a": None, "v": None},
            {"oid": 2, "a": 2, "v": "z"},
        ]

    @pytest.mark.parametrize(
        ("target_keys", "input_key", "shown"),
        [
            (["1", "01"], 1, "1"),
            ([0.0, -0.0], 0.0, "-?0"),
            ([2**53, 2**53 + 1], float(2**53), "9007199254740992"),
        ],
        ids=["strings-compared-as-integers", "signed-zeros", "integers-compared-as-floats"],
    )
    def test_keys_the_join_compares_as_equal_repeat(self, target_keys, input_key, shown):
        # Two values of the target's key that the join finds equal to one
        # input key match it twice, as one value in two rows does.
        prices = LTSeq.from_arrow(pa.table({"k": target_keys, "v": ["x", "y"]}))
        orders = LTSeq.from_arrow(pa.table({"oid": [1], "a": [input_key]}))

        result = orders.derive(v=lambda r: r.a.lookup(prices, "v", join_key="k"))

        with pytest.raises(ValueError, match=_repeated("k", shown)):
            result.to_dicts()

    @pytest.mark.parametrize("narrow", [pa.decimal32(5, 2), pa.decimal64(5, 2)], ids=str)
    def test_decimal_keys_the_join_tells_apart_do_not_repeat(self, narrow):
        # Next to an integer key the join compares 1.50 and 1.00 as decimals
        # (#241), so only 1.00 matches 1; as integers both would be 1.
        prices = LTSeq.from_arrow(
            pa.table({"k": pa.array([Decimal("1.50"), Decimal("1.00")], narrow), "v": ["x", "y"]})
        )
        orders = LTSeq.from_arrow(pa.table({"oid": [1, 2], "a": [1, 2]}))

        result = orders.derive(v=lambda r: r.a.lookup(prices, "v", join_key="k"))

        assert [row["v"] for row in _by_oid(result)] == ["y", None]

    def test_a_repeated_key_raises_in_every_input_layout(self, data_layout):
        # The rows of key 1 are the first and the last, so the many-part
        # layouts put them in different batches, files or row groups.
        prices = data_layout(pa.table({"k": [1, 2, 3, 4, 5, 6, 1], "v": list("abcdefg")}))
        orders = LTSeq.from_arrow(pa.table({"oid": [1, 2], "a": [2, 5]}))

        result = orders.derive(v=lambda r: r.a.lookup(prices, "v", join_key="k"))

        with pytest.raises(ValueError, match=_repeated("k", "1")):
            result.to_dicts()

    def test_a_target_with_one_row_per_key_reads_in_every_input_layout(self, data_layout):
        prices = data_layout(pa.table({"k": [1, 2, 3, 4, 5, 6], "v": list("abcdef")}))
        orders = LTSeq.from_arrow(pa.table({"oid": [1, 2, 3], "a": [6, 1, 9]}))

        result = orders.derive(v=lambda r: r.a.lookup(prices, "v", join_key="k"))

        assert [row["v"] for row in _by_oid(result)] == ["f", "a", None]

    def test_only_the_lookup_on_the_repeated_key_raises(self):
        # One target and two keys: "k" names one row each, "g" repeats "a".
        target = LTSeq.from_arrow(
            pa.table({"k": [1, 2, 3], "g": ["a", "b", "a"], "v": ["x", "y", "z"]})
        )
        orders = LTSeq.from_arrow(pa.table({"oid": [1], "a": [2], "code": ["b"]}))

        by_k = orders.derive(v=lambda r: r.a.lookup(target, "v", join_key="k"))
        by_g = orders.derive(v=lambda r: r.code.lookup(target, "v", join_key="g"))

        assert by_k.to_dicts() == [{"oid": 1, "a": 2, "code": "b", "v": "y"}]
        with pytest.raises(ValueError, match=_repeated("g", "'a'")):
            by_g.to_dicts()


def _shuffled_orders(n):
    """`n` orders in a shuffled order, so every order in a result comes from
    a sort. `oid` is unique and `copy` repeats it; `grp` takes 4 values.
    `pid` takes 9, and every 11th one is NULL."""
    oids = list(range(n))
    random.Random(350).shuffle(oids)
    return LTSeq.from_arrow(
        pa.table(
            {
                "oid": oids,
                "copy": oids,
                "grp": [i % 4 for i in oids],
                "pid": pa.array([None if i % 11 == 0 else i % 9 for i in oids], pa.int64()),
            }
        )
    )


def _names(key, n):
    """A target that names keys 0 to n - 1 of column `key`."""
    return LTSeq.from_arrow(pa.table({key: list(range(n)), "name": [f"{key}{i}" for i in range(n)]}))


class TestLookupOrder:
    """A derive with a lookup keeps the table's sort_keys and returns its
    rows in that order, as every derive does (#350)."""

    def test_the_issue_reproduction(self, engine_layout):
        # The result had sort_keys None, and at 2+ partitions its rows came
        # back in the order the join produced them.
        n = 3000
        orders = LTSeq.from_arrow(
            pa.table({"oid": list(range(n)), "pid": [i % 7 for i in range(n)]})
        ).sort("oid", desc=True)
        products = LTSeq.from_arrow(
            pa.table({"pid": list(range(7)), "name": [str(i) for i in range(7)]})
        )

        result = orders.derive(x=lambda r: r.pid.lookup(products, "name"))

        assert result.sort_keys == [("oid", True)]
        rows = result.to_dicts()
        assert [row["oid"] for row in rows] == list(range(n - 1, -1, -1))
        assert all(row["x"] == str(row["pid"]) for row in rows)

    def test_two_lookups_keep_an_order_of_two_keys(self, engine_layout):
        # Each lookup is a join of its own. pid 7 and 8 and the NULL pids
        # match no target row, and those rows keep their place.
        n = 3000
        orders = _shuffled_orders(n).sort("grp", "oid", desc=[True, False])
        by_pid, by_grp = _names("pid", 7), _names("grp", 3)

        result = orders.derive(
            product=lambda r: r.pid.lookup(by_pid, "name"),
            group=lambda r: r.grp.lookup(by_grp, "name"),
        )

        assert result.sort_keys == [("grp", True), ("oid", False)]
        rows = result.to_dicts()
        assert [row["oid"] for row in rows] == sorted(range(n), key=lambda i: (-(i % 4), i))
        for row in rows:
            pid, grp = row["pid"], row["grp"]
            assert row["product"] == (f"pid{pid}" if pid is not None and pid < 7 else None)
            assert row["group"] == (f"grp{grp}" if grp < 3 else None)

    def test_a_window_beside_a_lookup_reads_the_table_order(self, engine_layout):
        # The lookup's join left no sort_keys, so the window raised
        # SortRequiredError on a sorted table.
        products = _names("pid", 2)
        orders = LTSeq.from_arrow(pa.table({"oid": [3, 1, 2], "pid": [1, 0, 0]})).sort("oid")

        result = orders.derive(
            x=lambda r: r.pid.lookup(products, "name"), prev=lambda r: r.oid.shift(1)
        )

        assert result.sort_keys == [("oid", False)]
        assert result.to_dicts() == [
            {"oid": 1, "pid": 0, "x": "pid0", "prev": None},
            {"oid": 2, "pid": 0, "x": "pid0", "prev": 1},
            {"oid": 3, "pid": 1, "x": "pid1", "prev": 2},
        ]

    def test_a_lookup_replacing_a_sort_key_keeps_the_input_order(self, engine_layout):
        # Replacing oid ends sort_keys before it, as any derive does; the
        # rows still come in the order of both keys.
        n = 3000
        orders = _shuffled_orders(n).sort("grp", "oid")
        products = _names("pid", 9)

        result = orders.derive(oid=lambda r: r.pid.lookup(products, "name"))

        assert result.sort_keys == [("grp", False)]
        assert [row["copy"] for row in result.to_dicts()] == sorted(
            range(n), key=lambda i: (i % 4, i)
        )

    def test_a_large_target_keeps_the_order(self, engine_config):
        # A target this large is past DataFusion's limit for collecting the
        # join's build side into one partition, so the join hash-partitions
        # both sides on the key.
        engine_config(target_partitions=2 * CPUS)
        n, keys = 20_000, 200_000
        orders = LTSeq.from_arrow(
            pa.table({"oid": list(range(n)), "pid": [(i * 7919) % keys for i in range(n)]})
        ).sort("oid", desc=True)
        products = LTSeq.from_arrow(
            pa.table({"pid": list(range(keys)), "price": [float(i) for i in range(keys)]})
        )

        result = orders.derive(price=lambda r: r.pid.lookup(products, "price"))

        rows = result.to_dicts()
        assert [row["oid"] for row in rows] == list(range(n - 1, -1, -1))
        assert all(row["price"] == row["pid"] for row in rows)
