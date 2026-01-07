"""
Advanced Linking Examples - Phases 10, 11, and 13

This module demonstrates advanced linking patterns introduced in:
- Phase 10: Filtering on linked table columns with transparent materialization
- Phase 11: Selecting from linked table columns with transparent materialization
- Phase 13: Aggregating linked table data (coming soon)

These examples show how to work with linked tables without explicit materialization.
"""

from ltseq import LTSeq


def phase_10_filter_on_linked_columns():
    """
    Phase 10: Filter rows using conditions on linked table columns.

    The filter() method automatically detects when you reference linked columns
    (like r.prod_price) and materializes the join transparently.
    """
    print("\n" + "=" * 60)
    print("PHASE 10: Filtering on Linked Columns")
    print("=" * 60)

    # Load tables
    orders = LTSeq.read_csv("examples/orders.csv")
    products = LTSeq.read_csv("examples/products.csv")

    # Create linked table
    linked = orders.link(
        products, on=lambda o, p: o.product_id == p.product_id, as_="prod"
    )

    print("\n1. Filter on source columns (no materialization needed):")
    filtered = linked.filter(lambda r: r.quantity > 2)
    print(f"   Orders with quantity > 2: {len(filtered)} rows")
    filtered.show(3)

    print("\n2. Filter on linked columns (automatic materialization):")
    expensive = linked.filter(lambda r: r.prod_price > 100)
    print(f"   Orders with expensive products (price > 100): {len(expensive)} rows")
    expensive.show(3)

    print("\n3. Filter by product name (string comparison):")
    widgets = linked.filter(lambda r: r.prod_name == "Widget")
    print(f"   Orders for Widgets: {len(widgets)} rows")
    widgets.show(3)

    print("\n4. Complex filtering (single linked column condition):")
    # Note: For complex mixed conditions, filter multiple times
    mid_price = linked.filter(lambda r: r.prod_price > 50)
    mid_price = mid_price.filter(lambda r: r.quantity < 5)
    print(f"   Orders with price > 50 AND quantity < 5: {len(mid_price)} rows")
    mid_price.show(3)


def phase_11_select_from_linked_columns():
    """
    Phase 11: Select specific columns from linked tables.

    The select() method automatically detects when you reference linked columns
    and materializes the join transparently.
    """
    print("\n" + "=" * 60)
    print("PHASE 11: Selecting from Linked Columns")
    print("=" * 60)

    # Load tables
    orders = LTSeq.read_csv("examples/orders.csv")
    products = LTSeq.read_csv("examples/products.csv")

    # Create linked table
    linked = orders.link(
        products, on=lambda o, p: o.product_id == p.product_id, as_="prod"
    )

    print("\n1. Select source columns only:")
    selected = linked.select("id", "quantity")
    print(f"   Schema: {list(selected._schema.keys())}")
    selected.show(3)

    print("\n2. Select linked columns (automatic materialization):")
    selected = linked.select("prod_name", "prod_price")
    print(f"   Columns: prod_name, prod_price")
    selected.show(3)

    print("\n3. Select mixed source and linked columns:")
    selected = linked.select("id", "quantity", "prod_name", "prod_price")
    print(f"   Columns: id, quantity, prod_name, prod_price")
    selected.show(3)

    print("\n4. Select and then filter (chaining operations):")
    selected = linked.select("id", "prod_name", "prod_price")
    filtered = selected.filter(lambda r: r.prod_price > 100)
    print(
        f"   Selected {len(selected)} rows, filtered to {len(filtered)} expensive items"
    )
    filtered.show(3)


def phase_9_chained_linking():
    """
    Phase 9: Multi-level linking with intermediate materialization.

    Phase 9 fixed a bug that prevented materializing intermediate join results.
    Now you can materialize between link operations.
    """
    print("\n" + "=" * 60)
    print("PHASE 9: Chained Linking with Materialization")
    print("=" * 60)

    # Load all three tables
    orders = LTSeq.read_csv("examples/orders.csv")
    products = LTSeq.read_csv("examples/products.csv")
    categories = LTSeq.read_csv("examples/categories.csv")

    print("\n1. Link orders → products:")
    linked1 = orders.link(
        products, on=lambda o, p: o.product_id == p.product_id, as_="prod"
    )
    print(f"   Created linked table with schema: {list(linked1._schema.keys())}")

    print("\n2. Materialize first link:")
    mat1 = linked1._materialize()
    print(f"   Materialized to LTSeq with {len(mat1)} rows")
    print(f"   Schema: {list(mat1._schema.keys())}")

    print("\n3. Link materialized result → categories:")
    # For simplicity in this example, we'd need to adjust the join condition
    # In real use, you'd have proper category_id in products
    print("   (In practice, join using appropriate columns from both tables)")

    print("\n4. Alternative: Chain without explicit materialization:")
    linked2 = orders.link(
        products, on=lambda o, p: o.product_id == p.product_id, as_="prod"
    )
    result = linked2._materialize()
    print(f"   Final result: {len(result)} rows")
    result.show(3)


def phase_13_aggregate_linked_data():
    """
    Phase 13: Aggregate data from linked tables.

    The aggregate() method automatically materializes the join and then
    performs aggregation on the result.

    Note: This is a preview of Phase 13 functionality.
    """
    print("\n" + "=" * 60)
    print("PHASE 13: Aggregating Linked Data (Preview)")
    print("=" * 60)

    # Load tables
    orders = LTSeq.read_csv("examples/orders.csv")
    products = LTSeq.read_csv("examples/products.csv")

    # Create linked table
    linked = orders.link(
        products, on=lambda o, p: o.product_id == p.product_id, as_="prod"
    )

    print("\n1. Basic aggregation patterns:")
    print("   linked.aggregate({'total_orders': 'count'})")
    print("   linked.aggregate({'avg_quantity': 'avg', 'max_quantity': 'max'})")

    print("\n2. Common analytical patterns:")
    print("   # Get summary statistics per product")
    print("   linked.filter(lambda r: r.prod_price > 100)")
    print("          .select('prod_name', 'quantity')")
    print("          .group_by('prod_name')")
    print("          .aggregate({'total_quantity': 'sum'})")

    print("\n   # Count orders by product")
    print("   linked.select('prod_name')")
    print("         .group_by('prod_name')")
    print("         .aggregate({'order_count': 'count'})")


def comparison_manual_vs_transparent():
    """
    Show the difference between manual materialization and transparent.
    """
    print("\n" + "=" * 60)
    print("Manual vs Transparent Materialization")
    print("=" * 60)

    orders = LTSeq.read_csv("examples/orders.csv")
    products = LTSeq.read_csv("examples/products.csv")

    # Manual (old way - still works)
    print("\n1. MANUAL materialization:")
    print("   Before Phase 10-11:")
    print("   linked = orders.link(..., as_='prod')")
    print("   mat = linked._materialize()")
    print("   result = mat.filter(lambda r: r.prod_price > 100)")

    linked = orders.link(
        products, on=lambda o, p: o.product_id == p.product_id, as_="prod"
    )
    mat = linked._materialize()
    result = mat.filter(lambda r: r.prod_price > 100)
    print(f"   Result: {len(result)} rows")

    # Transparent (new way - Phase 10+)
    print("\n2. TRANSPARENT materialization (Phase 10+):")
    print("   linked.filter(lambda r: r.prod_price > 100)")

    linked = orders.link(
        products, on=lambda o, p: o.product_id == p.product_id, as_="prod"
    )
    result = linked.filter(lambda r: r.prod_price > 100)
    print(f"   Result: {len(result)} rows (same as manual!)")

    print("\n✓ Both approaches work. Use transparent approach for cleaner code.")


if __name__ == "__main__":
    print("=" * 60)
    print("Advanced Linking Examples - Phases 9-13")
    print("=" * 60)

    phase_10_filter_on_linked_columns()
    phase_11_select_from_linked_columns()
    phase_9_chained_linking()
    phase_13_aggregate_linked_data()
    comparison_manual_vs_transparent()

    print("\n" + "=" * 60)
    print("Examples complete!")
    print("=" * 60)
