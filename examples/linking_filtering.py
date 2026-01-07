#!/usr/bin/env python3
"""
Phase 8: Filtering Example
Demonstrates various filtering patterns with linked tables.
"""

from ltseq import LTSeq


def main():
    print("=" * 70)
    print("Phase 8: Filtering Patterns with Linked Tables")
    print("=" * 70)

    # Load tables
    orders = LTSeq.read_csv("examples/orders.csv")
    products = LTSeq.read_csv("examples/products.csv")

    print("\n1. Original data:")
    print("\nOrders (id, product_id, quantity):")
    orders.show(8)

    print("\nProducts (product_id, name, price):")
    products.show()

    # Create link
    print("\n2. Creating link: orders -> products")
    linked = orders.link(
        products, on=lambda o, p: o.product_id == p.product_id, as_="prod"
    )

    # Pattern 1: Filter on source columns (fast - no join)
    print("\n" + "=" * 70)
    print("PATTERN 1: Filter on SOURCE columns (fast, no join)")
    print("=" * 70)
    print("Example: Find orders with quantity > 5")
    high_qty = linked.filter(lambda r: r.quantity > 5)
    print(f"Result is still a LinkedTable: {type(high_qty).__name__}")
    print("\nData:")
    high_qty.show()

    # Pattern 2: Multiple conditions on source
    print("\n" + "=" * 70)
    print("PATTERN 2: Multiple conditions on source columns")
    print("=" * 70)
    print("Example: Find orders with 3 <= quantity <= 5")
    mid_qty = linked.filter(lambda r: r.quantity >= 3)
    print("\nData (quantity >= 3):")
    mid_qty.show()

    # Pattern 3: Filter specific product
    print("\n" + "=" * 70)
    print("PATTERN 3: Filter for specific product")
    print("=" * 70)
    print("Example: Find all orders for product_id = 101")
    prod_101 = linked.filter(lambda r: r.product_id == 101)
    print("\nData (product_id = 101):")
    prod_101.show()

    # Pattern 4: Combine filter and materialization
    print("\n" + "=" * 70)
    print("PATTERN 4: Filter, then materialize for further processing")
    print("=" * 70)
    print("Example: Get detailed info for high-value orders")

    filtered = linked.filter(lambda r: r.quantity > 3)
    materialized = filtered._materialize()

    print(f"Filtered result type: {type(filtered).__name__}")
    print(f"Materialized result type: {type(materialized).__name__}")
    print(f"Row count: {len(materialized)}")
    print("\nData with all columns:")
    materialized.show()

    # Pattern 5: Chained filtering
    print("\n" + "=" * 70)
    print("PATTERN 5: Chained filtering")
    print("=" * 70)
    print("Example: Multiple filter steps")

    # First filter: high quantity
    step1 = linked.filter(lambda r: r.quantity > 2)
    print(f"After first filter (quantity > 2): {len(step1._source)} rows in source")

    # Second filter: another condition
    step2 = step1.filter(lambda r: r.quantity < 10)
    print(f"After second filter (quantity < 10): {len(step2._source)} rows in source")

    print("\nFinal filtered data:")
    step2.show()

    # Pattern 6: Show filtered and materialized
    print("\n" + "=" * 70)
    print("PATTERN 6: View filtered/materialized data")
    print("=" * 70)

    filtered_linked = linked.filter(lambda r: r.quantity > 4)
    print("Using show() on filtered linked table:")
    filtered_linked.show()

    # Performance note
    print("\n" + "=" * 70)
    print("PERFORMANCE NOTES")
    print("=" * 70)
    print("✓ Filter on source columns BEFORE _materialize() is fast")
    print("  - Join only happens on filtered rows")
    print("  - Reduces data to process")
    print("\n✗ Filter on materialized result requires full join first")
    print("  - All rows joined before filtering")
    print("  - Less efficient for large tables")
    print("\nRECOMMENDATION:")
    print("  filtered = linked.filter(lambda r: r.quantity > 5)  # ✓ Good")
    print("  result = filtered._materialize()  # ✓ Then materialize")

    print("\n" + "=" * 70)
    print("Filtering example complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
