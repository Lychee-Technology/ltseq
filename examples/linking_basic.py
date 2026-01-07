#!/usr/bin/env python3
"""
Phase 8: Basic Linking Example
Demonstrates simple foreign key relationship between orders and products.
"""

from ltseq import LTSeq


def main():
    print("=" * 60)
    print("Phase 8: Basic Linking Example")
    print("=" * 60)

    # Load tables
    print("\n1. Loading tables...")
    orders = LTSeq.read_csv("examples/orders.csv")
    products = LTSeq.read_csv("examples/products.csv")

    print("\nOrders table:")
    orders.show(3)

    print("\nProducts table:")
    products.show(3)

    # Create a link
    print("\n2. Creating a link (orders -> products)...")
    print("   Condition: orders.product_id == products.product_id")
    print("   Alias: 'prod'")
    linked = orders.link(
        products, on=lambda o, p: o.product_id == p.product_id, as_="prod"
    )

    # Show linked schema
    print("\n3. Linked table schema:")
    print(f"   Columns: {list(linked._schema.keys())}")
    print(f"   Source columns: id, product_id, quantity")
    print(f"   Linked columns (with 'prod' prefix):")
    for col in sorted(linked._schema.keys()):
        if col.startswith("prod_"):
            print(f"     - {col}")

    # Show linked data (this materializes the join)
    print("\n4. Viewing linked data (first 5 rows):")
    linked.show(5)

    # Filter on source columns
    print("\n5. Filter for high-quantity orders (quantity > 3):")
    high_qty = linked.filter(lambda r: r.quantity > 3)
    high_qty.show()

    # Filter for specific products
    print("\n6. Filter for product_id = 101:")
    prod_101 = linked.filter(lambda r: r.product_id == 101)
    prod_101.show()

    # Materialize if needed for further processing
    print("\n7. Materializing the link for further processing...")
    result = linked._materialize()
    print(f"   Result type: {type(result).__name__}")
    print(f"   Row count: {len(result)}")
    print("\nMaterialized data (first 3 rows):")
    result.show(3)

    # Access materialized schema
    print("\n8. Accessing materialized columns:")
    print(f"   Available columns: {list(result._schema.keys())}")

    print("\n" + "=" * 60)
    print("Basic linking example complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
