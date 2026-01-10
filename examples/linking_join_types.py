#!/usr/bin/env python3
"""
Phase 8: Join Types Example
Demonstrates all four join types: INNER, LEFT, RIGHT, FULL
"""

from ltseq import LTSeq


def main():
    print("=" * 70)
    print("Phase 8: Join Types Example (INNER, LEFT, RIGHT, FULL)")
    print("=" * 70)

    # Load tables
    orders = LTSeq.read_csv("examples/orders.csv")
    products = LTSeq.read_csv("examples/products.csv")

    print("\nOrders table:")
    orders.show(8)

    print("\nProducts table:")
    products.show(4)

    # INNER join
    print("\n" + "=" * 70)
    print("1. INNER JOIN (default)")
    print("   - Only rows where product_id matches in both tables")
    print("=" * 70)
    inner = orders.link(
        products,
        on=lambda o, p: o.product_id == p.product_id,
        as_="prod",
        join_type="inner",
    )
    print(f"   Join type: {inner._join_type}")
    print(f"   Result:")
    inner.show()

    # LEFT join
    print("\n" + "=" * 70)
    print("2. LEFT JOIN")
    print("   - All orders kept, NULLs for unmatched products")
    print("=" * 70)
    left = orders.link(
        products,
        on=lambda o, p: o.product_id == p.product_id,
        as_="prod",
        join_type="left",
    )
    print(f"   Join type: {left._join_type}")
    print(f"   Result:")
    left.show()

    # RIGHT join
    print("\n" + "=" * 70)
    print("3. RIGHT JOIN")
    print("   - All products kept, NULLs for unmatched orders")
    print("=" * 70)
    right = orders.link(
        products,
        on=lambda o, p: o.product_id == p.product_id,
        as_="prod",
        join_type="right",
    )
    print(f"   Join type: {right._join_type}")
    print(f"   Result:")
    right.show()

    # FULL join
    print("\n" + "=" * 70)
    print("4. FULL JOIN (OUTER)")
    print("   - All rows from both tables, NULLs where no match")
    print("=" * 70)
    full = orders.link(
        products,
        on=lambda o, p: o.product_id == p.product_id,
        as_="prod",
        join_type="full",
    )
    print(f"   Join type: {full._join_type}")
    print(f"   Result:")
    full.show()

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    inner_result = inner._materialize()
    left_result = left._materialize()
    right_result = right._materialize()
    full_result = full._materialize()

    print(f"INNER: {len(inner_result)} rows")
    print(f"LEFT:  {len(left_result)} rows")
    print(f"RIGHT: {len(right_result)} rows")
    print(f"FULL:  {len(full_result)} rows")

    print("\nKey observations:")
    print(f"- LEFT (n={len(left_result)}) >= INNER (n={len(inner_result)}) ✓")
    print(f"- RIGHT (n={len(right_result)}) >= INNER (n={len(inner_result)}) ✓")
    print(f"- FULL (n={len(full_result)}) >= LEFT (n={len(left_result)}) ✓")
    print(f"- FULL (n={len(full_result)}) >= RIGHT (n={len(right_result)}) ✓")

    print("\n" + "=" * 70)
    print("Join types example complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
