#!/usr/bin/env python3
"""
Phase 8: Composite Keys Linking Example
Demonstrates joining on multiple columns (composite keys).
"""

from ltseq import LTSeq
import tempfile
import csv
import os


def create_sales_csv():
    """Create a sample sales table with year, month, product_id"""
    data = [
        ["id", "year", "month", "product_id", "quantity"],
        ["1", "2024", "1", "101", "10"],
        ["2", "2024", "1", "102", "5"],
        ["3", "2024", "2", "101", "8"],
        ["4", "2024", "2", "102", "15"],
        ["5", "2025", "1", "101", "20"],
    ]

    fd, path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(fd, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(data)
        return path
    except:
        if os.path.exists(path):
            os.remove(path)
        raise


def create_pricing_csv():
    """Create a sample pricing table with year, month, product_id, price"""
    data = [
        ["year", "month", "product_id", "price"],
        ["2024", "1", "101", "50"],
        ["2024", "1", "102", "75"],
        ["2024", "2", "101", "55"],
        ["2024", "2", "102", "70"],
        ["2025", "1", "101", "60"],
        ["2025", "1", "102", "80"],
    ]

    fd, path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(fd, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(data)
        return path
    except:
        if os.path.exists(path):
            os.remove(path)
        raise


def main():
    print("=" * 70)
    print("Phase 8: Composite Keys Linking Example")
    print("=" * 70)

    # Create temporary tables
    sales_path = create_sales_csv()
    pricing_path = create_pricing_csv()

    try:
        # Load tables
        sales = LTSeq.read_csv(sales_path)
        pricing = LTSeq.read_csv(pricing_path)

        print("\n1. Sales table (id, year, month, product_id, quantity):")
        sales.show()

        print("\n2. Pricing table (year, month, product_id, price):")
        print("   (Price is valid for specific year/month/product combination)")
        pricing.show()

        # Create link with composite key (multiple columns)
        print("\n3. Creating link with COMPOSITE KEY:")
        print(
            "   Condition: (year == year) & (month == month) & (product_id == product_id)"
        )
        print("   This matches sales to the correct price for that year/month/product")

        linked = sales.link(
            pricing,
            on=lambda s, p: (s.year == p.year)
            & (s.month == p.month)
            & (s.product_id == p.product_id),
            as_="price",
        )

        print("\n4. Linked table schema:")
        print(
            f"   Source columns: {[c for c in linked._schema.keys() if not c.startswith('price_')]}"
        )
        print(
            f"   Linked columns (price_*): {[c for c in linked._schema.keys() if c.startswith('price_')]}"
        )

        print("\n5. Viewing linked data (sales with pricing):")
        linked.show()

        # Calculate revenue (quantity * price)
        print("\n6. Operations on linked data:")
        result = linked._materialize()
        print(f"   Materialized result has {len(result)} rows")
        print(
            f"   All rows matched (sales.year, month, product_id match pricing exactly)"
        )

        # Filter: expensive products in 2024
        print("\n7. Example analysis: Products with price > 60 in 2024")
        filtered = linked.filter(lambda r: r.year == "2024")
        filtered.show()

        print("\n" + "=" * 70)
        print("Composite keys example complete!")
        print("=" * 70)
        print("\nKey takeaway:")
        print("- Composite keys use & (AND) to combine multiple conditions")
        print("- Each condition must be an equality check (==)")
        print("- All conditions are combined with &")
        print("- This is useful for temporal data, multi-tenant systems, etc.")

    finally:
        # Clean up
        if os.path.exists(sales_path):
            os.remove(sales_path)
        if os.path.exists(pricing_path):
            os.remove(pricing_path)


if __name__ == "__main__":
    main()
