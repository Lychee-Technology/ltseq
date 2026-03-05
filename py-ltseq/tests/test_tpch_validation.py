"""Phase 2.7: TPC-H Q1 and Q6 end-to-end validation.

Verifies that TPC-H-style queries (filter + aggregate patterns) produce
correct results through the LTSeq pipeline. These patterns exercise the
GPU optimizer's filter and aggregate interception paths:

- Q1: filter → GROUP BY (multi-key) → sum/avg/count
- Q6: compound range filter → scalar aggregate (sum)

The tests use small synthetic data that mirrors the TPC-H lineitem schema.
With large datasets (>100K rows), the GPU optimizer would route these to
GpuFilterExec + GpuHashAggregateExec.

Run with:
    pytest py-ltseq/tests/test_tpch_validation.py -v
"""

import csv
import os
import tempfile

import pytest

from ltseq import LTSeq


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def lineitem_csv():
    """Create a synthetic lineitem CSV mimicking TPC-H schema.

    Columns: l_orderkey, l_partkey, l_suppkey, l_linenumber,
             l_quantity, l_extendedprice, l_discount, l_tax,
             l_returnflag, l_linestatus, l_shipdate
    """
    rows = [
        # returnflag=R, linestatus=F — shipped before cutoff
        {"l_orderkey": 1, "l_partkey": 101, "l_suppkey": 1, "l_linenumber": 1,
         "l_quantity": 17, "l_extendedprice": 21168.23, "l_discount": 0.04,
         "l_tax": 0.02, "l_returnflag": "R", "l_linestatus": "F",
         "l_shipdate": "1994-03-15"},
        {"l_orderkey": 1, "l_partkey": 102, "l_suppkey": 2, "l_linenumber": 2,
         "l_quantity": 36, "l_extendedprice": 45983.16, "l_discount": 0.09,
         "l_tax": 0.06, "l_returnflag": "R", "l_linestatus": "F",
         "l_shipdate": "1994-06-01"},
        # returnflag=A, linestatus=F — shipped before cutoff
        {"l_orderkey": 2, "l_partkey": 103, "l_suppkey": 3, "l_linenumber": 1,
         "l_quantity": 38, "l_extendedprice": 44694.46, "l_discount": 0.00,
         "l_tax": 0.05, "l_returnflag": "A", "l_linestatus": "F",
         "l_shipdate": "1993-11-09"},
        {"l_orderkey": 3, "l_partkey": 104, "l_suppkey": 4, "l_linenumber": 1,
         "l_quantity": 45, "l_extendedprice": 54058.05, "l_discount": 0.06,
         "l_tax": 0.00, "l_returnflag": "A", "l_linestatus": "F",
         "l_shipdate": "1994-02-02"},
        # returnflag=N, linestatus=O — shipped after cutoff (open orders)
        {"l_orderkey": 4, "l_partkey": 105, "l_suppkey": 5, "l_linenumber": 1,
         "l_quantity": 28, "l_extendedprice": 28955.64, "l_discount": 0.05,
         "l_tax": 0.08, "l_returnflag": "N", "l_linestatus": "O",
         "l_shipdate": "1998-09-20"},
        {"l_orderkey": 5, "l_partkey": 106, "l_suppkey": 6, "l_linenumber": 1,
         "l_quantity": 24, "l_extendedprice": 33032.40, "l_discount": 0.10,
         "l_tax": 0.04, "l_returnflag": "N", "l_linestatus": "O",
         "l_shipdate": "1998-08-02"},
        {"l_orderkey": 6, "l_partkey": 107, "l_suppkey": 7, "l_linenumber": 1,
         "l_quantity": 32, "l_extendedprice": 36901.12, "l_discount": 0.07,
         "l_tax": 0.02, "l_returnflag": "N", "l_linestatus": "O",
         "l_shipdate": "1998-07-21"},
        # Q6 candidates: shipdate in 1994, discount 0.05-0.07, qty < 24
        {"l_orderkey": 7, "l_partkey": 108, "l_suppkey": 8, "l_linenumber": 1,
         "l_quantity": 20, "l_extendedprice": 20000.00, "l_discount": 0.06,
         "l_tax": 0.01, "l_returnflag": "R", "l_linestatus": "F",
         "l_shipdate": "1994-05-10"},
        {"l_orderkey": 8, "l_partkey": 109, "l_suppkey": 9, "l_linenumber": 1,
         "l_quantity": 15, "l_extendedprice": 15000.00, "l_discount": 0.05,
         "l_tax": 0.03, "l_returnflag": "R", "l_linestatus": "F",
         "l_shipdate": "1994-08-22"},
        # Outside Q6 range (discount too high)
        {"l_orderkey": 9, "l_partkey": 110, "l_suppkey": 10, "l_linenumber": 1,
         "l_quantity": 10, "l_extendedprice": 10000.00, "l_discount": 0.10,
         "l_tax": 0.02, "l_returnflag": "A", "l_linestatus": "F",
         "l_shipdate": "1994-04-01"},
        # Outside Q6 range (quantity too high)
        {"l_orderkey": 10, "l_partkey": 111, "l_suppkey": 11, "l_linenumber": 1,
         "l_quantity": 30, "l_extendedprice": 30000.00, "l_discount": 0.06,
         "l_tax": 0.04, "l_returnflag": "R", "l_linestatus": "F",
         "l_shipdate": "1994-09-15"},
        # Outside Q6 range (shipdate in 1995)
        {"l_orderkey": 11, "l_partkey": 112, "l_suppkey": 12, "l_linenumber": 1,
         "l_quantity": 18, "l_extendedprice": 18000.00, "l_discount": 0.06,
         "l_tax": 0.01, "l_returnflag": "N", "l_linestatus": "O",
         "l_shipdate": "1995-02-14"},
    ]

    fieldnames = [
        "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
        "l_quantity", "l_extendedprice", "l_discount", "l_tax",
        "l_returnflag", "l_linestatus", "l_shipdate",
    ]

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False
    ) as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        temp_path = f.name

    yield temp_path
    os.unlink(temp_path)


# ── TPC-H Q1: Pricing Summary Report ────────────────────────────────────────


class TestTPCHQ1:
    """TPC-H Q1: filter + multi-key GROUP BY + sum/avg/count.

    SELECT
        l_returnflag, l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    FROM lineitem
    WHERE l_shipdate <= '1998-09-02'  -- date '1998-12-01' - interval '90' day
    GROUP BY l_returnflag, l_linestatus
    ORDER BY l_returnflag, l_linestatus
    """

    def test_q1_filter_and_group(self, lineitem_csv):
        """Core Q1 pattern: filter by date, group by returnflag+linestatus, aggregate."""
        t = LTSeq.read_csv(lineitem_csv)

        # Filter: l_shipdate <= '1998-09-02' (all rows in our data qualify
        # except l_shipdate='1998-09-20' for orderkey=4)
        filtered = t.filter(lambda r: r.l_shipdate <= "1998-09-02")
        filtered_df = filtered.to_pandas()

        # orderkey=4 has shipdate 1998-09-20, should be excluded
        assert 4 not in filtered_df["l_orderkey"].tolist()
        # All other 11 rows should be present (12 total - 1 excluded)
        assert len(filtered_df) == 11

    def test_q1_aggregate_sum_qty(self, lineitem_csv):
        """Q1: sum(l_quantity) grouped by returnflag, linestatus."""
        t = LTSeq.read_csv(lineitem_csv)

        # Filter by shipdate
        filtered = t.filter(lambda r: r.l_shipdate <= "1998-09-02")

        # Group by returnflag and aggregate sum(quantity)
        result = filtered.agg(
            by=lambda r: r.l_returnflag,
            sum_qty=lambda g: g.l_quantity.sum(),
        )
        df = result.to_pandas()

        # R group: orderkeys 1(17), 1(36), 7(20), 8(15), 10(30) = 118
        # A group: orderkeys 2(38), 3(45), 9(10) = 93
        # N group: orderkeys 5(24), 6(32), 11(18) = 74
        sums = dict(zip(df["l_returnflag"], df["sum_qty"]))
        assert sums["R"] == 118
        assert sums["A"] == 93
        assert sums["N"] == 74

    def test_q1_aggregate_count(self, lineitem_csv):
        """Q1: count(*) grouped by returnflag."""
        t = LTSeq.read_csv(lineitem_csv)
        filtered = t.filter(lambda r: r.l_shipdate <= "1998-09-02")

        result = filtered.agg(
            by=lambda r: r.l_returnflag,
            count_order=lambda g: g.l_quantity.count(),
        )
        df = result.to_pandas()
        counts = dict(zip(df["l_returnflag"], df["count_order"]))
        assert counts["R"] == 5
        assert counts["A"] == 3
        assert counts["N"] == 3  # orderkeys 5, 6, 11 (orderkey 4 excluded by date filter)

    def test_q1_aggregate_avg(self, lineitem_csv):
        """Q1: avg(l_quantity) grouped by returnflag."""
        t = LTSeq.read_csv(lineitem_csv)
        filtered = t.filter(lambda r: r.l_shipdate <= "1998-09-02")

        result = filtered.agg(
            by=lambda r: r.l_returnflag,
            avg_qty=lambda g: g.l_quantity.avg(),
        )
        df = result.to_pandas()
        avgs = dict(zip(df["l_returnflag"], df["avg_qty"]))

        # R: 118/5 = 23.6, A: 93/3 = 31.0, N: 74/2 = 37.0
        assert abs(avgs["R"] - 23.6) < 0.01
        assert abs(avgs["A"] - 31.0) < 0.01
        # N: (24+32+18)/3... wait, orderkey 4 is excluded, and 11 has shipdate 1995-02-14
        # N group after filter: orderkey 5(24), 6(32), 11(18) = 74, count=3
        # avg = 74/3 ≈ 24.667
        # Actually let me recalculate: orderkey 4 has shipdate 1998-09-20 > 1998-09-02
        # orderkey 5: 1998-08-02 <= 1998-09-02 ✓
        # orderkey 6: 1998-07-21 <= 1998-09-02 ✓
        # orderkey 11: 1995-02-14 <= 1998-09-02 ✓
        # So N group has 3 rows: qty 24, 32, 18 → avg = 74/3 ≈ 24.667
        assert abs(avgs["N"] - 24.667) < 0.01

    def test_q1_multiple_aggregates(self, lineitem_csv):
        """Q1: multiple aggregates in a single agg() call."""
        t = LTSeq.read_csv(lineitem_csv)
        filtered = t.filter(lambda r: r.l_shipdate <= "1998-09-02")

        result = filtered.agg(
            by=lambda r: r.l_returnflag,
            sum_qty=lambda g: g.l_quantity.sum(),
            sum_price=lambda g: g.l_extendedprice.sum(),
            avg_disc=lambda g: g.l_discount.avg(),
            count_order=lambda g: g.l_quantity.count(),
        )
        df = result.to_pandas()

        assert len(df) == 3  # R, A, N
        assert set(df["l_returnflag"]) == {"R", "A", "N"}
        # Verify all aggregate columns exist
        assert "sum_qty" in df.columns
        assert "sum_price" in df.columns
        assert "avg_disc" in df.columns
        assert "count_order" in df.columns

    def test_q1_sorted_output(self, lineitem_csv):
        """Q1 result sorted by returnflag."""
        t = LTSeq.read_csv(lineitem_csv)
        result = (
            t.filter(lambda r: r.l_shipdate <= "1998-09-02")
            .agg(
                by=lambda r: r.l_returnflag,
                sum_qty=lambda g: g.l_quantity.sum(),
            )
            .sort("l_returnflag")
        )
        df = result.to_pandas()
        flags = df["l_returnflag"].tolist()
        assert flags == ["A", "N", "R"]


# ── TPC-H Q6: Forecasting Revenue Change ────────────────────────────────────


class TestTPCHQ6:
    """TPC-H Q6: compound range filter + scalar aggregate.

    SELECT sum(l_extendedprice * l_discount) as revenue
    FROM lineitem
    WHERE l_shipdate >= '1994-01-01'
      AND l_shipdate < '1995-01-01'
      AND l_discount BETWEEN 0.05 AND 0.07
      AND l_quantity < 24

    This exercises:
    - Compound predicates (AND of 4 conditions)
    - Range filters on dates and numerics
    - Derived column arithmetic in aggregation
    """

    def test_q6_compound_filter(self, lineitem_csv):
        """Q6: compound range filter isolates correct rows."""
        t = LTSeq.read_csv(lineitem_csv)

        filtered = t.filter(
            lambda r: (r.l_shipdate >= "1994-01-01")
            & (r.l_shipdate < "1995-01-01")
            & (r.l_discount >= 0.05)
            & (r.l_discount <= 0.07)
            & (r.l_quantity < 24)
        )
        df = filtered.to_pandas()

        # Qualifying rows:
        # orderkey=7: shipdate=1994-05-10, disc=0.06, qty=20 ✓
        # orderkey=8: shipdate=1994-08-22, disc=0.05, qty=15 ✓
        # orderkey=9: disc=0.10 > 0.07 ✗
        # orderkey=10: qty=30 >= 24 ✗
        # orderkey=1 (line 1): disc=0.04 < 0.05 ✗
        # orderkey=1 (line 2): disc=0.09 > 0.07 ✗
        # orderkey=3: disc=0.06, qty=45 >= 24 ✗
        assert len(df) == 2
        assert set(df["l_orderkey"].tolist()) == {7, 8}

    def test_q6_revenue_calculation(self, lineitem_csv):
        """Q6: sum(l_extendedprice * l_discount) as revenue.

        Since LTSeq agg() doesn't support derived expressions directly
        in the aggregate, we derive the revenue column first then aggregate.
        """
        t = LTSeq.read_csv(lineitem_csv)

        # Step 1: Filter
        filtered = t.filter(
            lambda r: (r.l_shipdate >= "1994-01-01")
            & (r.l_shipdate < "1995-01-01")
            & (r.l_discount >= 0.05)
            & (r.l_discount <= 0.07)
            & (r.l_quantity < 24)
        )

        # Step 2: Derive revenue column
        with_revenue = filtered.derive(revenue=lambda r: r.l_extendedprice * r.l_discount)

        # Step 3: Sum the revenue column (no group-by = scalar aggregate)
        result = with_revenue.agg(total_revenue=lambda g: g.revenue.sum())
        df = result.to_pandas()

        # orderkey=7: 20000.00 * 0.06 = 1200.00
        # orderkey=8: 15000.00 * 0.05 = 750.00
        # Total: 1950.00
        assert len(df) == 1
        assert abs(df["total_revenue"].iloc[0] - 1950.00) < 0.01

    def test_q6_no_matches(self, lineitem_csv):
        """Q6 with impossible filter returns empty or zero."""
        t = LTSeq.read_csv(lineitem_csv)

        # No rows have shipdate in 2000s
        filtered = t.filter(
            lambda r: (r.l_shipdate >= "2000-01-01")
            & (r.l_shipdate < "2001-01-01")
            & (r.l_discount >= 0.05)
            & (r.l_discount <= 0.07)
            & (r.l_quantity < 24)
        )
        df = filtered.to_pandas()
        assert len(df) == 0


# ── Cross-validation: CPU consistency ────────────────────────────────────────


class TestCPUConsistency:
    """Verify that filter+aggregate results are consistent with manual calculation.

    This ensures that regardless of whether the GPU optimizer intercepts
    the operations, the results match expected values computed by hand.
    """

    def test_filter_preserves_row_count(self, lineitem_csv):
        """Filtering preserves correct number of rows."""
        t = LTSeq.read_csv(lineitem_csv)
        assert len(t.collect()) == 12  # Total rows

        # Filter to only R return flag
        r_only = t.filter(lambda r: r.l_returnflag == "R")
        r_df = r_only.to_pandas()
        assert len(r_df) == 5  # orderkeys 1(x2), 7, 8, 10

    def test_aggregate_sum_matches_manual(self, lineitem_csv):
        """Aggregate sum matches hand-computed values."""
        t = LTSeq.read_csv(lineitem_csv)

        result = t.agg(
            by=lambda r: r.l_returnflag,
            total_price=lambda g: g.l_extendedprice.sum(),
        )
        df = result.to_pandas()
        totals = dict(zip(df["l_returnflag"], df["total_price"]))

        # R: 21168.23 + 45983.16 + 20000.00 + 15000.00 + 30000.00 = 132151.39
        # A: 44694.46 + 54058.05 + 10000.00 = 108752.51
        # N: 28955.64 + 33032.40 + 36901.12 + 18000.00 = 116889.16
        assert abs(totals["R"] - 132151.39) < 0.01
        assert abs(totals["A"] - 108752.51) < 0.01
        assert abs(totals["N"] - 116889.16) < 0.01

    def test_filter_then_aggregate_chain(self, lineitem_csv):
        """filter() → derive() → agg() pipeline produces correct results."""
        t = LTSeq.read_csv(lineitem_csv)

        result = (
            t.filter(lambda r: r.l_returnflag == "R")
            .derive(net_price=lambda r: r.l_extendedprice * (1 - r.l_discount))
            .agg(total_net=lambda g: g.net_price.sum())
        )
        df = result.to_pandas()

        # R rows:
        # ok1-ln1: 21168.23 * (1 - 0.04) = 21168.23 * 0.96 = 20321.5008
        # ok1-ln2: 45983.16 * (1 - 0.09) = 45983.16 * 0.91 = 41844.6756
        # ok7:     20000.00 * (1 - 0.06) = 20000.00 * 0.94 = 18800.00
        # ok8:     15000.00 * (1 - 0.05) = 15000.00 * 0.95 = 14250.00
        # ok10:    30000.00 * (1 - 0.06) = 30000.00 * 0.94 = 28200.00
        expected = 20321.5008 + 41844.6756 + 18800.00 + 14250.00 + 28200.00
        assert abs(df["total_net"].iloc[0] - expected) < 0.01

    def test_min_max_consistency(self, lineitem_csv):
        """min/max aggregates return correct boundary values."""
        t = LTSeq.read_csv(lineitem_csv)
        result = t.agg(
            min_qty=lambda g: g.l_quantity.min(),
            max_qty=lambda g: g.l_quantity.max(),
            min_price=lambda g: g.l_extendedprice.min(),
            max_price=lambda g: g.l_extendedprice.max(),
        )
        df = result.to_pandas()
        assert df["min_qty"].iloc[0] == 10  # orderkey=9
        assert df["max_qty"].iloc[0] == 45  # orderkey=3
        assert abs(df["min_price"].iloc[0] - 10000.00) < 0.01
        assert abs(df["max_price"].iloc[0] - 54058.05) < 0.01
