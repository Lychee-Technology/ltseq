"""Tests for GpuTable GPU-resident operations.

All tests require cuDF (RAPIDS). They are automatically skipped in environments
where cuDF is not installed (e.g. CUDA 13.x before RAPIDS 25.06).

Run with:
    pytest py-ltseq/tests/test_gpu_ops.py -v
"""
import pytest

cudf = pytest.importorskip("cudf")

from ltseq.gpu_table import GpuGroupBy, GpuTable


# ── helpers ─────────────────────────────────────────────────────────────────

def _make_table():
    """Small 6-row GpuTable used across tests."""
    gdf = cudf.DataFrame({
        "region":  ["A", "B", "A", "B", "A", "C"],
        "revenue": [100, 200, 150, 50, 300, 80],
        "qty":     [1, 2, 3, 4, 5, 6],
    })
    return GpuTable(gdf)


# ── sort ────────────────────────────────────────────────────────────────────

def test_sort_single_col_asc():
    t = _make_table()
    result = t.sort("revenue")
    revenues = result._gdf["revenue"].to_arrow().to_pylist()
    assert revenues == sorted(revenues)


def test_sort_single_col_desc():
    t = _make_table()
    result = t.sort("revenue", ascending=False)
    revenues = result._gdf["revenue"].to_arrow().to_pylist()
    assert revenues == sorted(revenues, reverse=True)


def test_sort_multi_col():
    t = _make_table()
    result = t.sort(["region", "revenue"], ascending=[True, False])
    assert len(result) == 6
    assert isinstance(result, GpuTable)


def test_sort_returns_gpu_table():
    assert isinstance(_make_table().sort("qty"), GpuTable)


# ── limit ────────────────────────────────────────────────────────────────────

def test_limit_basic():
    t = _make_table()
    assert len(t.limit(3)) == 3


def test_limit_zero():
    assert len(_make_table().limit(0)) == 0


def test_limit_larger_than_table():
    t = _make_table()
    assert len(t.limit(100)) == len(t)


def test_limit_returns_gpu_table():
    assert isinstance(_make_table().limit(2), GpuTable)


# ── rename ───────────────────────────────────────────────────────────────────

def test_rename_single():
    t = _make_table().rename(revenue="rev")
    assert "rev" in t.columns
    assert "revenue" not in t.columns


def test_rename_multiple():
    t = _make_table().rename(revenue="rev", qty="quantity")
    assert "rev" in t.columns
    assert "quantity" in t.columns
    assert "revenue" not in t.columns
    assert "qty" not in t.columns


def test_rename_returns_gpu_table():
    assert isinstance(_make_table().rename(qty="quantity"), GpuTable)


# ── groupby / agg ────────────────────────────────────────────────────────────

def test_groupby_returns_gpu_group_by():
    assert isinstance(_make_table().groupby("region"), GpuGroupBy)


def test_groupby_agg_sum():
    t = _make_table()
    result = t.groupby("region").agg({"revenue": "sum"})
    assert isinstance(result, GpuTable)
    assert "region" in result.columns
    assert "revenue" in result.columns
    # A: 100+150+300=550, B: 200+50=250, C: 80
    totals = dict(zip(
        result._gdf["region"].to_arrow().to_pylist(),
        result._gdf["revenue"].to_arrow().to_pylist(),
    ))
    assert totals["A"] == 550
    assert totals["B"] == 250
    assert totals["C"] == 80


def test_groupby_agg_count():
    result = _make_table().groupby("region").agg({"qty": "count"})
    assert isinstance(result, GpuTable)
    assert len(result) == 3  # three distinct regions


def test_groupby_agg_mean():
    result = _make_table().groupby("region").agg({"revenue": "mean"})
    assert isinstance(result, GpuTable)
    assert "revenue" in result.columns


def test_groupby_multi_key():
    gdf = cudf.DataFrame({
        "a": [1, 1, 2, 2],
        "b": ["x", "y", "x", "y"],
        "v": [10, 20, 30, 40],
    })
    result = GpuTable(gdf).groupby(["a", "b"]).agg({"v": "sum"})
    assert len(result) == 4


# ── join ─────────────────────────────────────────────────────────────────────

def _make_orders():
    gdf = cudf.DataFrame({
        "order_id": [1, 2, 3, 4],
        "customer": ["alice", "bob", "carol", "alice"],
    })
    return GpuTable(gdf)


def _make_items():
    gdf = cudf.DataFrame({
        "order_id": [1, 2, 2, 5],
        "amount":   [100, 200, 50, 999],
    })
    return GpuTable(gdf)


def test_join_inner():
    result = _make_orders().join(_make_items(), on="order_id", how="inner")
    assert isinstance(result, GpuTable)
    # order_ids 1 and 2 have matches; 3,4 and 5 do not
    ids = set(result._gdf["order_id"].to_arrow().to_pylist())
    assert ids == {1, 2}


def test_join_left():
    result = _make_orders().join(_make_items(), on="order_id", how="left")
    assert isinstance(result, GpuTable)
    # All 4 orders kept; unmatched amounts are null
    assert len(result) == 4


def test_join_returns_gpu_table():
    assert isinstance(_make_orders().join(_make_items(), on="order_id"), GpuTable)


# ── chained pipeline ─────────────────────────────────────────────────────────

def test_chain_filter_groupby_sort_limit():
    """Full pipeline: filter → groupby → sort → limit stays in GPU VRAM."""
    t = _make_table()
    result = (
        t.filter(lambda r: r.revenue > 50)
         .groupby("region").agg({"revenue": "sum"})
         .sort("revenue", ascending=False)
         .limit(2)
    )
    assert isinstance(result, GpuTable)
    assert len(result) <= 2


def test_chain_to_ltseq():
    """to_ltseq() materializes the GPU result to a DataFusion LTSeq."""
    from ltseq import LTSeq

    t = _make_table()
    ltseq_table = t.sort("revenue").limit(3).to_ltseq()
    assert isinstance(ltseq_table, LTSeq)
    assert len(ltseq_table.collect()) == 3
