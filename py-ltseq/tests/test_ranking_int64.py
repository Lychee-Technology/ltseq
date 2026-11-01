"""Ranking functions return int64 (#323).

DataFusion's ``row_number``, ``rank``, ``dense_rank`` and ``ntile`` return
UInt64, while ``.schema`` reported ``int64``. Arithmetic followed the real
type: the difference of two ranks wrapped to 18446744073709551615 instead of
going negative, and ``5 - ntile(4)`` widened to a decimal. Ranks are cast to
Int64 where they are lowered, so every consumer sees the type ``.schema``
reports.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq, dense_rank, ntile, rank, row_number

RANKINGS = {
    "row_number": lambda r: row_number().over(order_by=r.v),
    "rank": lambda r: rank().over(order_by=r.v),
    "dense_rank": lambda r: dense_rank().over(order_by=r.v),
    "ntile": lambda r: ntile(2).over(order_by=r.v),
}


def _table():
    return LTSeq.from_arrow(pa.table({"g": ["a", "b", "a", "b"], "v": [3, 1, 2, 4]}))


@pytest.mark.parametrize("name", sorted(RANKINGS))
def test_ranking_is_int64_in_arrow_schema_and_parquet(name, tmp_path, engine_layout):
    out = _table().derive(x=RANKINGS[name])
    assert out.to_arrow().schema.field("x").type == pa.int64()
    assert out.schema["x"] == "int64"
    path = tmp_path / "ranks.parquet"
    out.write_parquet(str(path))
    assert pq.read_schema(path).field("x").type == pa.int64()


@pytest.mark.parametrize("name", sorted(RANKINGS))
def test_partitioned_ranking_is_int64(name, engine_layout):
    over = {
        "row_number": lambda r: row_number().over(partition_by=r.g, order_by=r.v),
        "rank": lambda r: rank().over(partition_by=r.g, order_by=r.v),
        "dense_rank": lambda r: dense_rank().over(partition_by=r.g, order_by=r.v),
        "ntile": lambda r: ntile(2).over(partition_by=r.g, order_by=r.v),
    }[name]
    table = _table().derive(x=over).to_arrow()
    assert table.schema.field("x").type == pa.int64()


def test_ranking_without_order_by_uses_table_order_and_is_int64(engine_layout):
    table = _table().sort("v").derive(x=lambda r: row_number().over()).to_arrow()
    assert table.schema.field("x").type == pa.int64()
    assert table.column("x").to_pylist() == [1, 2, 3, 4]


def test_difference_of_two_ranks_goes_negative(engine_layout):
    # The issue's example: this used to be 18446744073709551615 on the first row.
    out = (
        LTSeq.from_dict({"v": [1, 2]})
        .derive(
            a=lambda r: rank().over(order_by=r.v),
            b=lambda r: rank().over(order_by=r.v, descending=True),
        )
        .derive(d=lambda r: r.a - r.b)
    )
    table = out.sort("v").to_arrow()
    assert table.schema.field("d").type == pa.int64()
    assert table.column("d").to_pylist() == [-1, 1]


def test_ranking_window_arithmetic_in_one_derive(engine_layout):
    # Arithmetic with an integer literal stays int64; it used to widen the
    # unsigned rank to decimal128(21, 0).
    out = _table().sort("v").derive(
        score=lambda r: 5 - ntile(4).over(order_by=r.v),
        below=lambda r: rank().over(order_by=r.v) - 2,
        gap=lambda r: rank().over(order_by=r.v) - rank().over(order_by=r.v, descending=True),
    )
    table = out.to_arrow()
    for name in ("score", "below", "gap"):
        assert table.schema.field(name).type == pa.int64(), name
    assert table.column("score").to_pylist() == [4, 3, 2, 1]
    assert table.column("below").to_pylist() == [-1, 0, 1, 2]
    assert table.column("gap").to_pylist() == [-3, -1, 1, 3]
