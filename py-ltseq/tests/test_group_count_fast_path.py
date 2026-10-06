"""group_ordered(...).first().count() kernel vs the DataFusion reference (issue #157).

On sorted Parquet the count kernel splits the row groups into chunks, counts
them in parallel and stitches the chunks at their first rows. These tests put
row-group seams everywhere, with NULLs on both sides of some, and compare the
kernel (`_inner.group_ordered_count`, which raises instead of falling back to
Python) with the DataFusion path, `len(group_ordered(pred).first().to_pandas())`.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq

# A NULL on either side of a comparison starts a group. Parquet reads a NULL
# slot back as 0, so `None, 0` in `u` checks that the kernel looks at the
# previous row's NULL bit and not just its value.
U = [1, 1, 1, None, 0, 0, 0, 0, None, None, 3, 3, 3, 4]
T = [0, 1, 2, 3, 4, None, 6, 20, 21, 22, 23, 24, None, 30]

PREDICATES = {
    "changes": lambda r: r.u != r.u.shift(1),
    "gap": lambda r: (r.t - r.t.shift(1)) > 4,
    "changes_or_gap": lambda r: (r.u != r.u.shift(1)) | ((r.t - r.t.shift(1)) > 4),
    "changes_and_gap": lambda r: (r.u != r.u.shift(1)) & ((r.t - r.t.shift(1)) > 4),
    "nested": lambda r: ((r.u != r.u.shift(1)) & ((r.t - r.t.shift(1)) > 4)) | (r.t != r.t.shift(1)),
    # `== None` / `!= None` build is_null() / is_not_null() (#154).
    "prev_null": lambda r: r.u.shift(1) == None,  # noqa: E711
    "prev_not_null": lambda r: (r.u.shift(1) != None) & (r.t != None),  # noqa: E711
}


def _events() -> pa.Table:
    return pa.table(
        {
            "i": pa.array(range(len(U)), pa.int64()),
            "u": pa.array(U, pa.int64()),
            # Int64: DataFusion computes Int32 differences in 32 bits, which
            # the kernel does not, so Int32 arithmetic is not counted there.
            "t": pa.array(T, pa.int64()),
        }
    )


def _reference(t: LTSeq, pred) -> int:
    return len(t.group_ordered(pred).first().to_pandas())


def _kernel(t: LTSeq, pred) -> int:
    return t._inner.group_ordered_count(t._capture_expr(pred))


@pytest.fixture(params=[1, 2, 3, 5, 1000], ids=lambda n: f"rows_per_group={n}")
def sorted_parquet(request, tmp_path) -> LTSeq:
    path = str(tmp_path / "events.parquet")
    pq.write_table(_events(), path, row_group_size=request.param)
    return LTSeq.read_parquet(path).assume_sorted("i")


@pytest.mark.parametrize("name", PREDICATES)
def test_parquet_count_matches_reference(sorted_parquet, name):
    pred = PREDICATES[name]
    assert _kernel(sorted_parquet, pred) == _reference(sorted_parquet, pred)


@pytest.mark.parametrize("name", PREDICATES)
def test_in_memory_count_matches_reference(name):
    t = LTSeq.from_arrow(_events()).sort("i")
    pred = PREDICATES[name]
    assert _kernel(t, pred) == _reference(t, pred)


def test_parquet_count_falls_back_for_unfused_predicate(sorted_parquet):
    # Swapped operands have no fused form: the parallel scan declines and the
    # kernel answers through the general linear-scan path.
    pred = lambda r: r.u.shift(1) != r.u  # noqa: E731
    assert _kernel(sorted_parquet, pred) == _reference(sorted_parquet, pred)


# #189's repro. Except for "changes", these predicates have no fused form, and
# the general linear-scan path miscounts them because `x` holds a NULL.
NULL_X = [1, 2, None, 10, 11, 30, 31, 32]
NULL_Y = [1, 1, 1, 2, 2, 2, 3, 3]

DIRECTORY_PREDICATES = {
    "changes": lambda r: r.x != r.x.shift(1),
    "swapped_gap": lambda r: (r.x.shift(1) - r.x) > -5,
    "swapped_gap_and_same_y": lambda r: ((r.x.shift(1) - r.x) < -5) & (r.y == r.y.shift(1)),
    "not_changes": lambda r: ~(r.x != r.x.shift(1)),
}


@pytest.mark.parametrize("name", DIRECTORY_PREDICATES)
def test_parquet_directory_count_matches_reference(tmp_path, name):
    # The direct Parquet reader opens a single file. For a directory the
    # kernel raises and count() materializes through DataFusion. It must not
    # answer through the general linear-scan path while #189 is open.
    events = pa.table(
        {
            "i": pa.array(range(len(NULL_X)), pa.int64()),
            "x": pa.array(NULL_X, pa.int64()),
            "y": pa.array(NULL_Y, pa.int64()),
        }
    )
    for part, start in enumerate(range(0, len(NULL_X), 3)):
        pq.write_table(events.slice(start, 3), tmp_path / f"part-{part}.parquet")
    t = LTSeq.read_parquet(str(tmp_path)).assume_sorted("i")
    pred = DIRECTORY_PREDICATES[name]
    assert t.group_ordered(pred).first().count() == _reference(t, pred)


# `%` and `//` in a boundary predicate (#147). The counting kernel does not
# evaluate them: with #189 open it would miscount every predicate below
# because `x` holds a NULL. count() must agree with the reference whichever
# path answers. Each predicate is paired with its group count in SQL
# three-valued logic, where a NULL result starts a group.
MOD_FLOORDIV_PREDICATES = {
    "mod_gap": (lambda r: (r.x % 10 - r.x.shift(1) % 10) > 0, 7),
    "mod_swapped_gap": (lambda r: (r.x.shift(1) % 10 - r.x % 10) > -5, 8),
    "mod_gap_and_same_y": (lambda r: ((r.x.shift(1) % 10 - r.x % 10) < -5) & (r.y == r.y.shift(1)), 2),
    "mod_not_changes": (lambda r: ~((r.x % 10) != (r.x.shift(1) % 10)), 3),
    "floordiv_gap": (lambda r: (r.x // 10 - r.x.shift(1) // 10) > 0, 4),
    "floordiv_gap_and_same_y": (lambda r: ((r.x.shift(1) // 10 - r.x // 10) < -1) & (r.y == r.y.shift(1)), 3),
    "floordiv_not_changes": (lambda r: ~((r.x // 10) != (r.x.shift(1) // 10)), 7),
}


@pytest.mark.parametrize("name", MOD_FLOORDIV_PREDICATES)
@pytest.mark.parametrize("source", ["memory", "parquet"])
def test_mod_and_floordiv_count_matches_reference(tmp_path, source, name):
    events = pa.table(
        {
            "i": pa.array(range(len(NULL_X)), pa.int64()),
            "x": pa.array(NULL_X, pa.int64()),
            "y": pa.array(NULL_Y, pa.int64()),
        }
    )
    if source == "memory":
        t = LTSeq.from_arrow(events).sort("i")
    else:
        path = str(tmp_path / "events.parquet")
        pq.write_table(events, path, row_group_size=3)
        t = LTSeq.read_parquet(path).assume_sorted("i")
    pred, expected = MOD_FLOORDIV_PREDICATES[name]
    assert t.group_ordered(pred).first().count() == expected
    assert _reference(t, pred) == expected


# Integer arithmetic DataFusion computes in fewer than 64 bits, or UInt64
# values the kernel would read as negative: the kernel declines these, so
# the count is the reference's (wrapping included).
NARROW_ARITHMETIC = {
    "uint32_decreasing": (pa.uint32(), [5, 3, 10], lambda r: (r.v - r.v.shift(1)) > 4),
    "int32_overflow": (pa.int32(), [2**31 - 1, -(2**31), 0], lambda r: (r.v - r.v.shift(1)) > 4),
    "uint64_above_i64_max": (pa.uint64(), [2**63, 2**63 + 1, 5, 5, 2**64 - 1, 1], lambda r: r.v > r.v.shift(1)),
}


@pytest.mark.parametrize("name", NARROW_ARITHMETIC)
def test_kernel_declines_what_it_would_compute_differently(name):
    dtype, values, pred = NARROW_ARITHMETIC[name]
    t = LTSeq.from_arrow(
        pa.table({"i": pa.array(range(len(values)), pa.int64()), "v": pa.array(values, dtype)})
    ).sort("i")
    with pytest.raises(ValueError, match="only supports shift-based boundary predicates"):
        _kernel(t, pred)
    assert t.group_ordered(pred).first().count() == _reference(t, pred)


# UInt64 values at or above 2^63, which the counting kernel reads as
# negative Int64 (#189). Read correctly, the first column is 5 mod 16
# throughout (one group), 2**64 - 3 is 1 mod 3 (every row starts a group),
# and a constant column has one `//` bucket (one group).
UINT64_PREDICATES = {
    "mod_changes": ([5, 2**63 + 5, 21, 2**64 - 11], lambda r: (r.u % 16) != (r.u.shift(1) % 16), 1),
    "mod_or_changes": ([2**64 - 3] * 3, lambda r: (r.u % 3 == 1) | (r.u != r.u.shift(1)), 3),
    "floordiv_changes": ([2**64 - 2] * 3, lambda r: (r.u // 4) != (r.u.shift(1) // 4), 1),
    "floordiv_column_changes": ([2**64 - 2] * 3, lambda r: (r.u // r.u) != (r.u.shift(1) // r.u.shift(1)), 1),
}


@pytest.mark.parametrize("name", UINT64_PREDICATES)
def test_mod_and_floordiv_count_on_uint64_above_i64_max(name):
    values, pred, expected = UINT64_PREDICATES[name]
    t = LTSeq.from_arrow(
        pa.table({"i": pa.array(range(len(values)), pa.int64()), "u": pa.array(values, pa.uint64())})
    ).sort("i")
    assert t.group_ordered(pred).first().count() == expected
    assert _reference(t, pred) == expected
