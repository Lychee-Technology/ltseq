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
}


def _events() -> pa.Table:
    return pa.table(
        {
            "i": pa.array(range(len(U)), pa.int64()),
            "u": pa.array(U, pa.int64()),
            "t": pa.array(T, pa.int32()),
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
