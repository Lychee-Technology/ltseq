"""asof_join never matches a NULL time (#263).

A right row whose time is NULL is never a match, and a left row whose time
is NULL has none, in every direction, with and without ``by=``, wherever
the input's sort puts the NULLs.
"""

import random

import pyarrow as pa
import pytest

from ltseq import LTSeq

DIRECTIONS = ["backward", "forward", "nearest"]


def matched_rids(joined):
    """The ``rid`` of the right row each left row matched, by left ``id``.

    Also checks that the join returns each left row once, and that a row
    without a match has NULL right columns."""
    rows = joined.to_dicts()
    ids = [row["id"] for row in rows]
    assert len(ids) == len(set(ids))
    for row in rows:
        if row["rid"] is None:
            assert row["t_right"] is None
    return {row["id"]: row["rid"] for row in rows}


def test_the_issue_reproduction():
    left = LTSeq.from_arrow(pa.table({"t": [10]}))
    right = LTSeq.from_arrow(pa.table({"t": [5, None], "v": ["valid", "null-time"]}))
    for r in (right, right.sort("t")):
        rows = left.asof_join(r, on="t", direction="backward", is_sorted=False).to_dicts()
        assert rows == [{"t": 10, "t_right": 5, "v": "valid"}]


@pytest.mark.parametrize(
    "direction, expected",
    [
        ("backward", [None, None, 0, 0]),
        ("forward", [0, 0, None, None]),
        ("nearest", [0, 0, 0, 0]),
    ],
)
@pytest.mark.parametrize("nulls", ["first", "last"])
def test_a_right_null_time_is_never_a_match(direction, expected, nulls):
    """A sorted right table puts its NULL times first or last; neither
    reaches the search."""
    left = LTSeq.from_arrow(pa.table({"id": [0, 1, 2, 3], "t": [1, 4, 6, 10]}))
    rows = {"rid": [0, 1, 2], "t": [5, None, None]}
    if nulls == "first":
        rows = {"rid": [1, 2, 0], "t": [None, None, 5]}
    right = LTSeq.from_arrow(pa.table(rows))
    joined = left.asof_join(right, on="t", strategy=direction, is_sorted=True)
    assert matched_rids(joined) == dict(enumerate(expected))


@pytest.mark.parametrize(
    "direction, expected",
    [
        ("backward", [None, 0, 0, 0, 2]),
        ("forward", [0, 0, 2, 2, None]),
        ("nearest", [0, 0, 0, 2, 2]),
    ],
)
@pytest.mark.parametrize("by", [None, "g"])
def test_a_right_null_time_between_others_is_never_a_match(direction, expected, by):
    """No sort by time puts a NULL between two times, but a table declared
    sorted can hold one there."""
    left = LTSeq.from_arrow(pa.table({"id": range(5), "t": [1, 3, 4, 6, 9], "g": ["A"] * 5}))
    right = LTSeq.from_arrow(pa.table({"rid": range(3), "t": [3, None, 7], "g": ["A"] * 3}))
    joined = left.asof_join(right, on="t", by=by, strategy=direction, is_sorted=True)
    assert matched_rids(joined) == dict(enumerate(expected))


@pytest.mark.parametrize("direction", DIRECTIONS)
def test_a_left_null_time_has_no_match(direction):
    left = LTSeq.from_arrow(pa.table({"id": [0, 1], "t": [None, 6]}))
    right = LTSeq.from_arrow(pa.table({"rid": [0, 1], "t": [5, 7], "v": ["a", "b"]}))
    rows = left.asof_join(right, on="t", strategy=direction).to_dicts()
    assert {"id": 0, "t": None, "rid": None, "t_right": None, "v": None} in rows
    assert len(rows) == 2


@pytest.mark.parametrize(
    "time_type, unit",
    [
        (pa.int32(), 1),
        (pa.int64(), 1),
        (pa.float64(), 1),
        (pa.timestamp("s"), 1),
        (pa.timestamp("ms"), 1),
        (pa.timestamp("us", tz="UTC"), 1),
        (pa.timestamp("ns"), 1),
        (pa.date32(), 1),
        (pa.date64(), 86_400_000),
    ],
    ids=str,
)
@pytest.mark.parametrize(
    "direction, expected",
    [
        ("backward", [None, 0, None]),
        ("forward", [0, None, None]),
        ("nearest", [0, 0, None]),
    ],
)
def test_null_times_never_match_for_each_time_type(time_type, unit, direction, expected):
    def times(values):
        scaled = [None if v is None else v * unit for v in values]
        return pa.array(scaled, time_type)

    left = LTSeq.from_arrow(pa.table({"id": [0, 1, 2], "t": times([1, 6, None])}))
    right = LTSeq.from_arrow(pa.table({"rid": [0, 1], "t": times([5, None])}))
    joined = left.asof_join(right, on="t", strategy=direction)
    assert matched_rids(joined) == dict(enumerate(expected))


def reference(left, right, direction, by):
    """The ``rid`` each left row matches, by left ``id``, found by checking
    every right row (contract §12.3, test row J6). Right times are unique
    within a group, so the match does not depend on how ties break."""
    expected = {}
    for row in left:
        candidates = [
            r for r in right if r["t"] is not None and (by is None or r["g"] == row["g"])
        ]
        t = row["t"]
        if t is None:
            expected[row["id"]] = None
            continue
        back = max((r for r in candidates if r["t"] <= t), key=lambda r: r["t"], default=None)
        fwd = min((r for r in candidates if r["t"] >= t), key=lambda r: r["t"], default=None)
        if direction == "backward":
            match = back
        elif direction == "forward":
            match = fwd
        elif back is None or (fwd is not None and fwd["t"] - t < t - back["t"]):
            match = fwd
        else:
            match = back
        expected[row["id"]] = None if match is None else match["rid"]
    return expected


def random_rows(rng, n, key, unique_times):
    """``n`` rows with a time that is NULL about a quarter of the time."""
    times = rng.sample(range(60), n) if unique_times else [rng.randrange(60) for _ in range(n)]
    return [
        {key: i, "t": None if rng.random() < 0.25 else times[i], "g": rng.choice("AB")}
        for i in range(n)
    ]


def in_order(rows, order, rng):
    """``rows`` shuffled, or sorted by time with their NULL times first,
    last, or at random places among the others."""
    if order == "shuffled":
        return rng.sample(rows, len(rows))
    valid = sorted((r for r in rows if r["t"] is not None), key=lambda r: r["t"])
    nulls = [r for r in rows if r["t"] is None]
    if order == "nulls between":
        for row in nulls:
            valid.insert(rng.randint(0, len(valid)), row)
        return valid
    return nulls + valid if order == "nulls first" else valid + nulls


def as_table(rows, key):
    return LTSeq.from_arrow(
        pa.table(
            {
                key: pa.array([r[key] for r in rows], pa.int64()),
                "t": pa.array([r["t"] for r in rows], pa.int64()),
                "g": pa.array([r["g"] for r in rows], pa.string()),
            }
        )
    )


@pytest.mark.parametrize("order", ["shuffled", "nulls first", "nulls last", "nulls between"])
@pytest.mark.parametrize("by", [None, "g"])
@pytest.mark.parametrize("direction", DIRECTIONS)
def test_each_direction_equals_a_brute_force_reference(engine_layout, direction, by, order):
    """Shuffled inputs go through ``is_sorted=False``, which sorts them; the
    others through ``is_sorted=True``, which searches them as given."""
    rng = random.Random(f"{direction}-{by}-{order}")
    for _ in range(4):
        left = random_rows(rng, rng.randint(1, 12), "id", unique_times=False)
        right = random_rows(rng, rng.randint(1, 12), "rid", unique_times=True)
        joined = as_table(in_order(left, order, rng), "id").asof_join(
            as_table(in_order(right, order, rng), "rid"),
            on="t",
            by=by,
            strategy=direction,
            is_sorted=order != "shuffled",
        )
        assert matched_rids(joined) == reference(left, right, direction, by)


def test_nearest_picks_the_closer_time_across_the_whole_int64_range():
    """The distances to these two times differ by more than ``2**63``."""
    left = LTSeq.from_arrow(pa.table({"id": [0], "t": [2**62]}))
    right = LTSeq.from_arrow(pa.table({"rid": [0, 1], "t": [-(2**62) - 1, 2**63 - 1]}))
    joined = left.asof_join(right, on="t", strategy="nearest", is_sorted=True)
    assert matched_rids(joined) == {0: 1}
