"""asof_join never matches a NULL or NaN time (#263, #369).

A right row whose time is NULL or NaN is never a match, and a left row whose
time is NULL or NaN has none, in every direction, with and without ``by=``,
wherever the input's sort puts them. Nor does it match a NULL ``by`` key
(#370): the brute-force reference below holds NULL keys too. Float times are
searched as floats, so the reference also holds fractions and infinities.
"""

import math
import random
import sys
from fractions import Fraction

import pyarrow as pa
import pytest

from ltseq import LTSeq

DIRECTIONS = ["backward", "forward", "nearest"]
NAN = float("nan")
INF = float("inf")


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


def absent(t):
    """Whether a time never matches: it is NULL or NaN."""
    return t is None or t != t


def reference(left, right, direction, by):
    """The ``rid`` each left row matches, by left ``id``, found by checking
    every right row (contract §12.3, test row J6). Right times are unique
    within a group, so the match does not depend on how ties break. A NULL
    ``by`` key matches nothing, not even another NULL (§12.1)."""
    expected = {}
    for row in left:
        candidates = [
            r
            for r in right
            if not absent(r["t"])
            and (by is None or (row["g"] is not None and r["g"] == row["g"]))
        ]
        t = row["t"]
        if absent(t):
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


# Few enough that an infinity is common among the rows drawn from them.
FLOAT_TIMES = [-INF, INF, *(v / 4 for v in range(-10, 10))]


def random_rows(rng, n, key, unique_times, time_type=pa.int64()):
    """``n`` rows with a time that is NULL about a quarter of the time, and a
    group that is NULL about a fifth of the time. Float times are fractions or
    infinities, and NaN of either sign about a fifth of the time."""
    floats = pa.types.is_floating(time_type)
    pool = FLOAT_TIMES if floats else range(60)
    times = rng.sample(pool, n) if unique_times else rng.choices(pool, k=n)
    if floats:
        times = [rng.choice([NAN, -NAN]) if rng.random() < 0.2 else t for t in times]
    return [
        {
            key: i,
            "t": None if rng.random() < 0.25 else times[i],
            "g": rng.choice(["A", "B", "A", "B", None]),
        }
        for i in range(n)
    ]


def in_order(rows, order, rng):
    """``rows`` shuffled, or sorted by time with their NULL and NaN times
    first, last, or at random places among the others."""
    if order == "shuffled":
        return rng.sample(rows, len(rows))
    valid = sorted((r for r in rows if not absent(r["t"])), key=lambda r: r["t"])
    nulls = [r for r in rows if absent(r["t"])]
    if order == "nulls between":
        for row in nulls:
            valid.insert(rng.randint(0, len(valid)), row)
        return valid
    return nulls + valid if order == "nulls first" else valid + nulls


def as_table(rows, key, time_type=pa.int64()):
    return LTSeq.from_arrow(
        pa.table(
            {
                key: pa.array([r[key] for r in rows], pa.int64()),
                "t": pa.array([r["t"] for r in rows], time_type),
                "g": pa.array([r["g"] for r in rows], pa.string()),
            }
        )
    )


@pytest.mark.parametrize("time_type", [pa.int64(), pa.float64()], ids=str)
@pytest.mark.parametrize("order", ["shuffled", "nulls first", "nulls last", "nulls between"])
@pytest.mark.parametrize("by", [None, "g"])
@pytest.mark.parametrize("direction", DIRECTIONS)
def test_each_direction_equals_a_brute_force_reference(
    engine_layout, direction, by, order, time_type
):
    """Shuffled inputs go through ``is_sorted=False``, which sorts them; the
    others through ``is_sorted=True``, which searches them as given."""
    rng = random.Random(f"{direction}-{by}-{order}-{time_type}")
    for _ in range(4):
        left = random_rows(rng, rng.randint(1, 12), "id", False, time_type)
        right = random_rows(rng, rng.randint(1, 12), "rid", True, time_type)
        joined = as_table(in_order(left, order, rng), "id", time_type).asof_join(
            as_table(in_order(right, order, rng), "rid", time_type),
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


@pytest.mark.parametrize(
    "direction, expected",
    [("backward", 4.0), ("forward", 5.5), ("nearest", 5.5)],
)
@pytest.mark.parametrize("by", [None, "g"])
def test_a_float_time_keeps_its_fraction(direction, expected, by):
    """Read as integers, 5.2 and 5.5 were both 5, so ``backward`` matched a
    right row later than the left one."""
    left = LTSeq.from_arrow(pa.table({"id": [0], "t": [5.2], "g": ["A"]}))
    right = LTSeq.from_arrow(pa.table({"rid": [0, 1], "t": [4.0, 5.5], "g": ["A", "A"]}))
    rows = left.asof_join(right, on="t", by=by, strategy=direction).to_dicts()
    assert [row["t_right"] for row in rows] == [expected]


@pytest.mark.parametrize(
    "direction, expected",
    [
        ("backward", [None, 3.0, 5.0]),
        ("forward", [3.0, 5.0, None]),
        ("nearest", [3.0, 3.0, 5.0]),
    ],
)
@pytest.mark.parametrize("by", [None, "g"])
@pytest.mark.parametrize("is_sorted", [True, False])
@pytest.mark.parametrize(
    "times",
    [
        [NAN, -NAN, 3.0, 5.0],
        [3.0, 5.0, NAN, -NAN],
        [-NAN, 3.0, 5.0, NAN],
        [3.0, NAN, -NAN, 5.0],
    ],
    ids=["first", "last", "around", "between"],
)
def test_a_right_nan_time_is_never_a_match(direction, expected, by, is_sorted, times):
    """Read as 0, a NaN was a match at time 0, and between other times it broke
    the search. A sort puts a negative NaN first and a positive one last; a
    table declared sorted can hold one anywhere."""
    left = LTSeq.from_arrow(pa.table({"t": [1.0, 4.0, 9.0], "g": ["A"] * 3}))
    right = LTSeq.from_arrow(pa.table({"t": times, "g": ["A"] * 4}))
    joined = left.asof_join(right, on="t", by=by, strategy=direction, is_sorted=is_sorted)
    assert joined.to_arrow().column("t_right").to_pylist() == expected


@pytest.mark.parametrize("direction", DIRECTIONS)
@pytest.mark.parametrize("by", [None, "g"])
@pytest.mark.parametrize("nan", [NAN, -NAN], ids=["nan", "-nan"])
def test_a_left_nan_time_has_no_match(direction, by, nan):
    left = LTSeq.from_arrow(pa.table({"id": [0, 1], "t": [nan, 4.0], "g": ["A", "A"]}))
    right = LTSeq.from_arrow(pa.table({"rid": [0, 1], "t": [-1.0, 4.0], "g": ["A", "A"]}))
    joined = left.asof_join(right, on="t", by=by, strategy=direction)
    assert matched_rids(joined) == {0: None, 1: 1}


@pytest.mark.parametrize(
    "direction, expected",
    [
        ("backward", {0: 0, 1: 0, 2: 1, 3: 2}),
        ("forward", {0: 0, 1: 1, 2: 2, 3: 2}),
        ("nearest", {0: 0, 1: 1, 2: 1, 3: 2}),
    ],
)
@pytest.mark.parametrize("by", [None, "g"])
def test_an_infinite_time_is_a_time(direction, expected, by):
    """An infinity matches itself, and from a finite time it is farther than
    every finite one."""
    left = LTSeq.from_arrow(pa.table({"id": range(4), "t": [-INF, 0.0, 9.0, INF], "g": ["A"] * 4}))
    right = LTSeq.from_arrow(pa.table({"rid": range(3), "t": [-INF, 5.0, INF], "g": ["A"] * 3}))
    joined = left.asof_join(right, on="t", by=by, strategy=direction, is_sorted=True)
    assert matched_rids(joined) == expected


@pytest.mark.parametrize("direction", DIRECTIONS)
@pytest.mark.parametrize("left_time", [0.0, -0.0], ids=str)
@pytest.mark.parametrize("right_time", [0.0, -0.0], ids=str)
def test_negative_zero_is_zero(direction, left_time, right_time):
    left = LTSeq.from_arrow(pa.table({"id": [0], "t": [left_time]}))
    right = LTSeq.from_arrow(pa.table({"rid": [0], "t": [right_time]}))
    assert matched_rids(left.asof_join(right, on="t", strategy=direction)) == {0: 0}


@pytest.mark.parametrize(
    "direction, int_left, float_left",
    [("backward", 0, 0), ("forward", 1, 1), ("nearest", 0, 1)],
)
def test_an_integer_time_meets_a_float_time_with_its_fraction(direction, int_left, float_left):
    """A pair of an integer and a float column is searched as floats, so the
    float side keeps its fraction whichever side it is on."""
    ints = pa.array([4, 6], pa.int64())
    floats = pa.array([4.5, 5.5], pa.float64())
    left = LTSeq.from_arrow(pa.table({"id": [0], "t": pa.array([5], pa.int64())}))
    right = LTSeq.from_arrow(pa.table({"rid": [0, 1], "t": floats}))
    joined = left.asof_join(right, on="t", strategy=direction, is_sorted=True)
    # 5 is a tie between 4.5 and 5.5, which `nearest` gives to `backward`.
    assert matched_rids(joined) == {0: int_left}

    left = LTSeq.from_arrow(pa.table({"id": [0], "t": pa.array([5.25], pa.float64())}))
    right = LTSeq.from_arrow(pa.table({"rid": [0, 1], "t": ints}))
    joined = left.asof_join(right, on="t", strategy=direction, is_sorted=True)
    # 5.25 is closer to 6 only when distances keep their fraction: 0.75 < 1.25.
    assert matched_rids(joined) == {0: float_left}


def test_nearest_tells_apart_distances_that_round_to_the_same_float():
    """``2**53 - 0.5`` and ``2**53 + 0.5`` both round to ``2**53``, so a
    subtraction calls this a tie and ``nearest`` would pick the farther row."""
    right = LTSeq.from_arrow(pa.table({"rid": [0, 1], "t": [-(2.0**53), 2.0**53]}))
    left = LTSeq.from_arrow(pa.table({"id": [0, 1, 2], "t": [0.5, -0.5, 0.0]}))
    joined = left.asof_join(right, on="t", strategy="nearest", is_sorted=True)
    assert matched_rids(joined) == {0: 1, 1: 0, 2: 0}


def random_float(rng):
    """A finite float of any magnitude, subnormals and the largest included."""
    magnitude = math.ldexp((rng.random() + 1) / 2, rng.randint(-1074, 1024))
    if rng.random() < 0.05:
        magnitude = rng.choice([sys.float_info.max, sys.float_info.min, 5e-324, 2.0**53])
    return rng.choice([-1, 1]) * magnitude


def test_nearest_compares_exact_distances():
    """``nearest`` agrees with exact rational arithmetic on triples that are
    close to a tie, at every magnitude. One group per triple, so that one join
    checks them all."""
    rng = random.Random("nearest-exact")
    triples = []
    while len(triples) < 20_000:
        t, step = random_float(rng), abs(random_float(rng))
        back, fwd = t - step, t + step
        for _ in range(rng.randint(0, 3)):
            fwd = math.nextafter(fwd, rng.choice([-INF, INF]))
        back, fwd = (back, fwd) if rng.random() < 0.5 else (2 * t - fwd, 2 * t - back)
        if all(map(math.isfinite, (back, fwd))) and back < t < fwd:
            triples.append((t, back, fwd))

    groups = range(len(triples))
    left = LTSeq.from_arrow(pa.table({"id": groups, "g": groups, "t": [x[0] for x in triples]}))
    right = LTSeq.from_arrow(
        pa.table(
            {
                "rid": [i for g in groups for i in (2 * g, 2 * g + 1)],
                "g": [g for g in groups for _ in range(2)],
                "t": [v for _, back, fwd in triples for v in (back, fwd)],
            }
        )
    )
    joined = left.asof_join(right, on="t", by="g", strategy="nearest", is_sorted=True)
    expected = {
        g: 2 * g if Fraction(t) - Fraction(back) <= Fraction(fwd) - Fraction(t) else 2 * g + 1
        for g, (t, back, fwd) in enumerate(triples)
    }
    assert matched_rids(joined) == expected
