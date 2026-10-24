"""A partition holds the rows of its key, for every key type (#258).

`partition("k")` fetched a partition by writing the Python key into a SQL
WHERE clause. `"k" = 2024-01-03` is the integer 2024 - 1 - 3, so every date
partition was empty, and timestamp, binary and NaN keys had no SQL notation
and raised `KeyError`. A partition now filters on its key's own Arrow
values. The tests check, on in-memory and Parquet inputs at every engine
layout:

- each key `keys()` lists holds exactly the rows whose key equals it, in
  table order and with the table's `sort_keys`, for date, naive and aware
  timestamp, binary, NaN, signed-zero, string and NULL keys, before and
  after `map()`;
- `p[k].count()` and `map()` agree with `filter(r.k == k)` wherever that
  filter has the literal and returns the right rows;
- dates that `read_csv` infers as `date32` (the common way to meet the bug);
- (derived) the other Arrow key types `filter` has no literal for (`date64`,
  `time64`, `duration`, `uint64` past Int64, fixed-size and large binary,
  ...) and those it has;
- (derived) the two occurrences of a repeated hour are two keys, found by
  the value `keys()` lists and by their UTC instants, also after `map()`,
  where a dict keyed by them kept one;
- (derived) NaN is one key whose partition holds every NaN row, whatever
  its bits, also on Parquet, where `filter(r.k == nan)` loses them to
  row-group pruning (#291); any NaN looks the key up.

The reference groups the input's rows in Python, by the key value as
`to_pylist` returns it, with an aware value taken as its instant and every
NaN as one value.
"""

from __future__ import annotations

import math
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import numpy as np
import pyarrow as pa
import pytest

from ltseq import LTSeq

from . import layouts

# 01:30 EDT and 01:30 EST in New York on 2024-11-03: an hour apart, but
# equal as values of one ZoneInfo (PEP 495).
FOLD_EARLIER = datetime(2024, 11, 3, 5, 30, tzinfo=timezone.utc)
FOLD_LATER = datetime(2024, 11, 3, 6, 30, tzinfo=timezone.utc)
# A NaN with its sign bit set, as x86 arithmetic gives (`inf - inf`).
NEGATIVE_NAN = -math.nan
INPUTS = ("many_batches", "parquet_row_groups", "parquet_files")
N = 24

_NAN = object()


def lookup(value):
    """The value the reference groups a key by."""
    if isinstance(value, datetime) and value.utcoffset() is not None:
        return value.astimezone(timezone.utc)
    if isinstance(value, float) and math.isnan(value):
        return _NAN
    return value


def reference(rows: pa.Table, column: str) -> dict:
    """The row ids (`v`) of each key value of `column`, in row order."""
    groups: dict = {}
    for v, value in zip(rows["v"].to_pylist(), rows[column].to_pylist()):
        groups.setdefault(lookup(value), []).append(v)
    return groups


def ids(t: LTSeq) -> list[int]:
    return t.to_arrow().column("v").to_pylist()


def cycle(values: list, period: int) -> list:
    """`values` spread over N rows so that no key's rows are consecutive."""
    return [values[(i * period) % len(values)] for i in range(N)]


def key_rows() -> pa.Table:
    """N rows whose key columns each repeat a few values out of run order."""
    return pa.table(
        {
            "v": pa.array(range(N), pa.int64()),
            "day": pa.array(cycle([date(2024, 1, 2), date(2024, 1, 3), date(2024, 2, 29)], 2), pa.date32()),
            "at": pa.array(
                cycle([datetime(2024, 1, 2, 3, 4, 5, 6), datetime(2024, 1, 3), datetime(1969, 7, 20, 20, 17)], 2),
                pa.timestamp("us"),
            ),
            "local": pa.array(
                cycle([FOLD_EARLIER, FOLD_LATER, datetime(2024, 3, 10, 7, 30, tzinfo=timezone.utc)], 2),
                pa.timestamp("us", tz="America/New_York"),
            ),
            "blob": pa.array(cycle([b"a", b"b'c", b""], 2), pa.binary()),
            "x": pa.array(cycle([1.5, math.nan, 0.0, -0.0], 3), pa.float64()),
            "s": pa.array(cycle(["it's", "West", None], 2), pa.string()),
        }
    )


KEY_COLUMNS = ("day", "at", "local", "blob", "x", "s")


def check_partitions(t: LTSeq, rows: pa.Table, column: str) -> None:
    """Each key of `t.partition(column)` holds its rows, and only it does."""
    expected = reference(rows, column)
    p = t.partition(column)
    keys = p.keys()
    assert len(p) == len(keys) == len(expected), (column, keys)
    assert {lookup(k) for k in keys} == set(expected), (column, keys)
    for key in keys:
        part = p[key]
        assert ids(part) == expected[lookup(key)], (column, key)
        assert part.sort_keys == t.sort_keys, (column, key)
    mapped = p.map(lambda part: part.select("v"))
    assert len(mapped) == len(expected), (column, mapped.keys())
    assert {lookup(k): ids(mapped[k]) for k in mapped.keys()} == expected, column


@pytest.mark.parametrize("layout", INPUTS)
def test_each_key_holds_the_rows_of_its_value(layout, engine_layout, tmp_path):
    rows = key_rows()
    t = layouts.build(layout, rows, tmp_path).sort("v")
    for column in KEY_COLUMNS:
        check_partitions(t, rows, column)


def test_a_partition_counts_as_the_filter_on_its_key(engine_layout):
    """The issue's acceptance: `p[k].count()` and `map` match
    `filter(r.k == k)`, here for the key types that have a literal, in
    memory, where that filter is right for NaN too (#291 is Parquet's)."""
    rows = key_rows()
    t = layouts.build("many_batches", rows, None)
    for column in ("day", "at", "local", "x", "s"):
        p = t.partition(column)
        counts = p.map(lambda part: part)
        for key in p.keys():
            if key is None:
                matching = t.filter(lambda r: getattr(r, column).is_null())
            else:
                matching = t.filter(lambda r: getattr(r, column) == key)
            assert p[key].count() == counts[key].count() == matching.count(), (column, key)


@pytest.mark.parametrize("layout", ("csv_file", "csv_files"))
def test_dates_read_from_csv_hold_their_rows(layout, engine_layout, tmp_path):
    rows = key_rows().select(["v", "day"])
    t = layouts.build(layout, rows, tmp_path)
    assert t.schema["day"] == "date32"
    check_partitions(t.sort("v"), rows, "day")


OTHER_TYPES = {
    "date64": pa.array([date(2024, 1, 2), date(2024, 1, 3)], pa.date64()),
    "timestamp_s": pa.array([datetime(2024, 1, 2, 3), datetime(2024, 1, 3)], pa.timestamp("s")),
    "timestamp_ns": pa.array([1, 2], pa.timestamp("ns")),
    "timestamp_ns_aware": pa.array([1, 2], pa.timestamp("ns", tz="Asia/Kolkata")),
    "timestamp_fixed_offset": pa.array([0, 3_600_000_000], pa.timestamp("us", tz="+05:30")),
    "time64": pa.array([time(1), time(2, 3, 4, 5)], pa.time64("us")),
    "duration": pa.array([timedelta(seconds=1), timedelta(days=-1)], pa.duration("us")),
    "uint64_past_int64": pa.array([2**64 - 1, 1], pa.uint64()),
    "int8": pa.array([-128, 127], pa.int8()),
    "decimal128": pa.array([Decimal("1.50"), Decimal("-2.25")], pa.decimal128(5, 2)),
    "decimal256": pa.array([Decimal("1.5"), Decimal("2.5")], pa.decimal256(40, 1)),
    "float32": pa.array([0.1, float("nan")], pa.float32()),
    "bool": pa.array([True, False], pa.bool_()),
    "large_string": pa.array(["x", "y'z"], pa.large_string()),
    "string_view": pa.array(["x", "y"], pa.string_view()),
    "dictionary": pa.array(["x", "y"]).dictionary_encode(),
    "large_binary": pa.array([b"\x00", b"\xff"], pa.large_binary()),
    "fixed_size_binary": pa.array([b"ab", b"cd"], pa.binary(2)),
}


@pytest.mark.parametrize("values", OTHER_TYPES.values(), ids=OTHER_TYPES.keys())
def test_other_key_types_hold_their_rows(values):
    keys = pa.concat_arrays([values.slice(i, 1) for i in (0, 1, 1, 0, 1)])
    rows = pa.table({"v": pa.array(range(len(keys)), pa.int64()), "k": keys})
    check_partitions(LTSeq.from_arrow(rows).sort("v"), rows, "k")


def test_a_repeated_hour_is_two_keys():
    rows = pa.table(
        {
            "v": [0, 1, 2],
            "k": pa.array([FOLD_EARLIER, FOLD_LATER, FOLD_EARLIER], pa.timestamp("us", tz="America/New_York")),
        }
    )
    p = LTSeq.from_arrow(rows).sort("v").partition("k")
    earlier, later = sorted(p.keys(), key=lambda k: k.astimezone(timezone.utc))
    # The keys are the values in the column's zone, equal as Python values.
    assert (earlier.fold, later.fold) == (0, 1) and earlier == later
    for key, expected in ((earlier, [0, 2]), (later, [1])):
        assert ids(p[key]) == expected
        assert ids(p[key.astimezone(timezone.utc)]) == expected
        assert ids(p[(key,)]) == expected
    mapped = p.map(lambda part: part)
    assert len(mapped) == 2
    assert [ids(mapped[k]) for k in (earlier, later)] == [[0, 2], [1]]
    assert len(mapped.map(lambda part: part)) == 2


@pytest.mark.parametrize("dictionary", (False, True), ids=("plain", "dictionary"))
def test_nan_is_one_key_holding_every_nan_row(dictionary, engine_layout, tmp_path):
    x = pa.array([1.0, math.nan, 2.0, NEGATIVE_NAN, 1.0, math.nan], pa.float64())
    assert math.copysign(1.0, x[3].as_py()) == -1.0
    rows = pa.table({"v": pa.array(range(6), pa.int64()), "x": x.dictionary_encode() if dictionary else x})
    for layout in ("many_batches", "parquet_row_groups"):
        t = layouts.build(layout, rows, tmp_path / layout).sort("v")
        check_partitions(t, rows, "x")
        p = t.partition("x")
        for nan in (math.nan, float("nan"), NEGATIVE_NAN, np.float64("nan")):
            assert ids(p[nan]) == [1, 3, 5], (layout, nan)


def test_keys_are_looked_up_as_python_compares_them():
    rows = pa.table(
        {
            "v": [0, 1, 2, 3, 4],
            "a": ["E", "E", None, "W", "E"],
            "b": pa.array([1, None, 1, 1, 1], pa.int64()),
            "x": [0.0, -0.0, 1.0, 1.0, 0.0],
        }
    )
    t = LTSeq.from_arrow(rows).sort("v")
    p = t.partition("a", "b")
    assert sorted(p.keys(), key=repr) == [("E", 1), ("E", None), ("W", 1), (None, 1)]
    assert ids(p[("E", 1)]) == [0, 4]
    assert ids(p[("E", None)]) == [1]
    assert ids(p[(None, 1)]) == [2]
    with pytest.raises(KeyError):
        p["E"]
    with pytest.raises(KeyError):
        p[("E", 2)]

    zeros = t.partition("x")
    assert len(zeros) == 2
    assert ids(zeros[-0.0]) == ids(zeros[0]) == [0, 1, 4]
    with pytest.raises(KeyError):
        zeros[2.0]
    with pytest.raises(KeyError):
        t.partition("a")["X"]


def test_the_issue_reproduction():
    t = LTSeq.from_arrow(pa.table({"k": [date(2024, 1, 2), date(2024, 1, 3)], "v": [0, 1]}))
    p = t.partition("k")
    assert sorted(p.keys()) == [date(2024, 1, 2), date(2024, 1, 3)]
    assert p[date(2024, 1, 2)].to_arrow().num_rows == 1
