"""`write_parquet` writes second units in milliseconds (#324).

Parquet has no second unit. The `parquet` crate wrote a `timestamp[s]` or
`time32[s]` column as a bare INT64 or INT32 with no logical type, so only
a reader that restores types from the stored Arrow schema, as LTSeq's
does, saw a time; pyarrow, pandas and DuckDB read integers.
`read_csv` infers `timestamp[s]` for text such as `2024-01-01 10:00:00`,
so an ordinary CSV-to-Parquet conversion wrote such files.

The writer now converts these values to milliseconds, as pyarrow does, at
any depth of nesting (contract §16.5). The conversion is exact, so a
`timestamp[s]` value that milliseconds cannot hold raises, and `path` is
left as it was.
"""

from __future__ import annotations

import datetime as dt
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq
from ltseq._testing import pinned_engine

# The largest second count whose milliseconds fit int64: (2**63 - 1) // 1000.
LIMIT = 9_223_372_036_854_775


def in_milliseconds(t: pa.DataType) -> pa.DataType:
    if pa.types.is_timestamp(t):
        return pa.timestamp("ms", tz=t.tz)
    return pa.time32("ms")


def bare_integer_leaves(path) -> list[str]:
    """Leaf columns stored as integers with no logical type, which a reader
    that does not trust the stored Arrow schema reads as integers."""
    schema = pq.ParquetFile(path).schema
    return [
        column.path
        for column in (schema.column(i) for i in range(len(schema)))
        if column.physical_type in ("INT32", "INT64") and column.logical_type.type == "NONE"
    ]


def write(tmp_path, **columns):
    path = tmp_path / "out.parquet"
    LTSeq.from_arrow(pa.table(columns)).write_parquet(str(path))
    return path


def test_csv_timestamps_read_as_datetimes(tmp_path):
    """The issue's reproduction: CSV text to Parquet, read by pandas."""
    csv = tmp_path / "ts.csv"
    csv.write_text("ts,v\n2024-01-01 10:00:00,1\n2024-01-01 10:00:01,2\n")
    t = LTSeq.read_csv(str(csv))
    assert t.to_arrow().schema.field("ts").type == pa.timestamp("s")
    path = tmp_path / "ts.parquet"
    t.write_parquet(str(path))

    ts = pd.read_parquet(path)["ts"]
    assert str(ts.dtype) == "datetime64[ms]"
    assert ts.tolist() == [pd.Timestamp("2024-01-01 10:00:00"), pd.Timestamp("2024-01-01 10:00:01")]

    back = LTSeq.read_parquet(str(path)).to_arrow()
    assert back.schema.field("ts").type == pa.timestamp("ms")
    assert back.column("ts").to_pylist() == [
        dt.datetime(2024, 1, 1, 10, 0, 0),
        dt.datetime(2024, 1, 1, 10, 0, 1),
    ]


def test_pandas_seconds_read_as_datetimes(tmp_path):
    df = pd.DataFrame(
        {
            "ts": pd.Series(pd.to_datetime(["2024-01-01 10:00:00", None])).astype("datetime64[s]"),
            "z": pd.Series(pd.to_datetime(["2024-01-01 10:00:00", "2024-06-01 00:00:00"]))
            .dt.tz_localize("Europe/Berlin")
            .astype("datetime64[s, Europe/Berlin]"),
        }
    )
    path = tmp_path / "out.parquet"
    LTSeq.from_pandas(df).write_parquet(str(path))

    back = pd.read_parquet(path)
    assert str(back["ts"].dtype) == "datetime64[ms]"
    assert str(back["z"].dtype) == "datetime64[ms, Europe/Berlin]"
    expected = df.astype({"ts": "datetime64[ms]", "z": "datetime64[ms, Europe/Berlin]"})
    pd.testing.assert_frame_equal(back, expected)


INSTANTS = [dt.datetime(2024, 1, 1, 10, 0, 1), None, dt.datetime(1969, 12, 31, 23, 59, 59)]
TIMES = [dt.time(10, 0, 1), None, dt.time(0, 0), dt.time(23, 59, 59)]

SECONDS = {
    "timestamp[s]": (pa.timestamp("s"), INSTANTS, "datetime64[ms]"),
    "timestamp[s, UTC]": (pa.timestamp("s", tz="UTC"), INSTANTS, "datetime64[ms, UTC]"),
    "timestamp[s, Europe/Berlin]": (
        pa.timestamp("s", tz="Europe/Berlin"),
        INSTANTS,
        "datetime64[ms, Europe/Berlin]",
    ),
    "timestamp[s, +05:30]": (pa.timestamp("s", tz="+05:30"), INSTANTS, "datetime64[ms, UTC+05:30]"),
    # pandas has no time dtype; it reads `datetime.time` objects.
    "time32[s]": (pa.time32("s"), TIMES, "object"),
}


@pytest.mark.parametrize("kind", SECONDS)
def test_second_units_are_written_in_milliseconds(tmp_path, kind):
    t, values, pandas_dtype = SECONDS[kind]
    source = pa.array(values, t)
    expected = source.cast(in_milliseconds(t))
    path = write(tmp_path, x=source)

    assert bare_integer_leaves(path) == []
    assert pq.read_schema(path).field("x").type == expected.type
    assert pq.read_table(path).column("x").to_pylist() == source.to_pylist()

    x = pd.read_parquet(path)["x"]
    assert str(x.dtype) == pandas_dtype
    pd.testing.assert_series_equal(x, expected.to_pandas(), check_names=False)

    back = LTSeq.read_parquet(str(path)).to_arrow().column("x")
    assert back.type == expected.type
    assert back.to_pylist() == source.to_pylist()


# Each place a table can hold a second unit, built from (type, value) so that
# the same builder gives the column written and the column read back.
# Parquet stores run-end encoded values plainly, and LTSeq reads them so.
READ_BACK = {"run-end values": lambda t, v: pa.array([v, v], t)}
CONTAINERS = {
    "list": lambda t, v: pa.array([[v, None], None, []], pa.list_(t)),
    "large_list": lambda t, v: pa.array([[v]], pa.large_list(t)),
    "fixed_size_list": lambda t, v: pa.array([[v, v]], pa.list_(t, 2)),
    "list_view": lambda t, v: pa.ListViewArray.from_arrays([0], [1], pa.array([v], t)),
    "struct field": lambda t, v: pa.array(
        [{"a": "x", "b": v}], pa.struct([("a", pa.string()), ("b", t)])
    ),
    "map key": lambda t, v: pa.array([[(v, "x")]], pa.map_(t, pa.string())),
    "map value": lambda t, v: pa.array([[("k", v)]], pa.map_(pa.string(), t)),
    "list of struct": lambda t, v: pa.array([[{"b": v}]], pa.list_(pa.struct([("b", t)]))),
    "dictionary values": lambda t, v: pa.array([v, v], t).dictionary_encode(),
    "run-end values": lambda t, v: pa.RunEndEncodedArray.from_arrays([2], pa.array([v], t)),
}

NESTED_UNITS = {
    "timestamp[s]": (pa.timestamp("s"), dt.datetime(2024, 1, 1, 10, 0, 1)),
    "timestamp[s, Europe/Berlin]": (
        pa.timestamp("s", tz="Europe/Berlin"),
        dt.datetime(2024, 1, 1, 10, 0, 1),
    ),
    "time32[s]": (pa.time32("s"), dt.time(10, 0, 1)),
}


@pytest.mark.parametrize("unit", NESTED_UNITS)
@pytest.mark.parametrize("where", CONTAINERS)
def test_nested_second_units_are_written_in_milliseconds(tmp_path, where, unit):
    t, value = NESTED_UNITS[unit]
    source = CONTAINERS[where](t, value)
    expected = READ_BACK.get(where, CONTAINERS[where])(in_milliseconds(t), value)
    path = write(tmp_path, x=source)

    assert bare_integer_leaves(path) == []
    assert pq.read_table(path).column("x").to_pylist() == source.to_pylist()

    back = LTSeq.read_parquet(str(path)).to_arrow().column("x")
    assert back.type == expected.type
    assert back.to_pylist() == source.to_pylist()


def test_two_columns_with_seconds_keep_the_others(tmp_path):
    path = write(
        tmp_path,
        a=pa.array([1, 2], pa.int64()),
        ts=pa.array([0, 1], pa.timestamp("s")),
        s=pa.array(["x", "y"]),
        t=pa.array([0, 1], pa.time32("s")),
    )
    back = LTSeq.read_parquet(str(path)).to_arrow()
    assert back.column_names == ["a", "ts", "s", "t"]
    assert back.schema.field("a").type == pa.int64()
    assert back.schema.field("ts").type == pa.timestamp("ms")
    assert back.schema.field("t").type == pa.time32("ms")
    assert back.column("ts").cast(pa.int64()).to_pylist() == [0, 1000]
    assert back.column("t").cast(pa.int32()).to_pylist() == [0, 1000]
    assert back.column("a").to_pylist() == [1, 2]
    assert back.column("s").to_pylist() == ["x", "y"]


# Types Parquet stores as they are, or that the stored Arrow schema restores.
UNCHANGED = {
    "timestamp[ms]": pa.timestamp("ms"),
    "timestamp[us, UTC]": pa.timestamp("us", tz="UTC"),
    "timestamp[ns]": pa.timestamp("ns"),
    "time32[ms]": pa.time32("ms"),
    "time64[us]": pa.time64("us"),
    "duration[s]": pa.duration("s"),
    "list<duration[s]>": pa.list_(pa.duration("s")),
}


@pytest.mark.parametrize("kind", UNCHANGED)
def test_other_units_are_written_unchanged(tmp_path, kind):
    t = UNCHANGED[kind]
    value = [1] if pa.types.is_list(t) else 1
    source = pa.array([value, None], t)
    path = write(tmp_path, x=source)
    back = LTSeq.read_parquet(str(path)).to_arrow().column("x")
    assert back.type == t
    assert back.to_pylist() == source.to_pylist()


@pytest.mark.parametrize("value", [LIMIT, -LIMIT])
def test_seconds_at_the_millisecond_limit_convert(tmp_path, value):
    path = write(
        tmp_path,
        x=pa.array([value], pa.timestamp("s")),
        l=pa.array([[value]], pa.list_(pa.timestamp("s"))),
    )
    back = LTSeq.read_parquet(str(path)).to_arrow()
    assert back.column("x").cast(pa.int64()).to_pylist() == [value * 1000]
    assert back.column("l").type == pa.list_(pa.timestamp("ms"))
    assert back.column("l").cast(pa.list_(pa.int64())).to_pylist() == [[value * 1000]]


@pytest.mark.parametrize("nested", [False, True], ids=["top level", "list"])
@pytest.mark.parametrize("value", [LIMIT + 1, -LIMIT - 1])
def test_seconds_beyond_milliseconds_raise_and_keep_path(tmp_path, value, nested):
    """The failing value comes in the last batch, after rows were written."""

    def column(v):
        if nested:
            return pa.array([[v]], pa.list_(pa.timestamp("s")))
        return pa.array([v], pa.timestamp("s"))

    path = tmp_path / "out.parquet"
    path.write_bytes(b"previous output\n")
    batches = [pa.record_batch({"x": column(v)}) for v in [0, 1, value]]
    with pinned_engine(target_partitions=1):
        t = LTSeq.from_arrow(pa.Table.from_batches(batches))
    with pytest.raises(ValueError, match=r"column 'x' .*milliseconds"):
        t.write_parquet(str(path))
    assert path.read_bytes() == b"previous output\n"
    assert os.listdir(tmp_path) == ["out.parquet"]


def sliced(array, hidden):
    return array.slice(1) if hidden else array


SECOND = pa.timestamp("s")

# Each builds a column holding 0 and `v` in seconds. When `hidden`, `v` sits
# where a container keeps a value that no row holds: under a null slot,
# outside a slice, or in a dictionary entry no key uses. Otherwise a row
# holds it.
HOLDERS = {
    "struct under a null row": lambda v, hidden: pa.StructArray.from_arrays(
        [pa.array([0, v], SECOND)], names=["b"], mask=pa.array([False, hidden])
    ),
    "list slot that is null": lambda v, hidden: pa.ListArray.from_arrays(
        pa.array([0, 1, 2], pa.int32()), pa.array([0, v], SECOND), mask=pa.array([False, hidden])
    ),
    "list outside a slice": lambda v, hidden: sliced(
        pa.array([[v], [0]], pa.list_(SECOND)), hidden
    ),
    "fixed_size_list slot that is null": lambda v, hidden: pa.FixedSizeListArray.from_arrays(
        pa.array([0, v], SECOND), 1, mask=pa.array([False, hidden])
    ),
    "list_view outside every view": lambda v, hidden: pa.ListViewArray.from_arrays(
        [0], [1 if hidden else 2], pa.array([0, v], SECOND)
    ),
    "map slot that is null": lambda v, hidden: pa.MapArray.from_arrays(
        [0, 1, 2], pa.array(["a", "b"]), pa.array([0, v], SECOND), mask=pa.array([False, hidden])
    ),
    "dictionary entry no key uses": lambda v, hidden: pa.DictionaryArray.from_arrays(
        pa.array([0, 0 if hidden else 1], pa.int32()), pa.array([0, v], SECOND)
    ),
    "run-end values outside a slice": lambda v, hidden: sliced(
        pa.RunEndEncodedArray.from_arrays([1, 2], pa.array([v, 0], SECOND)), hidden
    ),
    "struct in a list slot that is null": lambda v, hidden: pa.ListArray.from_arrays(
        pa.array([0, 1, 2], pa.int32()),
        pa.StructArray.from_arrays([pa.array([0, v], SECOND)], names=["b"]),
        mask=pa.array([False, hidden]),
    ),
}


@pytest.mark.parametrize("where", HOLDERS)
def test_values_no_row_holds_are_not_checked(tmp_path, where):
    """A container's values that no row holds can be anything, so one beyond
    the limit does not fail the write. Arrow's `cast` checked them all."""
    source = HOLDERS[where](LIMIT + 1, True)
    path = write(tmp_path, x=source)

    assert bare_integer_leaves(path) == []
    assert pq.read_table(path).column("x").to_pylist() == source.to_pylist()
    back = LTSeq.read_parquet(str(path)).to_arrow().column("x")
    assert back.to_pylist() == source.to_pylist()


@pytest.mark.parametrize("where", HOLDERS)
def test_values_a_row_holds_are_checked(tmp_path, where):
    path = tmp_path / "out.parquet"
    t = LTSeq.from_arrow(pa.table({"x": HOLDERS[where](LIMIT + 1, False)}))
    with pytest.raises(ValueError, match=r"column 'x' .*milliseconds"):
        t.write_parquet(str(path))
    assert os.listdir(tmp_path) == []


def test_sorted_rows_keep_their_order(tmp_path, engine_layout):
    seconds = [(i * 7919) % 101 for i in range(101)]
    t = LTSeq.from_arrow(
        pa.table({"ts": pa.array(seconds, pa.timestamp("s")), "i": pa.array(range(101))})
    ).sort("ts")
    path = tmp_path / "out.parquet"
    t.write_parquet(str(path))
    back = pq.read_table(path)
    assert back.schema.field("ts").type == pa.timestamp("ms")
    assert back.column("ts").cast(pa.int64()).to_pylist() == [s * 1000 for s in sorted(seconds)]
