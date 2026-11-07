"""`read_parquet` keeps the zone of a timestamp stored in another unit (#362).

pyarrow writes a `timestamp[s, tz=Z]` column as `TIMESTAMP(MILLIS,
isAdjustedToUTC=true)` and keeps `timestamp[s, tz=Z]` in the Arrow schema it
stores in the file, and reads it back as `timestamp[ms, tz=Z]`. The `parquet`
crate takes a stored type only when its unit is the column's, so LTSeq read
such a column as `timestamp[ms, tz=UTC]`: the right instants, in the wrong zone
for `dt.hour`, dates and display.

The file's column is still decoded as `UTC` and cast to the table's type, so
the tests also run what reads a column through the file's own type: pruning,
statistics, and the Parquet fast path that bypasses DataFusion.
"""

from __future__ import annotations

import datetime as dt

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq

ZONE = "Europe/Berlin"
SECONDS = pa.timestamp("s", tz=ZONE)
MILLIS = pa.timestamp("ms", tz=ZONE)
UTC = dt.timezone.utc

# 2024-06-01 10:00:00 UTC, 12:00 in Berlin, and the following hours.
START = 1717236000
HOURS = [START + 3600 * i for i in range(6)]


def at(seconds: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(seconds, UTC)


def column_type(table: pa.Table, name: str = "a") -> pa.DataType:
    return table.schema.field(name).type


def write(path, values, data_type=SECONDS, **options) -> str:
    pq.write_table(pa.table({"a": pa.array(values, data_type)}), path, **options)
    return str(path)


@pytest.fixture
def path(tmp_path):
    """A file of `HOURS` in seconds, with a row group for each two rows."""
    return write(tmp_path / "x.parquet", HOURS, row_group_size=2)


def test_top_level_column_keeps_its_zone_and_instants(path):
    got = LTSeq.read_parquet(path).to_arrow()

    assert column_type(got) == MILLIS
    assert column_type(got) == pq.read_schema(path).field("a").type
    assert pc.cast(got.column("a"), pa.int64()).to_pylist() == [s * 1000 for s in HOURS]


@pytest.mark.parametrize(
    "stored, expected, values",
    [
        (pa.list_(SECONDS), pa.list_(MILLIS), [[START, START + 1], None, []]),
        (pa.large_list(SECONDS), pa.large_list(MILLIS), [[START], None]),
        (pa.list_(SECONDS, 2), pa.list_(MILLIS, 2), [[START, START + 1], None]),
        (pa.list_view(SECONDS), pa.list_view(MILLIS), [[START], None]),
        (
            pa.struct([("x", SECONDS), ("y", pa.int64())]),
            pa.struct([("x", MILLIS), ("y", pa.int64())]),
            [{"x": START, "y": 1}, None],
        ),
        (
            pa.list_(pa.struct([("x", SECONDS)])),
            pa.list_(pa.struct([("x", MILLIS)])),
            [[{"x": START}], None],
        ),
        (
            pa.struct([("x", pa.list_(SECONDS))]),
            pa.struct([("x", pa.list_(MILLIS))]),
            [{"x": [START]}, None],
        ),
        (pa.list_(pa.list_(SECONDS)), pa.list_(pa.list_(MILLIS)), [[[START]], None]),
        (
            pa.map_(pa.string(), SECONDS),
            pa.map_(pa.string(), MILLIS),
            [[("k", START)], None],
        ),
        (pa.map_(SECONDS, pa.string()), pa.map_(MILLIS, pa.string()), [[(START, "k")]]),
    ],
    ids=lambda p: str(p) if isinstance(p, pa.DataType) else "",
)
def test_nested_column_keeps_its_zone_and_instants(tmp_path, stored, expected, values):
    path = write(tmp_path / "x.parquet", values, stored)

    got = LTSeq.read_parquet(path).to_arrow()

    assert column_type(got) == expected
    assert column_type(got) == pq.read_schema(path).field("a").type
    # The written array, not `pq.read_table`: pyarrow 25 cannot read back a
    # fixed-size list with a null (fixed in 26), and seconds to ms is exact.
    assert got.column("a").to_pylist() == pa.array(values, stored).to_pylist()


@pytest.mark.parametrize(
    "unit, written, expected",
    [("us", "ms", "ms"), ("ns", "ms", "ms"), ("ns", "us", "us"), ("ms", "us", "us"), ("s", "us", "us")],
)
def test_any_stored_unit_other_than_the_columns_keeps_the_zone(tmp_path, unit, written, expected):
    """`coerce_timestamps` stores the source's unit too, also a finer one."""
    path = write(
        tmp_path / "x.parquet",
        [0, 1000],
        pa.timestamp(unit, tz=ZONE),
        coerce_timestamps=written,
        allow_truncated_timestamps=True,
    )

    got = LTSeq.read_parquet(path).to_arrow()

    assert column_type(got) == pa.timestamp(expected, tz=ZONE)
    assert got.column("a").to_pylist() == pq.read_table(path).column("a").to_pylist()


@pytest.mark.parametrize("zone", ["+05:30", "America/New_York", "UTC"])
def test_every_kind_of_zone_name_is_kept(tmp_path, zone):
    path = write(tmp_path / "x.parquet", [START], pa.timestamp("s", tz=zone))

    assert column_type(LTSeq.read_parquet(path).to_arrow()) == pa.timestamp("ms", tz=zone)


@pytest.mark.parametrize("unit", ["ms", "us", "ns"])
def test_a_stored_unit_that_matches_reads_as_before(tmp_path, unit):
    data_type = pa.timestamp(unit, tz=ZONE)
    path = write(tmp_path / "x.parquet", [0, 1000], data_type)

    got = LTSeq.read_parquet(path).to_arrow()

    assert column_type(got) == data_type
    assert got.column("a").to_pylist() == pq.read_table(path).column("a").to_pylist()


def test_a_file_ltseq_wrote_reads_as_before(tmp_path, path):
    out = str(tmp_path / "out.parquet")
    LTSeq.read_parquet(path).write_parquet(out)

    assert pq.read_schema(out).field("a").type == MILLIS
    got = LTSeq.read_parquet(out).to_arrow()
    assert column_type(got) == MILLIS
    assert pc.cast(got.column("a"), pa.int64()).to_pylist() == [s * 1000 for s in HOURS]


@pytest.mark.parametrize(
    "data_type, options, expected",
    [
        (SECONDS, {"store_schema": False}, pa.timestamp("ms", tz="UTC")),
        (pa.timestamp("s"), {}, pa.timestamp("ms")),
    ],
    ids=["no stored schema", "naive"],
)
def test_without_a_stored_zone_there_is_none_to_keep(tmp_path, data_type, options, expected):
    path = write(tmp_path / "x.parquet", [START], data_type, **options)

    got = LTSeq.read_parquet(path).to_arrow()

    assert column_type(got) == expected
    assert column_type(got) == pq.read_schema(path).field("a").type


def test_a_dictionary_reads_as_pyarrow_reads_it(tmp_path):
    """pyarrow takes no zone for a dictionary of timestamps stored in another
    unit either, so the column is plain `UTC` timestamps in both."""
    path = tmp_path / "x.parquet"
    pq.write_table(pa.table({"a": pa.array([START], SECONDS).dictionary_encode()}), path)

    got = LTSeq.read_parquet(str(path)).to_arrow()

    assert column_type(got) == pa.timestamp("ms", tz="UTC")
    assert column_type(got) == pq.read_schema(path).field("a").type


def test_int64_columns_beside_a_zoned_one_read_as_pyarrow_reads_them(tmp_path):
    """Finding a stored zone reads the footer with its INT64 leaves untyped.
    That read must take nothing but zones into the table's schema."""
    path = tmp_path / "x.parquet"
    rows = pa.table(
        {
            "zoned": pa.array([START], SECONDS),
            "i": pa.array([1], pa.int64()),
            "u": pa.array([2**63], pa.uint64()),
            "t": pa.array([1], pa.time64("us")),
            "d": pa.array([1], pa.duration("ms")),
            "dec": pa.array([1], pa.decimal128(18, 2)),
            "naive": pa.array([START], pa.timestamp("us")),
            "aware": pa.array([START], pa.timestamp("us", tz="Asia/Tokyo")),
            "l": pa.array([[1]], pa.list_(pa.int64())),
        }
    )
    pq.write_table(rows, path, store_decimal_as_integer=True)

    got = LTSeq.read_parquet(str(path)).to_arrow()

    assert got.schema.equals(pq.read_schema(path))
    assert got.to_pylist() == pq.read_table(path).to_pylist()


def test_a_legacy_list_keeps_its_zone(tmp_path):
    """A list layout older than the format's LIST rules, with the crate's
    other reading of a repeated group."""
    path = tmp_path / "x.parquet"
    rows = pa.table({"a": pa.array([[START], None], pa.list_(SECONDS))})
    pq.write_table(rows, path, use_compliant_nested_type=False)

    got = LTSeq.read_parquet(str(path)).to_arrow()

    assert got.schema.field("a").type.value_type == MILLIS
    assert got.column("a").to_pylist() == [[at(START)], None]


def test_local_time_operations_use_the_zone(path):
    t = LTSeq.read_parquet(path).derive(h=lambda r: r.a.dt.hour())

    assert t.to_arrow().column("h").to_pylist()[:2] == [12, 13]
    assert t.to_dicts()[0]["a"].utcoffset() == dt.timedelta(hours=2)


def test_filter_prunes_row_groups_by_the_right_instants(path, engine_layout):
    for bound, expected in [(HOURS[3], HOURS[3:]), (HOURS[0], HOURS), (HOURS[5] + 1, [])]:
        got = LTSeq.read_parquet(path).filter(lambda r: r.a >= at(bound)).to_arrow()

        assert column_type(got) == MILLIS
        assert pc.cast(got.column("a"), pa.int64()).to_pylist() == [s * 1000 for s in expected]


def test_a_nested_column_reads_after_a_filter_on_another_column(tmp_path, engine_layout):
    path = tmp_path / "x.parquet"
    rows = pa.table({"k": [1, 2], "a": pa.array([[START], [START + 1]], pa.list_(SECONDS))})
    pq.write_table(rows, path, row_group_size=1)

    got = LTSeq.read_parquet(str(path)).filter(lambda r: r.k == 2).to_arrow()

    assert column_type(got) == pa.list_(MILLIS)
    assert got.column("a").to_pylist() == [[at(START + 1)]]


@pytest.mark.parametrize("values", [[START], [START, START, START]], ids=["one row", "three rows"])
def test_a_column_of_one_value_keeps_its_zone(tmp_path, values, engine_layout):
    """A scan reads a column whose file min equals its max as that value."""
    path = write(tmp_path / "x.parquet", values)

    t = LTSeq.read_parquet(path)
    got = t.to_arrow()
    kept = t.filter(lambda r: r.a == at(START)).derive(h=lambda r: r.a.dt.hour()).to_arrow()

    assert column_type(got) == MILLIS
    assert got.column("a").to_pylist() == [at(s) for s in values]
    assert column_type(kept) == MILLIS
    assert kept.column("h").to_pylist() == [12] * len(values)


def test_aggregates_answered_from_statistics_keep_the_zone(path, engine_layout):
    got = (
        LTSeq.read_parquet(path)
        .agg(low=lambda g: g.a.min(), high=lambda g: g.a.max(), n=lambda g: g.a.count())
        .to_arrow()
    )

    assert column_type(got, "low") == MILLIS
    assert column_type(got, "high") == MILLIS
    assert got.to_pylist() == [{"low": at(HOURS[0]), "high": at(HOURS[-1]), "n": len(HOURS)}]


def test_sort_with_a_limit_across_files(tmp_path, engine_layout):
    """The dynamic filter of a top-k prunes by the files' statistics."""
    directory = tmp_path / "d"
    directory.mkdir()
    write(directory / "a.parquet", HOURS[:3])
    write(directory / "b.parquet", HOURS[3:])

    got = LTSeq.read_parquet(str(directory)).sort("a", desc=True).slice(0, 2).to_arrow()

    assert column_type(got) == MILLIS
    assert got.column("a").to_pylist() == [at(HOURS[5]), at(HOURS[4])]


def test_assume_sorted_keeps_the_zone_and_the_fast_path_counts_groups(path, engine_layout):
    """`group_ordered` counts over the file's own `UTC` column, bypassing the
    table's type, which holds the same instants."""
    t = LTSeq.read_parquet(path).assume_sorted("a")

    assert column_type(t.to_arrow()) == MILLIS
    grouped = t.group_ordered(lambda r: r.a != r.a.shift(1))
    assert grouped.first().count() == len(HOURS)


@pytest.mark.parametrize(
    "bound, expected",
    [
        # Midnight on 2 June in Berlin is 22:00 UTC on 1 June: 4 rows are after it.
        (dt.datetime(2024, 6, 2), 4),
        (dt.date(2024, 6, 2), 4),
        # Midnight on 2 June in UTC: 2 rows are after it.
        (dt.datetime(2024, 6, 2, tzinfo=UTC), 2),
    ],
    ids=["naive", "date", "aware"],
)
def test_the_operations_that_may_read_the_file_directly_agree(
    tmp_path, engine_layout, bound, expected
):
    """`group_ordered` and `search_pattern_count` read a sorted file directly,
    as its own `UTC` column, when their predicate allows it. A naive or date
    bound reads in the column's zone, and the key of `group_ordered` depends on
    it: the rows' days of June are 1, 2, 2, 2, 3 in Berlin and 1, 1, 1, 2, 2 in
    UTC."""
    path = tmp_path / "x.parquet"
    late = START + 11 * 3600  # 2024-06-01 21:00:00 UTC, 23:00 in Berlin
    instants = [late + 3600 * hours for hours in [0, 1, 2, 24, 25]]
    rows = pa.table({"k": [1] * 5, "a": pa.array(instants, SECONDS)})
    pq.write_table(rows, path, row_group_size=2)

    t = LTSeq.read_parquet(str(path)).assume_sorted("a")

    assert t.group_ordered(lambda r: r.a.dt.day()).first().count() == 3
    assert t.filter(lambda r: r.a >= bound).count() == expected
    assert t.search_pattern_count(lambda r: r.a >= bound) == expected
    assert t.search_pattern_count(lambda r: r.a >= bound, partition_by="k") == expected
    assert t.search_pattern_count(lambda r: r.k == 1, partition_by="k") == 5


def test_scan_parquet_keeps_the_zone(path):
    batches = list(LTSeq.scan_parquet(path))

    assert {batch.schema.field("a").type for batch in batches} == {MILLIS}
    assert sum(batch.num_rows for batch in batches) == len(HOURS)


def test_files_of_one_zone_read_together(tmp_path):
    """One file stores seconds and one milliseconds: both read as `ms`."""
    directory = tmp_path / "d"
    directory.mkdir()
    write(directory / "a.parquet", HOURS[:3])
    write(directory / "b.parquet", [s * 1000 for s in HOURS[3:]], MILLIS)

    got = LTSeq.read_parquet(str(directory)).sort("a").to_arrow()

    assert column_type(got) == MILLIS
    assert got.column("a").to_pylist() == [at(s) for s in HOURS]


def test_a_column_only_some_files_have_keeps_its_zone(tmp_path):
    directory = tmp_path / "d"
    directory.mkdir()
    write(directory / "a.parquet", HOURS[:1])
    pq.write_table(pa.table({"k": [1]}), directory / "b.parquet")

    got = LTSeq.read_parquet(str(directory)).to_arrow()

    assert column_type(got) == MILLIS


@pytest.mark.parametrize(
    "first, second",
    [
        (pa.string(), pa.large_string()),
        (pa.binary(), pa.large_binary()),
        (pa.string(), pa.string_view()),
    ],
    ids=str,
)
def test_files_of_different_string_types_still_fail_to_merge(tmp_path, first, second):
    """The engine reads strings as views after the merge, not before it, where
    they would all be one type."""
    directory = tmp_path / "d"
    directory.mkdir()
    write(directory / "a.parquet", ["x"], first)
    write(directory / "b.parquet", ["y"], second)

    with pytest.raises(ValueError, match="Fail to merge schema field 'a'"):
        LTSeq.read_parquet(str(directory))


def test_files_of_one_string_type_read_together(tmp_path):
    directory = tmp_path / "d"
    directory.mkdir()
    write(directory / "a.parquet", ["x"], pa.string())
    write(directory / "b.parquet", ["y"], pa.string())

    got = LTSeq.read_parquet(str(directory)).sort("a").to_arrow()

    assert got.column("a").to_pylist() == ["x", "y"]


@pytest.mark.parametrize(
    "unit, values", [("s", [START]), ("ms", [START * 1000])], ids=["stored seconds", "stored millis"]
)
def test_files_of_different_zones_fail_alike_whatever_their_stored_unit(tmp_path, unit, values):
    """Each file's column has the type pyarrow reports for that file."""
    directory = tmp_path / "d"
    directory.mkdir()
    write(directory / "a.parquet", values, pa.timestamp(unit, tz=ZONE))
    write(directory / "b.parquet", values, pa.timestamp(unit, tz="America/New_York"))

    with pytest.raises(ValueError, match="Fail to merge schema field 'a'"):
        LTSeq.read_parquet(str(directory))
