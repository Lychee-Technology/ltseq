"""Parquet scans keep the NaN rows of float columns (#291, #355).

Parquet writers leave NaN out of a float column's min and max, in the footer
and in the page index. DataFusion 55 read them as bounds on every value: a
scan skipped a file, a row group or a page whose min and max a predicate
could not match, though its NaN rows matched it, and it read a column whose
min equalled its max in a file, with no NULL, as that one value, NaN rows
included. Every predicate reached that pruning: a filter, and also the
dynamic filter of a top-k. A bloom filter, which hashes a value's bits,
skipped a row group whose zero had the other sign.

Results are compared with the same rows read from memory. For NaN without
the sign bit, the memory answer is the contract's: §9.4 puts every NaN above
`+inf`, and §18 makes `NaN == NaN` true. Comparisons that decide a NaN by its
sign bit or payload are #290's, so filters here use `float("nan")`; reads are
compared bit for bit, so they cover every NaN.
"""

from __future__ import annotations

import math
import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq
from ltseq.expr import LiteralExpr

from . import layouts

NAN = math.nan
NEG_NAN = math.copysign(math.nan, -1.0)
# A quiet NaN whose payload is 1 rather than 0.
PAYLOAD_NAN = struct.unpack("<d", struct.pack("<Q", 0x7FF8_0000_0000_0001))[0]

FLOAT_TYPES = {"float64": pa.float64(), "float32": pa.float32(), "float16": pa.float16()}


def floats(values, type_=pa.float64()):
    # pyarrow builds float16 only from numpy's float16, so go through float32.
    if type_ == pa.float16():
        return pa.array(values, pa.float32()).cast(pa.float16())
    return pa.array(values, type_)


def numbered(values, type_=pa.float64()):
    """``values`` as column ``x``, with ``i`` numbering the rows."""
    return pa.table({"i": pa.array(range(len(values)), pa.int64()), "x": floats(values, type_)})


def ids(t):
    return sorted(row["i"] for row in t.to_dicts())


def bits(column):
    """The bits of each value of a float column, NULL as ``None``."""
    width = {pa.float64(): pa.int64(), pa.float32(): pa.int32(), pa.float16(): pa.int16()}
    return column.combine_chunks().view(width[column.type]).to_pylist()


# The issue's table, with float16 added: rows, type, predicate and the rows
# it keeps.
FILTERS = {
    "x == nan": ([1.0, NAN, 2.0, NAN], "float64", lambda r: r.x == NAN, [1, 3]),
    "x > 2.0": ([1.0, NAN, 2.0, NAN], "float64", lambda r: r.x > 2.0, [1, 3]),
    "x != 1.0": ([1.0, NAN, 1.0], "float64", lambda r: r.x != 1.0, [1]),
    "x.is_in([nan])": ([1.0, NAN, 2.0], "float64", lambda r: r.x.is_in([NAN]), [1]),
    "float32 x > 2.0": ([1.0, NAN, 2.0], "float32", lambda r: r.x > 2.0, [1]),
    "float16 x > 2.0": ([1.0, NAN, 2.0], "float16", lambda r: r.x > 2.0, [1]),
}

# Row groups of 1, 3 and every row, with and without a page index and a
# bloom filter.
WRITES = {
    f"groups-of-{size or 'all'}{'-page-index' if index else ''}{'-bloom' if bloom else ''}": {
        "row_group_size": size,
        "write_page_index": index,
        "bloom_filter_options": {"x": {"ndv": 16, "fpp": 0.01}} if bloom else None,
    }
    for size in (1, 3, None)
    for index in (False, True)
    for bloom in (False, True)
}


@pytest.mark.parametrize("write", list(WRITES.values()), ids=list(WRITES))
@pytest.mark.parametrize("values, type_, predicate, kept", list(FILTERS.values()), ids=list(FILTERS))
def test_a_filter_keeps_nan_rows(values, type_, predicate, kept, write, tmp_path):
    rows = numbered(values, FLOAT_TYPES[type_])
    path = tmp_path / "t.parquet"
    pq.write_table(rows, path, **write)
    assert ids(LTSeq.from_arrow(rows).filter(predicate)) == kept
    assert ids(LTSeq.read_parquet(str(path)).filter(predicate)) == kept


def test_a_bloom_filter_keeps_a_zero_of_the_other_sign(tmp_path):
    """`x == r.z` on a derived `z = -0.0` reaches the scan as `x = -0.0`,
    which takes `0.0` to be equal."""
    rows = numbered([0.0, 1.0, 2.0])
    path = tmp_path / "t.parquet"
    pq.write_table(rows, path, bloom_filter_options={"x": {"ndv": 16, "fpp": 0.01}})

    def zeros(t):
        return ids(t.derive(z=lambda r: LiteralExpr(-0.0)).filter(lambda r: r.x == r.z))

    assert zeros(LTSeq.from_arrow(rows)) == [0]
    assert zeros(LTSeq.read_parquet(str(path))) == [0]


def test_three_row_groups_with_31_nans(tmp_path):
    """The launch probe: no row group's min and max reach a NaN's match."""
    values = [NAN if i % 97 == 0 else float(i % 100) for i in range(3000)]
    nans = [i for i, v in enumerate(values) if math.isnan(v)]
    assert len(nans) == 31
    path = tmp_path / "t.parquet"
    pq.write_table(numbered(values), path, row_group_size=1000)
    t = LTSeq.read_parquet(str(path))
    assert ids(t.filter(lambda r: r.x == NAN)) == nans
    assert ids(t.filter(lambda r: r.x > 100)) == nans


def test_a_row_group_of_one_reading_and_a_nan(tmp_path):
    """The launch probe's sensor file: `x != 20.0` keeps the NaN of the row
    group whose other readings are all 20.0."""
    values = [20.0] * 999 + [NAN] + [100.0 + v for v in range(1000)]
    path = tmp_path / "t.parquet"
    pq.write_table(numbered(values), path, row_group_size=1000)
    kept = ids(LTSeq.read_parquet(str(path)).filter(lambda r: r.x != 20.0))
    assert kept == list(range(999, 2000))


OVER = [1.0, NAN, 2.0, NAN, 0.5]
OVER_ROWS = {
    "file": lambda rows, d: layouts.write_parquet(rows, d, files=1),
    "row_groups": lambda rows, d: layouts.write_parquet(rows, d, files=1, row_groups=len(OVER)),
    "files": lambda rows, d: layouts.write_parquet(rows, d, files=layouts.MANY),
}


@pytest.mark.parametrize("partitions", [1, layouts.PARTITION_COUNTS["p2xcpu"]])
@pytest.mark.parametrize("source", list(OVER_ROWS))
def test_other_operations_keep_nan_rows(engine_config, partitions, source, tmp_path):
    """Counts, positional reads and a top-k over the rows a float predicate
    keeps, which are the NaNs only. A directory splits the rows as
    `[1.0, NaN]`, `[2.0, NaN]` and `[0.5]`."""
    engine_config(target_partitions=partitions)
    rows = numbered(OVER)
    t = LTSeq.read_parquet(str(OVER_ROWS[source](rows, tmp_path)))

    def over(r):
        return r.x > 2.5

    assert t.filter(over).count() == 2
    assert ids(t.assume_sorted("i").filter(over)) == [1, 3]
    assert ids(t.sort("i").filter(over).head(1)) == [1]
    assert ids(t.sort("i").search_first(over)) == [1]
    # The two NaNs are the largest values; their order is #268's.
    assert ids(t.sort("x", desc=True).head(2)) == [1, 3]


READS = {
    "float64": ([20.0, NAN, 20.0], "float64"),
    "float32": ([20.0, NAN, 20.0], "float32"),
    "float16": ([20.0, NAN, 20.0], "float16"),
    "sign-bit NaN": ([20.0, NEG_NAN, 20.0], "float64"),
    "payload NaN": ([20.0, PAYLOAD_NAN, 20.0], "float64"),
    "NULL": ([20.0, NAN, None], "float64"),
    "another value": ([20.0, NAN, 21.0], "float64"),
}


@pytest.mark.parametrize("row_group_size", [1, None])
@pytest.mark.parametrize("values, type_", list(READS.values()), ids=list(READS))
def test_a_read_keeps_nan_values(values, type_, row_group_size, tmp_path):
    """#355: a file whose other values are all one value, with no NULL, read
    each NaN as that value."""
    column = floats(values, FLOAT_TYPES[type_])
    path = tmp_path / "t.parquet"
    pq.write_table(pa.table({"x": column}), path, row_group_size=row_group_size)
    written = bits(pa.chunked_array([column]))
    assert bits(LTSeq.read_parquet(str(path)).to_arrow().column("x")) == written
    assert bits(LTSeq.scan_parquet(str(path)).to_arrow().column("x")) == written


def test_a_directory_keeps_nan_values(tmp_path):
    """#355's directory: only the first file holds one value and a NaN."""
    parts = [floats([20.0, NAN]), floats([1.0, 2.0])]
    for part, path in zip(parts, layouts.part_paths(tmp_path, len(parts), "parquet")):
        pq.write_table(pa.table({"x": part}), path)
    read = LTSeq.read_parquet(str(tmp_path)).to_arrow().column("x")
    assert sorted(bits(read)) == sorted(bits(pa.chunked_array(parts)))


def test_a_join_on_a_float_key_reads_the_nan_key(tmp_path):
    """#355 through a join: the right file's keys are a NaN and 5.0 only."""
    right = pa.table({"x": [NAN, 5.0], "tag": ["nan", "five"]})
    pq.write_table(right, tmp_path / "right.parquet")
    left = LTSeq.from_arrow(pa.table({"x": [NAN, 5.0, 1.0], "i": [0, 1, 2]}))

    def tags(other):
        joined = left.join(other, on=lambda a, b: a.x == b.x, how="inner")
        return sorted((row["i"], row["tag"]) for row in joined.to_dicts())

    expected = tags(LTSeq.from_arrow(right))
    assert expected == [(0, "nan"), (1, "five")]
    assert tags(LTSeq.read_parquet(str(tmp_path / "right.parquet"))) == expected


def test_null_counts_still_answer_count(tmp_path):
    """The scan keeps a float column's null counts, so its `count()` is still
    answered from Parquet statistics, and its plan scans no file."""
    path = tmp_path / "t.parquet"
    pq.write_table(pa.table({"x": [1.0, NAN, None, 2.0]}), path)
    counted = LTSeq.read_parquet(str(path)).agg(n=lambda g: g.x.count())
    assert counted.to_dicts() == [{"n": 3}]
    assert "DataSourceExec" not in counted.explain_plan()[1]
