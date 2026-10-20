"""A failed write leaves the destination as it was (#259).

`write_csv` and `write_parquet` used to open `path` with `File::create`,
which truncates it, before the step that could fail, so any failure
destroyed the file that was there: an unsupported column type, an
execution error, a 0-row Parquet table (whose error path deleted `path`),
or a Parquet table that streams from the very file it replaces. Both now
write to a temporary file in the same directory and rename it over `path`
only after success.

`write_parquet` also panicked on `fixed_size_binary(0)` inside the
`parquet` crate, and a `PanicException` is not an `Exception`. It now
refuses that type, and a `struct` without fields, at any depth, with
`TypeError` before anything is written.

Every failure case checks the destination's bytes and the directory's
listing, so a temporary file left behind fails the test too.
"""

from __future__ import annotations

import os
import stat
import sys
import threading

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq
from ltseq._testing import pinned_engine

OLD = b"previous output\n"
WRITERS = ["write_csv", "write_parquet"]


def table(**columns) -> LTSeq:
    return LTSeq.from_arrow(pa.table(columns))


def ints() -> LTSeq:
    return table(x=pa.array([1, 2, 3], pa.int64()))


def read_back(writer: str, path) -> list[int]:
    """The `x` column of a written file."""
    if writer == "write_parquet":
        return pq.read_table(path).column("x").to_pylist()
    lines = path.read_text().splitlines()
    assert lines[0] == "x"
    return [int(v) for v in lines[1:]]


def existing(tmp_path, name="out"):
    path = tmp_path / name
    path.write_bytes(OLD)
    return path


def assert_failed_cleanly(path, listing):
    assert path.read_bytes() == OLD
    assert sorted(os.listdir(path.parent)) == listing


def divide_by_zero_on_last_row() -> LTSeq:
    # One row per batch in one partition, so the error comes after rows
    # were handed to the writer.
    batches = [
        pa.record_batch({"n": pa.array([n], pa.int64()), "d": pa.array([int(n < 19)], pa.int64())})
        for n in range(20)
    ]
    with pinned_engine(target_partitions=1):
        t = LTSeq.from_arrow(pa.Table.from_batches(batches))
        return t.derive(lambda r: {"q": r.n // r.d})


# (writer, table, exception, message). Where the class is `Exception`, the
# writers disagree on it (#266 settles classes); it still excludes a panic.
FAILURES = {
    # The types refused at the call.
    "parquet struct<>": (
        "write_parquet",
        lambda: table(x=pa.array([{}], pa.struct([]))),
        TypeError,
        "Parquet cannot store",
    ),
    "parquet fixed_size_binary(0)": (
        "write_parquet",
        lambda: table(x=pa.array([b""], pa.binary(0))),
        TypeError,
        "Parquet cannot store",
    ),
    # The CSV writer's own error, raised after the header was written.
    "csv list<int64>": (
        "write_csv",
        lambda: table(x=pa.array([[1]], pa.list_(pa.int64()))),
        Exception,
        "not supported in CSV",
    ),
    "parquet execution error": (
        "write_parquet",
        divide_by_zero_on_last_row,
        Exception,
        "Divide by zero",
    ),
    "csv execution error": ("write_csv", divide_by_zero_on_last_row, Exception, "Divide by zero"),
    # Until #270 writes a valid file for 0 rows.
    "parquet 0 rows": (
        "write_parquet",
        lambda: table(x=pa.array([], pa.int64())),
        ValueError,
        "No data to write",
    ),
}


@pytest.mark.parametrize("case", FAILURES)
def test_failed_write_keeps_existing_file(tmp_path, case):
    writer, build, error, message = FAILURES[case]
    path = existing(tmp_path)
    listing = sorted(os.listdir(tmp_path))
    t = build()
    with pytest.raises(error, match=message):
        getattr(t, writer)(str(path))
    assert_failed_cleanly(path, listing)


def test_panic_during_write_keeps_existing_file(tmp_path):
    """A panic unwinds through the writer and still removes its temporary file.

    A sparse union panics inside the `parquet` crate. #270 refuses it at the
    call, after which this only checks the refusal.
    """
    path = existing(tmp_path)
    listing = sorted(os.listdir(tmp_path))
    t = table(x=pa.UnionArray.from_sparse(pa.array([0], pa.int8()), [pa.array([1])]))
    with pytest.raises(BaseException):
        t.write_parquet(str(path))
    assert_failed_cleanly(path, listing)


REFUSED = {
    "fixed_size_binary of width 0": (pa.binary(0), b""),
    "struct without fields": (pa.struct([]), {}),
}

# Each place a table can hold a refused type, built from (type, value).
CONTAINERS = {
    "top level": lambda t, v: pa.array([v], t),
    "0 rows": lambda t, v: pa.array([], t),
    "list": lambda t, v: pa.array([[v]], pa.list_(t)),
    "large_list": lambda t, v: pa.array([[v]], pa.large_list(t)),
    "fixed_size_list": lambda t, v: pa.array([[v]], pa.list_(t, 1)),
    "list_view": lambda t, v: pa.ListViewArray.from_arrays([0], [1], pa.array([v], t)),
    "struct field": lambda t, v: pa.array(
        [{"a": 1, "b": v}], pa.struct([("a", pa.int64()), ("b", t)])
    ),
    "map value": lambda t, v: pa.array([[("k", v)]], pa.map_(pa.string(), t)),
    "list of struct": lambda t, v: pa.array([[{"b": v}]], pa.list_(pa.struct([("b", t)]))),
    "dictionary values": lambda t, v: pa.array([v], t).dictionary_encode(),
    "run-end values": lambda t, v: pa.RunEndEncodedArray.from_arrays([1], pa.array([v], t)),
}


@pytest.mark.parametrize(
    ("kind", "where"),
    [
        (kind, where)
        for kind in REFUSED
        for where in CONTAINERS
        # pyarrow does not dictionary-encode a struct.
        if (kind, where) != ("struct without fields", "dictionary values")
    ],
)
def test_parquet_refuses_unstorable_types(tmp_path, kind, where):
    column = CONTAINERS[where](*REFUSED[kind])
    path = tmp_path / "new.parquet"
    t = table(ok=pa.array([1] * len(column), pa.int64()), bad=column)
    with pytest.raises(TypeError, match=rf"column 'bad' .*Parquet cannot store a {kind}"):
        t.write_parquet(str(path))
    assert os.listdir(tmp_path) == []


@pytest.mark.parametrize("writer", WRITERS)
def test_write_replaces_existing_file(tmp_path, writer):
    path = existing(tmp_path)
    getattr(ints(), writer)(str(path))
    assert read_back(writer, path) == [1, 2, 3]
    assert os.listdir(tmp_path) == ["out"]


@pytest.mark.parametrize("writer", WRITERS)
def test_write_over_own_source(tmp_path, writer):
    """The plan reads the file it replaces; `write_parquet` streams from it."""
    parquet = writer == "write_parquet"
    path = tmp_path / ("data.parquet" if parquet else "data.csv")
    getattr(table(x=pa.array(range(10), pa.int64())), writer)(str(path))
    source = (LTSeq.read_parquet if parquet else LTSeq.read_csv)(str(path))
    getattr(source.filter(lambda r: r.x >= 7), writer)(str(path))
    assert read_back(writer, path) == [7, 8, 9]
    assert os.listdir(tmp_path) == [path.name]


# What writing over a file kept from File::create: the path's target, its
# mode, and its refusal of files the caller cannot write.

unix_only = pytest.mark.skipif(sys.platform == "win32", reason="POSIX file modes and symlinks")


@unix_only
@pytest.mark.parametrize("writer", WRITERS)
def test_symlink_target_is_replaced(tmp_path, writer):
    target = existing(tmp_path, "target")
    link = tmp_path / "link"
    link.symlink_to(target)
    getattr(ints(), writer)(str(link))
    assert link.is_symlink()
    assert read_back(writer, target) == [1, 2, 3]
    assert sorted(os.listdir(tmp_path)) == ["link", "target"]


@pytest.mark.parametrize("writer", WRITERS)
def test_relative_path_is_resolved_at_the_call(tmp_path, monkeypatch, writer):
    """A relative `path` names a file in the working directory of the call.

    The writers run with the GIL released, so another thread can change the
    working directory before a write finishes. The file still goes where
    `path` pointed at the call, as when `File::create` opened it there.
    """
    start, moved = tmp_path / "start", tmp_path / "moved"
    start.mkdir()
    moved.mkdir()
    monkeypatch.chdir(start)
    # Enough rows that the write is still running when the directory changes.
    t = table(x=pa.array(range(2_000_000), pa.int64()))
    failures = []

    def write():
        try:
            getattr(t, writer)("out")
        except BaseException as e:
            failures.append(e)

    thread = threading.Thread(target=write)
    thread.start()
    # The writer's first file in `start` shows the call has begun.
    while not os.listdir(start) and thread.is_alive():
        pass
    os.chdir(moved)
    changed_during_write = thread.is_alive()
    thread.join()
    if failures:
        raise failures[0]
    assert changed_during_write, "the write ended before the directory changed"
    assert os.listdir(moved) == []
    assert os.listdir(start) == ["out"]


@unix_only
@pytest.mark.parametrize("writer", WRITERS)
def test_existing_file_keeps_its_mode(tmp_path, writer):
    path = existing(tmp_path)
    path.chmod(0o640)
    getattr(ints(), writer)(str(path))
    assert stat.S_IMODE(path.stat().st_mode) == 0o640


@unix_only
@pytest.mark.parametrize("writer", WRITERS)
def test_new_file_mode_follows_umask(tmp_path, writer):
    umask = os.umask(0o027)
    try:
        path = tmp_path / "new"
        getattr(ints(), writer)(str(path))
    finally:
        os.umask(umask)
    assert stat.S_IMODE(path.stat().st_mode) == 0o640


@unix_only
@pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0, reason="root writes any file")
@pytest.mark.parametrize("writer", WRITERS)
def test_read_only_file_is_refused(tmp_path, writer):
    path = existing(tmp_path)
    path.chmod(0o444)
    listing = sorted(os.listdir(tmp_path))
    with pytest.raises(ValueError, match="Permission denied"):
        getattr(ints(), writer)(str(path))
    assert_failed_cleanly(path, listing)


@pytest.mark.parametrize("writer", WRITERS)
def test_directory_path_is_refused(tmp_path, writer):
    (tmp_path / "dir").mkdir()
    with pytest.raises(ValueError, match="(?i)is a directory"):
        getattr(ints(), writer)(str(tmp_path / "dir"))
    assert sorted(os.listdir(tmp_path)) == ["dir"]
    assert os.listdir(tmp_path / "dir") == []


@pytest.mark.parametrize("writer", WRITERS)
def test_missing_directory_is_refused(tmp_path, writer):
    with pytest.raises(ValueError, match="No such file or directory"):
        getattr(ints(), writer)(str(tmp_path / "missing" / "out"))
    assert os.listdir(tmp_path) == []


@pytest.mark.skipif(not os.path.exists("/dev/null"), reason="needs /dev/null")
@pytest.mark.parametrize("writer", WRITERS)
def test_device_is_written_in_place(writer):
    getattr(ints(), writer)("/dev/null")
    assert stat.S_ISCHR(os.stat("/dev/null").st_mode)
