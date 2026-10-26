"""A file-backed table reads the files its reader listed at the call (#262).

`read_csv` and `read_parquet` list their files when called. Each execution
then reads every listed file as it is at that moment, and one that has
disappeared fails the execution, naming it; a file that the directory or
glob matches only later is never read (contract §4.1 and §5.1).

Execution raises its existing error for the missing file, a `ValueError`;
v0.5 gives it the class `SourceNotFoundError` (#266).
"""

import os
import re
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq, ltseq_core

FIRST = [1, 2, 3]
SECOND = [4, 5]


def write(path: Path, xs: list[int]) -> None:
    if path.suffix == ".parquet":
        pq.write_table(pa.table({"x": pa.array(xs, pa.int64())}), path)
    else:
        path.write_text("x\n" + "".join(f"{x}\n" for x in xs))


def rewrite(path: Path, xs: list[int], writer=write) -> None:
    """Write `xs` over `path` and give it a later modification time.

    DataFusion keeps a file's footer and statistics while its size and
    modification time are unchanged; the explicit time keeps a rewrite of the
    same size from depending on the filesystem's timestamp resolution.
    """
    before = path.stat().st_mtime_ns
    writer(path, xs)
    after = max(path.stat().st_mtime_ns, before + 1_000_000_000)
    os.utime(path, ns=(after, after))


def read(fmt: str, source: str) -> LTSeq:
    if "*" in source or source.startswith("file:"):
        # read_csv and read_parquet return an empty table for a glob or a URL
        # without calling the kernel (#261), so call the kernel's reader
        # directly.
        inner = (
            ltseq_core.LTSeqTable.from_csv(source, True)
            if fmt == "csv"
            else ltseq_core.LTSeqTable.from_parquet(source)
        )
        return LTSeq._from_inner(inner)
    return LTSeq.read_csv(source) if fmt == "csv" else LTSeq.read_parquet(source)


def xs(t: LTSeq) -> list[int]:
    return sorted(t.to_arrow().column("x").to_pylist())


@dataclass
class Source:
    fmt: str
    path: str
    files: list[Path]  # the files the reader lists: FIRST's, then SECOND's
    later: Path  # a file the directory or glob matches once it exists

    def read(self) -> LTSeq:
        return read(self.fmt, self.path)

    @property
    def rest(self) -> list[int]:
        """The values of the listed files other than the first."""
        return SECOND if len(self.files) > 1 else []


def make_source(tmp_path: Path, fmt: str, shape: str) -> Source:
    d = tmp_path / "data"
    d.mkdir()
    names = ["first", "second"] if shape != "file" else ["first"]
    files = [d / f"{name}.{fmt}" for name in names]
    for path, values in zip(files, [FIRST, SECOND]):
        write(path, values)
    path = {"file": str(files[0]), "directory": str(d), "glob": str(d / f"*.{fmt}")}[shape]
    return Source(fmt, path, files, d / f"later.{fmt}")


FORMATS = ["csv", "parquet"]


@pytest.fixture(params=["file", "directory", "glob"])
def source(request, tmp_path):
    def build(fmt):
        return make_source(tmp_path, fmt, request.param)

    return build


@pytest.fixture(params=["directory", "glob"])
def listing(request, tmp_path):
    def build(fmt):
        return make_source(tmp_path, fmt, request.param)

    return build


def raises_naming(path: Path):
    return pytest.raises(ValueError, match=re.escape(path.name))


@pytest.mark.parametrize("fmt", FORMATS)
def test_a_deleted_file_fails_the_next_execution(source, fmt):
    s = source(fmt)
    t = s.read()
    assert t.count() == len(FIRST + s.rest)

    s.files[0].unlink()
    with raises_naming(s.files[0]):
        t.count()
    with raises_naming(s.files[0]):
        t.to_arrow()
    with raises_naming(s.files[0]):
        t.filter(lambda r: r.x > 0).to_pandas()


@pytest.mark.parametrize("fmt", FORMATS)
def test_a_file_matched_later_is_not_read(listing, fmt):
    s = listing(fmt)
    t = s.read()
    write(s.later, [100, 200])

    assert t.count() == len(FIRST + SECOND)
    assert xs(t) == FIRST + SECOND
    assert len(t.to_dicts()) == len(FIRST + SECOND)


@pytest.mark.parametrize("fmt", FORMATS)
def test_a_rewritten_file_is_read_as_it_is_now(source, fmt):
    s = source(fmt)
    t = s.read()
    assert t.count() == len(FIRST + s.rest)

    # As many rows, and bytes, as before: only the values tell the new file
    # from the old, and a filter pruning by the old maximum would drop them.
    rewrite(s.files[0], [7, 8, 9])
    assert xs(t) == sorted([7, 8, 9] + s.rest)
    assert t.filter(lambda r: r.x > 6).count() == 3

    # More rows than before, so an old footer or row count shows.
    rewrite(s.files[0], [10, 20, 30, 40])
    assert t.count() == 4 + len(s.rest)
    assert xs(t) == sorted([10, 20, 30, 40] + s.rest)


@pytest.mark.parametrize("fmt", FORMATS)
def test_a_listed_file_is_read_by_its_name(tmp_path, fmt):
    """Whatever characters it holds, glob and URL ones included."""
    d = tmp_path / "data"
    d.mkdir()
    # Read as a glob, the name would match the other file and not itself;
    # read as a URL, `%20` would be a space and `#` would end the path.
    odd = d / f"a b%20[1]#?*é.{fmt}"
    write(odd, FIRST)
    write(d / f"a b%201#xé.{fmt}", SECOND)
    t = read(fmt, str(d))
    assert xs(t) == FIRST + SECOND

    odd.unlink()
    with raises_naming(odd):
        t.count()


def url_of_directory(tmp_path: Path, fmt: str) -> tuple[str, list[Path]]:
    """A `file` URL of a directory, without the trailing `/` that the reader
    gives a path of one. Its name ends with the format's extension, which the
    reader requires of a URL that does not end with `/`."""
    d = tmp_path / f"data.{fmt}"
    d.mkdir()
    files = [d / f"first.{fmt}", d / f"second.{fmt}"]
    for path, values in zip(files, [FIRST, SECOND]):
        write(path, values)
    return d.as_uri(), files


@pytest.mark.parametrize("fmt", FORMATS)
def test_a_url_of_a_directory_lists_the_directory(tmp_path, fmt):
    url, files = url_of_directory(tmp_path, fmt)
    t = read(fmt, url)
    assert xs(t) == FIRST + SECOND

    files[0].unlink()
    with raises_naming(files[0]):
        t.count()


@pytest.mark.parametrize("fmt", FORMATS)
def test_a_cursor_lists_a_url_of_a_directory(tmp_path, fmt):
    url, _ = url_of_directory(tmp_path, fmt)
    cursor = LTSeq.scan(url) if fmt == "csv" else LTSeq.scan_parquet(url)
    assert sorted(cursor.to_arrow().column("x").to_pylist()) == FIRST + SECOND


@pytest.mark.parametrize("fmt", FORMATS)
def test_a_file_replaced_by_a_directory_fails_the_next_execution(source, fmt):
    """The directory is not a listed file, whatever files it holds."""
    s = source(fmt)
    t = s.read()
    s.files[0].unlink()
    s.files[0].mkdir()
    write(s.files[0] / f"inner.{fmt}", [100])
    with raises_naming(s.files[0]):
        t.count()


class TestAssumeSorted:
    """`assume_sorted` declares an order on the reader's files: it reads no
    file the reader did not list, and each execution reads them afresh."""

    def test_reads_only_the_files_listed_at_the_reader(self, listing):
        s = listing("parquet")
        t = s.read()
        write(s.later, [6, 7])
        sorted_t = t.assume_sorted("x")
        assert sorted_t.count() == len(FIRST + SECOND)
        assert xs(sorted_t) == FIRST + SECOND

    def test_reads_the_files_as_they_are_now(self, source):
        s = source("parquet")
        t = s.read().assume_sorted("x")

        rewrite(s.files[0], [1, 2, 3, 3])
        assert t.count() == 4 + len(s.rest)
        assert xs(t) == sorted([1, 2, 3, 3] + s.rest)

        s.files[0].unlink()
        with raises_naming(s.files[0]):
            t.count()
        with raises_naming(s.files[0]):
            t.head(2).to_arrow()


class TestParquetFastPaths:
    """`group_ordered(...).first().count()` and `search_pattern_count` read a
    sorted Parquet file through their own kernels, which follow the rule too."""

    @staticmethod
    def events(path: Path, xs: list[int]) -> None:
        pq.write_table(
            pa.table({"g": pa.array([0] * len(xs), pa.int64()), "x": pa.array(xs, pa.int64())}),
            path,
        )

    def test_a_deleted_file_fails_them(self, tmp_path):
        path = tmp_path / "events.parquet"
        self.events(path, [1, 1, 2, 3, 3])
        t = LTSeq.read_parquet(str(path)).assume_sorted("x")
        assert t.group_ordered(lambda r: r.x != r.x.shift(1)).first().count() == 3

        path.unlink()
        with raises_naming(path):
            t.group_ordered(lambda r: r.x != r.x.shift(1)).first().count()
        with pytest.raises(RuntimeError, match="No such file"):
            t.search_pattern_count(lambda r: r.x == 1, lambda r: r.x == 2, partition_by="g")

    def test_a_rewritten_file_is_read_as_it_is_now(self, tmp_path):
        path = tmp_path / "events.parquet"
        self.events(path, [1, 1, 2, 3, 3])
        t = LTSeq.read_parquet(str(path)).assume_sorted("x")
        assert t.group_ordered(lambda r: r.x != r.x.shift(1)).first().count() == 3
        assert t.search_pattern_count(lambda r: r.x == 1, lambda r: r.x == 2, partition_by="g") == 1

        rewrite(path, [1, 2, 3, 4, 5, 6, 7], self.events)
        assert t.group_ordered(lambda r: r.x != r.x.shift(1)).first().count() == 7
        assert t.search_pattern_count(lambda r: r.x == 1, lambda r: r.x == 2, partition_by="g") == 1


@pytest.mark.parametrize("fmt", FORMATS)
def test_a_cursor_reads_the_files_listed_at_the_call(tmp_path, fmt):
    s = make_source(tmp_path, fmt, "directory")
    cursor = LTSeq.scan(s.path) if fmt == "csv" else LTSeq.scan_parquet(s.path)
    write(s.later, [100, 200])
    assert sorted(cursor.to_arrow().column("x").to_pylist()) == FIRST + SECOND
