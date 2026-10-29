"""What a reader's path names, resolved at the call (#261).

`read_csv`, `read_parquet`, `scan` and `scan_parquet` resolve a path the same
way (contract §4.1): an existing file or directory is read as itself, even
when its name holds `*`, `?` or `[`; any other path that holds them is a glob
that reads the files of the format it matches; and a path that names no file
raises `FileNotFoundError` naming it, at the call (owner decision OD-L4).
v0.5 raises `SourceNotFoundError`, a subclass of it (#266).
"""

import glob
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq

FORMATS = ["csv", "parquet"]


def write(path: Path, xs: list[int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".parquet":
        pq.write_table(pa.table({"x": pa.array(xs, pa.int64())}), path)
    else:
        path.write_text("x\n" + "".join(f"{x}\n" for x in xs))


def open_table(fmt: str, path: str) -> LTSeq:
    return LTSeq.read_csv(path) if fmt == "csv" else LTSeq.read_parquet(path)


def open_cursor(fmt: str, path: str):
    return LTSeq.scan(path) if fmt == "csv" else LTSeq.scan_parquet(path)


@dataclass(frozen=True)
class Reader:
    """A reader's call alone (`open`), which raises for a path that names no
    file before anything reads the table, and the sorted values it reads."""

    open: Callable[[str, str], Any]
    values: Callable[[Any], list[int]]

    def __call__(self, fmt: str, path: str) -> list[int]:
        return sorted(self.values(self.open(fmt, path)))


read = Reader(open_table, lambda t: t.to_arrow().column("x").to_pylist())
scan = Reader(open_cursor, lambda c: [x for b in c for x in b.column("x").to_pylist()])
READERS = {"read": read, "scan": scan}


@pytest.fixture(params=sorted(READERS))
def reader(request):
    return READERS[request.param]


def test_the_issue_reproduction(tmp_path):
    d = tmp_path / "d"
    for i in range(3):
        write(d / f"part{i}.parquet", [i])
        write(d / f"c{i}.csv", [i])

    assert LTSeq.read_parquet(f"{d}/part*.parquet").count() == 3
    assert LTSeq.read_parquet(str(d)).count() == 3
    assert LTSeq.read_csv(f"{d}/c*.csv").count() == 3


@pytest.mark.parametrize("fmt", FORMATS)
def test_a_glob_reads_the_rows_of_the_files_it_matches(tmp_path, fmt, reader):
    d = tmp_path / "d"
    write(d / f"a1.{fmt}", [1, 2])
    write(d / f"a2.{fmt}", [3])
    write(d / f"b1.{fmt}", [4])

    assert reader(fmt, f"{d}/a*.{fmt}") == [1, 2, 3]
    assert reader(fmt, f"{d}/*.{fmt}") == reader(fmt, str(d)) == [1, 2, 3, 4]


# Files of the format, hidden ones and others in subdirectories, and one
# of another, each holding a value of its own; Python's glob says which of
# them a pattern matches.
LAYOUT = [
    "a.{fmt}",
    "b.{fmt}",
    "ab.{fmt}",
    ".hidden.{fmt}",
    "notes.txt",
    "sub/c.{fmt}",
    "sub/deeper/d.{fmt}",
    "sub/.private/e.{fmt}",
    "other.{fmt}/f.{fmt}",
]

PATTERNS = [
    "*.{fmt}",
    "*",
    "?.{fmt}",
    "[ab].{fmt}",
    "[!a].{fmt}",
    ".*.{fmt}",
    "*/*.{fmt}",
    "s*/*.{fmt}",
    "*/deeper/*.{fmt}",
    "**/*.{fmt}",
    "sub/**/*.{fmt}",
    "**/d.{fmt}",
    "./*.{fmt}",
    "sub//*.{fmt}",
    "sub/.private/*.{fmt}",
    "**/.private/*.{fmt}",
    "s*/.p*/*.{fmt}",
]


@pytest.mark.parametrize("pattern", PATTERNS)
@pytest.mark.parametrize("fmt", FORMATS)
def test_a_glob_matches_the_files_python_glob_matches(tmp_path, fmt, pattern):
    d = tmp_path / "d"
    values = {}
    for i, name in enumerate(LAYOUT):
        path = d / name.format(fmt=fmt)
        write(path, [i])
        values[str(path)] = i
    pattern = f"{d}/{pattern.format(fmt=fmt)}"
    matched = [
        values[os.path.normpath(p)]
        for p in glob.glob(pattern, recursive=True)
        if os.path.isfile(p) and p.endswith(f".{fmt}")
    ]
    assert matched, "every pattern here matches a file of the format"

    assert read(fmt, pattern) == sorted(matched)


@pytest.mark.parametrize("fmt", FORMATS)
def test_a_path_with_glob_characters_names_that_file(tmp_path, fmt, reader):
    # Read as globs, `x[1]` and `x?` would match `x1`, and `x*` every file.
    d = tmp_path / "d"
    write(d / f"x1.{fmt}", [1])
    for value, name in enumerate(["x[1]", "x?", "x*", "x[!"], start=10):
        write(d / f"{name}.{fmt}", [value])
        assert reader(fmt, f"{d}/{name}.{fmt}") == [value], name


@pytest.mark.parametrize("fmt", FORMATS)
def test_a_directory_with_glob_characters_names_that_directory(tmp_path, fmt, reader):
    write(tmp_path / "data[2024]" / f"a.{fmt}", [1, 2])
    write(tmp_path / "data2" / f"a.{fmt}", [3])

    assert reader(fmt, str(tmp_path / "data[2024]")) == [1, 2]
    # A path below it is a glob, in which `[2024]` matches one character, as
    # in Python's glob; `glob.escape` matches the name itself.
    assert reader(fmt, f"{tmp_path}/data[2024]/*.{fmt}") == [3]
    assert reader(fmt, f"{tmp_path}/{glob.escape('data[2024]')}/*.{fmt}") == [1, 2]


def raises_not_found(path: str):
    return pytest.raises(FileNotFoundError, match=re.escape(f"'{path}'"))


@pytest.mark.parametrize(
    "missing",
    ["typo.{fmt}", "nodir/typo.{fmt}", "nodir/", "noext"],
)
@pytest.mark.parametrize("fmt", FORMATS)
def test_a_missing_path_raises_naming_it(tmp_path, fmt, reader, missing):
    path = str(tmp_path / missing.format(fmt=fmt)) + ("/" if missing.endswith("/") else "")
    with raises_not_found(path):
        reader.open(fmt, path)


@pytest.mark.parametrize("fmt", FORMATS)
def test_a_missing_file_url_raises_naming_it(tmp_path, fmt, reader):
    url = (tmp_path / f"typo.{fmt}").as_uri()
    with raises_not_found(url):
        reader.open(fmt, url)


@pytest.mark.parametrize("fmt", FORMATS)
def test_a_glob_that_matches_no_file_of_the_format_raises(tmp_path, fmt, reader):
    d = tmp_path / "d"
    write(d / f"sub.{fmt}" / f"a.{fmt}", [1])  # a directory the glob matches
    (d / "notes.txt").write_text("x\n1\n")
    for pattern in [f"{d}/z*.{fmt}", f"{d}/*.txt", f"{d}/no*/*.{fmt}", f"{d}/*.{fmt}"]:
        with raises_not_found(pattern):
            reader.open(fmt, pattern)


@pytest.mark.parametrize("fmt", FORMATS)
def test_a_directory_without_a_file_of_the_format_raises(tmp_path, fmt, reader):
    empty = tmp_path / "empty"
    empty.mkdir()
    other = tmp_path / "other"
    write(other / f"sub/a.{fmt}", [1])  # subdirectories are not read
    (other / "notes.txt").write_text("x\n1\n")
    for d in [empty, other]:
        with raises_not_found(str(d)):
            reader.open(fmt, str(d))


class TestExistingFilesAreNotMissing:
    """A file that exists is read, whatever it holds."""

    def test_a_zero_byte_csv_reads_as_no_columns(self, tmp_path):
        path = tmp_path / "zero.csv"
        path.touch()
        t = LTSeq.read_csv(str(path))
        assert t.columns == [] and t.count() == 0

    def test_a_header_only_csv_reads_as_its_columns(self, tmp_path):
        path = tmp_path / "header.csv"
        path.write_text("a,b\n")
        t = LTSeq.read_csv(str(path))
        assert t.columns == ["a", "b"] and t.count() == 0
        assert sum(b.num_rows for b in LTSeq.scan(str(path))) == 0

    def test_a_zero_row_parquet_file_reads_as_its_schema(self, tmp_path):
        path = tmp_path / "zero.parquet"
        pq.write_table(pa.table({"a": pa.array([], pa.int64())}), path)
        t = LTSeq.read_parquet(str(path))
        assert t.to_arrow().schema == pa.schema([("a", pa.int64())]) and t.count() == 0
        assert sum(b.num_rows for b in LTSeq.scan_parquet(str(path))) == 0


@pytest.mark.parametrize("fmt", FORMATS)
def test_a_relative_path_resolves_against_the_working_directory(
    tmp_path, monkeypatch, fmt, reader
):
    write(tmp_path / "d" / f"a.{fmt}", [1])
    write(tmp_path / "d" / f"b[1].{fmt}", [2])
    write(tmp_path / "e" / f"c.{fmt}", [3])
    monkeypatch.chdir(tmp_path / "e")

    assert reader(fmt, f"../d/a.{fmt}") == [1]
    assert reader(fmt, f"./../d/b[1].{fmt}") == [2]
    assert reader(fmt, f"../d/*.{fmt}") == [1, 2]
    assert reader(fmt, f"./../e/../d/[ab]*.{fmt}") == [1, 2]
    assert reader(fmt, "../d") == [1, 2]
    assert reader(fmt, f"c.{fmt}") == [3]
    with raises_not_found(f"../d/z*.{fmt}"):
        reader.open(fmt, f"../d/z*.{fmt}")
