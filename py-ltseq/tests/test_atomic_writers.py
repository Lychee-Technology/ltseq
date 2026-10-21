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

import errno
import os
import shutil
import stat
import struct
import subprocess
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


def parse(writer: str, data: bytes) -> list[int]:
    """The `x` column of what a writer wrote."""
    if writer == "write_parquet":
        return pq.read_table(pa.BufferReader(data)).column("x").to_pylist()
    lines = data.decode().splitlines()
    assert lines[0] == "x"
    return [int(v) for v in lines[1:]]


def read_back(writer: str, path) -> list[int]:
    return parse(writer, path.read_bytes())


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


UNIONS = {
    "sparse": lambda: pa.UnionArray.from_sparse(pa.array([0], pa.int8()), [pa.array([1])]),
    "dense": lambda: pa.UnionArray.from_dense(
        pa.array([0], pa.int8()), pa.array([0], pa.int32()), [pa.array([1])]
    ),
}


@pytest.mark.parametrize("union", UNIONS)
def test_panic_during_write_keeps_existing_file(tmp_path, union):
    """A panic unwinds through the writer and still removes its temporary file.

    Sparse and dense unions both panic inside the `parquet` crate. #270
    refuses a union at the call, after which this only checks the refusal.
    """
    path = existing(tmp_path)
    listing = sorted(os.listdir(tmp_path))
    t = table(x=UNIONS[union]())
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
    # A union is refused itself only with #270; until then its members are checked.
    "sparse union member": lambda t, v: pa.UnionArray.from_sparse(
        pa.array([1], pa.int8()), [pa.array([0], pa.int64()), pa.array([v], t)]
    ),
    "dense union member": lambda t, v: pa.UnionArray.from_dense(
        pa.array([1], pa.int8()),
        pa.array([0], pa.int32()),
        [pa.array([], pa.int64()), pa.array([v], t)],
    ),
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
    assert os.readlink(link) == str(target)
    assert read_back(writer, target) == [1, 2, 3]
    assert sorted(os.listdir(tmp_path)) == ["link", "target"]


@unix_only
@pytest.mark.parametrize("writer", WRITERS)
@pytest.mark.parametrize("chain", ["relative", "absolute", "through a second link"])
def test_dangling_symlink_target_is_created(tmp_path, writer, chain):
    """A link whose target does not exist yet gets its target written."""
    (tmp_path / "data").mkdir()
    target = tmp_path / "data" / "target"
    link = tmp_path / "link"
    if chain == "relative":
        link.symlink_to("data/target")
    elif chain == "absolute":
        link.symlink_to(target)
    else:
        # Relative to its own directory, not to the first link's.
        (tmp_path / "data" / "hop").symlink_to("target")
        link.symlink_to("data/hop")
    links = sorted(os.listdir(tmp_path / "data"))
    texts = link_texts(tmp_path)
    getattr(ints(), writer)(str(link))
    assert link.is_symlink()
    assert link_texts(tmp_path) == texts
    assert read_back(writer, target) == [1, 2, 3]
    assert sorted(os.listdir(tmp_path)) == ["data", "link"]
    assert sorted(os.listdir(tmp_path / "data")) == sorted([*links, "target"])


def link_texts(root) -> dict[str, str]:
    """Every symlink under `root`, by path relative to it, with its text."""
    return {
        os.path.relpath(os.path.join(d, name), root): os.readlink(os.path.join(d, name))
        for d, dirs, files in os.walk(root)
        for name in dirs + files
        if os.path.islink(os.path.join(d, name))
    }


def listings(root) -> dict[str, list[str]]:
    """The listing of `root` and of every directory under it."""
    return {os.path.relpath(d, root): sorted(dirs + files) for d, dirs, files in os.walk(root)}


@unix_only
@pytest.mark.parametrize("writer", WRITERS)
@pytest.mark.parametrize("target", ["existing", "dangling"])
def test_failed_write_through_symlink_keeps_link_and_target(tmp_path, writer, target):
    """A write that fails after its temporary file exists leaves the link.

    An existing target keeps its bytes, and a dangling link's target is
    not created.
    """
    (tmp_path / "data").mkdir()
    if target == "existing":
        existing(tmp_path / "data", "target")
    link = tmp_path / "link"
    link.symlink_to("data/target")
    before = listings(tmp_path)
    t = divide_by_zero_on_last_row()
    with pytest.raises(Exception, match="Divide by zero"):
        getattr(t, writer)(str(link))
    assert link_texts(tmp_path) == {"link": "data/target"}
    assert listings(tmp_path) == before
    if target == "existing":
        assert (tmp_path / "data" / "target").read_bytes() == OLD


@unix_only
@pytest.mark.parametrize("writer", WRITERS)
def test_symlink_cycle_is_refused(tmp_path, writer):
    (tmp_path / "a").symlink_to("b")
    (tmp_path / "b").symlink_to("a")
    with pytest.raises(ValueError, match="Too many levels of symbolic links"):
        getattr(ints(), writer)(str(tmp_path / "a"))
    assert link_texts(tmp_path) == {"a": "b", "b": "a"}


@unix_only
@pytest.mark.parametrize("writer", WRITERS)
def test_dangling_symlink_into_missing_directory_is_refused(tmp_path, writer):
    """As with `File::create`, a link's missing directory is not created."""
    (tmp_path / "link").symlink_to("missing/target")
    with pytest.raises(ValueError, match="No such file or directory"):
        getattr(ints(), writer)(str(tmp_path / "link"))
    assert link_texts(tmp_path) == {"link": "missing/target"}
    assert os.listdir(tmp_path) == ["link"]


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


@pytest.mark.parametrize("writer", WRITERS)
def test_concurrent_writes_leave_one_complete_file(tmp_path, writer):
    """Two writes to one path each rename a whole file; the last one wins.

    `File::create` gave both writers the same file, so their output could
    interleave. Whichever order the writes finish in, the file is one of
    the two tables, whole.
    """
    path = existing(tmp_path)
    sizes = {"a": 300_000, "b": 200_000}
    tables = {k: table(x=pa.array(range(n), pa.int64())) for k, n in sizes.items()}
    failures = []

    def write(t):
        try:
            getattr(t, writer)(str(path))
        except BaseException as e:
            failures.append(e)

    threads = [threading.Thread(target=write, args=(t,)) for t in tables.values()]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if failures:
        raise failures[0]
    assert read_back(writer, path) in [list(range(n)) for n in sizes.values()]
    assert os.listdir(tmp_path) == ["out"]


# Preloaded, it renames $SWAP_DIR/0, then /1, ... over $SWAP_PATH each time
# the writer resolves that path, as another write's commit would, until none
# is left. That lands the commit between the writer's look at what `path`
# opens and its look at where `path` leads, which threads hit only by chance.
SWAP_SHIM = r"""
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

char *realpath(const char *path, char *resolved) {
    static char *(*real)(const char *, char *);
    static int next;
    const char *target = getenv("SWAP_PATH"), *dir = getenv("SWAP_DIR");
    if (!real)
        real = (char *(*)(const char *, char *))dlsym(RTLD_NEXT, "realpath");
    if (target && dir && strcmp(path, target) == 0) {
        char from[4096];
        snprintf(from, sizeof from, "%s/%d", dir, next);
        if (rename(from, target) == 0)
            next++;
    }
    return real(path, resolved);
}
"""


@pytest.fixture(scope="module")
def swap_shim(tmp_path_factory):
    if not sys.platform.startswith("linux"):
        pytest.skip("Linux LD_PRELOAD")
    if shutil.which("cc") is None:
        pytest.skip("needs cc to build the LD_PRELOAD library on Linux")
    root = tmp_path_factory.mktemp("shim")
    src, lib = root / "swap.c", root / "swap.so"
    src.write_text(SWAP_SHIM)
    subprocess.run(["cc", "-shared", "-fPIC", "-o", lib, src, "-ldl"], check=True)
    return lib


def write_while_committed(tmp_path, swap_shim, writer: str, commits: int):
    """Write a failing table to `out` while `commits` other writes replace it.

    Each commit is a complete file the same writer wrote, and `out` must
    end up holding one, with nothing else left in the directory. Returns
    the commits left unused and the error the write raised.
    """
    path = existing(tmp_path)
    swap = tmp_path / "swap"
    swap.mkdir()
    getattr(ints(), writer)(str(swap / "0"))
    committed = (swap / "0").read_bytes()
    for k in range(1, commits):
        (swap / str(k)).write_bytes(committed)
    code = (
        "import sys, pyarrow as pa\n"
        "from ltseq import LTSeq\n"
        "t = LTSeq.from_arrow(pa.table({'n': pa.array([1, 2]), 'd': pa.array([1, 0])}))\n"
        "t = t.derive(lambda r: {'q': r.n // r.d})\n"
        "try:\n"
        "    getattr(t, sys.argv[1])(sys.argv[2])\n"
        "except Exception as e:\n"
        "    print(f'{type(e).__name__}: {e}')\n"
        "    sys.exit(3)\n"
    )
    env = dict(os.environ, LD_PRELOAD=str(swap_shim), SWAP_PATH=str(path), SWAP_DIR=str(swap))
    run = subprocess.run(
        [sys.executable, "-c", code, writer, str(path)], env=env, capture_output=True
    )
    # An Exception, not a panic or a crash.
    assert run.returncode == 3, run.stderr.decode()
    assert sorted(os.listdir(tmp_path)) == ["out", "swap"]
    assert path.read_bytes() == committed
    return os.listdir(swap), run.stdout.decode()


@pytest.mark.parametrize("writer", WRITERS)
def test_write_committed_while_path_is_checked_survives_a_failed_write(
    tmp_path, swap_shim, writer
):
    """Another write replaces `path` while this one checks it, then this one fails.

    The two looks then see different files. That used to be taken for a
    /proc descriptor link and written in place, so `File::create` truncated
    the other write's complete file before this one's plan failed. This one
    looks again, finds a regular file, and goes through a temporary file.
    """
    unused, error = write_while_committed(tmp_path, swap_shim, writer, commits=1)
    assert unused == []
    assert "Divide by zero" in error


@pytest.mark.parametrize("writer", WRITERS)
def test_write_replaced_at_every_check_fails_untouched(tmp_path, swap_shim, writer):
    """Writes that keep replacing `path` while this one checks it.

    It gives up before writing anything, and `path` holds the last of their
    complete files.
    """
    unused, error = write_while_committed(tmp_path, swap_shim, writer, commits=100)
    assert unused, "the write outlasted every other write instead of giving up"
    assert error.startswith("ValueError:")
    assert "replaced each time it was checked" in error


@unix_only
@pytest.mark.parametrize("writer", WRITERS)
def test_existing_file_keeps_its_mode(tmp_path, writer):
    path = existing(tmp_path)
    path.chmod(0o640)
    getattr(ints(), writer)(str(path))
    assert stat.S_IMODE(path.stat().st_mode) == 0o640


@unix_only
@pytest.mark.parametrize("writer", WRITERS)
def test_existing_file_keeps_its_group(tmp_path, writer):
    """The group bits keep applying to the group they were set for."""
    others = [g for g in os.getgroups() if g != os.getegid()]
    if not others:
        pytest.skip("needs a supplementary group")
    path = existing(tmp_path)
    os.chown(path, -1, others[0])
    path.chmod(0o640)
    getattr(ints(), writer)(str(path))
    st = path.stat()
    assert (st.st_gid, stat.S_IMODE(st.st_mode)) == (others[0], 0o640)


ACCESS_ACL, DEFAULT_ACL = "system.posix_acl_access", "system.posix_acl_default"


def posix_acl(owner: int, user: int, group: int, other: int) -> bytes:
    """An ACL in Linux's extended-attribute format (`posix_acl_xattr.h`).

    It grants another user (our uid + 1) `user` permissions, with the mask
    set to what that user and the group may do.
    """
    undefined = 0xFFFFFFFF
    entries = [
        (0x01, owner, undefined),  # user::
        (0x02, user, os.getuid() + 1),  # user:<uid>:
        (0x04, group, undefined),  # group::
        (0x10, user | group, undefined),  # mask::
        (0x20, other, undefined),  # other::
    ]
    return struct.pack("<I", 2) + b"".join(struct.pack("<HHI", *e) for e in entries)


def set_acl(path, name: str, value: bytes) -> None:
    try:
        os.setxattr(path, name, value)
    except OSError as e:
        if e.errno in (errno.ENOTSUP, errno.EOPNOTSUPP):
            pytest.skip("the filesystem has no POSIX ACLs")
        raise


def access_acl(path) -> bytes | None:
    try:
        return os.getxattr(path, ACCESS_ACL)
    except OSError as e:
        if e.errno == errno.ENODATA:
            return None
        raise


linux_acls = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux POSIX ACLs")


@linux_acls
@pytest.mark.parametrize("writer", WRITERS)
def test_existing_file_keeps_its_acl(tmp_path, writer):
    """An ACL entry for another user keeps applying to that user alone.

    The mode's group bits show the ACL's mask, so the mode without the ACL
    would grant the file's group what only that user had.
    """
    path = existing(tmp_path)
    path.chmod(0o600)
    set_acl(path, ACCESS_ACL, posix_acl(owner=6, user=4, group=0, other=0))
    before = access_acl(path)
    getattr(ints(), writer)(str(path))
    assert access_acl(path) == before
    assert stat.S_IMODE(path.stat().st_mode) == 0o640


@linux_acls
@pytest.mark.parametrize("writer", WRITERS)
def test_directory_default_acl_grants_nothing_new(tmp_path, writer):
    """A replacement does not take its directory's default ACL.

    The old file was written before the default was set and has no ACL, so
    the user the default names could not read it. A new file inherits the
    default, as with `File::create`; a replacement keeps the old access.
    """
    path = existing(tmp_path)
    path.chmod(0o640)
    set_acl(tmp_path, DEFAULT_ACL, posix_acl(owner=7, user=6, group=5, other=5))
    getattr(ints(), writer)(str(path))
    assert access_acl(path) is None
    assert stat.S_IMODE(path.stat().st_mode) == 0o640


macos_acls = pytest.mark.skipif(sys.platform != "darwin", reason="macOS ACLs")


def macos_acl(path) -> list[str]:
    """The entries of `path`'s macOS ACL, as `ls -le` lists them."""
    listing = subprocess.run(
        ["ls", "-led", str(path)], capture_output=True, text=True, check=True
    ).stdout
    return [line.split(": ", 1)[1] for line in listing.splitlines()[1:]]


def add_macos_acl(path, entry: str) -> None:
    subprocess.run(["chmod", "+a", entry, str(path)], check=True)


@macos_acls
@pytest.mark.parametrize("writer", WRITERS)
def test_existing_file_keeps_its_macos_acl(tmp_path, writer):
    """An entry that denies a user access keeps denying it.

    The mode lets every user read the file, so without its ACL the file
    would grant `nobody` what the ACL denied.
    """
    path = existing(tmp_path)
    path.chmod(0o644)
    add_macos_acl(path, "user:nobody deny read")
    add_macos_acl(path, "user:daemon allow write")
    before = macos_acl(path)
    getattr(ints(), writer)(str(path))
    assert macos_acl(path) == before
    assert stat.S_IMODE(path.stat().st_mode) == 0o644


@macos_acls
@pytest.mark.parametrize("writer", WRITERS)
def test_directory_inherited_acl_grants_nothing_new_on_macos(tmp_path, writer):
    """A replacement does not take the entries its directory gives new files.

    The old file was written before the directory's entry was added, so
    `nobody` could not read it. On macOS an allow entry applies whatever the
    mode, so the replacement must not carry it even while owner-only.
    """
    path = existing(tmp_path)
    path.chmod(0o600)
    add_macos_acl(tmp_path, "user:nobody allow read,file_inherit")
    getattr(ints(), writer)(str(path))
    assert macos_acl(path) == []
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


@macos_acls
@pytest.mark.parametrize("writer", WRITERS)
def test_new_file_inherits_directory_acl_on_macos(tmp_path, writer):
    """A new file takes its directory's inherited entries, as `File::create`'s did."""
    add_macos_acl(tmp_path, "user:nobody allow read,file_inherit")
    control = tmp_path / "control"
    control.write_bytes(b"")
    path = tmp_path / "new"
    getattr(ints(), writer)(str(path))
    assert macos_acl(path) == macos_acl(control) != []


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


proc_fd = pytest.mark.skipif(
    not os.path.isdir("/proc/self/fd"), reason="Linux /proc descriptor links"
)


@pytest.mark.skipif(not os.path.exists("/dev/stdout"), reason="needs /dev/stdout")
@pytest.mark.parametrize("writer", WRITERS)
@pytest.mark.parametrize(
    "path", ["/dev/stdout", pytest.param("/proc/self/fd/1", marks=proc_fd)]
)
def test_piped_stdout_is_written_in_place(writer, path):
    """Standard output that is a pipe, as in a shell pipeline.

    On Linux `/dev/stdout` links to `/proc/self/fd/1`, whose link text is
    then `pipe:[inode]`, not a path. The write goes to the pipe.
    """
    code = (
        "import pyarrow as pa\n"
        "from ltseq import LTSeq\n"
        "t = LTSeq.from_arrow(pa.table({'x': pa.array([1, 2, 3], pa.int64())}))\n"
        f"t.{writer}({path!r})\n"
    )
    run = subprocess.run([sys.executable, "-c", code], capture_output=True)
    assert run.returncode == 0, run.stderr.decode()
    assert parse(writer, run.stdout) == [1, 2, 3]


@pytest.mark.skipif(not os.path.exists("/dev/fd/1"), reason="needs /dev/fd")
@pytest.mark.parametrize("writer", WRITERS)
@pytest.mark.parametrize("path", ["/dev/stdout", "/dev/fd/1"])
@pytest.mark.parametrize("name", ["out", "1"])
def test_stdout_redirected_to_a_file(tmp_path, writer, path, name):
    """`python script.py > out`, where the script writes to standard output.

    On Linux the descriptor link names `out`, which is replaced. On macOS
    `/dev/fd/1` is the descriptor itself, in a directory where nothing can
    be created, so it is written in place as `File::create` wrote it. There
    `realpath` gives `/dev/fd/out`, which names no file, but for a file
    named `1` it gives `/dev/fd/1`, the descriptor.
    """
    code = (
        "import pyarrow as pa\n"
        "from ltseq import LTSeq\n"
        "t = LTSeq.from_arrow(pa.table({'x': pa.array([1, 2, 3], pa.int64())}))\n"
        f"t.{writer}({path!r})\n"
    )
    out = tmp_path / name
    with open(out, "wb") as stdout:
        run = subprocess.run([sys.executable, "-c", code], stdout=stdout, stderr=subprocess.PIPE)
    assert run.returncode == 0, run.stderr.decode()
    assert read_back(writer, out) == [1, 2, 3]
    assert os.listdir(tmp_path) == [name]


@proc_fd
@pytest.mark.parametrize("writer", WRITERS)
def test_descriptor_link_to_deleted_file_is_written_in_place(tmp_path, writer):
    """`/proc/self/fd/N` of a file deleted since it was opened.

    Its link text, `.../out (deleted)`, names no file, so nothing can be
    renamed over it. The write goes to the open file, as `File::create`'s
    did, and nothing is created in the directory.
    """
    path = existing(tmp_path)
    fd = os.open(path, os.O_RDONLY)
    try:
        path.unlink()
        getattr(ints(), writer)(f"/proc/self/fd/{fd}")
        written = os.pread(fd, os.fstat(fd).st_size, 0)
    finally:
        os.close(fd)
    assert parse(writer, written) == [1, 2, 3]
    assert os.listdir(tmp_path) == []


@pytest.mark.skipif(not os.path.isdir("/dev/fd"), reason="needs /dev/fd")
@pytest.mark.parametrize("writer", WRITERS)
def test_dev_fd_of_deleted_file_is_written_in_place(tmp_path, writer):
    """`/dev/fd/N` of a file deleted since it was opened.

    Linux links `/dev/fd` to `/proc/self/fd`. On macOS opening `/dev/fd/N`
    duplicates the descriptor, so it must be open for writing, and the
    write starts at its offset without truncating, as `File::create`'s did;
    the file starts empty so that both read the same. Either way the file
    has no name to rename over, and the write goes to it.
    """
    path = tmp_path / "out"
    path.write_bytes(b"")
    fd = os.open(path, os.O_RDWR)
    try:
        path.unlink()
        getattr(ints(), writer)(f"/dev/fd/{fd}")
        written = os.pread(fd, os.fstat(fd).st_size, 0)
    finally:
        os.close(fd)
    assert parse(writer, written) == [1, 2, 3]
    assert os.listdir(tmp_path) == []


def can_bind_mount(tmp_path) -> bool:
    """Whether this user may bind-mount in a namespace of its own."""
    probe = tmp_path / "probe"
    probe.write_bytes(b"")
    try:
        run = subprocess.run(
            ["unshare", "-rm", "mount", "--bind", str(probe), str(probe)], capture_output=True
        )
    except FileNotFoundError:
        return False
    finally:
        probe.unlink()
    return run.returncode == 0


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux mount namespaces")
@pytest.mark.parametrize("writer", WRITERS)
def test_mount_point_is_not_replaced(tmp_path, writer):
    """A file that is itself a mount point, as in a Docker single-file bind mount.

    No rename can replace it, and unlike a device it is not written in
    place, as `File::create` wrote it: the write fails with EBUSY once the
    table is written. The mounted file is unchanged and no temporary file
    is left. The write runs in a user and mount namespace (`unshare -rm`)
    where `out` is a bind mount of `mounted`.
    """
    if not can_bind_mount(tmp_path):
        pytest.skip("needs unshare -rm, Linux mount namespaces without root")
    mounted = tmp_path / "mounted"
    mounted.write_bytes(b"mounted file\n")
    path = existing(tmp_path)
    listing = sorted(os.listdir(tmp_path))
    code = (
        "import sys, pyarrow as pa\n"
        "from ltseq import LTSeq\n"
        "t = LTSeq.from_arrow(pa.table({'x': pa.array([1, 2, 3], pa.int64())}))\n"
        f"t.{writer}(sys.argv[1])\n"
    )
    script = 'mount --bind "$1" "$2" && exec "$3" -c "$4" "$2"'
    run = subprocess.run(
        ["unshare", "-rm", "sh", "-c", script, "sh", mounted, path, sys.executable, code],
        capture_output=True,
    )
    assert run.returncode != 0
    assert f"(os error {errno.EBUSY})" in run.stderr.decode()
    assert mounted.read_bytes() == b"mounted file\n"
    assert_failed_cleanly(path, listing)
