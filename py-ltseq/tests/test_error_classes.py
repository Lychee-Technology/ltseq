"""Which exception class a failure raises, and that it does not depend on the path (#152).

Three things are pinned here:

- A column the transpiler cannot find raises ``ColumnNotFoundError`` from
  every operation that lowers an expression. ``SchemaProxy`` refuses an
  unknown name before Rust sees it, so these tests hand the native layer a
  serialized expression naming a column the table does not have.
- The same kind of failure raises the same class from every terminal and
  writer: a failed execution is a ``ValueError`` whether ``to_dicts()``,
  ``collect()``, ``count()`` or a writer runs the plan, and an I/O failure is a
  ``ValueError`` from readers and writers alike. These are today's classes;
  the v0.5 contract's ``ExecutionError`` and ``LTSeqIOError`` are M15
  (#266).
- A Parquet fast path that cannot count an input leaves it to the general
  path, and one that fails reports the failure. The two are told apart by
  type, not by a marker in the message.
"""

import datetime
import os
import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq
from ltseq.exceptions import ColumnNotFoundError
from ltseq.expr import ColumnExpr

MISSING = {"type": "Column", "name": "nope"}
X = {"type": "Column", "name": "x"}
ONE = {"type": "Literal", "value": 1, "dtype": "Int64"}


def _gt(left, right):
    return {"type": "BinOp", "op": "Gt", "left": left, "right": right}


def _call(func, on, *args):
    return {"type": "Call", "func": func, "args": list(args), "kwargs": {}, "on": on}


def _table():
    return LTSeq.from_arrow(pa.table({"x": pa.array([1, 2, 3], pa.int64())}))


# A missing column deep inside the expression, not only at its root.
NESTED = _gt(_call("abs", MISSING), ONE)

OPERATIONS = {
    "filter": lambda inner: inner.filter(NESTED),
    "search_first": lambda inner: inner.search_first(NESTED),
    "select": lambda inner: inner.select([_call("abs", MISSING)]),
    "derive": lambda inner: inner.derive({"v": _call("abs", MISSING)}),
    "derive_window": lambda inner: inner.derive({"v": _call("shift", MISSING, ONE)}),
    "sort": lambda inner: inner.sort([MISSING], [False], [True]),
    "agg_group": lambda inner: inner.agg(MISSING, {"n": _call("count", X)}),
    "agg_where": lambda inner: inner.agg(None, {"n": _call("count_if", None, NESTED)}),
}


@pytest.mark.parametrize("operation", OPERATIONS)
def test_missing_column_reaching_rust_is_column_not_found(operation):
    """Each of these used to raise a plain ValueError (or, for sort and
    search_first, one classed as a validation error) naming the column."""
    with pytest.raises(ColumnNotFoundError, match="Column 'nope' not found in schema"):
        OPERATIONS[operation](_table()._inner)


def _sorted_table():
    return LTSeq.from_arrow(pa.table({"x": [1, 2, 3], "g": [1, 1, 2]})).sort("x")


def test_missing_column_named_like_the_nested_window_refusal_is_column_not_found():
    """derive's nested-window hint read the error's text, so a missing
    column whose name contained "requires DataFrame context" lost its class
    and got an unrelated hint."""
    t = _sorted_table()
    with pytest.raises(ColumnNotFoundError, match="not found in schema") as raised:
        t.derive(out=lambda r: ColumnExpr("missing requires DataFrame context").shift(1))
    assert "split it into two steps" not in str(raised.value)


def test_window_over_a_nested_window_keeps_the_hint():
    with pytest.raises(ValueError, match="split it into two steps") as raised:
        _sorted_table().derive(out=lambda r: r.x.rolling(3).mean().shift(1).rolling(2).sum())
    assert not isinstance(raised.value, ColumnNotFoundError)


# Aggregates that take a bare column build its reference without lowering an
# expression; they raised DataFusion's RuntimeError for a missing one.
BARE_AGGREGATES = {
    "sum": lambda c: c.sum(),
    "min": lambda c: c.min(),
    "max": lambda c: c.max(),
    "mean": lambda c: c.mean(),
    "median": lambda c: c.median(),
    "std": lambda c: c.std(),
    "count": lambda c: c.count(),
    "first": lambda c: c.first(),
    "last": lambda c: c.last(),
    "mode": lambda c: c.mode(),
    "skew": lambda c: c.skew(),
    "top_k": lambda c: c.top_k(2),
}


@pytest.mark.parametrize("aggregate", BARE_AGGREGATES)
def test_bare_column_aggregate_of_a_missing_column_is_column_not_found(aggregate):
    t = _sorted_table()
    with pytest.raises(ColumnNotFoundError, match="Column 'missing' not found in schema"):
        t.agg(n=lambda r: BARE_AGGREGATES[aggregate](ColumnExpr("missing"))).to_dicts()


GROUP_WINDOWS = {
    "derive GroupAgg": lambda n: n.derive(lambda g: {"m": g.max("missing")}),
    "derive GroupRowColumn": lambda n: n.derive(lambda g: {"m": g.first().missing}),
    "filter GroupAgg": lambda n: n.filter(lambda g: g.sum("missing") > 1),
    "filter GroupRowColumn": lambda n: n.filter(lambda g: g.last().missing > 1),
}


@pytest.mark.parametrize("case", GROUP_WINDOWS)
def test_group_window_of_a_missing_column_is_column_not_found(case):
    """The group dialect's aggregates and row picks raised RuntimeError."""
    nested = _sorted_table().group_ordered(lambda r: r.g)
    with pytest.raises(ColumnNotFoundError, match="Column 'missing' not found in schema"):
        GROUP_WINDOWS[case](nested).to_dicts()


def _divides_by_zero():
    # The last row divides by zero, so every terminal demands a failing value.
    t = LTSeq.from_arrow(pa.table({"n": pa.array(range(5), pa.int64()), "d": pa.array([1, 1, 1, 1, 0], pa.int64())}))
    return t.filter(lambda r: r.n // r.d >= 0)


TERMINALS = {
    "to_arrow": lambda t, _: t.to_arrow(),
    "to_dicts": lambda t, _: t.to_dicts(),
    "to_pandas": lambda t, _: t.to_pandas(),
    "count": lambda t, _: t.count(),
    "collect": lambda t, _: t.collect(),
    "write_csv": lambda t, path: t.write_csv(str(path / "out.csv")),
    "write_parquet": lambda t, path: t.write_parquet(str(path / "out.parquet")),
}


@pytest.mark.parametrize("terminal", TERMINALS)
def test_failed_execution_raises_one_class_from_every_terminal(terminal, tmp_path):
    """write_parquet and collect raised RuntimeError here, and the other
    terminals ValueError."""
    if terminal == "to_pandas":
        pytest.importorskip("pandas")
    with pytest.raises(ValueError, match="Divide by zero") as raised:
        TERMINALS[terminal](_divides_by_zero(), tmp_path)
    assert not isinstance(raised.value, RuntimeError)


@pytest.mark.parametrize("writer", ["write_csv", "write_parquet"])
def test_writer_io_failure_is_a_value_error(writer, tmp_path):
    with pytest.raises(ValueError, match="I/O error") as raised:
        getattr(_table(), writer)(str(tmp_path / "missing" / "out"))
    assert not isinstance(raised.value, RuntimeError)


def test_reader_io_failure_is_a_value_error(tmp_path):
    path = tmp_path / "not.parquet"
    path.write_text("x\n1\n")
    with pytest.raises(ValueError, match="I/O error") as raised:
        LTSeq.read_parquet(str(path))
    assert not isinstance(raised.value, RuntimeError)


# ---- Parquet fast paths: an unsupported input falls back, a failure raises ----


def _events(key_type):
    keys = ["a", "a", "a", "b", "b", "c", "c", "c"]
    return pa.table(
        {
            "i": pa.array(range(8), pa.int64()),
            "k": pa.array(keys if key_type == "string" else [ord(k) for k in keys], key_type if key_type != "string" else pa.string()),
            "e": pa.array([1, 1, 1, 1, 1, 1, 0, 1], pa.int64()),
        }
    )


STEPS = (lambda r: r.e == 1, lambda r: r.e == 1)


@pytest.mark.parametrize("key_type", ["string", pa.float64()], ids=["string", "float64"])
def test_pattern_count_with_a_key_the_fast_path_cannot_compare_falls_back(key_type, tmp_path):
    """The parallel pattern count compares integer keys only. A table read
    from sorted Parquet with another key type raised "Partition column has
    unsupported type" instead of being counted by the general path."""
    table = _events(key_type)
    path = str(tmp_path / "events.parquet")
    pq.write_table(table, path, row_group_size=3)
    expected = LTSeq.from_arrow(table).sort("i").search_pattern_count(*STEPS, partition_by="k")
    assert expected == 3

    got = LTSeq.read_parquet(path).assume_sorted("i").search_pattern_count(*STEPS, partition_by="k")

    assert got == expected


def test_pattern_count_reports_a_source_it_cannot_read_as_the_general_path_does(tmp_path):
    """A read failure inside the fast path is an error, not a fallback, and
    of the class the general path raises for it. The fast path
    (``assume_sorted`` on Parquet) raised RuntimeError here and the general
    path (``sort``) ValueError."""
    path = str(tmp_path / "events.parquet")
    pq.write_table(_events(pa.int64()), path, row_group_size=3)
    fast = LTSeq.read_parquet(path).assume_sorted("i")
    general = LTSeq.read_parquet(path).sort("i")
    os.remove(path)

    with pytest.raises(ValueError, match="I/O error .*Failed to open Parquet") as raised:
        fast.search_pattern_count(*STEPS, partition_by="k")
    assert not isinstance(raised.value, RuntimeError)
    with pytest.raises(ValueError) as raised:
        general.search_pattern_count(*STEPS, partition_by="k")
    assert not isinstance(raised.value, RuntimeError)


# ---- group_ordered(...).first().count(): a decline falls back, a failure raises ----


def _sessions(t):
    return t.group_ordered(lambda r: r.k != r.k.shift(1)).first()


class _FailingCount:
    """A grouped table whose count kernel fails as a read inside it does,
    with an ``LtseqError::Io``; every other method is the table's own, so
    the materialized retry would succeed."""

    def __init__(self, inner):
        self._inner = inner

    def group_ordered_count(self, expr_dict):
        raise ValueError("I/O error: Failed to open Parquet 'events.parquet': transient")

    def __getattr__(self, name):
        return getattr(self._inner, name)


def test_group_count_reports_a_fast_path_failure_the_retry_would_hide(monkeypatch):
    """count() caught every error of the count kernel and counted the
    materialized first rows instead, so a failure the retry did not meet
    was discarded (R2-F1 of #422's review). Only a decline (None) falls
    back now."""
    from ltseq.grouping.nested_table import NestedTable

    t = LTSeq.from_arrow(_events(pa.int64())).sort("i")
    assert _sessions(t).count() == 3
    assert len(_sessions(t).to_arrow()) == 3

    real = NestedTable._staged_key

    def failing(self, *args, **kwargs):
        staged, expr_dict, hidden = real(self, *args, **kwargs)
        return _FailingCount(staged), expr_dict, hidden

    monkeypatch.setattr(NestedTable, "_staged_key", failing)
    assert len(_sessions(t).to_arrow()) == 3, "the materialized retry succeeds"
    with pytest.raises(ValueError, match="transient"):
        _sessions(t).count()
    with pytest.raises(ValueError, match="transient"):
        len(_sessions(t))


def test_group_count_reports_a_source_the_fast_path_cannot_read(tmp_path):
    """A read failure of the Parquet count is the error count() raises, not
    a retry through DataFusion that reports something else."""
    path = str(tmp_path / "events.parquet")
    pq.write_table(_events(pa.int64()), path, row_group_size=3)
    fast = LTSeq.read_parquet(path).assume_sorted("i")
    with open(path, "wb") as f:
        f.write(b"not a parquet file")

    with pytest.raises(ValueError, match="Failed to read Parquet metadata") as raised:
        _sessions(fast).count()
    assert not isinstance(raised.value, RuntimeError)


def test_group_count_falls_back_for_a_predicate_the_kernel_declines():
    t = LTSeq.from_arrow(_events(pa.int64())).sort("i")
    pred = lambda r: r.k.shift(1, default=0) != r.k  # noqa: E731
    assert t._inner.group_ordered_count(t._capture_expr(pred)) is None
    assert t.group_ordered(pred).first().count() == len(t.group_ordered(pred).first().to_arrow())


@pytest.mark.skipif(
    not hasattr(os, "geteuid") or os.geteuid() == 0,
    reason="needs a POSIX user that directory permissions apply to",
)
def test_counts_read_the_listed_file_where_its_relative_path_cannot_be_looked_up(tmp_path, monkeypatch):
    """The direct scans open the file the reader listed, by the absolute path
    it listed at the call, not the relative path as written, so a working
    directory in which that path cannot be searched does not change the
    count (R3-F1 and R6-F2 of #422's review)."""
    data = tmp_path / "data"
    (data / "sub").mkdir(parents=True)
    pq.write_table(_events(pa.int64()), str(data / "sub" / "events.parquet"))
    monkeypatch.chdir(data)
    fast = LTSeq.read_parquet("sub/events.parquet").assume_sorted("i")
    patterns = fast.search_pattern_count(*STEPS, partition_by="k")

    elsewhere = tmp_path / "elsewhere"
    (elsewhere / "sub").mkdir(parents=True)
    monkeypatch.chdir(elsewhere)
    (elsewhere / "sub").chmod(0)
    try:
        assert _sessions(fast).count() == len(_sessions(fast).to_arrow()) == 3
        assert fast.search_pattern_count(*STEPS, partition_by="k") == patterns
    finally:
        (elsewhere / "sub").chmod(0o755)


def test_group_count_falls_back_for_a_parquet_directory(tmp_path):
    """A directory, which the direct reader does not open, is still counted
    through the materialized first rows."""
    pq.write_table(_events(pa.int64()), str(tmp_path / "part-0.parquet"))
    t = LTSeq.read_parquet(str(tmp_path)).assume_sorted("i")
    assert _sessions(t).count() == len(_sessions(t).to_arrow()) == 3


def test_group_count_execution_failure_raises_the_class_of_the_materialized_count():
    """The general count path raised RuntimeError for a failed execution,
    where the materialized first rows raise ValueError (R3-F2 of #422's
    review)."""
    t = LTSeq.from_arrow(
        pa.table(
            {
                "i": pa.array(range(5), pa.int64()),
                "k": pa.array([1, 1, 2, 2, 3], pa.int64()),
                "n": pa.array([1, 2, 3, 4, 5], pa.int64()),
                "d": pa.array([1, 1, 1, 1, 0], pa.int64()),
            }
        )
    ).sort("i")
    failing = t.filter(lambda r: r.n // r.d > 0)
    with pytest.raises(ValueError, match="Divide by zero"):
        len(_sessions(failing).to_arrow())
    with pytest.raises(ValueError, match="Divide by zero") as raised:
        _sessions(failing).count()
    assert not isinstance(raised.value, RuntimeError)


def test_group_count_boundary_evaluation_failure_raises_the_class_of_the_materialized_count():
    """A failure evaluating the boundary predicate in the count kernel raised
    RuntimeError, where the materialized first rows raise ValueError for the
    same division by zero (R6-F3 of #422's review). The division is outside
    every ``&`` and ``|``, so both paths evaluate it on every row, and the
    kernel is the one that raises here."""
    t = LTSeq.from_arrow(
        pa.table(
            {
                "i": pa.array(range(4), pa.int64()),
                "n": pa.array([1, 2, 3, 4], pa.int64()),
                "d": pa.array([1, 0, 1, 1], pa.int64()),
            }
        )
    ).sort("i")
    sessions = t.group_ordered(lambda r: (r.n / r.d) > (r.n.shift(1) / r.d.shift(1))).first()
    with pytest.raises(ValueError, match="Divide by zero"):
        len(sessions.to_arrow())
    with pytest.raises(ValueError, match="group boundary.*Divide by zero") as raised:
        sessions.count()
    assert not isinstance(raised.value, RuntimeError)


def _guarded_division_rows():
    # No ``k`` is negative, so the reference never needs ``n / d``, and the
    # zero divisors in rows 1 and 3 are never divided by.
    return pa.table(
        {
            "i": pa.array(range(4), pa.int64()),
            "k": pa.array([1, 1, 2, 2], pa.int64()),
            "n": pa.array([1, 1, 1, 1], pa.int64()),
            "d": pa.array([1, 0, 1, 0], pa.int64()),
        }
    )


@pytest.mark.parametrize("source", ["memory", "parquet"])
@pytest.mark.parametrize(
    "boundary",
    [
        pytest.param(
            lambda r: (r.k != r.k.shift(1)) | ((r.k < 0) & ((r.n / r.d) > 0.0)), id="or-and"
        ),
        pytest.param(
            lambda r: ((r.k < 0) & ((r.n / r.d) > 0.0)) | (r.k != r.k.shift(1)), id="guard-first"
        ),
    ],
)
def test_group_count_does_not_divide_where_a_boolean_guard_skips_it(tmp_path, source, boundary):
    """The kernel evaluated both operands of ``&`` and ``|`` on every row, so a
    division the reference skips raised divide by zero where the materialized
    first rows count 2; the old retry hid it (R8-F1 of #422's review). The
    kernel now declines a division beneath a Boolean operator."""
    if source == "memory":
        t = LTSeq.from_arrow(_guarded_division_rows()).sort("i")
    else:
        path = tmp_path / "events.parquet"
        pq.write_table(_guarded_division_rows(), str(path))
        t = LTSeq.read_parquet(str(path)).assume_sorted("i")
    expected = len(t.group_ordered(boundary).first().to_arrow())
    assert expected == 2
    assert t.group_ordered(boundary).first().count() == expected


def _mixed_subtraction_rows():
    """An Int64 ``n`` and a Float64 ``f``, which DataFusion coerces to Float64."""
    return pa.table(
        {
            "i": pa.array(range(4), pa.int64()),
            "n": pa.array([1, 4, 2, 0], pa.int64()),
            "f": pa.array([0.5] * 4, pa.float64()),
        }
    )


@pytest.mark.parametrize("source", ["memory", "parquet"])
@pytest.mark.parametrize(
    "boundary",
    [
        pytest.param(lambda r: (r.n - r.f.shift(1)) > 2.0, id="int-minus-float"),
        pytest.param(lambda r: (r.f.shift(1) - r.n) > -2.0, id="float-minus-int"),
    ],
)
def test_group_count_of_mixed_integer_float_subtraction_is_the_materialized_count(
    tmp_path, source, boundary
):
    """The kernel subtracted only two Float64 or two integer arrays, so an
    integer minus a Float64 raised ``Sub: unsupported types`` where the
    materialized first rows count; the old retry hid it (R9-F1 of #422's
    review). The kernel now declines a subtraction whose operands are not
    both Float64 or both integers."""
    if source == "memory":
        t = LTSeq.from_arrow(_mixed_subtraction_rows()).sort("i")
    else:
        path = tmp_path / "mixed.parquet"
        pq.write_table(_mixed_subtraction_rows(), str(path))
        t = LTSeq.read_parquet(str(path)).assume_sorted("i")
    expected = len(t.group_ordered(boundary).first().to_arrow())
    assert t.group_ordered(boundary).first().count() == expected


def _other_events():
    """Rows of the same schema as ``_events`` with one session and no pattern."""
    return pa.table(
        {
            "i": pa.array(range(3), pa.int64()),
            "k": pa.array([1, 1, 1], pa.int64()),
            "e": pa.array([0, 0, 0], pa.int64()),
        }
    )


@pytest.mark.parametrize("other", [False, True], ids=["nothing", "another-file"])
@pytest.mark.parametrize(
    "name", ["e.parquet", "e[1].parquet", "e*?.parquet", "source:events.parquet"]
)
def test_counts_read_the_listed_file_from_another_working_directory(tmp_path, monkeypatch, name, other):
    """A relative path names nothing, or another file, from another working
    directory. The direct scans opened the path as written there, so they
    raised for nothing and counted the other file, where every other read
    of the table reads the file listed at the call (R4-F1, R5-F1 and R6-F2
    of #422's review). A name with `*`, `?` or `[` is read as that file, not
    as a glob, and one that reads like a URL, `source:events.parquet`, as
    the local file it names: its lookup was taken for a URL that names
    nothing and the count fell back."""
    pq.write_table(_events(pa.int64()), str(tmp_path / name))
    monkeypatch.chdir(tmp_path)
    t = LTSeq.read_parquet(name).assume_sorted("i")
    patterns = LTSeq.read_parquet(name).sort("i").search_pattern_count(*STEPS, partition_by="k")
    assert patterns == 3
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    if other:
        pq.write_table(_other_events(), str(elsewhere / name))
    monkeypatch.chdir(elsewhere)

    assert _sessions(t).count() == len(_sessions(t).to_arrow()) == 3
    assert t.search_pattern_count(*STEPS, partition_by="k") == patterns


def test_counts_raise_naming_the_listed_file_when_it_is_gone(tmp_path, monkeypatch):
    """A listed file that cannot be opened is a failure of both direct scans,
    raised naming the file, not a fallback to the general path."""
    pq.write_table(_events(pa.int64()), str(tmp_path / "events.parquet"))
    monkeypatch.chdir(tmp_path)
    t = LTSeq.read_parquet("events.parquet").assume_sorted("i")
    os.remove(tmp_path / "events.parquet")
    listed = re.escape(str(tmp_path / "events.parquet"))

    with pytest.raises(ValueError, match=f"Failed to open Parquet '{listed}'") as raised:
        _sessions(t).count()
    assert not isinstance(raised.value, RuntimeError)
    with pytest.raises(ValueError, match=f"Failed to open Parquet '{listed}'") as raised:
        t.search_pattern_count(*STEPS, partition_by="k")
    assert not isinstance(raised.value, RuntimeError)


def test_group_count_falls_back_for_a_relative_parquet_glob_that_names_nothing_now(tmp_path, monkeypatch):
    """A glob the reader matched is counted through the materialized first
    rows, which read the files it listed, even where it matches nothing now
    (R5-F1 of #422's review)."""
    pq.write_table(_events(pa.int64()), str(tmp_path / "e1.parquet"))
    monkeypatch.chdir(tmp_path)
    t = LTSeq.read_parquet("e?.parquet").assume_sorted("i")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    assert _sessions(t).count() == len(_sessions(t).to_arrow()) == 3


def _sources(tmp_path):
    pq.write_table(_events(pa.int64()), str(tmp_path / "part-0.parquet"))
    return {
        "glob": str(tmp_path / "part-?.parquet"),
        "directory": str(tmp_path),
        "file-url": (tmp_path / "part-0.parquet").as_uri(),
    }


@pytest.mark.parametrize("kind", ["glob", "directory", "file-url"])
def test_pattern_count_counts_the_files_a_source_lists(tmp_path, kind):
    """The pattern count's direct scan opened the path as written whatever
    the reader had made of it, so a glob or a directory raised a failed read
    and a file URL did too, where the general path counted the files listed
    at the call (R6-F1 of #422's review)."""
    path = _sources(tmp_path)[kind]
    fast = LTSeq.read_parquet(path).assume_sorted("i")
    general = LTSeq.read_parquet(path).sort("i")
    expected = general.search_pattern_count(*STEPS, partition_by="k")
    assert expected == 3
    assert fast.search_pattern_count(*STEPS, partition_by="k") == expected
    assert _sessions(fast).count() == len(_sessions(fast).to_arrow()) == 3


def test_pattern_count_of_a_glob_ignores_a_file_named_like_it(tmp_path):
    """A file created after the read at the glob's own name is not one the
    glob listed. The direct scan opened it and counted its rows (R6-F1 of
    #422's review)."""
    pq.write_table(_events(pa.int64()), str(tmp_path / "part-0.parquet"))
    glob = str(tmp_path / "part-?.parquet")
    t = LTSeq.read_parquet(glob).assume_sorted("i")
    pq.write_table(_other_events(), glob)
    assert t.search_pattern_count(*STEPS, partition_by="k") == 3
    assert _sessions(t).count() == 3


def test_group_count_falls_back_for_a_parquet_glob(tmp_path):
    """A glob, which names nothing as written, is still counted through the
    materialized first rows."""
    pq.write_table(_events(pa.int64()), str(tmp_path / "part-0.parquet"))
    t = LTSeq.read_parquet(str(tmp_path / "*.parquet")).assume_sorted("i")
    assert _sessions(t).count() == len(_sessions(t).to_arrow()) == 3


@pytest.mark.parametrize(
    "boundary",
    [
        pytest.param(lambda r: (r.k != r.k.shift(1)) & ((r.n + 1) > 0), id="add"),
        pytest.param(lambda r: (r.k != r.k.shift(1)) & ((r.n * 2) > 0), id="mul"),
    ],
)
def test_group_count_wraps_int64_overflow_as_the_materialized_count(boundary):
    """The count kernel added and multiplied with Arrow's checked kernels, so
    an Int64 overflow raised where the materialized first rows, which wrap
    as DataFusion does, counted; the old retry hid it (R7-F1 of #422's
    review). The wrapped values (-2**63 and -2) make the second row's
    predicate false, so only the key change at the third row is a boundary."""
    big = 2**63 - 1
    t = LTSeq.from_arrow(
        pa.table(
            {
                "i": pa.array([0, 1, 2], pa.int64()),
                "k": pa.array([1, 1, 2], pa.int64()),
                "n": pa.array([big, big, 1], pa.int64()),
            }
        )
    ).sort("i")
    expected = len(t.group_ordered(boundary).first().to_arrow())
    assert expected == 2
    assert t.group_ordered(boundary).first().count() == expected


def _rewrite_source(tmp_path, kind):
    """A Parquet file of an Int32 key read through ``kind`` and declared
    sorted by ``i``, then rewritten in place: with Int64 keys that do not fit
    the planned Int32, or without the key."""
    path = tmp_path / "events.parquet"
    original = pa.table(
        {"i": pa.array([0, 1, 2], pa.int64()), "k": pa.array([1, 1, 2], pa.int32())}
    )
    pq.write_table(original, str(path))
    fast = LTSeq.read_parquet(path.as_uri()).assume_sorted("i")
    general = LTSeq.read_parquet(path.as_uri()).sort("i")
    if kind == "wider-key":
        rewritten = pa.table(
            {
                "i": pa.array([0, 1, 2], pa.int64()),
                "k": pa.array([2**32, 2**32, 2**32 + 1], pa.int64()),
            }
        )
    else:
        rewritten = pa.table({"i": pa.array([0, 1, 2], pa.int64())})
    pq.write_table(rewritten, str(path))
    return fast, general


def _outcome(count):
    try:
        return count()
    except Exception as e:  # noqa: BLE001 - the class is what is compared
        return type(e)


@pytest.mark.parametrize("kind", ["wider-key", "dropped-key"])
def test_counts_of_a_rewritten_file_url_follow_the_planned_schema(tmp_path, kind):
    """A ``file://`` URL naming one file was opened by the direct scans, which
    read the file's current schema, not the one planned at the read: after a
    rewrite to Int64 keys beyond Int32 they counted 0 and 3 where the general
    paths raise a failed cast, and after the key was dropped the pattern
    count raised ColumnNotFoundError where the general path counted a NULL
    key (R7-F2 of #422's review). Such a URL is now counted by the general
    paths."""
    fast, general = _rewrite_source(tmp_path, kind)

    def pattern(t):
        return t.search_pattern_count(lambda r: r.k.is_null() | (r.k > 0), partition_by="k")

    def groups(t):
        return t.group_ordered(lambda r: r.k != r.k.shift(1)).first()

    expected_pattern = _outcome(lambda: pattern(general))
    expected_groups = _outcome(lambda: len(groups(general).to_arrow()))
    if kind == "wider-key":
        assert expected_pattern is ValueError
        assert expected_groups is ValueError
    else:
        assert isinstance(expected_pattern, int)
    assert _outcome(lambda: pattern(fast)) == expected_pattern
    assert _outcome(lambda: groups(fast).count()) == expected_groups


def _dated():
    return LTSeq.from_arrow(
        pa.table(
            {
                "x": pa.array([1, 2], pa.int64()),
                "d": pa.array([datetime.date(2020, 1, 1)] * 2),
            }
        )
    ).sort("x")


WRONG_KIND = {
    "filter": lambda t: t.filter(lambda r: r.d.dt.add(days="1") > r.d),
    "select": lambda t: t.select(lambda r: r.d.dt.add(days="1")),
    "derive": lambda t: t.derive(v=lambda r: r.d.dt.add(days="1")),
}


@pytest.mark.parametrize("operation", WRONG_KIND)
def test_wrong_kind_literal_gives_its_message_alone_in_every_operation(operation):
    """A literal argument that lowering reads and finds of the wrong kind is a
    validation error, without the transpilation prefix, whichever operation
    lowers it (R8-F2 of #422's review)."""
    with pytest.raises(ValueError) as raised:
        WRONG_KIND[operation](_dated()).to_arrow()
    assert str(raised.value).startswith("dt_add days must be an integer, got a String literal '1'")


@pytest.mark.parametrize(
    "operation",
    [
        pytest.param(lambda t: t.filter(lambda r: r.x.shift("1") == 0), id="filter"),
        pytest.param(lambda t: t.select(lambda r: r.x.shift("1")), id="select"),
    ],
)
def test_row_lowering_refuses_a_window_before_reading_its_offset(operation):
    """``filter`` and ``select`` refuse a window in row context before its
    offset is read, so ``shift("1")`` there is the window refusal, with the
    prefix, not the wrong-kind literal (R8-F2 of #422's review)."""
    with pytest.raises(ValueError, match=r"^Transpilation error: Window function 'shift'"):
        operation(_dated()).to_arrow()


@pytest.mark.parametrize("offset", ["1.5", "9223372036854775808"])
def test_derive_rejects_a_decimal_shift_offset_it_cannot_represent(offset):
    """``require_i64`` rejects a ``Decimal`` that is not integral or does not
    fit in Int64 as a validation error, with its message alone, though
    ``Decimal("1")`` of the same kind is accepted (R9-F2 of #422's review)."""
    from decimal import Decimal

    t = _dated()
    assert t.derive(v=lambda r: r.x.shift(Decimal("1"))).to_arrow().num_rows == 2
    with pytest.raises(ValueError, match=r"^shift\(\) offset must be an integer, got a Decimal128"):
        t.derive(v=lambda r: r.x.shift(Decimal(offset))).to_arrow()


def test_derive_reads_a_shift_offset_of_the_wrong_kind():
    with pytest.raises(ValueError, match=r"^shift\(\) offset must be an integer"):
        _dated().derive(v=lambda r: r.x.shift("1")).to_arrow()
