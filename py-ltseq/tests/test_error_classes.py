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

import os

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
