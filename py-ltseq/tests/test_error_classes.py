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


def test_pattern_count_reports_a_source_it_cannot_read(tmp_path):
    """A read failure inside the fast path is an error, not a fallback."""
    path = str(tmp_path / "events.parquet")
    pq.write_table(_events(pa.int64()), path, row_group_size=3)
    t = LTSeq.read_parquet(path).assume_sorted("i")
    os.remove(path)

    with pytest.raises(RuntimeError, match="Failed to open Parquet"):
        t.search_pattern_count(*STEPS, partition_by="k")
