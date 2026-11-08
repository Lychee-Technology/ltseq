"""#256's spike of the v0.5 stream error paths (cargo feature `stream_spike`).

These hold the spike to what benchmarks/stream_errors/REPORT.md reports, so
a later pyarrow, DuckDB or Polars that changes an outcome fails here rather
than leaving the report stale:

- a pyarrow reader over the Rust iterator raises the LTSeq class on
  `read_next_batch`, iteration and `read_all` (§15.1);
- the spike's C stream ends with `EIO` or `EINVAL` and a message that starts
  with the class name, which pyarrow raises as `OSError` or `ArrowInvalid`
  (§16.4);
- `close()` on that reader does not stop its execution; dropping it does
  (OD-X6);
- a Cython subclass of `pyarrow.RecordBatchReader` that shares that reader's
  C++ reader and closes the iterator on `close()` keeps the class on every
  Python read and stops the execution on `close()` (OD-X6's option (e));
  these tests compile it, and skip without Cython.

A default build has no spike, and every test but the first is skipped.
"""

from __future__ import annotations

import gc
import importlib.util
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq
from ltseq import _stream_spike as spike

spike_build = pytest.mark.skipif(
    not spike.available(), reason="needs a spike build (--features stream_spike)"
)

ROWS = 40
BATCH = 4
AFTER = 3
INT64_MIN = -(2**63)

# The four classes #256 names, with the contract's bases, and the two
# classes of the other errno.
CLASSES = [
    ("ArithmeticOverflowError", OverflowError, pa.ArrowInvalid),
    ("CastError", ValueError, pa.ArrowInvalid),
    ("OrderViolationError", ValueError, pa.ArrowInvalid),
    ("SourceNotFoundError", FileNotFoundError, OSError),
    ("LTSeqIOError", OSError, OSError),
    ("ExecutionError", RuntimeError, pa.ArrowInvalid),
]


def test_default_build_refuses_the_spike():
    if spike.available():
        pytest.skip("this extension is a spike build (--features stream_spike)")
    with pytest.raises(RuntimeError, match="--features stream_spike"):
        spike.to_batches(LTSeq.from_arrow(pa.table({"x": [1]})))


@pytest.fixture(params=[1, 4], ids=lambda p: f"p{p}")
def layout(request, engine_config):
    engine_config(target_partitions=request.param, batch_size=BATCH)


def _table() -> LTSeq:
    return LTSeq.from_arrow(
        pa.table(
            {
                "x": list(range(ROWS)),
                "z": [1] * 30 + [0] * (ROWS - 30),
                "s": ["1"] * 30 + ["a"] * (ROWS - 30),
            }
        )
    )


def _overflow_table() -> LTSeq:
    # INT64_MIN // -1 does not fit in int64: a real execution overflow.
    return LTSeq.from_arrow(
        pa.table(
            {
                "x": list(range(30)) + [INT64_MIN] * (ROWS - 30),
                "y": [1] * 30 + [-1] * (ROWS - 30),
            }
        )
    )


@pytest.fixture(autouse=True)
def no_execution_left_behind():
    yield
    if spike.available():
        gc.collect()
        assert spike.live() == 0


def _read_next_batch(reader: pa.RecordBatchReader) -> int:
    n = 0
    while True:
        try:
            reader.read_next_batch()
        except StopIteration:
            return n
        n += 1


def _iterate(reader: pa.RecordBatchReader) -> int:
    n = 0
    for _ in reader:
        n += 1
    return n


def _read_all(reader: pa.RecordBatchReader) -> int:
    return reader.read_all().num_rows


READS = [_read_next_batch, _iterate, _read_all]


@spike_build
@pytest.mark.usefixtures("layout")
def test_a_clean_stream_reads_every_row_in_order():
    t = _table()
    expected = pa.table({"x": list(range(ROWS))})
    assert spike.to_batches(t).read_all().select(["x"]).equals(expected)
    assert pa.table(spike.Stream(t)).select(["x"]).equals(expected)


@spike_build
@pytest.mark.usefixtures("layout")
@pytest.mark.parametrize("read", READS, ids=lambda f: f.__name__.lstrip("_"))
@pytest.mark.parametrize("name, base, _", CLASSES, ids=[c[0] for c in CLASSES])
def test_python_reads_raise_the_ltseq_class(read, name, base, _):
    reader = spike.to_batches(_table(), (AFTER, name, "boom"))
    with pytest.raises(spike.CLASSES[name], match="^boom$") as caught:
        read(reader)
    assert type(caught.value) is spike.CLASSES[name]
    assert isinstance(caught.value, base)


@spike_build
@pytest.mark.usefixtures("layout")
@pytest.mark.parametrize("gil", ["attached", "poll"])
def test_every_gil_mode_reads_the_rows_and_raises_the_class(gil):
    t = _table()
    assert spike.to_batches(t, gil=gil).read_all().column("x").to_pylist() == list(range(ROWS))
    with pytest.raises(spike.OrderViolationError):
        spike.to_batches(t, (AFTER, "OrderViolationError", "boom"), gil=gil).read_all()


@spike_build
def test_an_unknown_gil_mode_or_class_is_refused():
    t = _table()
    with pytest.raises(ValueError, match="gil must be"):
        spike.to_batches(t, gil="never")
    with pytest.raises(ValueError, match="unknown exception class"):
        spike.to_batches(t, (0, "NoSuchError", "boom"))


@spike_build
@pytest.mark.usefixtures("layout")
def test_read_next_batch_returns_the_batches_before_the_error():
    reader = spike.to_batches(_table(), (AFTER, "CastError", "boom"))
    for _ in range(AFTER):
        assert reader.read_next_batch().num_rows == BATCH
    with pytest.raises(spike.CastError):
        reader.read_next_batch()
    # The error ended the stream and released its execution.
    assert spike.live() == 0


@spike_build
@pytest.mark.usefixtures("layout")
@pytest.mark.parametrize("read", READS, ids=lambda f: f.__name__.lstrip("_"))
@pytest.mark.parametrize(
    "make, derive, cls",
    [
        (_table, lambda r: r.x // r.z, "DivisionByZeroError"),
        (_table, lambda r: r.s.cast("int64"), "CastError"),
        (_overflow_table, lambda r: r.x // r.y, "ArithmeticOverflowError"),
    ],
    ids=["divide_by_zero", "cast", "overflow"],
)
def test_engine_errors_raise_their_class(read, make, derive, cls):
    reader = spike.to_batches(make().derive(z=derive))
    with pytest.raises(spike.CLASSES[cls]) as caught:
        read(reader)
    assert type(caught.value) is spike.CLASSES[cls]


OVERFLOW_MESSAGE = f"Overflow happened on: {INT64_MIN} / -1"


@spike_build
@pytest.mark.usefixtures("layout")
@pytest.mark.parametrize("read", READS, ids=lambda f: f.__name__.lstrip("_"))
def test_a_real_overflow_raises_arithmetic_overflow_error(read):
    reader = spike.to_batches(_overflow_table().derive(z=lambda r: r.x // r.y))
    with pytest.raises(spike.ArithmeticOverflowError) as caught:
        read(reader)
    assert isinstance(caught.value, OverflowError)
    assert str(caught.value) == OVERFLOW_MESSAGE


@spike_build
@pytest.mark.usefixtures("layout")
@pytest.mark.parametrize(
    "consume",
    [
        lambda s: pa.table(s),
        lambda s: pa.RecordBatchReader.from_stream(s).read_all(),
    ],
    ids=["pa.table", "from_stream"],
)
def test_a_real_overflow_ends_the_c_stream_with_einval_and_its_class(consume):
    with pytest.raises(pa.ArrowInvalid) as caught:
        consume(spike.Stream(_overflow_table().derive(z=lambda r: r.x // r.y)))
    assert type(caught.value) is pa.ArrowInvalid
    assert str(caught.value) == f"ArithmeticOverflowError: {OVERFLOW_MESSAGE}"


@spike_build
def test_a_file_deleted_before_execution_raises_source_not_found_at_the_call(tmp_path):
    path = tmp_path / "gone.parquet"
    pq.write_table(pa.table({"x": [1, 2]}), path)
    t = LTSeq.read_parquet(str(path))
    os.remove(path)
    # The footer is read when the execution starts (#360), so the call
    # raises, before any read, for both candidates.
    with pytest.raises(spike.SourceNotFoundError):
        spike.to_batches(t)
    with pytest.raises(spike.SourceNotFoundError):
        pa.table(spike.Stream(t))


@spike_build
@pytest.mark.usefixtures("layout")
@pytest.mark.parametrize("name, _, raised", CLASSES, ids=[c[0] for c in CLASSES])
@pytest.mark.parametrize(
    "consume",
    [
        lambda s: pa.table(s),
        lambda s: pa.RecordBatchReader.from_stream(s).read_all(),
    ],
    ids=["pa.table", "from_stream"],
)
def test_the_c_stream_carries_errno_and_a_message_starting_with_the_class(
    consume, name, _, raised
):
    with pytest.raises(raised) as caught:
        consume(spike.Stream(_table(), (AFTER, name, "boom")))
    # EIO reaches pyarrow as OSError, EINVAL as ArrowInvalid (a ValueError);
    # the message is the producer's, unchanged.
    assert type(caught.value) is raised
    assert str(caught.value) == f"{name}: boom"


@spike_build
@pytest.mark.usefixtures("layout")
def test_importing_the_reader_through_the_c_stream_loses_the_class():
    # §15.1: a consumer that imports to_batches() through the C stream
    # raises its own exception, with a message pyarrow composes.
    reader = pa.RecordBatchReader.from_stream(
        spike.to_batches(_table(), (AFTER, "OrderViolationError", "boom"))
    )
    with pytest.raises(pa.ArrowInvalid) as caught:
        reader.read_all()
    assert not isinstance(caught.value, spike.LTSeqError)
    assert "OrderViolationError: boom" in str(caught.value)


@spike_build
@pytest.mark.usefixtures("layout")
def test_closing_the_reader_does_not_stop_the_execution_but_dropping_it_does():
    # OD-X6: pyarrow's reader over a Python iterator has no close() of its
    # own, so close() leaves the iterator, and the execution, running.
    reader = spike.to_batches(_table())
    reader.read_next_batch()
    assert spike.live() == 1
    reader.close()
    assert spike.live() == 1
    assert reader.read_next_batch().num_rows > 0
    del reader
    gc.collect()
    assert spike.live() == 0


@spike_build
@pytest.mark.usefixtures("layout")
def test_closing_a_reader_imported_from_the_c_stream_stops_the_execution():
    reader = pa.RecordBatchReader.from_stream(spike.Stream(_table()))
    reader.read_next_batch()
    assert spike.live() == 1
    reader.close()
    assert spike.live() == 0


@spike_build
@pytest.mark.usefixtures("layout")
def test_duckdb_and_polars_carry_the_message():
    duckdb = pytest.importorskip("duckdb")
    pl = pytest.importorskip("polars")
    s = spike.Stream(_table(), (AFTER, "OrderViolationError", "boom"))  # noqa: F841
    with pytest.raises(duckdb.Error, match="OrderViolationError: boom"):
        duckdb.sql("select count(*) from s").fetchall()
    with pytest.raises(pl.exceptions.ComputeError, match="OrderViolationError: boom"):
        pl.DataFrame(spike.Stream(_table(), (AFTER, "OrderViolationError", "boom")))
    # Polars reads a pyarrow reader in Python, so the class survives.
    with pytest.raises(spike.OrderViolationError):
        pl.from_arrow(spike.to_batches(_table(), (AFTER, "OrderViolationError", "boom")))


# --- OD-X6's option (e): a native subclass whose close() reaches the iterator


@pytest.fixture(scope="module")
def closing_reader():
    pytest.importorskip("Cython")
    if not spike.available():
        pytest.skip("needs a spike build (--features stream_spike)")
    path = Path(__file__).resolve().parents[2] / "benchmarks" / "stream_errors" / "closing_reader.py"
    spec = importlib.util.spec_from_file_location("closing_reader", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.load()  # compiles it against this pyarrow on first use
    return module


@spike_build
@pytest.mark.usefixtures("layout")
@pytest.mark.parametrize("read", READS, ids=lambda f: f.__name__.lstrip("_"))
@pytest.mark.parametrize("name, base, _", CLASSES, ids=[c[0] for c in CLASSES])
def test_the_native_subclass_keeps_the_class(closing_reader, read, name, base, _):
    reader = closing_reader.to_batches(_table(), (AFTER, name, "boom"))
    assert isinstance(reader, pa.RecordBatchReader)
    with pytest.raises(spike.CLASSES[name], match="^boom$") as caught:
        read(reader)
    assert type(caught.value) is spike.CLASSES[name]
    assert isinstance(caught.value, base)


@spike_build
@pytest.mark.usefixtures("layout")
@pytest.mark.parametrize("read", READS, ids=lambda f: f.__name__.lstrip("_"))
def test_the_native_subclass_keeps_the_class_of_a_real_error(closing_reader, read):
    reader = closing_reader.to_batches(_overflow_table().derive(z=lambda r: r.x // r.y))
    with pytest.raises(spike.ArithmeticOverflowError, match=f"^{OVERFLOW_MESSAGE}$"):
        read(reader)


@spike_build
@pytest.mark.usefixtures("layout")
def test_closing_the_native_subclass_stops_the_execution(closing_reader):
    reader = closing_reader.to_batches(_table())
    reader.read_next_batch()
    assert spike.live() == 1
    reader.close()
    assert spike.live() == 0
    # The iterator is closed, so the reader is at its end.
    with pytest.raises(StopIteration):
        reader.read_next_batch()
    with closing_reader.to_batches(_table()) as reader:
        reader.read_next_batch()
        assert spike.live() == 1
    assert spike.live() == 0


@spike_build
@pytest.mark.usefixtures("layout")
def test_dropping_the_native_subclass_stops_the_execution(closing_reader):
    reader = closing_reader.to_batches(_table())
    reader.read_next_batch()
    assert spike.live() == 1
    del reader
    gc.collect()
    assert spike.live() == 0


@spike_build
@pytest.mark.usefixtures("layout")
def test_consumers_read_the_native_subclass(closing_reader):
    expected = list(range(ROWS))
    assert pa.table(closing_reader.to_batches(_table())).column("x").to_pylist() == expected
    duckdb = pytest.importorskip("duckdb")
    pl = pytest.importorskip("polars")
    reader = closing_reader.to_batches(_table())  # noqa: F841 - named for DuckDB's scan
    assert duckdb.sql("select x from reader order by x").fetchall() == [(x,) for x in expected]
    assert pl.from_arrow(closing_reader.to_batches(_table()))["x"].to_list() == expected
