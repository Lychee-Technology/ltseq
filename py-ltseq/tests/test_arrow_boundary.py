"""Arrow C Data Interface boundary (issue #143).

Data crosses the Python/Rust boundary as shared Arrow buffers (C Data
Interface / PyCapsule protocol), not as IPC bytes. These tests pin down:

- round-trips for primitive, temporal, decimal, nested and dictionary types;
- chunked input preserved in order;
- empty schemas / empty tables in both directions;
- buffer lifetime: results outlive the objects they were exported from;
- the ``__arrow_c_stream__`` protocol on ``LTSeq`` (``pa.table(t)`` etc.) and
  its documented semantics (lazy, repeatable, cancellable, requested_schema
  validated but not honored, execution errors surface as exceptions);
- the cursor yielding ``pyarrow.RecordBatch`` objects, with early stop;
- a source guard so the IPC tunnel cannot come back silently.
"""

from __future__ import annotations

import gc
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq

REPO_ROOT = Path(__file__).resolve().parents[2]


def rich_table() -> pa.Table:
    """One column per type family that crosses the boundary."""
    return pa.table(
        {
            "i64": pa.array([1, 2, None, 4], pa.int64()),
            "i32": pa.array([1, 2, 3, None], pa.int32()),
            "f64": pa.array([1.5, None, 3.5, 4.5], pa.float64()),
            "flag": pa.array([True, False, None, True]),
            "s": pa.array(["a", "bb", None, "dddd"], pa.string()),
            "large_s": pa.array(["x", "y", "z", None], pa.large_string()),
            "raw": pa.array([b"\x00\x01", b"", None, b"z"], pa.binary()),
            "ts": pa.array(
                [
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                    datetime(2024, 1, 2, 12, 30, tzinfo=timezone.utc),
                    None,
                    datetime(2025, 6, 30, 23, 59, 59, tzinfo=timezone.utc),
                ],
                pa.timestamp("us", tz="UTC"),
            ),
            "d": pa.array([date(2024, 1, 1), date(2024, 2, 29), None, date(2030, 12, 31)], pa.date32()),
            "dec": pa.array([Decimal("1.23"), Decimal("-99.99"), None, Decimal("0.01")], pa.decimal128(10, 2)),
            "lst": pa.array([[1, 2], [], None, [3]], pa.list_(pa.int64())),
            "st": pa.array(
                [{"a": 1, "b": "x"}, {"a": 2, "b": None}, None, {"a": None, "b": "w"}],
                pa.struct([("a", pa.int64()), ("b", pa.string())]),
            ),
            "dict": pa.array(["u", "v", "u", None]).dictionary_encode(),
        }
    )


@pytest.fixture
def parquet_path(tmp_path):
    path = tmp_path / "boundary.parquet"
    table = pa.table({"id": list(range(10_000)), "v": [float(i) for i in range(10_000)]})
    pq.write_table(table, path, row_group_size=1_000)
    return str(path)


# ---------------------------------------------------------------------------
# Round trips
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_rich_types_round_trip(self):
        source = rich_table()
        result = LTSeq.from_arrow(source).to_arrow()
        assert result.schema == source.schema
        assert result.equals(source)

    def test_rich_types_via_protocol(self):
        source = rich_table()
        result = pa.table(LTSeq.from_arrow(source))
        assert result.equals(source)

    def test_chunked_input_preserves_order_and_chunks(self):
        parts = [pa.table({"id": list(range(i * 100, (i + 1) * 100))}) for i in range(3)]
        source = pa.concat_tables(parts)
        assert source["id"].num_chunks == 3

        result = LTSeq.from_arrow(source).to_arrow()
        assert result["id"].to_pylist() == list(range(300))
        assert result["id"].num_chunks == 3

    def test_from_record_batch(self):
        batch = pa.record_batch({"x": [1, 2, 3]})
        assert LTSeq.from_arrow(batch).to_arrow()["x"].to_pylist() == [1, 2, 3]

    def test_from_record_batch_reader(self):
        schema = pa.schema([("x", pa.int64())])
        reader = pa.RecordBatchReader.from_batches(
            schema, [pa.record_batch({"x": [1, 2]}), pa.record_batch({"x": [3]})]
        )
        t = LTSeq.from_arrow(reader)
        assert t.to_arrow()["x"].to_pylist() == [1, 2, 3]

    def test_from_pandas_round_trip(self):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        out = LTSeq.from_pandas(df).to_pandas()
        assert out["x"].tolist() == [1, 2, 3]
        assert out["y"].tolist() == ["a", "b", "c"]

    def test_non_arrow_object_raises_type_error(self):
        with pytest.raises(TypeError, match="__arrow_c_stream__"):
            LTSeq.from_arrow({"not": "a table"})
        with pytest.raises(TypeError):
            LTSeq.from_arrow([1, 2, 3])

    def test_query_after_import(self):
        t = LTSeq.from_arrow(rich_table())
        assert t.filter(lambda r: r.i64 > 1).count() == 2
        got = t.sort(lambda r: r.i64).select(lambda r: r.s).to_arrow()
        assert got.num_rows == 4
        assert got.column_names == ["s"]


# ---------------------------------------------------------------------------
# Empty schemas and tables
# ---------------------------------------------------------------------------


class TestEmpty:
    def test_empty_schema_table(self):
        t = LTSeq.from_arrow(pa.table({}))
        assert t.count() == 0
        out = t.to_arrow()
        assert out.num_rows == 0 and out.num_columns == 0

    def test_empty_table_keeps_typed_schema(self):
        schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
        t = LTSeq.from_arrow(schema.empty_table())
        assert t.count() == 0
        assert t.to_arrow().schema == schema
        assert pa.table(t).schema == schema
        assert t.filter(lambda r: r.id > 0).count() == 0

    def test_filter_to_zero_rows_keeps_types(self):
        t = LTSeq.from_arrow(pa.table({"id": pa.array([1, 2], pa.int64()), "s": ["a", "b"]}))
        out = t.filter(lambda r: r.id > 100).to_arrow()
        assert out.num_rows == 0
        assert out.schema.field("id").type == pa.int64()
        assert out.schema.field("s").type == pa.string()

    def test_unloaded_ltseq_exports(self):
        t = LTSeq()
        assert t.to_arrow().num_rows == 0
        via_protocol = pa.table(t)
        assert via_protocol.num_rows == 0 and via_protocol.num_columns == 0


# ---------------------------------------------------------------------------
# Buffer lifetime across the boundary
# ---------------------------------------------------------------------------


class TestBufferLifetime:
    def test_import_survives_source_release(self):
        source = pa.table({"x": list(range(100_000)), "s": ["v"] * 100_000})
        expected_sum = sum(range(100_000))
        t = LTSeq.from_arrow(source)
        del source
        gc.collect()
        out = t.to_arrow()
        assert out.num_rows == 100_000
        assert sum(out["x"].to_pylist()) == expected_sum
        assert out["s"][99_999].as_py() == "v"

    def test_export_survives_table_release(self):
        t = LTSeq.from_arrow(pa.table({"x": list(range(50_000))}))
        out = t.to_arrow()
        via_protocol = pa.table(t)
        del t
        gc.collect()
        assert out["x"].to_pylist() == list(range(50_000))
        assert via_protocol.equals(out)

    def test_cursor_batch_survives_cursor_release(self, parquet_path):
        cursor = LTSeq.scan_parquet(parquet_path)
        batch = next(iter(cursor))
        del cursor
        gc.collect()
        assert isinstance(batch, pa.RecordBatch)
        assert batch["id"][0].as_py() == 0
        assert batch.num_rows > 0


# ---------------------------------------------------------------------------
# __arrow_c_stream__ protocol
# ---------------------------------------------------------------------------


class TestArrowStreamProtocol:
    def test_capsule_name(self):
        t = LTSeq.from_arrow(pa.table({"x": [1]}))
        capsule = t.__arrow_c_stream__()
        assert type(capsule).__name__ == "PyCapsule"
        # Hand it to pyarrow through the public protocol.
        class _Holder:
            def __arrow_c_stream__(self, requested_schema=None):
                return capsule

        assert pa.table(_Holder())["x"].to_pylist() == [1]

    def test_pa_table_equals_to_arrow(self):
        t = LTSeq.from_arrow(pa.table({"x": [3, 1, 2], "s": ["c", "a", "b"]})).sort(lambda r: r.x)
        assert pa.table(t).equals(t.to_arrow())
        assert pa.table(t)["x"].to_pylist() == [1, 2, 3]

    def test_record_batch_reader_from_stream(self):
        t = LTSeq.from_arrow(pa.table({"x": list(range(10))}))
        reader = pa.RecordBatchReader.from_stream(t)
        assert reader.schema == pa.schema([("x", pa.int64())])
        batches = list(reader)
        assert sum(b.num_rows for b in batches) == 10

    def test_lazy_plan_is_executed_and_table_stays_usable(self):
        t = LTSeq.from_arrow(pa.table({"x": list(range(10))})).filter(lambda r: r.x % 2 == 0)
        assert pa.table(t)["x"].to_pylist() == [0, 2, 4, 6, 8]
        # Repeatable: a second export runs the plan again.
        assert pa.table(t)["x"].to_pylist() == [0, 2, 4, 6, 8]
        assert t.count() == 5

    def test_partial_consumption_then_release(self):
        t = LTSeq.from_arrow(pa.concat_tables([pa.table({"x": [i] * 1000}) for i in range(5)]))
        reader = pa.RecordBatchReader.from_stream(t)
        first = reader.read_next_batch()
        assert first.num_rows == 1000
        del reader
        gc.collect()
        # Dropping the consumer cancels that execution; the table is intact.
        assert t.count() == 5000
        assert pa.table(t).num_rows == 5000

    def test_requested_schema_for_same_fields_returns_native_schema(self):
        # A request for another representation of the same fields (other
        # integer width, other string encoding) is not honored: the stream
        # carries the table's own schema, as the protocol allows, and a
        # consumer such as pyarrow casts downstream if it insists.
        t = LTSeq.from_arrow(pa.table({"x": [1, 2], "y": ["a", "b"]}))
        native = pa.schema([("x", pa.int64()), ("y", pa.string())])
        requested = pa.schema([("x", pa.int32()), ("y", pa.large_string())])
        reader = pa.RecordBatchReader.from_stream(t, schema=requested)
        assert reader.schema == native
        assert reader.read_all()["x"].to_pylist() == [1, 2]
        cast = pa.table(t, schema=requested)
        assert cast.schema == requested
        assert cast["x"].to_pylist() == [1, 2]

    @pytest.mark.parametrize(
        "requested",
        [
            pa.schema([("x", pa.int64())]),
            pa.schema([("x", pa.int64()), ("y", pa.string()), ("z", pa.int64())]),
            pa.schema([("x", pa.int64()), ("q", pa.string())]),
            pa.schema([("y", pa.string()), ("x", pa.int64())]),
        ],
        ids=["fewer_fields", "more_fields", "renamed_field", "reordered_fields"],
    )
    def test_requested_schema_for_different_fields_raises(self, requested):
        # The protocol says a producer should raise on a request that is not a
        # representation of its data, rather than return a stream the consumer
        # did not ask for and let it fail (or not) downstream.
        t = LTSeq.from_arrow(pa.table({"x": [1, 2], "y": ["a", "b"]}))
        with pytest.raises(ValueError, match="requested_schema is not compatible"):
            pa.RecordBatchReader.from_stream(t, schema=requested)
        with pytest.raises(ValueError, match="requested_schema is not compatible"):
            pa.table(t, schema=requested)
        with pytest.raises(ValueError, match="requested_schema is not compatible"):
            t.__arrow_c_stream__(requested.__arrow_c_schema__())
        # A rejected request leaves the table untouched.
        assert pa.table(t).num_rows == 2

    def test_requested_schema_must_be_a_schema_capsule(self):
        t = LTSeq.from_arrow(pa.table({"x": [1]}))
        with pytest.raises(TypeError, match="arrow_schema"):
            t.__arrow_c_stream__(pa.schema([("x", pa.int64())]))
        with pytest.raises(ValueError):
            # A capsule with the wrong name (an arrow_array_stream capsule).
            t.__arrow_c_stream__(t.__arrow_c_stream__())

    def test_execution_error_surfaces_as_exception(self):
        t = LTSeq.from_arrow(pa.table({"x": [1, 2], "zero": [0, 0]}))
        failing = t.derive(q=lambda r: r.x / r.zero)
        with pytest.raises((RuntimeError, ValueError, pa.ArrowException)):
            pa.table(failing)

    def test_duckdb_reads_ltseq_directly(self):
        duckdb = pytest.importorskip("duckdb")
        t = LTSeq.from_arrow(pa.table({"x": [1, 2, 3, 4]})).filter(lambda r: r.x > 1)
        total = duckdb.sql("SELECT sum(x) FROM t").fetchone()[0]
        assert total == 9


# ---------------------------------------------------------------------------
# Cursor
# ---------------------------------------------------------------------------


class TestCursor:
    def test_batches_are_record_batches(self, parquet_path):
        batches = list(LTSeq.scan_parquet(parquet_path))
        assert batches and all(isinstance(b, pa.RecordBatch) for b in batches)
        combined = pa.Table.from_batches(batches)
        assert combined.equals(LTSeq.read_parquet(parquet_path).to_arrow())

    def test_early_stop_releases_stream(self, parquet_path):
        cursor = LTSeq.scan_parquet(parquet_path)
        first = next(iter(cursor))
        assert first.num_rows > 0
        assert not cursor.exhausted
        del cursor
        gc.collect()
        # A fresh cursor over the same file drains completely.
        assert LTSeq.scan_parquet(parquet_path).count() == 10_000

    def test_exhaustion(self, parquet_path):
        cursor = LTSeq.scan_parquet(parquet_path)
        assert cursor.count() == 10_000
        assert cursor.exhausted
        assert cursor._inner.next_batch() is None


# ---------------------------------------------------------------------------
# Source guard
# ---------------------------------------------------------------------------


def test_no_ipc_tunnel_in_boundary_code():
    rust = "\n".join(p.read_text() for p in (REPO_ROOT / "src").rglob("*.rs"))
    for token in ("ipc::", "StreamWriter"):
        assert token not in rust, f"IPC-era token {token!r} is back in src/"
    package = "\n".join(p.read_text() for p in (REPO_ROOT / "py-ltseq" / "ltseq").rglob("*.py"))
    for token in ("pa.ipc", "ipc.open_stream", "new_stream("):
        assert token not in package, f"IPC-era token {token!r} is back in the package"
