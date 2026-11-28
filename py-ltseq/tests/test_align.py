"""Tests for LTSeq.align() method."""

import os
import tempfile
import pandas as pd
import pytest
from ltseq import LTSeq


@pytest.fixture
def sample_csv():
    """Create a sample CSV with gaps in dates."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("date,value,name\n")
        f.write("2024-01-02,100,Alice\n")
        f.write("2024-01-04,200,Bob\n")
        f.write("2024-01-05,300,Charlie\n")
        path = f.name
    yield path
    os.unlink(path)


@pytest.fixture
def int_key_csv():
    """Create a sample CSV with integer keys."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("id,value\n")
        f.write("2,20\n")
        f.write("4,40\n")
        f.write("5,50\n")
        path = f.name
    yield path
    os.unlink(path)


class TestAlignBasic:
    """Basic functionality tests for align()."""

    def test_align_reorders_rows(self, sample_csv):
        """Test that align reorders rows to match ref_sequence."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-04", "2024-01-02"]  # Reversed order
        result = t.align(ref, key=lambda r: r.date)

        df = result.to_pandas()
        assert len(df) == 2
        assert str(df.iloc[0]["date"]) == "2024-01-04"
        assert str(df.iloc[1]["date"]) == "2024-01-02"

    def test_align_inserts_null_payload_rows(self, sample_csv):
        """Test that align inserts a row with NULL payload for each missing
        key (its key holds the reference key, #317)."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-01", "2024-01-02", "2024-01-03"]
        result = t.align(ref, key=lambda r: r.date)

        df = result.to_pandas()
        assert len(df) == 3
        # First row: 2024-01-01 not in original -> NULL payload
        assert df.iloc[0]["value"] is None or (
            hasattr(df.iloc[0]["value"], "__float__")
            and str(df.iloc[0]["value"]) == "nan"
        )
        # Second row: 2024-01-02 exists
        assert df.iloc[1]["value"] == 100
        # Third row: 2024-01-03 not in original -> NULL payload
        assert df.iloc[2]["value"] is None or (
            hasattr(df.iloc[2]["value"], "__float__")
            and str(df.iloc[2]["value"]) == "nan"
        )

    def test_align_excludes_unmatched(self, sample_csv):
        """Test that rows not in ref_sequence are excluded."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02", "2024-01-04"]  # Excludes 2024-01-05
        result = t.align(ref, key=lambda r: r.date)

        df = result.to_pandas()
        assert len(df) == 2
        dates = df["date"].tolist()
        assert "2024-01-05" not in dates

    def test_align_preserves_schema(self, sample_csv):
        """Test that align preserves the original schema."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02"]
        result = t.align(ref, key=lambda r: r.date)

        assert set(result._schema.keys()) == {"date", "value", "name"}

    def test_align_returns_ltseq(self, sample_csv):
        """Test that align returns an LTSeq instance."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02"]
        result = t.align(ref, key=lambda r: r.date)

        assert isinstance(result, LTSeq)


class TestAlignDuplicates:
    """Tests for duplicate key handling."""

    def test_align_duplicate_ref_keys(self, sample_csv):
        """Test that duplicate ref_sequence keys produce duplicate rows."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02", "2024-01-02", "2024-01-04"]
        result = t.align(ref, key=lambda r: r.date)

        df = result.to_pandas()
        assert len(df) == 3
        assert str(df.iloc[0]["date"]) == "2024-01-02"
        assert str(df.iloc[1]["date"]) == "2024-01-02"
        assert str(df.iloc[2]["date"]) == "2024-01-04"


class TestAlignIntKeys:
    """Tests for integer key handling."""

    def test_align_with_int_keys(self, int_key_csv):
        """Test align with integer keys."""
        t = LTSeq.read_csv(int_key_csv)
        ref = [1, 2, 3, 4]
        result = t.align(ref, key=lambda r: r.id)

        df = result.to_pandas()
        assert len(df) == 4
        # id=1 and id=3 are not in the table: their payloads are NULL
        import pandas as pd

        assert pd.isna(df.iloc[0]["value"])
        assert df.iloc[1]["value"] == 20
        assert pd.isna(df.iloc[2]["value"])
        assert df.iloc[3]["value"] == 40


class TestAlignErrors:
    """Error handling tests for align()."""

    def test_align_empty_ref_sequence(self, sample_csv):
        """Test that empty ref_sequence raises ValueError."""
        t = LTSeq.read_csv(sample_csv)
        with pytest.raises(ValueError, match="ref_sequence cannot be empty"):
            t.align([], key=lambda r: r.date)

    def test_align_no_schema(self):
        """Test that align raises if schema not initialized."""
        t = LTSeq()
        with pytest.raises(ValueError, match="Schema not initialized"):
            t.align(["a"], key=lambda r: r.col)

    def test_align_invalid_key_type(self, sample_csv):
        """Test that align raises for non-callable key."""
        t = LTSeq.read_csv(sample_csv)
        with pytest.raises(TypeError, match="key must be callable"):
            t.align(["a"], key="date")  # type: ignore

    def test_align_nonexistent_column(self, sample_csv):
        """Test that align raises for nonexistent key column."""
        t = LTSeq.read_csv(sample_csv)
        with pytest.raises(AttributeError, match="not found in schema"):
            t.align(["a"], key=lambda r: r.nonexistent)

    def test_align_complex_expression(self, sample_csv):
        """Test that align raises for complex key expressions."""
        t = LTSeq.read_csv(sample_csv)
        with pytest.raises(TypeError, match="simple column reference"):
            t.align(["a"], key=lambda r: r.value + 1)


class TestAlignEdgeCases:
    """Edge case tests for align()."""

    def test_align_all_keys_missing(self, sample_csv):
        """Test align when all ref keys are missing from table."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-10", "2024-01-11"]
        result = t.align(ref, key=lambda r: r.date)

        df = result.to_pandas()
        assert len(df) == 2
        import pandas as pd

        assert all(pd.isna(df["value"]))

    def test_align_all_keys_present(self, sample_csv):
        """Test align when all ref keys exist in table."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02", "2024-01-04", "2024-01-05"]
        result = t.align(ref, key=lambda r: r.date)

        df = result.to_pandas()
        assert len(df) == 3
        import pandas as pd

        assert all(pd.notna(df["value"]))

    def test_align_single_row(self, sample_csv):
        """Test align with single ref key."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02"]
        result = t.align(ref, key=lambda r: r.date)

        df = result.to_pandas()
        assert len(df) == 1
        assert df.iloc[0]["value"] == 100

    def test_align_preserves_column_order(self, sample_csv):
        """Test that align preserves the original column order."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02"]
        result = t.align(ref, key=lambda r: r.date)

        df = result.to_pandas()
        columns = list(df.columns)
        # Original schema order should be preserved
        assert columns == ["date", "value", "name"]

    def test_align_clears_sort_keys(self, sample_csv):
        """Test that align clears sort key tracking."""
        t = LTSeq.read_csv(sample_csv)
        t_sorted = t.sort("date")
        assert t_sorted.sort_keys is not None

        ref = ["2024-01-02", "2024-01-04"]
        result = t_sorted.align(ref, key=lambda r: r.date)

        # Sort keys should be cleared after align (order is now from ref_sequence)
        assert result.sort_keys is None


class TestAlignChaining:
    """Tests for chaining align with other operations."""

    def test_align_then_filter(self, sample_csv):
        """Test align followed by filter."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
        result = t.align(ref, key=lambda r: r.date).filter(
            lambda r: r.value.is_not_null()
        )

        df = result.to_pandas()
        assert len(df) == 2  # Only 2024-01-02 and 2024-01-04 have values
        import pandas as pd

        assert all(pd.notna(df["value"]))

    def test_align_then_derive(self, sample_csv):
        """Test align followed by derive."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02", "2024-01-04"]
        result = t.align(ref, key=lambda r: r.date).derive(
            doubled=lambda r: r.value * 2
        )

        df = result.to_pandas()
        assert len(df) == 2
        assert df.iloc[0]["doubled"] == 200
        assert df.iloc[1]["doubled"] == 400


class TestAlignArrowKeyTypes:
    """Arrow-type fidelity of the typed ref key (PR #104 review).

    The legacy SQL path compared keys as strings (CAST AS VARCHAR); the
    native path builds the ref key array in the key column's exact Arrow
    type. These tests pin the type-family coverage, especially the UInt64
    values above i64::MAX that a naive i64 path would reject.
    """

    def test_uint64_key_above_i64_max(self):
        import pyarrow as pa
        from ltseq import LTSeq

        big = 2**63  # > i64::MAX
        t = LTSeq.from_arrow(
            pa.table(
                {
                    "k": pa.array([big, 1], type=pa.uint64()),
                    "v": pa.array([10, 20], type=pa.int64()),
                }
            )
        )
        df = t.align([1, big], key=lambda r: r.k).to_pandas()
        assert df["v"].tolist() == [20, 10]

    def test_float32_key(self):
        import pyarrow as pa
        from ltseq import LTSeq

        t = LTSeq.from_arrow(
            pa.table(
                {
                    "k": pa.array([1.5, 2.5], type=pa.float32()),
                    "v": pa.array([10, 20], type=pa.int64()),
                }
            )
        )
        df = t.align([2.5, 1.5], key=lambda r: r.k).to_pandas()
        assert df["v"].tolist() == [20, 10]

    def test_utf8_key(self):
        import pyarrow as pa
        from ltseq import LTSeq

        t = LTSeq.from_arrow(
            pa.table(
                {
                    "k": pa.array(["b", "a"], type=pa.string()),
                    "v": pa.array([10, 20], type=pa.int64()),
                }
            )
        )
        df = t.align(["a", "b", "c"], key=lambda r: r.k).to_pandas()
        assert df["v"].tolist()[:2] == [20, 10]
        assert pd.isna(df["v"].tolist()[2])

    def test_narrow_int_key_out_of_range_ref_raises(self):
        """safe=false cast: an out-of-range ref value errors instead of
        silently becoming NULL (which would corrupt the join)."""
        import pyarrow as pa
        from ltseq import LTSeq

        t = LTSeq.from_arrow(
            pa.table(
                {
                    "k": pa.array([1, 2], type=pa.int8()),
                    "v": pa.array([10, 20], type=pa.int64()),
                }
            )
        )
        with pytest.raises(Exception, match="outside key column"):
            t.align([1, 300], key=lambda r: r.k)

    def test_date32_key_string_refs(self):
        """The documented usage: string refs against a Date32 column. The
        native path parses the strings into Date32 via arrow cast (the
        legacy path compared everything as VARCHAR)."""
        import datetime

        import pyarrow as pa
        from ltseq import LTSeq

        t = LTSeq.from_arrow(
            pa.table(
                {
                    "k": pa.array(
                        [datetime.date(2024, 1, 2), datetime.date(2024, 1, 1)],
                        type=pa.date32(),
                    ),
                    "v": pa.array([20, 10], type=pa.int64()),
                }
            )
        )
        df = t.align(["2024-01-01", "2024-01-02"], key=lambda r: r.k).to_pandas()
        assert df["v"].tolist() == [10, 20]

    def test_unsupported_key_type_raises(self):
        import pyarrow as pa
        from ltseq import LTSeq

        t = LTSeq.from_arrow(
            pa.table(
                {
                    "k": pa.array([[1, 2]], type=pa.list_(pa.int64())),
                    "v": pa.array([1], type=pa.int64()),
                }
            )
        )
        with pytest.raises(Exception, match="unsupported type"):
            t.align([[1, 2]], key=lambda r: r.k)


# -- #317: the key of an inserted row is its reference key -----------------


def _rows(t):
    return t.to_arrow().to_pylist()


class TestAlignFillsInsertedKeys:
    """A row `align` inserts for a reference key the table lacks holds that
    key in the key column, and NULL in every other column (#317). Before
    the fix the key was NULL too, so the row could not be sorted back into
    place, joined or labelled."""

    def test_issue_reproducer(self, engine_layout):
        t = LTSeq.from_rows([{"k": 0, "v": 1.0}, {"k": 1, "v": 2.0}, {"k": 3, "v": 4.0}])
        rows = t.align([0, 1, 2, 3], key=lambda r: r.k).to_dicts()
        assert [r["k"] for r in rows] == [0, 1, 2, 3]
        assert [r["v"] for r in rows] == [1.0, 2.0, None, 4.0]

    def test_all_reference_keys_missing(self):
        import pyarrow as pa

        t = LTSeq.from_rows([{"k": 1, "v": 1.0, "s": "a"}])
        assert _rows(t.align([7, 5], key=lambda r: r.k)) == [
            {"k": 7, "v": None, "s": None},
            {"k": 5, "v": None, "s": None},
        ]

        empty = LTSeq.from_arrow(
            pa.table({"k": pa.array([], pa.int64()), "v": pa.array([], pa.float64())})
        )
        out = empty.align([2, 0], key=lambda r: r.k).to_arrow()
        assert out.to_pylist() == [{"k": 2, "v": None}, {"k": 0, "v": None}]
        assert out.schema.field("k").type == pa.int64()

    def test_matched_keys_keep_their_own_value(self):
        """A matched row keeps the table's key, not the reference's, even
        where the join's equality matches two representations: -0.0 is
        matched by the reference value 0.0 and stays -0.0."""
        import math

        import pyarrow as pa

        t = LTSeq.from_arrow(
            pa.table({"k": pa.array([-0.0, 1.0]), "v": pa.array([1, 2])})
        )
        rows = _rows(t.align([0.0, 1.0, 2.5], key=lambda r: r.k))
        assert [r["v"] for r in rows] == [1, 2, None]
        assert math.copysign(1.0, rows[0]["k"]) == -1.0
        assert [r["k"] for r in rows[1:]] == [1.0, 2.5]

    def test_reference_order_and_excluded_rows(self, engine_layout):
        t = LTSeq.from_rows([{"k": k, "v": k * 10} for k in (1, 2, 4, 5, 9)])
        rows = _rows(t.align([5, 3, 1, 8, 2], key=lambda r: r.k))
        assert rows == [
            {"k": 5, "v": 50},
            {"k": 3, "v": None},
            {"k": 1, "v": 10},
            {"k": 8, "v": None},
            {"k": 2, "v": 20},
        ]

    def test_duplicate_reference_keys(self):
        t = LTSeq.from_rows([{"k": 1, "v": 10}])
        rows = _rows(t.align([2, 1, 2, 1], key=lambda r: r.k))
        assert rows == [
            {"k": 2, "v": None},
            {"k": 1, "v": 10},
            {"k": 2, "v": None},
            {"k": 1, "v": 10},
        ]

    def test_duplicate_source_keys_keep_join_multiplicity(self):
        """Each reference position holds every matching row; the order of
        rows within one position is not specified."""
        t = LTSeq.from_rows(
            [{"k": 1, "v": 10}, {"k": 1, "v": 11}, {"k": 3, "v": 30}, {"k": 1, "v": 12}]
        )
        rows = _rows(t.align([3, 2, 1], key=lambda r: r.k))
        assert [r["k"] for r in rows] == [3, 2, 1, 1, 1]
        assert rows[0] == {"k": 3, "v": 30}
        assert rows[1] == {"k": 2, "v": None}
        assert sorted(r["v"] for r in rows[2:]) == [10, 11, 12]

    def test_sort_keys_stay_none(self):
        t = LTSeq.from_rows([{"k": 0, "v": 1}, {"k": 2, "v": 3}]).sort("k")
        assert t.sort_keys is not None
        assert t.align([0, 1, 2], key=lambda r: r.k).sort_keys is None

    def test_schema_is_unchanged_and_working_columns_do_not_leak(self):
        """Names, order, types and nullability of the result are those the
        LEFT JOIN gave before #317: every column nullable."""
        import pyarrow as pa

        t = LTSeq.from_arrow(
            pa.table(
                {
                    "a": pa.array(["x", "y"]),
                    "k": pa.array([1, 3], pa.int32()),
                    "z": pa.array([1.5, 2.5], pa.float32()),
                }
            )
        )
        out = t.align([3, 2, 1], key=lambda r: r.k)
        schema = out.to_arrow().schema
        assert schema.names == ["a", "k", "z"]
        assert [f.type for f in schema] == [pa.string(), pa.int32(), pa.float32()]
        assert [c["nullable"] for c in out._explain_pipeline()["columns"]] == [True] * 3
        assert _rows(out) == [
            {"a": "y", "k": 3, "z": 2.5},
            {"a": None, "k": 2, "z": None},
            {"a": "x", "k": 1, "z": 1.5},
        ]

    def test_key_field_keeps_its_metadata(self, engine_layout):
        """The key field keeps the key column's metadata through the plan
        and the Arrow export, as the other fields keep theirs; an extension
        type held there survives. DataFusion plans the filled key as a
        CASE, whose field has no metadata of its own."""
        import pyarrow as pa

        key = pa.field("k", pa.int64(), metadata={"units": "minutes"})
        val = pa.field("v", pa.float64(), metadata={"units": "celsius"})
        t = LTSeq.from_arrow(
            pa.Table.from_arrays(
                [pa.array([3, 0, 1]), pa.array([4.0, 1.0, 2.0])],
                schema=pa.schema([key, val]),
            )
        )
        out = t.align([0, 1, 2, 3], key=lambda r: r.k).to_arrow()
        assert out.schema.field("k").type == pa.int64()
        assert out.schema.field("k").metadata == {b"units": b"minutes"}
        assert out.schema.field("v").metadata == {b"units": b"celsius"}
        assert out.column("k").to_pylist() == [0, 1, 2, 3]
        assert out.column("v").to_pylist() == [1.0, 2.0, None, 4.0]

        storage = pa.array(['"a"', '"c"'], pa.string())
        js = LTSeq.from_arrow(
            pa.table({"k": pa.ExtensionArray.from_storage(pa.json_(), storage), "v": [1, 3]})
        )
        out = js.align(['"c"', '"b"', '"a"'], key=lambda r: r.k).to_arrow()
        assert out.schema.field("k").type == pa.json_()
        assert out.column("k").combine_chunks().storage.to_pylist() == ['"c"', '"b"', '"a"']
        assert out.column("v").to_pylist() == [3, None, 1]

    def test_plan_stays_lazy_and_native(self):
        t = LTSeq.from_rows([{"k": 0, "v": 1}, {"k": 2, "v": 3}])
        info = t.align([0, 1, 2], key=lambda r: r.k)._explain_pipeline()
        assert info["operator_counts"].get("HashJoinExec", 0) + info["operator_counts"].get(
            "SortMergeJoinExec", 0
        ) == 1
        assert "Left Join" in info["logical_plan"]

    def test_case27_align_sort_forward_fill(self):
        """Tutorial case 27 (#308): align to the minute axis, sort by
        minute, forward fill. With a NULL key, the sort moved minute 2's
        row to the end and the fill carried 20 into it instead of 21."""
        per_min = LTSeq.from_rows(
            [
                {"minute": 0, "avg_temp": 20.5},
                {"minute": 1, "avg_temp": 21.0},
                {"minute": 3, "avg_temp": 19.5},
                {"minute": 4, "avg_temp": 20.0},
            ]
        )
        filled = (
            per_min.align([0, 1, 2, 3, 4], key=lambda r: r.minute)
            .sort("minute")
            .fold(
                lambda s, r: r["avg_temp"] if r["avg_temp"] is not None else s,
                init=None,
                into="avg_ffill",
            )
        )
        rows = filled.to_dicts()
        assert [r["minute"] for r in rows] == [0, 1, 2, 3, 4]
        assert [r["avg_ffill"] for r in rows] == [20.5, 21.0, 21.0, 19.5, 20.0]

    def test_inserted_key_joins_downstream(self):
        t = LTSeq.from_rows([{"k": 1, "v": 10}, {"k": 3, "v": 30}])
        labels = LTSeq.from_rows([{"k": k, "label": f"L{k}"} for k in (1, 2, 3, 4)])
        joined = t.align([3, 2, 1], key=lambda r: r.k).join(labels, on="k", how="inner")
        rows = sorted(
            ((r["k"], r["v"], r["label"]) for r in joined.to_dicts()), key=lambda x: x[0]
        )
        assert rows == [(1, 10, "L1"), (2, None, "L2"), (3, 30, "L3")]


class TestAlignInsertedKeyTypes:
    """The inserted key has the key column's exact Arrow type, checked on
    representative types and units of each supported family (#317 AC7); no
    type is added."""

    @staticmethod
    def _check(arrow_type, present, missing, ref=None):
        import pyarrow as pa

        t = LTSeq.from_arrow(
            pa.table({"k": pa.array([present], arrow_type), "v": pa.array([1], pa.int64())})
        )
        ref = ref if ref is not None else [missing, present]
        out = t.align(ref, key=lambda r: r.k).to_arrow()
        assert out.schema.field("k").type == arrow_type
        assert out.column("k").to_pylist() == [missing, present]
        assert out.column("v").to_pylist() == [None, 1]

    @pytest.mark.parametrize(
        "type_name", ["int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64"]
    )
    def test_integers(self, type_name):
        import pyarrow as pa

        self._check(getattr(pa, type_name)(), 5, 7)

    def test_uint64_above_i64_max(self):
        import pyarrow as pa

        self._check(pa.uint64(), 2**63 + 1, 2**64 - 1)

    @pytest.mark.parametrize("type_name", ["float32", "float64"])
    def test_floats(self, type_name):
        import pyarrow as pa

        self._check(getattr(pa, type_name)(), 1.5, 2.25)

    @pytest.mark.parametrize("type_name", ["string", "large_string", "string_view"])
    def test_strings(self, type_name):
        import pyarrow as pa

        self._check(getattr(pa, type_name)(), "b", "a")

    def test_dates(self):
        import datetime

        import pyarrow as pa

        d1, d2 = datetime.date(2024, 1, 2), datetime.date(2024, 1, 1)
        self._check(pa.date32(), d1, d2, ref=["2024-01-01", "2024-01-02"])
        self._check(pa.date32(), d1, d2, ref=[d2, d1])

    @pytest.mark.parametrize("unit", ["s", "ms", "us"])
    def test_timestamps(self, unit):
        import datetime

        import pyarrow as pa

        t1, t2 = datetime.datetime(2024, 1, 1, 0, 1), datetime.datetime(2024, 1, 1, 0, 2)
        self._check(pa.timestamp(unit), t1, t2, ref=[t2, t1])

    def test_timestamp_with_zone(self):
        import datetime

        import pyarrow as pa

        utc = datetime.timezone.utc
        t1 = datetime.datetime(2024, 1, 1, 0, 1, tzinfo=utc)
        t2 = datetime.datetime(2024, 1, 1, 0, 2, tzinfo=utc)
        self._check(pa.timestamp("us", tz="UTC"), t1, t2, ref=[t2, t1])

    @pytest.mark.parametrize("arrow_type", ["time32", "time64"])
    def test_times(self, arrow_type):
        import datetime

        import pyarrow as pa

        typ = pa.time32("s") if arrow_type == "time32" else pa.time64("us")
        self._check(typ, datetime.time(0, 1), datetime.time(0, 2), ref=["00:02:00", "00:01:00"])
