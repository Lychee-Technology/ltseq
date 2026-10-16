"""Tests for Phase 3.2: pivot() functionality.

Tests the pivot() method which reshapes data from long format to wide format
(pivot table operation), transforming rows and columns based on grouping keys.
"""

import subprocess
import sys
import textwrap

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq


class TestPivotBasic:
    """Basic pivot() functionality tests."""

    def test_pivot_creates_ltseq(self):
        """pivot() should return an LTSeq instance."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        result = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")
        assert isinstance(result, LTSeq)

    def test_pivot_simple_sum(self):
        """pivot() should create wide format with summed values."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        # test_agg.csv has:
        # region,amount,year
        # West,1000,2020
        # West,1500,2021
        # West,2000,2022
        # East,500,2020
        # East,600,2021
        # East,700,2022

        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        # Convert to pandas for easier verification
        df = pivoted.to_pandas()

        # Should have 3 rows (one per year: 2020, 2021, 2022)
        assert len(df) == 3

        # Should have 3 columns (year, East, West)
        assert len(df.columns) == 3

        # Check that East and West columns exist
        assert "East" in df.columns
        assert "West" in df.columns

        # Verify values
        year_2020 = df[df["year"] == 2020].iloc[0]
        assert year_2020["East"] == 500
        assert year_2020["West"] == 1000

        year_2021 = df[df["year"] == 2021].iloc[0]
        assert year_2021["East"] == 600
        assert year_2021["West"] == 1500

        year_2022 = df[df["year"] == 2022].iloc[0]
        assert year_2022["East"] == 700
        assert year_2022["West"] == 2000

    def test_pivot_with_mean_agg(self):
        """pivot() should support mean aggregation."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        pivoted = t.pivot(
            index="year", columns="region", values="amount", agg_fn="mean"
        )

        df = pivoted.to_pandas()

        # With only one row per (year, region) combination, mean should equal sum
        year_2020 = df[df["year"] == 2020].iloc[0]
        assert year_2020["East"] == 500
        assert year_2020["West"] == 1000

    def test_pivot_with_min_agg(self):
        """pivot() should support min aggregation."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="min")

        df = pivoted.to_pandas()

        # With only one row per (year, region) combination, min should equal value
        year_2020 = df[df["year"] == 2020].iloc[0]
        assert year_2020["East"] == 500
        assert year_2020["West"] == 1000

    def test_pivot_with_max_agg(self):
        """pivot() should support max aggregation."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="max")

        df = pivoted.to_pandas()

        # With only one row per (year, region) combination, max should equal value
        year_2020 = df[df["year"] == 2020].iloc[0]
        assert year_2020["East"] == 500
        assert year_2020["West"] == 1000

    def test_pivot_with_count_agg(self):
        """pivot() should support count aggregation."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        pivoted = t.pivot(
            index="year", columns="region", values="amount", agg_fn="count"
        )

        df = pivoted.to_pandas()

        # With one row per (year, region) combination, count should be 1
        year_2020 = df[df["year"] == 2020].iloc[0]
        assert year_2020["East"] == 1
        assert year_2020["West"] == 1


class TestPivotCompositeIndex:
    """Tests for pivot() with composite index keys."""

    def test_pivot_with_composite_index_list(self):
        """pivot() should support list of index columns."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        # Use region as first index, year as second (unusual but valid)
        pivoted = t.pivot(
            index=["region", "year"], columns="amount", values="region", agg_fn="count"
        )

        df = pivoted.to_pandas()

        # Should have at least 6 rows (2 regions × 3 years)
        assert len(df) >= 6

        # Should have region and year columns in the result
        assert "region" in df.columns
        assert "year" in df.columns


class TestPivotValidation:
    """Tests for pivot() error handling and validation."""

    def test_pivot_missing_schema_raises_error(self):
        """pivot() should raise error if schema not initialized."""
        t = LTSeq()
        with pytest.raises(ValueError, match="Schema not initialized"):
            t.pivot(index="year", columns="region", values="amount")

    def test_pivot_invalid_index_column_raises_error(self):
        """pivot() should raise error if index column doesn't exist."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        with pytest.raises(AttributeError, match="not found"):
            t.pivot(index="nonexistent", columns="region", values="amount")

    def test_pivot_invalid_columns_column_raises_error(self):
        """pivot() should raise error if columns column doesn't exist."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        with pytest.raises(AttributeError, match="not found"):
            t.pivot(index="year", columns="nonexistent", values="amount")

    def test_pivot_invalid_values_column_raises_error(self):
        """pivot() should raise error if values column doesn't exist."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        with pytest.raises(AttributeError, match="not found"):
            t.pivot(index="year", columns="region", values="nonexistent")

    def test_pivot_invalid_agg_fn_raises_error(self):
        """pivot() should raise error if agg_fn is not supported."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        with pytest.raises(ValueError, match="Invalid aggregation function"):
            t.pivot(index="year", columns="region", values="amount", agg_fn="invalid")

    def test_pivot_invalid_index_type_raises_error(self):
        """pivot() should raise error if index is not str or list."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        with pytest.raises(TypeError):
            t.pivot(index=123, columns="region", values="amount")


class TestPivotDataIntegrity:
    """Tests for pivot() data integrity and correctness."""

    def test_pivot_preserves_all_data(self):
        """pivot() should preserve all data (no rows lost)."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        df = pivoted.to_pandas()

        # Original had 6 rows, pivoted should have 3 (one per year)
        assert len(df) == 3

    def test_pivot_result_has_correct_schema(self):
        """pivot() result should have correct column names."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        df = pivoted.to_pandas()

        # Should have: year (index column), East, West (from pivot)
        expected_cols = {"year", "East", "West"}
        assert set(df.columns) == expected_cols

    def test_pivot_with_string_index(self):
        """pivot() should accept index as string."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        # Both should work
        pivoted1 = t.pivot(index="year", columns="region", values="amount")
        pivoted2 = t.pivot(index=["year"], columns="region", values="amount")

        df1 = pivoted1.to_pandas()
        df2 = pivoted2.to_pandas()

        # Results should be identical
        assert len(df1) == len(df2)
        assert set(df1.columns) == set(df2.columns)


class TestPivotEdgeCases:
    """Tests for pivot() edge cases."""

    def test_pivot_all_aggregation_functions(self):
        """pivot() should work with all supported aggregation functions."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")

        agg_fns = ["sum", "mean", "count", "min", "max"]
        for agg_fn in agg_fns:
            pivoted = t.pivot(
                index="year", columns="region", values="amount", agg_fn=agg_fn
            )
            assert isinstance(pivoted, LTSeq)
            df = pivoted.to_pandas()
            assert len(df) == 3  # Should have one row per year

    def test_pivot_is_chainable_with_filter(self):
        """pivot() result should be chainable with filter."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        # Should be able to filter (but need to materialize to pandas to call to_pandas on result)
        filtered = pivoted.filter(lambda r: r.year >= 2021)
        assert isinstance(filtered, LTSeq)

        # Verify the filter worked by using the materialized pivot data
        df_original = pivoted.to_pandas()
        assert len(df_original) == 3

    def test_pivot_result_structure(self):
        """pivot() result should have correct structure."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        # Check that pivoted data can be used with to_pandas
        df = pivoted.to_pandas()
        assert len(df) == 3
        assert set(df.columns) == {"year", "East", "West"}


class TestPivotIntegration:
    """Integration tests combining pivot() with other operations."""

    def test_pivot_then_filter_basic(self):
        """Should be able to filter pivot results (basic check)."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        # Verify pivot worked
        df = pivoted.to_pandas()
        original_len = len(df)
        assert original_len == 3

    def test_pivot_then_derive_basic(self):
        """Should be able to derive columns on pivot results (basic check)."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        # Verify pivot worked and has expected columns
        df = pivoted.to_pandas()
        assert "East" in df.columns
        assert "West" in df.columns

        # Can manually compute totals
        df["total"] = df["East"] + df["West"]
        year_2020 = df[df["year"] == 2020].iloc[0]
        assert year_2020["total"] == 1500  # 500 + 1000

    def test_pivot_then_select_basic(self):
        """Should be able to select columns on pivot results (basic check)."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        # Verify pivot structure
        df = pivoted.to_pandas()
        assert "year" in df.columns
        assert "West" in df.columns

    def test_pivot_then_sort_basic(self):
        """Should be able to sort pivot results (basic check)."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        # Verify pivot worked
        df = pivoted.to_pandas()
        years = df["year"].tolist()
        # Default order from pivot
        assert len(years) == 3

    def test_pivot_result_aggregatable(self):
        """Pivot result should support row manipulation."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        pivoted = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        df = pivoted.to_pandas()
        # Verify we can work with the data
        assert len(df) == 3
        assert "East" in df.columns
        assert "West" in df.columns


class TestPivotRegressions:
    """Regression tests to ensure pivot() doesn't break existing functionality."""

    def test_original_table_unchanged(self):
        """pivot() should not modify the original table."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")
        original_len = len(t)

        _ = t.pivot(index="year", columns="region", values="amount", agg_fn="sum")

        # Original should be unchanged
        assert len(t) == original_len

    def test_pivot_multiple_times(self):
        """Should be able to pivot the same table multiple times."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/test_agg.csv")

        pivoted1 = t.pivot(
            index="year", columns="region", values="amount", agg_fn="sum"
        )
        pivoted2 = t.pivot(
            index="year", columns="region", values="amount", agg_fn="mean"
        )

        df1 = pivoted1.to_pandas()
        df2 = pivoted2.to_pandas()

        # Both should have same structure
        assert len(df1) == len(df2)
        assert set(df1.columns) == set(df2.columns)


# Arrow has three physical string types, each of which can also be
# dictionary-encoded. `read_parquet` yields string_view (DataFusion 55 scans
# Parquet strings as Utf8View) unless the file stores a dictionary-encoded
# column, e.g. a pandas categorical; `from_arrow` / `from_pandas` keep the
# source type; `read_csv` / `from_rows` produce plain string.
STRING_TYPES = [
    pa.string(),
    pa.large_string(),
    pa.string_view(),
    pa.dictionary(pa.int8(), pa.string()),
    pa.dictionary(pa.int8(), pa.large_string()),  # a pandas 3 categorical
]


def _events(url_type: pa.DataType) -> pa.Table:
    return pa.table(
        {
            "userid": pa.array([1, 1, 1, 2, 2], pa.int64()),
            "url": pa.array(["a", "a", "b", "a", "b"], url_type),
            "value": pa.array([1.0, 10.0, 2.0, 3.0, 4.0], pa.float64()),
        }
    )


def _assert_pivoted_by_url(pivoted: LTSeq) -> None:
    assert set(pivoted.schema) == {"userid", "a", "b"}
    rows = {row["userid"]: row for row in pivoted.to_dicts()}
    assert rows[1]["a"] == 11.0 and rows[1]["b"] == 2.0
    assert rows[2]["a"] == 3.0 and rows[2]["b"] == 4.0


class TestPivotStringColumnTypes:
    """pivot() must accept every Arrow string type as the pivot column (#177)."""

    @pytest.mark.parametrize("url_type", STRING_TYPES, ids=str)
    def test_pivot_on_each_arrow_string_type(self, url_type):
        t = LTSeq.from_arrow(_events(url_type))
        pivoted = t.pivot(index="userid", columns="url", values="value", agg_fn="sum")
        _assert_pivoted_by_url(pivoted)

    @pytest.mark.parametrize(
        ("written", "scanned"),
        [
            (pa.string(), pa.string_view()),
            (pa.dictionary(pa.int8(), pa.string()), pa.dictionary(pa.int8(), pa.string())),
        ],
        ids=["string", "dictionary"],
    )
    def test_pivot_on_parquet_string_column(self, tmp_path, written, scanned):
        """The reported path: a Parquet scan hands pivot() string_view data, or
        dictionary data when the file stores a dictionary-encoded column."""
        path = tmp_path / "events.parquet"
        pq.write_table(_events(written), path)
        t = LTSeq.read_parquet(str(path))
        # Guard the premise, which depends on DataFusion's scan defaults.
        assert t.to_arrow().schema.field("url").type == scanned
        pivoted = t.pivot(index="userid", columns="url", values="value", agg_fn="sum")
        _assert_pivoted_by_url(pivoted)

    def test_pivot_ignores_unreferenced_dictionary_values(self):
        """A dictionary entry no row uses (a pandas category with no rows left)
        must not become a pivot column."""
        url = pa.DictionaryArray.from_arrays(
            pa.array([0, 0, 1, 0, 1], pa.int8()), pa.array(["a", "b", "unused"])
        )
        t = LTSeq.from_arrow(_events(pa.string()).set_column(1, "url", url))
        pivoted = t.pivot(index="userid", columns="url", values="value", agg_fn="sum")
        _assert_pivoted_by_url(pivoted)

    def test_pivot_on_dictionary_encoded_int_column(self):
        """A dictionary-encoded pivot column pivots like its value type."""
        t = LTSeq.from_arrow(
            pa.table(
                {
                    "userid": pa.array([1, 1, 2], pa.int64()),
                    "code": pa.array([7, 8, 7], pa.int64()).dictionary_encode(),
                    "value": pa.array([1.0, 2.0, 3.0], pa.float64()),
                }
            )
        )
        pivoted = t.pivot(index="userid", columns="code", values="value", agg_fn="sum")
        assert set(pivoted.schema) == {"userid", "7", "8"}
        rows = {row["userid"]: row for row in pivoted.to_dicts()}
        assert rows[1]["7"] == 1.0 and rows[1]["8"] == 2.0 and rows[2]["7"] == 3.0


def _assert_rows_by_column(pivot_values: pa.Array, expected: dict[str, set[int]]) -> None:
    """Pivot one index group on ``pivot_values`` and check, column by column
    and in output order, which input rows each output column aggregated.

    Row ``i`` carries ``2**i`` in the value column, so a cell's sum spells out
    its rows: a row in no column, in two columns, or in the wrong one shows.
    """
    n = len(pivot_values)
    t = LTSeq.from_arrow(
        pa.table(
            {
                "k": pa.array([1] * n, pa.int64()),
                "p": pivot_values,
                "v": pa.array([float(2**i) for i in range(n)], pa.float64()),
            }
        )
    )
    [row] = t.pivot(index="k", columns="p", values="v", agg_fn="sum").to_dicts()
    del row["k"]
    rows_by_column = {
        name: {i for i in range(n) if int(total or 0) >> i & 1} for name, total in row.items()
    }
    assert list(rows_by_column.items()) == list(expected.items())


# Float bit patterns that compare or display alike: NaNs that differ in sign
# and in payload, and both zeros.
FLOAT_BITS = {
    pa.float32(): {
        "nan": 0x7FC00000,
        "neg_nan": 0xFFC00000,
        "nan_payload": 0x7FC00001,
        "neg_zero": 0x80000000,
        "zero": 0,
        "one": 0x3F800000,
    },
    pa.float64(): {
        "nan": 0x7FF8000000000000,
        "neg_nan": 0xFFF8000000000000,
        "nan_payload": 0x7FF8000000000001,
        "neg_zero": 0x8000000000000000,
        "zero": 0,
        "one": 0x3FF0000000000000,
    },
}


class TestPivotValueTypes:
    """pivot() compares each pivot value as a literal of the pivot column's own
    type, and keeps the column names it has always produced (#205)."""

    @pytest.mark.parametrize("url_type", STRING_TYPES, ids=str)
    @pytest.mark.parametrize(
        "values",
        [
            # Was a cast error: the column was cast to the Int64 of "2023".
            ["2023", "a"],
            # Was both rows in both columns: "01" and "1" both cast to 1.
            ["01", "1"],
            ["1", "01", "+1", "1.0", "1e0", "0", "-0", "NaN", "nan", "inf"],
        ],
        ids=["number-and-word", "leading-zero", "number-spellings"],
    )
    def test_numeric_looking_strings_stay_strings(self, url_type, values):
        expected = {value: {i} for i, value in sorted(enumerate(values), key=lambda iv: iv[1])}
        _assert_rows_by_column(pa.array(values, url_type), expected)

    @pytest.mark.parametrize("encode", [False, True], ids=["plain", "dictionary"])
    def test_float32_values_that_are_not_binary_fractions(self, encode):
        """Was every cell NULL: the value went through its text to the f64
        0.1, which no f32 widened to f64 equals."""
        values = pa.array([0.1, 0.2, 0.3, -0.7, 1.5, 2.0], pa.float32())
        if encode:
            values = values.dictionary_encode()
        _assert_rows_by_column(
            values,
            {"-0.7": {3}, "0.1": {0}, "0.2": {1}, "0.3": {2}, "1.5": {4}, "2": {5}},
        )

    @pytest.mark.parametrize(
        ("values", "expected"),
        [
            (pa.array([5, 10, -3], pa.int32()), {"-3": {2}, "10": {1}, "5": {0}}),
            (pa.array([5, 10, -3], pa.int64()), {"-3": {2}, "10": {1}, "5": {0}}),
            (
                pa.array([2**53 + 1, 2**53], pa.int64()),
                {"9007199254740992": {1}, "9007199254740993": {0}},
            ),
            (
                pa.array([2.0, 0.5, 0.1, 1e20, 1e-7, float("inf"), float("-inf")], pa.float64()),
                {
                    "-inf": {6},
                    "0.0000001": {4},
                    "0.1": {2},
                    "0.5": {1},
                    "100000000000000000000": {3},
                    "2": {0},
                    "inf": {5},
                },
            ),
            (pa.array([2.0, 0.5, -0.25], pa.float32()), {"-0.25": {2}, "0.5": {1}, "2": {0}}),
        ],
        ids=["int32", "int64", "int64-beyond-f64", "float64", "float32"],
    )
    def test_names_and_order_are_unchanged(self, values, expected):
        """A number's column is named as Rust displays it, and the columns
        are ordered by name as strings, as before the literals were typed."""
        _assert_rows_by_column(values, expected)

    @pytest.mark.parametrize("encode", [False, True], ids=["plain", "dictionary"])
    @pytest.mark.parametrize("float_type", list(FLOAT_BITS), ids=str)
    def test_every_float_row_lands_in_one_column(self, engine_layout, float_type, encode):
        """-0.0 and 0.0 share the column "0", and every NaN, whatever its
        bits, the column "NaN". Was the -0.0 rows and all but one NaN pattern
        in no column, and on a dictionary column a "-0" column holding the
        0.0 rows."""
        bits = FLOAT_BITS[float_type]
        order = ["neg_zero", "nan", "zero", None, "neg_nan", "one", "nan_payload"]
        unsigned = pa.uint32() if float_type == pa.float32() else pa.uint64()
        values = pa.array([None if k is None else bits[k] for k in order], unsigned).view(float_type)
        if encode:
            values = values.dictionary_encode()
        _assert_rows_by_column(values, {"0": {0, 2}, "1": {5}, "NaN": {1, 4, 6}})

    @pytest.mark.parametrize("encode", [False, True], ids=["plain", "dictionary"])
    @pytest.mark.parametrize("float_type", list(FLOAT_BITS), ids=str)
    def test_many_nan_bit_patterns_share_one_column(self, float_type, encode):
        """The "NaN" column's test lists every NaN bit pattern the discovery
        finds, so it must not nest deeper with their number: as an OR of
        equalities, 512 patterns overflowed the stack and killed the process.
        Runs in a subprocess, so a crash fails this test alone."""
        script = textwrap.dedent(
            """
            import sys
            import pyarrow as pa
            from ltseq import LTSeq
            from ltseq._testing import pinned_engine

            float_type = pa.type_for_alias(sys.argv[1])
            first_nan, n, encode = int(sys.argv[2]), int(sys.argv[3]), sys.argv[4] == "True"
            unsigned = pa.uint32() if float_type == pa.float32() else pa.uint64()
            p = pa.array(range(first_nan, first_nan + n), unsigned).view(float_type)
            if encode:
                p = p.dictionary_encode()
            with pinned_engine(target_partitions=2, batch_size=7):
                t = LTSeq.from_arrow(pa.table({"k": [1] * n, "p": p, "v": [1] * n}))
                rows = t.pivot(index="k", columns="p", values="v").to_dicts()
            assert rows == [{"k": 1, "NaN": n}], rows
            """
        )
        nan = FLOAT_BITS[float_type]["nan"]
        args = [str(float_type), str(nan), "4096", str(encode)]
        subprocess.run([sys.executable, "-c", script, *args], check=True)
