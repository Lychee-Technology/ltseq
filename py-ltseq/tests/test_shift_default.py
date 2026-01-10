"""Tests for Phase 4.4: Window Default Values.

Tests shift(n, default=value) functionality that fills NULL boundary values
with user-specified defaults.
"""

import math
import os
import tempfile

import pytest

from ltseq import LTSeq


class TestShiftWithDefault:
    """Tests for shift() with default parameter."""

    @pytest.fixture
    def sample_table(self):
        """Create sample table for shift tests."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("id,value\n")
        csv.write("1,100\n")
        csv.write("2,200\n")
        csv.write("3,300\n")
        csv.write("4,400\n")
        csv.write("5,500\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_shift_positive_with_default_zero(self, sample_table):
        """shift(1, default=0) should fill first row with 0 instead of NULL."""
        result = sample_table.derive(
            lambda r: {"prev_value": r.value.shift(1, default=0)}
        )
        df = result.to_pandas()

        assert len(df) == 5
        # First row should have default value 0 (no previous row)
        assert df["prev_value"].iloc[0] == 0
        # Subsequent rows should have shifted values
        assert df["prev_value"].iloc[1] == 100
        assert df["prev_value"].iloc[2] == 200
        assert df["prev_value"].iloc[3] == 300
        assert df["prev_value"].iloc[4] == 400

    def test_shift_positive_with_default_value(self, sample_table):
        """shift(1, default=-1) should fill first row with -1."""
        result = sample_table.derive(
            lambda r: {"prev_value": r.value.shift(1, default=-1)}
        )
        df = result.to_pandas()

        assert df["prev_value"].iloc[0] == -1
        assert df["prev_value"].iloc[1] == 100

    def test_shift_negative_with_default(self, sample_table):
        """shift(-1, default=0) should fill last row with 0 instead of NULL."""
        result = sample_table.derive(
            lambda r: {"next_value": r.value.shift(-1, default=0)}
        )
        df = result.to_pandas()

        assert len(df) == 5
        # First rows should have shifted values
        assert df["next_value"].iloc[0] == 200
        assert df["next_value"].iloc[1] == 300
        assert df["next_value"].iloc[2] == 400
        assert df["next_value"].iloc[3] == 500
        # Last row should have default value 0 (no next row)
        assert df["next_value"].iloc[4] == 0

    def test_shift_two_with_default(self, sample_table):
        """shift(2, default=0) should fill first two rows with default."""
        result = sample_table.derive(lambda r: {"prev_2": r.value.shift(2, default=0)})
        df = result.to_pandas()

        # First two rows should have default value 0
        assert df["prev_2"].iloc[0] == 0
        assert df["prev_2"].iloc[1] == 0
        # Subsequent rows should have shifted values
        assert df["prev_2"].iloc[2] == 100
        assert df["prev_2"].iloc[3] == 200
        assert df["prev_2"].iloc[4] == 300

    def test_shift_without_default_has_null(self, sample_table):
        """shift(1) without default should produce NULL at boundary."""
        result = sample_table.derive(lambda r: {"prev_value": r.value.shift(1)})
        df = result.to_pandas()

        # First row should be NULL/NaN
        assert math.isnan(df["prev_value"].iloc[0]) or df["prev_value"].iloc[0] is None
        # Second row should have previous value
        assert df["prev_value"].iloc[1] == 100

    def test_shift_default_with_string_column(self):
        """shift with default should work on string columns."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("id,name\n")
        csv.write("1,Alice\n")
        csv.write("2,Bob\n")
        csv.write("3,Carol\n")
        csv.close()

        try:
            t = LTSeq.read_csv(csv.name)
            # Note: Don't use "N/A", "NA", "NULL", "None", "NaN" as defaults
            # because pandas interprets these as missing values during CSV round-trip
            result = t.derive(
                lambda r: {"prev_name": r.name.shift(1, default="(none)")}
            )
            df = result.to_pandas()

            assert df["prev_name"].iloc[0] == "(none)"
            assert df["prev_name"].iloc[1] == "Alice"
            assert df["prev_name"].iloc[2] == "Bob"
        finally:
            os.unlink(csv.name)


class TestShiftDefaultInExpressions:
    """Tests for shift with default in complex expressions."""

    @pytest.fixture
    def price_table(self):
        """Create table for price change calculations."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("date,price\n")
        # Use float values to avoid integer division issues
        csv.write("2024-01-01,100.0\n")
        csv.write("2024-01-02,105.0\n")
        csv.write("2024-01-03,103.0\n")
        csv.write("2024-01-04,110.0\n")
        csv.close()
        yield LTSeq.read_csv(csv.name)
        os.unlink(csv.name)

    def test_price_change_with_default(self, price_table):
        """Calculate price change using shift with default."""
        result = price_table.derive(
            lambda r: {"change": r.price - r.price.shift(1, default=0)}
        )
        df = result.to_pandas()

        # First row: 100 - 0 = 100 (price - default)
        assert df["change"].iloc[0] == 100
        # Second row: 105 - 100 = 5
        assert df["change"].iloc[1] == 5
        # Third row: 103 - 105 = -2
        assert df["change"].iloc[2] == -2
        # Fourth row: 110 - 103 = 7
        assert df["change"].iloc[3] == 7

    def test_percentage_change_with_default(self, price_table):
        """Calculate percentage change avoiding division by zero with default."""
        # Use shift with same price as default to get 0% change for first row
        # Note: Use float default (100.0) to match float column type
        result = price_table.derive(
            lambda r: {
                "prev_price": r.price.shift(1, default=100.0),
                "pct_change": (r.price - r.price.shift(1, default=100.0))
                / r.price.shift(1, default=100.0)
                * 100,
            }
        )
        df = result.to_pandas()

        # First row: (100 - 100) / 100 * 100 = 0%
        assert df["pct_change"].iloc[0] == 0.0
        # Second row: (105 - 100) / 100 * 100 = 5%
        assert df["pct_change"].iloc[1] == 5.0

    def test_multiple_shifts_with_different_defaults(self, price_table):
        """Multiple shift columns can have different defaults."""
        result = price_table.derive(
            lambda r: {
                "prev_1": r.price.shift(1, default=0),
                "prev_2": r.price.shift(2, default=-1),
            }
        )
        df = result.to_pandas()

        # prev_1 first row default is 0
        assert df["prev_1"].iloc[0] == 0
        # prev_2 first two rows default is -1
        assert df["prev_2"].iloc[0] == -1
        assert df["prev_2"].iloc[1] == -1


class TestShiftDefaultEdgeCases:
    """Edge case tests for shift with default."""

    def test_shift_default_single_row(self):
        """shift with default on single-row table."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("value\n")
        csv.write("42\n")
        csv.close()

        try:
            t = LTSeq.read_csv(csv.name)
            result = t.derive(lambda r: {"prev": r.value.shift(1, default=0)})
            df = result.to_pandas()

            assert len(df) == 1
            assert df["prev"].iloc[0] == 0
        finally:
            os.unlink(csv.name)

    def test_shift_default_empty_table(self):
        """shift with default on empty table should not error."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("value\n")
        csv.close()

        try:
            t = LTSeq.read_csv(csv.name)
            result = t.derive(lambda r: {"prev": r.value.shift(1, default=0)})
            df = result.to_pandas()

            assert len(df) == 0
        finally:
            os.unlink(csv.name)

    def test_shift_zero_with_default(self):
        """shift(0, default=...) should return original values (default not used)."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("value\n")
        csv.write("100\n")
        csv.write("200\n")
        csv.close()

        try:
            t = LTSeq.read_csv(csv.name)
            result = t.derive(lambda r: {"same": r.value.shift(0, default=999)})
            df = result.to_pandas()

            # shift(0) returns the same value, default should not be used
            assert df["same"].iloc[0] == 100
            assert df["same"].iloc[1] == 200
        finally:
            os.unlink(csv.name)

    def test_shift_default_float_value(self):
        """shift with float default value."""
        csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        csv.write("value\n")
        csv.write("1.5\n")
        csv.write("2.5\n")
        csv.close()

        try:
            t = LTSeq.read_csv(csv.name)
            result = t.derive(lambda r: {"prev": r.value.shift(1, default=0.0)})
            df = result.to_pandas()

            assert df["prev"].iloc[0] == 0.0
            assert df["prev"].iloc[1] == 1.5
        finally:
            os.unlink(csv.name)
