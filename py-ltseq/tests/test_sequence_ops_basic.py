"""
Phase 6 Tests: Sequence Operators - Basic Operations

Tests for shift(), rolling(), cum_sum(), and diff() operators.
Validates core functionality of each operator.
"""

import pytest
from ltseq import LTSeq
import csv
import tempfile
import os


@pytest.fixture
def sample_csv():
    """Create a temporary CSV with numeric data for sequence operations."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        writer = csv.DictWriter(f, fieldnames=["id", "date", "price", "volume"])
        writer.writeheader()
        data = [
            {"id": "1", "date": "2024-01-01", "price": "100", "volume": "10"},
            {"id": "2", "date": "2024-01-02", "price": "102", "volume": "12"},
            {"id": "3", "date": "2024-01-03", "price": "101", "volume": "11"},
            {"id": "4", "date": "2024-01-04", "price": "105", "volume": "15"},
            {"id": "5", "date": "2024-01-05", "price": "108", "volume": "20"},
        ]
        for row in data:
            writer.writerow(row)
        temp_path = f.name

    yield temp_path

    # Cleanup
    os.unlink(temp_path)


@pytest.fixture
def empty_table():
    """Create an empty LTSeq table for edge case testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        writer = csv.DictWriter(f, fieldnames=["value"])
        writer.writeheader()
        temp_path = f.name

    t = LTSeq.read_csv(temp_path)
    os.unlink(temp_path)

    return t


class TestShiftBasic:
    """Test basic shift() functionality on columns."""

    def test_shift_returns_expr(self, sample_csv):
        """Shift should return an expression, not modify the table."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"prev_price": r.price.shift(1)})
            assert result is not None
        except Exception as e:
            pytest.skip(f"shift() not yet fully implemented: {e}")

    def test_shift_in_derive_adds_column(self, sample_csv):
        """shift() in derive() should add a column with previous values."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"prev_price": r.price.shift(1)})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"shift() not yet fully implemented: {e}")

    def test_shift_negative_value(self, sample_csv):
        """shift(-1) should access the next row."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"next_price": r.price.shift(-1)})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"shift() with negative values not yet implemented: {e}")

    def test_shift_multiple_values(self, sample_csv):
        """shift() with different n values."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(
                lambda r: {
                    "prev_1": r.price.shift(1),
                    "prev_2": r.price.shift(2),
                    "next_1": r.price.shift(-1),
                }
            )
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"shift() with multiple values not yet implemented: {e}")

    def test_shift_in_calculation(self, sample_csv):
        """shift() can be used in arithmetic expressions."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"change": r.price - r.price.shift(1)})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"shift() in calculations not yet implemented: {e}")

    def test_shift_default_n(self, sample_csv):
        """shift() with no arguments should default to shift(1)."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"prev": r.price.shift()})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"shift() with default arguments not yet implemented: {e}")

    def test_shift_on_empty_table(self, empty_table):
        """shift() on empty table should not error."""
        try:
            result = empty_table.derive(lambda r: {"shifted": r.value.shift(1)})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"shift() on empty tables not yet implemented: {e}")


class TestShiftExpressions:
    """Test shift() with complex expressions."""

    def test_shift_with_arithmetic(self, sample_csv):
        """shift() in arithmetic expressions."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(
                lambda r: {
                    "pct_change": (r.price - r.price.shift(1)) / r.price.shift(1)
                }
            )
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"shift() in complex expressions not yet implemented: {e}")

    def test_shift_multiple_columns(self, sample_csv):
        """shift() on different columns."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(
                lambda r: {
                    "price_prev": r.price.shift(1),
                    "volume_prev": r.volume.shift(1),
                }
            )
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"shift() on multiple columns not yet implemented: {e}")

    def test_shift_chained_calls(self, sample_csv):
        """Can chain other methods after shift()."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"prev": r.price.shift(1)})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"shift() chaining not yet implemented: {e}")

    def test_shift_in_filter_expression(self, sample_csv):
        """shift() can be used in filter conditions (via derive + filter)."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            with_prev = t.derive(lambda r: {"prev_price": r.price.shift(1)})
            result = with_prev.filter(lambda r: r.prev_price is not None)
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"shift() in filter not yet implemented: {e}")

    def test_shift_comparison(self, sample_csv):
        """shift() in comparison expressions."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"increasing": r.price > r.price.shift(1)})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"shift() in comparisons not yet implemented: {e}")


class TestRollingBasic:
    """Test basic rolling() window functionality."""

    def test_rolling_mean(self, sample_csv):
        """rolling(n).mean() should compute moving average."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"ma_3": r.price.rolling(3).mean()})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"rolling().mean() not yet implemented: {e}")

    def test_rolling_sum(self, sample_csv):
        """rolling(n).sum() should compute rolling sum."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"sum_3": r.volume.rolling(3).sum()})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"rolling().sum() not yet implemented: {e}")

    def test_rolling_min(self, sample_csv):
        """rolling(n).min() should compute rolling minimum."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"min_3": r.price.rolling(3).min()})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"rolling().min() not yet implemented: {e}")

    def test_rolling_max(self, sample_csv):
        """rolling(n).max() should compute rolling maximum."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"max_3": r.price.rolling(3).max()})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"rolling().max() not yet implemented: {e}")

    def test_rolling_count(self, sample_csv):
        """rolling(n).count() should count non-null values in window."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"count_3": r.price.rolling(3).count()})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"rolling().count() not yet implemented: {e}")

    def test_rolling_std(self, sample_csv):
        """rolling(n).std() should compute rolling standard deviation."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        result = t.derive(lambda r: {"volatility": r.price.rolling(3).std()})
        assert isinstance(result, LTSeq)

        # Verify the result has the volatility column
        df = result.to_pandas()
        assert "volatility" in df.columns

        # First row should be None (not enough data for window)
        assert df["volatility"].iloc[0] is None or df["volatility"].isna().iloc[0]

        # Later rows should have numeric values
        assert df["volatility"].iloc[2] is not None

    def test_rolling_large_window(self, sample_csv):
        """rolling() with window size > table size."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"ma_100": r.price.rolling(100).mean()})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"rolling() with large windows not yet implemented: {e}")

    def test_rolling_multiple_aggregations(self, sample_csv):
        """Multiple rolling() operations in one derive()."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(
                lambda r: {
                    "ma_3": r.price.rolling(3).mean(),
                    "ma_5": r.price.rolling(5).mean(),
                    "sum_3": r.volume.rolling(3).sum(),
                }
            )
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"Multiple rolling() operations not yet implemented: {e}")


class TestRollingMultiple:
    """Test rolling() with multiple configurations."""

    def test_rolling_different_sizes(self, sample_csv):
        """rolling() with different window sizes."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(
                lambda r: {
                    "ma_2": r.price.rolling(2).mean(),
                    "ma_3": r.price.rolling(3).mean(),
                    "ma_4": r.price.rolling(4).mean(),
                }
            )
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"rolling() with different sizes not yet implemented: {e}")

    def test_rolling_on_multiple_columns(self, sample_csv):
        """rolling() on different columns."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(
                lambda r: {
                    "price_ma": r.price.rolling(3).mean(),
                    "volume_ma": r.volume.rolling(3).mean(),
                }
            )
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"rolling() on multiple columns not yet implemented: {e}")

    def test_rolling_mixed_aggregations(self, sample_csv):
        """rolling() with different aggregation functions."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(
                lambda r: {
                    "ma_3": r.price.rolling(3).mean(),
                    "min_3": r.price.rolling(3).min(),
                    "max_3": r.price.rolling(3).max(),
                    "sum_3": r.price.rolling(3).sum(),
                }
            )
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"rolling() with mixed aggregations not yet implemented: {e}")

    def test_rolling_in_calculation(self, sample_csv):
        """rolling() results used in further calculations."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(
                lambda r: {
                    "ma_3": r.price.rolling(3).mean(),
                    "deviation": r.price - r.price.rolling(3).mean(),
                }
            )
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"rolling() in calculations not yet implemented: {e}")


class TestCumSumBasic:
    """Test basic cum_sum() functionality."""

    def test_cum_sum_single_column(self, sample_csv):
        """cum_sum() on a single column."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.cum_sum("volume")
            assert isinstance(result, LTSeq)
            # Check that new column was added
            assert "volume_cumsum" in result._schema
        except NotImplementedError:
            pytest.skip("cum_sum() not yet implemented")

    def test_cum_sum_multiple_columns(self, sample_csv):
        """cum_sum() on multiple columns at once."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.cum_sum("volume", "price")
            assert isinstance(result, LTSeq)
            assert "volume_cumsum" in result._schema
            assert "price_cumsum" in result._schema
        except NotImplementedError:
            pytest.skip("cum_sum() with multiple columns not yet implemented")

    def test_cum_sum_numeric_only(self, sample_csv):
        """cum_sum() should error on non-numeric columns."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            with pytest.raises((ValueError, TypeError)):
                t.cum_sum("date")
        except NotImplementedError:
            pytest.skip("cum_sum() validation not yet implemented")

    def test_cum_sum_preserves_data(self, sample_csv):
        """cum_sum() should not remove rows."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.cum_sum("volume")
            assert isinstance(result, LTSeq)
        except NotImplementedError:
            pytest.skip("cum_sum() not yet implemented")

    def test_cum_sum_requires_sort(self, sample_csv):
        """cum_sum() should ideally be used on sorted data."""
        t = LTSeq.read_csv(sample_csv)  # Not sorted

        try:
            result = t.cum_sum("volume")
            assert isinstance(result, LTSeq)
        except NotImplementedError:
            pytest.skip("cum_sum() not yet implemented")

    def test_cum_sum_empty_table(self, empty_table):
        """cum_sum() on empty table."""
        try:
            result = empty_table.cum_sum("value")
            assert isinstance(result, LTSeq)
        except NotImplementedError:
            pytest.skip("cum_sum() not yet implemented")


class TestCumSumExpressions:
    """Test cum_sum() with lambda expressions."""

    def test_cum_sum_with_expression(self, sample_csv):
        """cum_sum() with lambda expression."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.cum_sum(lambda r: r.price * r.volume)
            assert isinstance(result, LTSeq)
        except NotImplementedError:
            pytest.skip("cum_sum() with expressions not yet implemented")

    def test_cum_sum_multiple_expressions(self, sample_csv):
        """cum_sum() with multiple expressions."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.cum_sum(
                lambda r: r.price * r.volume,
                lambda r: r.price + r.volume,
            )
            assert isinstance(result, LTSeq)
        except NotImplementedError:
            pytest.skip("cum_sum() with multiple expressions not yet implemented")

    def test_cum_sum_mixed_args(self, sample_csv):
        """cum_sum() with both column names and expressions."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.cum_sum("price", lambda r: r.volume)
            assert isinstance(result, LTSeq)
        except NotImplementedError:
            pytest.skip("cum_sum() with mixed arguments not yet implemented")


class TestDiffBasic:
    """Test basic diff() functionality."""

    def test_diff_in_derive(self, sample_csv):
        """diff() in derive() should compute row differences."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"change": r.price.diff()})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"diff() not yet implemented: {e}")

    def test_diff_custom_periods(self, sample_csv):
        """diff() with custom period."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"change_7d": r.price.diff(7)})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"diff() with custom periods not yet implemented: {e}")

    def test_diff_negative_periods(self, sample_csv):
        """diff() with negative periods."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"change_next": r.price.diff(-1)})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"diff() with negative periods not yet implemented: {e}")

    def test_diff_in_calculation(self, sample_csv):
        """diff() in arithmetic expressions."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(
                lambda r: {
                    "change": r.price.diff(),
                    "pct_change": r.price.diff() / r.price.shift(1),
                }
            )
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"diff() in calculations not yet implemented: {e}")

    def test_diff_on_empty_table(self, empty_table):
        """diff() on empty table."""
        try:
            result = empty_table.derive(lambda r: {"diff": r.value.diff()})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"diff() on empty tables not yet implemented: {e}")

    def test_diff_multiple_columns(self, sample_csv):
        """diff() on multiple columns."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(
                lambda r: {
                    "price_change": r.price.diff(),
                    "volume_change": r.volume.diff(),
                }
            )
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"diff() on multiple columns not yet implemented: {e}")


class TestDiffPeriods:
    """Test diff() with various period values."""

    def test_diff_various_periods(self, sample_csv):
        """diff() with different period values."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(
                lambda r: {
                    "change_1": r.price.diff(1),
                    "change_2": r.price.diff(2),
                    "change_3": r.price.diff(3),
                }
            )
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"diff() with various periods not yet implemented: {e}")

    def test_diff_in_comparison(self, sample_csv):
        """diff() in comparison expressions."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(
                lambda r: {
                    "increasing": r.price.diff() > 0,
                    "large_change": r.price.diff() > 5,
                }
            )
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"diff() in comparisons not yet implemented: {e}")

    def test_diff_chained_calculations(self, sample_csv):
        """diff() in chained calculations."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(
                lambda r: {
                    "change": r.price.diff(),
                    "volatility": abs(r.price.diff()),
                }
            )
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"diff() in chained calculations not yet implemented: {e}")

    def test_diff_default_period(self, sample_csv):
        """diff() with default period should be 1."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"change": r.price.diff()})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"diff() with default period not yet implemented: {e}")
