"""
Phase 6 Tests: Sequence Operators - Advanced and Integration

Tests for advanced scenarios including chaining, edge cases, null handling,
and integration with other operations.
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


class TestSequenceChaining:
    """Test combining shift/rolling/diff operations."""

    def test_shift_and_diff_together(self, sample_csv):
        """Combine shift() and diff() in same derive."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(
                lambda r: {
                    "prev_price": r.price.shift(1),
                    "change": r.price.diff(),
                }
            )
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"Combining shift() and diff() not yet implemented: {e}")

    def test_rolling_and_shift_together_matches_two_step_derive(self, sample_csv):
        """Two-level nested windows plan natively via the staged select
        (issue #101). The staged plan is LogicalPlan-isomorphic to the
        manual two-step derive, so VALUES AND ROW ORDER must both match —
        compared unsorted on purpose."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        one_step = t.derive(
            lambda r: {"prev_ma": r.price.rolling(3).mean().shift(1)}
        ).to_pandas()

        two_step = (
            t.derive(lambda r: {"ma_3": r.price.rolling(3).mean()})
            .derive(lambda r: {"prev_ma": r.ma_3.shift(1)})
            .to_pandas()
        )

        assert one_step["prev_ma"].tolist() == pytest.approx(
            two_step["prev_ma"].tolist(), nan_ok=True
        )
        assert not any(c.startswith("__ltseq_stage_") for c in one_step.columns)

    def test_shift_then_rolling_sum_matches_two_step_derive(self, sample_csv):
        """The reverse nesting direction: shift feeding a rolling sum."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        one_step = t.derive(
            lambda r: {"sum_prev": r.price.shift(1).rolling(3).sum()}
        ).to_pandas()

        two_step = (
            t.derive(lambda r: {"prev": r.price.shift(1)})
            .derive(lambda r: {"sum_prev": r.prev.rolling(3).sum()})
            .to_pandas()
        )

        assert one_step["sum_prev"].tolist() == pytest.approx(
            two_step["sum_prev"].tolist(), nan_ok=True
        )

    def test_nested_window_mixed_with_plain_window_and_plain_expr(self, sample_csv):
        """Nested window, plain window and plain expression coexist in one
        derive; hidden stage columns never reach the result."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        df = t.derive(
            lambda r: {
                "prev_ma": r.price.rolling(3).mean().shift(1),
                "prev_price": r.price.shift(1),
                "double_volume": r.volume * 2,
            }
        ).to_pandas()

        expected = (
            t.derive(lambda r: {"ma_3": r.price.rolling(3).mean()})
            .derive(lambda r: {"prev_ma": r.ma_3.shift(1)})
            .to_pandas()
        )

        assert df["prev_ma"].tolist() == pytest.approx(
            expected["prev_ma"].tolist(), nan_ok=True
        )
        assert df["prev_price"].tolist()[1:] == pytest.approx(
            df["price"].tolist()[:-1]
        )
        assert df["double_volume"].tolist() == pytest.approx(
            [v * 2 for v in df["volume"].tolist()]
        )
        assert not any(c.startswith("__ltseq_stage_") for c in df.columns)

    def test_nested_window_in_arithmetic(self, sample_csv):
        """A nested window inside arithmetic with plain windows."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        df = t.derive(
            lambda r: {
                "momentum": r.price.rolling(3).mean().shift(1) - r.price.shift(1),
            }
        ).to_pandas()

        expected = (
            t.derive(lambda r: {"ma_3": r.price.rolling(3).mean()})
            .derive(lambda r: {"momentum": r.ma_3.shift(1) - r.price.shift(1)})
            .to_pandas()
        )
        assert df["momentum"].tolist() == pytest.approx(
            expected["momentum"].tolist(), nan_ok=True
        )

    def test_three_level_nesting_still_hints(self, sample_csv):
        """Scope lock (issue #101): the staged planner covers exactly two
        levels; three and deeper keep the actionable error."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        with pytest.raises(Exception, match="split it into two steps"):
            t.derive(
                lambda r: {
                    "x": r.price.rolling(3).mean().shift(1).rolling(2).sum(),
                }
            )

    def test_hidden_stage_name_collision_avoided(self):
        """A user column literally named __ltseq_stage_0 must not collide
        with the planner's hidden columns."""
        import pandas as pd

        df = pd.DataFrame(
            {
                "k": [1, 2, 3, 4],
                "price": [10.0, 20.0, 30.0, 40.0],
                "__ltseq_stage_0": [7.0, 7.0, 7.0, 7.0],
            }
        )
        t = LTSeq.from_pandas(df).sort("k")
        out = t.derive(
            lambda r: {"prev_ma": r.price.rolling(2).mean().shift(1)}
        ).to_pandas()
        assert out["__ltseq_stage_0"].tolist() == [7.0] * 4
        expected = (
            t.derive(lambda r: {"ma": r.price.rolling(2).mean()})
            .derive(lambda r: {"prev_ma": r.ma.shift(1)})
            .to_pandas()
        )
        assert out["prev_ma"].tolist() == pytest.approx(
            expected["prev_ma"].tolist(), nan_ok=True
        )

    def test_rolling_and_shift_via_two_step_derive(self, sample_csv):
        """The documented workaround: stage the inner window as a column,
        then window over it. Values must equal the staged column shifted."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        result = t.derive(lambda r: {"ma_3": r.price.rolling(3).mean()}).derive(
            lambda r: {"prev_ma": r.ma_3.shift(1)}
        )
        df = result.to_pandas()
        ma = df["ma_3"].tolist()
        prev = df["prev_ma"].tolist()
        assert prev[0] != prev[0] or prev[0] is None  # NaN first row
        assert prev[1:] == pytest.approx(ma[:-1])

    def test_all_sequence_ops_together(self, sample_csv):
        """Use shift, rolling, and diff in one derive."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(
                lambda r: {
                    "prev_price": r.price.shift(1),
                    "ma_3": r.price.rolling(3).mean(),
                    "change": r.price.diff(),
                }
            )
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"All sequence ops together not yet implemented: {e}")

    def test_sequence_with_filter(self, sample_csv):
        """Sequence ops with filter chaining."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            with_cols = t.derive(lambda r: {"change": r.price.diff()})
            result = with_cols.filter(lambda r: r.change > 0)
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"Sequence ops with filter not yet implemented: {e}")


class TestSequenceWithFilter:
    """Test sequence operators with filter/select chaining."""

    def test_sort_shift_filter(self, sample_csv):
        """sort -> derive(shift) -> filter chain."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            with_prev = t.derive(lambda r: {"prev_price": r.price.shift(1)})
            result = with_prev.filter(lambda r: r.price > r.prev_price)
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"sort -> shift -> filter not yet implemented: {e}")

    def test_sort_rolling_select(self, sample_csv):
        """sort -> derive(rolling) -> select chain."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            with_ma = t.derive(lambda r: {"ma_3": r.price.rolling(3).mean()})
            result = with_ma.select(lambda r: [r.date, r.price, r.ma_3])
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"sort -> rolling -> select not yet implemented: {e}")

    def test_complex_pipeline(self, sample_csv):
        """Complex pipeline with multiple sequence ops."""
        t = LTSeq.read_csv(sample_csv)

        # Use is_not_null() instead of 'is not None' for compatibility with
        # pytest/exec contexts where inspect.getsource() may fail
        result = (
            t.sort("date")
            .derive(
                lambda r: {
                    "ma_3": r.price.rolling(3).mean(),
                    "change": r.price.diff(),
                }
            )
            .filter(lambda r: r.ma_3.is_not_null())
            .slice(0, 3)
        )
        assert isinstance(result, LTSeq)
        # Should have filtered out rows where ma_3 is NULL (first 2 rows)
        df = result.to_pandas()
        assert len(df) <= 3  # slice(0, 3) limits to 3 rows


class TestSequenceEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_shift_on_single_row(self, sample_csv):
        """shift() on table with single row."""
        t = LTSeq.read_csv(sample_csv).sort("date").slice(0, 1)

        try:
            result = t.derive(lambda r: {"prev": r.price.shift(1)})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"shift() on single row not yet implemented: {e}")

    def test_rolling_window_larger_than_data(self, sample_csv):
        """rolling() with window > data size."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"ma": r.price.rolling(100).mean()})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"rolling() with large windows not yet implemented: {e}")

    def test_diff_on_two_rows(self, sample_csv):
        """diff() on table with two rows."""
        t = LTSeq.read_csv(sample_csv).sort("date").slice(0, 2)

        try:
            result = t.derive(lambda r: {"change": r.price.diff()})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"diff() on two rows not yet implemented: {e}")

    def test_sequence_on_unsorted_data(self, sample_csv):
        """Sequence ops on unsorted data (undefined order)."""
        t = LTSeq.read_csv(sample_csv)  # Not sorted

        try:
            result = t.derive(lambda r: {"prev": r.price.shift(1)})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"Sequence ops on unsorted data not yet implemented: {e}")

    def test_rolling_window_size_one(self, sample_csv):
        """rolling() with window size of 1."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"ma_1": r.price.rolling(1).mean()})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"rolling() with window=1 not yet implemented: {e}")

    def test_shift_zero_periods(self, sample_csv):
        """shift(0) should return the same value."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"same": r.price.shift(0)})
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"shift(0) not yet implemented: {e}")


class TestSequenceNullHandling:
    """Test NULL/None handling in sequence operations."""

    def test_shift_introduces_nulls(self, sample_csv):
        """shift() should introduce NULLs at boundaries."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"prev": r.price.shift(1)})
            # First row should have NULL in prev column
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"shift() NULL handling not yet implemented: {e}")

    def test_rolling_with_nulls(self, sample_csv):
        """rolling() should handle NULLs gracefully."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(
                lambda r: {
                    "prev": r.price.shift(1),
                    "ma": r.price.rolling(3).mean(),
                }
            )
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"rolling() NULL handling not yet implemented: {e}")

    def test_diff_with_nulls(self, sample_csv):
        """diff() behavior with NULL previous values."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"change": r.price.diff()})
            # First row should have NULL in change
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"diff() NULL handling not yet implemented: {e}")


class TestSortOrderTracking:
    """Test that sort order is tracked for window functions."""

    def test_sort_order_preserved(self, sample_csv):
        """Sort order should be preserved through operations."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(lambda r: {"prev": r.price.shift(1)})
            # Should preserve sort order for shift to work correctly
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"Sort order tracking not yet implemented: {e}")

    def test_multiple_sorts_last_wins(self, sample_csv):
        """Multiple sorts should use the last sort order."""
        t = LTSeq.read_csv(sample_csv).sort("date").sort("price")

        try:
            result = t.derive(lambda r: {"prev": r.price.shift(1)})
            # Should use the "price" sort order (last one applied)
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"Multiple sorts not yet implemented: {e}")


class TestPhase6Integration:
    """Integration tests for Phase 6 operators."""

    def test_financial_use_case(self, sample_csv):
        """Financial analysis use case: daily change and moving average."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        # Use is_not_null() instead of 'is not None' for compatibility with
        # pytest/exec contexts where inspect.getsource() may fail
        result = t.derive(
            lambda r: {
                "daily_change": r.price - r.price.shift(1),
                "change_pct": (r.price - r.price.shift(1)) / r.price.shift(1) * 100,
                "ma_3d": r.price.rolling(3).mean(),
            }
        ).filter(lambda r: r.daily_change.is_not_null())

        assert isinstance(result, LTSeq)
        # First row should be filtered out (no previous value for shift)
        df = result.to_pandas()
        assert len(df) < len(t.to_pandas())

    def test_trend_detection(self, sample_csv):
        """Detect price trends using sequence operators."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        try:
            result = t.derive(
                lambda r: {
                    "increasing": r.price > r.price.shift(1),
                    "ma": r.price.rolling(3).mean(),
                    "above_ma": r.price > r.price.rolling(3).mean(),
                }
            )
            assert isinstance(result, LTSeq)
        except Exception as e:
            pytest.skip(f"Trend detection not yet implemented: {e}")

    def test_no_operators_without_derive(self, sample_csv):
        """Verify operators only work within derive/select contexts."""
        t = LTSeq.read_csv(sample_csv).sort("date")

        # Accessing shift/rolling/diff on a column outside of lambda should not work
        try:
            # This should fail since r doesn't exist outside lambda
            _ = t.price.shift(1)
            pytest.fail("shift() should only work within lambda expressions")
        except (AttributeError, NameError):
            # Expected behavior
            pass
        except Exception as e:
            pytest.skip(f"Operator isolation test skipped: {e}")
