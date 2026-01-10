"""Tests for asof_join() functionality.

Tests the asof_join() method which performs as-of (nearest time) joins
commonly used in time-series and financial data processing.
"""

import pytest
import pandas as pd
import numpy as np
from ltseq import LTSeq


# Test data files
TRADES_CSV = "py-ltseq/tests/test_data/trades.csv"
QUOTES_CSV = "py-ltseq/tests/test_data/quotes.csv"


class TestAsofJoinBasic:
    """Basic asof_join() functionality tests."""

    def test_asof_join_creates_ltseq(self):
        """asof_join() should return an LTSeq instance."""
        trades = LTSeq.read_csv(TRADES_CSV)
        quotes = LTSeq.read_csv(QUOTES_CSV)
        result = trades.asof_join(quotes, on=lambda t, q: t.time >= q.time)
        assert isinstance(result, LTSeq)

    def test_asof_join_backward_default(self):
        """asof_join() should use backward direction by default."""
        trades = LTSeq.read_csv(TRADES_CSV)
        quotes = LTSeq.read_csv(QUOTES_CSV)

        # Trade at time=1000 should match quote at time=998 (backward)
        result = trades.asof_join(quotes, on=lambda t, q: t.time >= q.time)
        df = result.to_pandas()

        # First trade (time=1000) should match quote at time=998
        first_trade = df[df["time"] == 1000].iloc[0]
        assert first_trade["_other_time"] == 998

    def test_asof_join_forward_direction(self):
        """asof_join() should support forward direction."""
        trades = LTSeq.read_csv(TRADES_CSV)
        quotes = LTSeq.read_csv(QUOTES_CSV)

        result = trades.asof_join(
            quotes, on=lambda t, q: t.time >= q.time, direction="forward"
        )
        df = result.to_pandas()

        # Trade at time=1000 should match quote at time=1002 (forward)
        first_trade = df[df["time"] == 1000].iloc[0]
        assert first_trade["_other_time"] == 1002

    def test_asof_join_nearest_direction(self):
        """asof_join() should support nearest direction."""
        trades = LTSeq.read_csv(TRADES_CSV)
        quotes = LTSeq.read_csv(QUOTES_CSV)

        result = trades.asof_join(
            quotes, on=lambda t, q: t.time >= q.time, direction="nearest"
        )
        df = result.to_pandas()

        # Trade at time=1000 should match nearest quote
        # quote at 998 is 2 away, quote at 1002 is 2 away -> backward bias picks 998
        first_trade = df[df["time"] == 1000].iloc[0]
        assert first_trade["_other_time"] == 998

    def test_asof_join_preserves_all_left_rows(self):
        """asof_join() should preserve all rows from left table (LEFT JOIN semantics)."""
        trades = LTSeq.read_csv(TRADES_CSV)
        quotes = LTSeq.read_csv(QUOTES_CSV)

        result = trades.asof_join(quotes, on=lambda t, q: t.time >= q.time)

        # Result should have same number of rows as left table
        assert len(result) == len(trades)


class TestAsofJoinSchema:
    """Tests for asof_join() schema handling."""

    def test_asof_join_result_has_prefixed_columns(self):
        """asof_join() result should have right columns prefixed with _other_."""
        trades = LTSeq.read_csv(TRADES_CSV)
        quotes = LTSeq.read_csv(QUOTES_CSV)

        result = trades.asof_join(quotes, on=lambda t, q: t.time >= q.time)
        df = result.to_pandas()

        # Left columns should be present without prefix
        assert "time" in df.columns
        assert "symbol" in df.columns
        assert "price" in df.columns
        assert "quantity" in df.columns

        # Right columns should be prefixed with _other_
        assert "_other_time" in df.columns
        assert "_other_symbol" in df.columns
        assert "_other_bid" in df.columns
        assert "_other_ask" in df.columns

    def test_asof_join_result_is_chainable(self):
        """asof_join() result should be chainable with other operations."""
        trades = LTSeq.read_csv(TRADES_CSV)
        quotes = LTSeq.read_csv(QUOTES_CSV)

        result = trades.asof_join(quotes, on=lambda t, q: t.time >= q.time)

        # Should be able to chain select
        selected = result.select("time", "price", "_other_bid", "_other_ask")
        assert isinstance(selected, LTSeq)

        df = selected.to_pandas()
        assert list(df.columns) == ["time", "price", "_other_bid", "_other_ask"]


class TestAsofJoinEdgeCases:
    """Tests for asof_join() edge cases."""

    def test_asof_join_no_match_returns_null(self):
        """asof_join() should return NULL for unmatched rows."""
        # Create trades with time before any quotes
        trades_data = "time,symbol,price\n500,AAPL,100.0"
        quotes_data = "time,bid,ask\n1000,150.0,151.0"

        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            trades_path = os.path.join(tmpdir, "trades.csv")
            quotes_path = os.path.join(tmpdir, "quotes.csv")

            with open(trades_path, "w") as f:
                f.write(trades_data)
            with open(quotes_path, "w") as f:
                f.write(quotes_data)

            trades = LTSeq.read_csv(trades_path)
            quotes = LTSeq.read_csv(quotes_path)

            # Backward: no quote at or before time=500
            result = trades.asof_join(
                quotes, on=lambda t, q: t.time >= q.time, direction="backward"
            )
            df = result.to_pandas()

            # Should have NULL for right columns
            assert pd.isna(df.iloc[0]["_other_time"])
            assert pd.isna(df.iloc[0]["_other_bid"])

    def test_asof_join_exact_match(self):
        """asof_join() should handle exact time matches."""
        trades_data = "time,price\n1000,100.0"
        quotes_data = "time,bid\n1000,99.0"

        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            trades_path = os.path.join(tmpdir, "trades.csv")
            quotes_path = os.path.join(tmpdir, "quotes.csv")

            with open(trades_path, "w") as f:
                f.write(trades_data)
            with open(quotes_path, "w") as f:
                f.write(quotes_data)

            trades = LTSeq.read_csv(trades_path)
            quotes = LTSeq.read_csv(quotes_path)

            result = trades.asof_join(
                quotes, on=lambda t, q: t.time >= q.time, direction="backward"
            )
            df = result.to_pandas()

            # Exact match at time=1000
            assert df.iloc[0]["_other_time"] == 1000
            assert df.iloc[0]["_other_bid"] == 99.0

    def test_asof_join_is_sorted_true(self):
        """asof_join() should skip sorting when is_sorted=True."""
        trades = LTSeq.read_csv(TRADES_CSV)
        quotes = LTSeq.read_csv(QUOTES_CSV)

        # Pre-sort the tables
        trades_sorted = trades.sort("time")
        quotes_sorted = quotes.sort("time")

        # Use is_sorted=True
        result = trades_sorted.asof_join(
            quotes_sorted, on=lambda t, q: t.time >= q.time, is_sorted=True
        )

        assert isinstance(result, LTSeq)
        assert len(result) == len(trades)

    def test_asof_join_auto_sorts_when_unsorted(self):
        """asof_join() should auto-sort tables when is_sorted=False."""
        # Create unsorted data
        trades_data = "time,price\n1005,150.0\n1000,149.0\n1010,151.0"
        quotes_data = "time,bid\n1002,148.0\n998,147.0\n1008,149.0"

        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            trades_path = os.path.join(tmpdir, "trades.csv")
            quotes_path = os.path.join(tmpdir, "quotes.csv")

            with open(trades_path, "w") as f:
                f.write(trades_data)
            with open(quotes_path, "w") as f:
                f.write(quotes_data)

            trades = LTSeq.read_csv(trades_path)
            quotes = LTSeq.read_csv(quotes_path)

            # Should auto-sort and still work correctly
            result = trades.asof_join(
                quotes,
                on=lambda t, q: t.time >= q.time,
                is_sorted=False,  # default
            )

            assert isinstance(result, LTSeq)
            assert len(result) == 3


class TestAsofJoinErrors:
    """Tests for asof_join() error handling."""

    def test_asof_join_missing_schema_raises_error(self):
        """asof_join() should raise error if schema not initialized."""
        t1 = LTSeq()
        t2 = LTSeq.read_csv(TRADES_CSV)
        with pytest.raises(ValueError, match="Schema not initialized"):
            t1.asof_join(t2, on=lambda a, b: a.time >= b.time)

    def test_asof_join_other_missing_schema_raises_error(self):
        """asof_join() should raise error if other table schema not initialized."""
        t1 = LTSeq.read_csv(TRADES_CSV)
        t2 = LTSeq()
        with pytest.raises(ValueError, match="Other table schema not initialized"):
            t1.asof_join(t2, on=lambda a, b: a.time >= b.time)

    def test_asof_join_invalid_direction_raises_error(self):
        """asof_join() should raise error for invalid direction."""
        trades = LTSeq.read_csv(TRADES_CSV)
        quotes = LTSeq.read_csv(QUOTES_CSV)
        with pytest.raises(ValueError, match="Invalid direction"):
            trades.asof_join(
                quotes, on=lambda t, q: t.time >= q.time, direction="invalid"
            )

    def test_asof_join_invalid_table_raises_error(self):
        """asof_join() should raise error if other is not LTSeq."""
        trades = LTSeq.read_csv(TRADES_CSV)
        with pytest.raises(TypeError, match="must be LTSeq"):
            trades.asof_join("not a table", on=lambda t, q: t.time >= q.time)

    def test_asof_join_invalid_condition_raises_error(self):
        """asof_join() should raise error for invalid join condition."""
        trades = LTSeq.read_csv(TRADES_CSV)
        quotes = LTSeq.read_csv(QUOTES_CSV)
        with pytest.raises(TypeError, match="Invalid asof join condition"):
            trades.asof_join(quotes, on=lambda t, q: t.nonexistent >= q.nonexistent)


class TestAsofJoinRegression:
    """Regression tests for asof_join()."""

    def test_asof_join_does_not_modify_source(self):
        """asof_join() should not modify source tables."""
        trades = LTSeq.read_csv(TRADES_CSV)
        quotes = LTSeq.read_csv(QUOTES_CSV)

        original_trades_len = len(trades)
        original_quotes_len = len(quotes)

        _ = trades.asof_join(quotes, on=lambda t, q: t.time >= q.time)

        assert len(trades) == original_trades_len
        assert len(quotes) == original_quotes_len

    def test_asof_join_multiple_calls(self):
        """Should be able to call asof_join() multiple times."""
        trades = LTSeq.read_csv(TRADES_CSV)
        quotes = LTSeq.read_csv(QUOTES_CSV)

        result1 = trades.asof_join(
            quotes, on=lambda t, q: t.time >= q.time, direction="backward"
        )
        result2 = trades.asof_join(
            quotes, on=lambda t, q: t.time >= q.time, direction="forward"
        )
        result3 = trades.asof_join(
            quotes, on=lambda t, q: t.time >= q.time, direction="nearest"
        )

        assert isinstance(result1, LTSeq)
        assert isinstance(result2, LTSeq)
        assert isinstance(result3, LTSeq)


class TestAsofJoinDirectionSemantics:
    """Tests for direction-specific semantics in asof_join()."""

    def test_backward_finds_largest_less_or_equal(self):
        """backward direction should find largest right.time <= left.time."""
        trades_data = "time,price\n1005,100.0"
        quotes_data = "time,bid\n1000,90.0\n1002,92.0\n1004,94.0\n1010,100.0"

        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            trades_path = os.path.join(tmpdir, "trades.csv")
            quotes_path = os.path.join(tmpdir, "quotes.csv")

            with open(trades_path, "w") as f:
                f.write(trades_data)
            with open(quotes_path, "w") as f:
                f.write(quotes_data)

            trades = LTSeq.read_csv(trades_path)
            quotes = LTSeq.read_csv(quotes_path)

            result = trades.asof_join(
                quotes, on=lambda t, q: t.time >= q.time, direction="backward"
            )
            df = result.to_pandas()

            # Trade at 1005 should match quote at 1004 (largest <= 1005)
            assert df.iloc[0]["_other_time"] == 1004
            assert df.iloc[0]["_other_bid"] == 94.0

    def test_forward_finds_smallest_greater_or_equal(self):
        """forward direction should find smallest right.time >= left.time."""
        trades_data = "time,price\n1005,100.0"
        quotes_data = "time,bid\n1000,90.0\n1002,92.0\n1004,94.0\n1010,100.0"

        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            trades_path = os.path.join(tmpdir, "trades.csv")
            quotes_path = os.path.join(tmpdir, "quotes.csv")

            with open(trades_path, "w") as f:
                f.write(trades_data)
            with open(quotes_path, "w") as f:
                f.write(quotes_data)

            trades = LTSeq.read_csv(trades_path)
            quotes = LTSeq.read_csv(quotes_path)

            result = trades.asof_join(
                quotes, on=lambda t, q: t.time >= q.time, direction="forward"
            )
            df = result.to_pandas()

            # Trade at 1005 should match quote at 1010 (smallest >= 1005)
            assert df.iloc[0]["_other_time"] == 1010
            assert df.iloc[0]["_other_bid"] == 100.0

    def test_nearest_picks_closer_with_backward_bias(self):
        """nearest direction should pick closer match with backward bias on ties."""
        # Test case where backward and forward are equidistant
        trades_data = "time,price\n1005,100.0"
        quotes_data = "time,bid\n1000,90.0\n1010,100.0"  # Both 5 away

        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            trades_path = os.path.join(tmpdir, "trades.csv")
            quotes_path = os.path.join(tmpdir, "quotes.csv")

            with open(trades_path, "w") as f:
                f.write(trades_data)
            with open(quotes_path, "w") as f:
                f.write(quotes_data)

            trades = LTSeq.read_csv(trades_path)
            quotes = LTSeq.read_csv(quotes_path)

            result = trades.asof_join(
                quotes, on=lambda t, q: t.time >= q.time, direction="nearest"
            )
            df = result.to_pandas()

            # Both are 5 away, backward bias should pick 1000
            assert df.iloc[0]["_other_time"] == 1000
            assert df.iloc[0]["_other_bid"] == 90.0

    def test_nearest_picks_actually_closer(self):
        """nearest direction should pick actually closer match regardless of direction."""
        trades_data = "time,price\n1005,100.0"
        quotes_data = "time,bid\n1000,90.0\n1006,96.0"  # 1006 is closer (1 away vs 5)

        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            trades_path = os.path.join(tmpdir, "trades.csv")
            quotes_path = os.path.join(tmpdir, "quotes.csv")

            with open(trades_path, "w") as f:
                f.write(trades_data)
            with open(quotes_path, "w") as f:
                f.write(quotes_data)

            trades = LTSeq.read_csv(trades_path)
            quotes = LTSeq.read_csv(quotes_path)

            result = trades.asof_join(
                quotes, on=lambda t, q: t.time >= q.time, direction="nearest"
            )
            df = result.to_pandas()

            # 1006 is only 1 away, should be picked
            assert df.iloc[0]["_other_time"] == 1006
            assert df.iloc[0]["_other_bid"] == 96.0
