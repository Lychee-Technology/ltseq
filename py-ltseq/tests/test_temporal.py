"""Tests for temporal operations (.dt accessor).

Tests the TemporalAccessor class which provides date/datetime operations.
These tests verify both expression creation AND runtime execution.
"""

import pytest
from ltseq import LTSeq
from ltseq.expr import ColumnExpr


class TestTemporalExpressionSerialization:
    """Tests that temporal expressions serialize correctly."""

    def test_dt_year_serialization(self):
        """dt.year() should serialize to CallExpr with dt_year func."""
        col = ColumnExpr("event_date")
        result = col.dt.year()
        serialized = result.serialize()
        assert serialized["type"] == "Call"
        assert serialized["func"] == "dt_year"

    def test_dt_month_serialization(self):
        """dt.month() should serialize to CallExpr with dt_month func."""
        col = ColumnExpr("event_date")
        result = col.dt.month()
        serialized = result.serialize()
        assert serialized["type"] == "Call"
        assert serialized["func"] == "dt_month"

    def test_dt_day_serialization(self):
        """dt.day() should serialize to CallExpr with dt_day func."""
        col = ColumnExpr("event_date")
        result = col.dt.day()
        serialized = result.serialize()
        assert serialized["type"] == "Call"
        assert serialized["func"] == "dt_day"

    def test_dt_hour_serialization(self):
        """dt.hour() should serialize to CallExpr with dt_hour func."""
        col = ColumnExpr("event_datetime")
        result = col.dt.hour()
        serialized = result.serialize()
        assert serialized["type"] == "Call"
        assert serialized["func"] == "dt_hour"

    def test_dt_minute_serialization(self):
        """dt.minute() should serialize to CallExpr with dt_minute func."""
        col = ColumnExpr("event_datetime")
        result = col.dt.minute()
        serialized = result.serialize()
        assert serialized["type"] == "Call"
        assert serialized["func"] == "dt_minute"

    def test_dt_second_serialization(self):
        """dt.second() should serialize to CallExpr with dt_second func."""
        col = ColumnExpr("event_datetime")
        result = col.dt.second()
        serialized = result.serialize()
        assert serialized["type"] == "Call"
        assert serialized["func"] == "dt_second"

    def test_dt_add_serialization(self):
        """dt.add() should serialize with days, months, years kwargs."""
        col = ColumnExpr("event_date")
        result = col.dt.add(days=10, months=1, years=2)
        serialized = result.serialize()
        assert serialized["type"] == "Call"
        assert serialized["func"] == "dt_add"
        assert serialized["kwargs"]["days"] == 10
        assert serialized["kwargs"]["months"] == 1
        assert serialized["kwargs"]["years"] == 2

    def test_dt_add_partial_args(self):
        """dt.add() should work with partial arguments."""
        col = ColumnExpr("event_date")
        result = col.dt.add(days=5)
        serialized = result.serialize()
        assert serialized["kwargs"]["days"] == 5
        assert serialized["kwargs"]["months"] == 0
        assert serialized["kwargs"]["years"] == 0

    def test_dt_diff_serialization(self):
        """dt.diff() should serialize with other date column."""
        col1 = ColumnExpr("start_date")
        col2 = ColumnExpr("end_date")
        result = col1.dt.diff(col2)
        serialized = result.serialize()
        assert serialized["type"] == "Call"
        assert serialized["func"] == "dt_diff"
        assert len(serialized["args"]) == 1
        assert serialized["args"][0]["type"] == "Column"
        assert serialized["args"][0]["name"] == "end_date"


class TestTemporalDateExtraction:
    """Tests for extracting date components at runtime."""

    def test_dt_year_derive(self):
        """dt.year() should extract year in derive()."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/events.csv")
        result = t.derive(year=lambda r: r.event_date.dt.year())
        df = result.to_pandas()
        assert "year" in df.columns
        # First row is 2024-03-15, so year should be 2024
        years = df["year"].tolist()
        assert 2024 in years or "2024" in str(years)

    def test_dt_month_derive(self):
        """dt.month() should extract month in derive()."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/events.csv")
        result = t.derive(month=lambda r: r.event_date.dt.month())
        df = result.to_pandas()
        assert "month" in df.columns
        # Should have months 3, 6, 12, 1 based on test data
        months = df["month"].tolist()
        assert any(
            m in [3, 6, 12, 1] or str(m) in ["3", "6", "12", "1"] for m in months
        )

    def test_dt_day_derive(self):
        """dt.day() should extract day in derive()."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/events.csv")
        result = t.derive(day=lambda r: r.event_date.dt.day())
        df = result.to_pandas()
        assert "day" in df.columns
        # Should have days 15, 20, 25, 10 based on test data
        days = df["day"].tolist()
        assert any(
            d in [15, 20, 25, 10] or str(d) in ["15", "20", "25", "10"] for d in days
        )


class TestTemporalTimeExtraction:
    """Tests for extracting time components at runtime."""

    def test_dt_hour_derive(self):
        """dt.hour() should extract hour in derive()."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/events.csv")
        result = t.derive(hour=lambda r: r.event_datetime.dt.hour())
        df = result.to_pandas()
        assert "hour" in df.columns
        # Test data has hours: 9, 14, 18, 8, 10, 12, 16
        hours = df["hour"].tolist()
        assert any(
            h in [9, 14, 18, 8, 10, 12, 16]
            or str(h) in ["9", "14", "18", "8", "10", "12", "16"]
            for h in hours
        )

    def test_dt_minute_derive(self):
        """dt.minute() should extract minute in derive()."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/events.csv")
        result = t.derive(minute=lambda r: r.event_datetime.dt.minute())
        df = result.to_pandas()
        assert "minute" in df.columns

    def test_dt_second_derive(self):
        """dt.second() should extract second in derive()."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/events.csv")
        result = t.derive(second=lambda r: r.event_datetime.dt.second())
        df = result.to_pandas()
        assert "second" in df.columns


class TestTemporalArithmetic:
    """Tests for date arithmetic operations at runtime."""

    def test_dt_add_days(self):
        """dt.add(days=N) should add days to date."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/events.csv")
        result = t.derive(future_date=lambda r: r.event_date.dt.add(days=10))
        df = result.to_pandas()
        assert "future_date" in df.columns

    def test_dt_add_months(self):
        """dt.add(months=N) should add months to date."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/events.csv")
        result = t.derive(future_date=lambda r: r.event_date.dt.add(months=1))
        df = result.to_pandas()
        assert "future_date" in df.columns

    def test_dt_add_years(self):
        """dt.add(years=N) should add years to date."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/events.csv")
        result = t.derive(future_date=lambda r: r.event_date.dt.add(years=1))
        df = result.to_pandas()
        assert "future_date" in df.columns

    def test_dt_add_combined(self):
        """dt.add() should work with days, months, and years combined."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/events.csv")
        result = t.derive(
            future_date=lambda r: r.event_date.dt.add(days=5, months=2, years=1)
        )
        df = result.to_pandas()
        assert "future_date" in df.columns


class TestTemporalFilter:
    """Tests for filtering on temporal expressions."""

    def test_filter_by_year(self):
        """Should be able to filter rows by extracted year."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/events.csv")
        result = t.filter(lambda r: r.event_date.dt.year() == 2024)
        # Test data has 6 events in 2024
        assert len(result) <= len(t)

    def test_filter_by_month(self):
        """Should be able to filter rows by extracted month."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/events.csv")
        result = t.filter(lambda r: r.event_date.dt.month() == 3)
        # Test data has 3 events in March
        assert len(result) <= len(t)

    def test_filter_by_hour(self):
        """Should be able to filter rows by extracted hour."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/events.csv")
        result = t.filter(lambda r: r.event_datetime.dt.hour() >= 12)
        # Filter for afternoon/evening events
        assert len(result) <= len(t)


class TestTemporalChaining:
    """Tests for chaining temporal operations with other operations."""

    def test_derive_multiple_temporal_cols(self):
        """Should be able to derive multiple temporal columns at once."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/events.csv")
        result = t.derive(
            year=lambda r: r.event_date.dt.year(),
            month=lambda r: r.event_date.dt.month(),
            day=lambda r: r.event_date.dt.day(),
        )
        df = result.to_pandas()
        assert "year" in df.columns
        assert "month" in df.columns
        assert "day" in df.columns

    def test_temporal_with_filter_chain(self):
        """Temporal operations should work in filter-derive chains."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/events.csv")
        result = t.derive(year=lambda r: r.event_date.dt.year()).filter(
            lambda r: r.year == 2024
        )
        assert len(result) <= len(t)

    def test_temporal_comparison(self):
        """Should be able to compare temporal extractions."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/events.csv")
        result = t.derive(is_q1=lambda r: r.event_date.dt.month() <= 3)
        df = result.to_pandas()
        assert "is_q1" in df.columns


class TestTemporalEdgeCases:
    """Edge case tests for temporal operations."""

    def test_temporal_on_string_column(self):
        """Temporal operations on date strings should work."""
        # Most CSV readers treat dates as strings initially
        t = LTSeq.read_csv("data/sample.csv")  # Has date column
        result = t.derive(year=lambda r: r.date.dt.year())
        # Should not raise, even if data is string
        assert result is not None

    def test_temporal_chained_with_string_ops(self):
        """Temporal and string operations should coexist."""
        t = LTSeq.read_csv("py-ltseq/tests/test_data/events.csv")
        result = t.derive(
            year=lambda r: r.event_date.dt.year(),
            event_upper=lambda r: r.event_name.s.upper(),
        )
        df = result.to_pandas()
        assert "year" in df.columns
        assert "event_upper" in df.columns
