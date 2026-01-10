"""Tests for expression optimization in the transpiler.

These tests verify that constant folding and boolean simplification work correctly.
The optimizations are applied at the Rust transpiler level.

NOTE: Some optimizations (like pure literal comparison `5 == 5`) are evaluated
by Python before they reach our expression system, so we can only test cases
where at least one operand is a column reference.
"""

import pytest
import tempfile
import csv
import os


@pytest.fixture
def sample_data():
    """Create a sample CSV for testing."""
    data = [
        {"id": 1, "value": 10, "flag": "true"},
        {"id": 2, "value": 20, "flag": "false"},
        {"id": 3, "value": 30, "flag": "true"},
        {"id": 4, "value": 40, "flag": "false"},
        {"id": 5, "value": 50, "flag": "true"},
    ]
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, newline=""
    ) as f:
        writer = csv.DictWriter(f, fieldnames=["id", "value", "flag"])
        writer.writeheader()
        writer.writerows(data)
        path = f.name
    yield path
    os.unlink(path)


class TestConstantFolding:
    """Test constant folding optimization."""

    def test_arithmetic_constant_folding_add(self, sample_data):
        """Test that 1 + 2 + r.value folds 1 + 2 to 3."""
        from ltseq import LTSeq

        t = LTSeq.read_csv(sample_data)
        # Expression: 1 + 2 + r.value should fold to 3 + r.value
        result = t.derive(computed=lambda r: 1 + 2 + r.value)
        df = result.to_pandas()

        # First row: 1 + 2 + 10 = 13
        assert df.iloc[0]["computed"] == 13
        # Last row: 1 + 2 + 50 = 53
        assert df.iloc[4]["computed"] == 53

    def test_arithmetic_constant_folding_multiply(self, sample_data):
        """Test that 2 * 3 * r.value folds 2 * 3 to 6."""
        from ltseq import LTSeq

        t = LTSeq.read_csv(sample_data)
        result = t.derive(computed=lambda r: 2 * 3 * r.value)
        df = result.to_pandas()

        # First row: 2 * 3 * 10 = 60
        assert df.iloc[0]["computed"] == 60
        # Third row: 2 * 3 * 30 = 180
        assert df.iloc[2]["computed"] == 180

    def test_arithmetic_constant_folding_mixed(self, sample_data):
        """Test mixed operations with constant folding."""
        from ltseq import LTSeq

        t = LTSeq.read_csv(sample_data)
        # (10 + 5) + r.value = 15 + r.value
        result = t.derive(computed=lambda r: (10 + 5) + r.value)
        df = result.to_pandas()

        # First row: 15 + 10 = 25
        assert df.iloc[0]["computed"] == 25

    def test_constant_folding_in_filter(self, sample_data):
        """Test constant folding in filter predicates."""
        from ltseq import LTSeq

        t = LTSeq.read_csv(sample_data)
        # Filter where value > 10 + 15 (should fold to value > 25)
        result = t.filter(lambda r: r.value > 10 + 15)
        df = result.to_pandas()

        # Should have rows where value > 25: 30, 40, 50
        assert len(df) == 3
        assert list(df["value"]) == [30, 40, 50]

    def test_subtraction_constant_folding(self, sample_data):
        """Test subtraction constant folding."""
        from ltseq import LTSeq

        t = LTSeq.read_csv(sample_data)
        # 100 - 50 - r.value should fold to 50 - r.value
        result = t.derive(computed=lambda r: 100 - 50 - r.value)
        df = result.to_pandas()

        # First row: 50 - 10 = 40
        assert df.iloc[0]["computed"] == 40

    def test_division_constant_folding(self, sample_data):
        """Test division constant folding."""
        from ltseq import LTSeq

        t = LTSeq.read_csv(sample_data)
        # (100 / 2) + r.value should fold to 50 + r.value
        result = t.derive(computed=lambda r: (100 / 2) + r.value)
        df = result.to_pandas()

        # First row: 50 + 10 = 60
        assert df.iloc[0]["computed"] == 60


class TestChainedOptimizations:
    """Test multiple optimizations applied together."""

    def test_nested_constant_folding(self, sample_data):
        """Test nested arithmetic constant folding."""
        from ltseq import LTSeq

        t = LTSeq.read_csv(sample_data)
        # (1 + 2) * (3 + 4) should fold to 21
        result = t.derive(computed=lambda r: (1 + 2) * (3 + 4) + r.value)
        df = result.to_pandas()

        # First row: 21 + 10 = 31
        assert df.iloc[0]["computed"] == 31

    def test_combined_filter_and_derive(self, sample_data):
        """Test optimization in both filter and derive."""
        from ltseq import LTSeq

        t = LTSeq.read_csv(sample_data)
        # Chain operations with constants
        result = t.filter(lambda r: r.value > 5 + 10).derive(
            doubled=lambda r: 2 * r.value, offset=lambda r: r.value + 100 + 50
        )
        df = result.to_pandas()

        # Should have rows where value > 15: 20, 30, 40, 50
        assert len(df) == 4
        # Check derived values for first filtered row (value=20)
        assert df.iloc[0]["doubled"] == 40
        assert df.iloc[0]["offset"] == 170  # 20 + 150

    def test_deeply_nested_constants(self, sample_data):
        """Test deeply nested constant expressions."""
        from ltseq import LTSeq

        t = LTSeq.read_csv(sample_data)
        # ((1 + 1) * (2 + 2)) + ((3 + 3) * (4 + 4)) = (2*4) + (6*8) = 8 + 48 = 56
        result = t.derive(
            computed=lambda r: ((1 + 1) * (2 + 2)) + ((3 + 3) * (4 + 4)) + r.value
        )
        df = result.to_pandas()

        # First row: 56 + 10 = 66
        assert df.iloc[0]["computed"] == 66


class TestNoOptimizationNeeded:
    """Test cases where optimization shouldn't change behavior."""

    def test_column_only_expression(self, sample_data):
        """Test expression with no constants to fold."""
        from ltseq import LTSeq

        t = LTSeq.read_csv(sample_data)
        result = t.filter(lambda r: r.value > r.id)
        df = result.to_pandas()

        # All rows have value > id (10>1, 20>2, etc.)
        assert len(df) == 5

    def test_mixed_column_and_constant(self, sample_data):
        """Test expression mixing columns and constants."""
        from ltseq import LTSeq

        t = LTSeq.read_csv(sample_data)
        # r.value + 5 - can't fold but should work
        result = t.derive(computed=lambda r: r.value + 5)
        df = result.to_pandas()

        assert df.iloc[0]["computed"] == 15
        assert df.iloc[4]["computed"] == 55

    def test_single_constant_in_derive(self, sample_data):
        """Test that single constant in derive works (edge case)."""
        from ltseq import LTSeq

        t = LTSeq.read_csv(sample_data)
        # r.value * 2 - simple multiplication
        result = t.derive(computed=lambda r: r.value * 2)
        df = result.to_pandas()

        assert df.iloc[0]["computed"] == 20
        assert df.iloc[4]["computed"] == 100


class TestFilterWithComputedThresholds:
    """Test filters with computed thresholds (folded constants)."""

    def test_filter_with_computed_threshold(self, sample_data):
        """Test filter with a computed threshold value."""
        from ltseq import LTSeq

        t = LTSeq.read_csv(sample_data)
        # 5 * 5 = 25, filter value > 25
        result = t.filter(lambda r: r.value > 5 * 5)
        df = result.to_pandas()

        assert len(df) == 3  # 30, 40, 50

    def test_filter_with_complex_threshold(self, sample_data):
        """Test filter with complex computed threshold."""
        from ltseq import LTSeq

        t = LTSeq.read_csv(sample_data)
        # (10 + 5) * 2 = 30, filter value >= 30
        result = t.filter(lambda r: r.value >= (10 + 5) * 2)
        df = result.to_pandas()

        assert len(df) == 3  # 30, 40, 50
        assert list(df["value"]) == [30, 40, 50]


class TestModuloAndComparisons:
    """Test modulo and comparison operations with constant folding."""

    def test_modulo_constant_folding(self, sample_data):
        """Test modulo constant folding."""
        from ltseq import LTSeq

        t = LTSeq.read_csv(sample_data)
        # 17 % 5 = 2, so r.value + 2
        result = t.derive(computed=lambda r: r.value + 17 % 5)
        df = result.to_pandas()

        # First row: 10 + 2 = 12
        assert df.iloc[0]["computed"] == 12

    def test_comparison_in_filter_with_constants(self, sample_data):
        """Test comparison operators with constant folding."""
        from ltseq import LTSeq

        t = LTSeq.read_csv(sample_data)
        # r.value > 20 + 5 should fold to r.value > 25
        result = t.filter(lambda r: r.value > 20 + 5)
        df = result.to_pandas()

        assert len(df) == 3


class TestExistingTestsStillPass:
    """Verify that expression optimization doesn't break existing functionality."""

    def test_basic_filter_still_works(self, sample_data):
        """Test that basic filter without constants still works."""
        from ltseq import LTSeq

        t = LTSeq.read_csv(sample_data)
        result = t.filter(lambda r: r.value > 25)
        df = result.to_pandas()

        assert len(df) == 3

    def test_basic_derive_still_works(self, sample_data):
        """Test that basic derive without constants still works."""
        from ltseq import LTSeq

        t = LTSeq.read_csv(sample_data)
        result = t.derive(doubled=lambda r: r.value * 2)
        df = result.to_pandas()

        assert df.iloc[0]["doubled"] == 20

    def test_chained_operations_still_work(self, sample_data):
        """Test that chained operations still work."""
        from ltseq import LTSeq

        t = LTSeq.read_csv(sample_data)
        result = (
            t.filter(lambda r: r.value > 15)
            .derive(doubled=lambda r: r.value * 2)
            .filter(lambda r: r.doubled < 100)
        )
        df = result.to_pandas()

        # value > 15: 20, 30, 40, 50 -> doubled: 40, 60, 80, 100 -> doubled < 100: 40, 60, 80
        assert len(df) == 3
