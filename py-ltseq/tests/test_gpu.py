"""Tests for GPU-accelerated filter execution.

These tests verify that:
1. GPU filter produces the same results as CPU filter
2. CPU fallback works for unsupported types
3. Empty result sets are handled correctly
4. Various comparison operators work correctly
5. All supported column types (i32, i64, f32) work

Note: These tests exercise the WgpuFilterExec through the normal LTSeq API.
The GPU optimizer transparently replaces FilterExec with WgpuFilterExec
for compatible predicates. The GPU_ROW_THRESHOLD (100K) means small test
batches use CPU fallback within WgpuFilterExec, but the optimizer replacement
and batch-level routing logic are still tested.
"""

import pytest
import pyarrow as pa
from ltseq import LTSeq, gpu_available


class TestGpuAvailability:
    """Test GPU availability detection."""

    def test_gpu_available_returns_bool(self):
        result = gpu_available()
        assert isinstance(result, bool)

    def test_gpu_available_with_env_var(self, monkeypatch):
        """When LTSEQ_DISABLE_GPU=1 is set at build time, gpu_available() reflects it.

        Note: This can't truly test the env var because the GPU context is
        initialized once at process start via LazyLock. This test documents
        the expected behavior.
        """
        result = gpu_available()
        # Just verify it returns a bool without error
        assert isinstance(result, bool)


class TestGpuFilterI64:
    """Test GPU filter path for Int64 columns.

    CSV files infer integer columns as Int64, so most tests use this type.
    """

    @pytest.fixture
    def table(self, tmp_path):
        csv = tmp_path / "data.csv"
        csv.write_text(
            "id,value,name\n"
            "1,100,alice\n"
            "2,200,bob\n"
            "3,300,charlie\n"
            "4,400,dave\n"
            "5,500,eve\n"
        )
        return LTSeq.read_csv(str(csv))

    def test_filter_gt(self, table):
        result = table.filter(lambda r: r.value > 300)
        assert len(result) == 2
        df = result.to_pandas()
        assert list(df["value"]) == [400, 500]

    def test_filter_gte(self, table):
        result = table.filter(lambda r: r.value >= 300)
        assert len(result) == 3
        df = result.to_pandas()
        assert list(df["value"]) == [300, 400, 500]

    def test_filter_lt(self, table):
        result = table.filter(lambda r: r.value < 300)
        assert len(result) == 2
        df = result.to_pandas()
        assert list(df["value"]) == [100, 200]

    def test_filter_lte(self, table):
        result = table.filter(lambda r: r.value <= 300)
        assert len(result) == 3
        df = result.to_pandas()
        assert list(df["value"]) == [100, 200, 300]

    def test_filter_eq(self, table):
        result = table.filter(lambda r: r.value == 300)
        assert len(result) == 1
        df = result.to_pandas()
        assert list(df["value"]) == [300]

    def test_filter_neq(self, table):
        result = table.filter(lambda r: r.value != 300)
        assert len(result) == 4
        df = result.to_pandas()
        assert list(df["value"]) == [100, 200, 400, 500]

    def test_filter_all_pass(self, table):
        """All rows match the predicate."""
        result = table.filter(lambda r: r.value > 0)
        assert len(result) == 5

    def test_filter_none_pass(self, table):
        """No rows match — empty result set."""
        result = table.filter(lambda r: r.value > 9999)
        assert len(result) == 0
        # Verify we can still get a valid Arrow table
        arrow = result.to_arrow()
        assert arrow.num_rows == 0

    def test_filter_preserves_other_columns(self, table):
        """Filtered result preserves all columns, not just the filtered one."""
        result = table.filter(lambda r: r.value == 300)
        df = result.to_pandas()
        assert list(df.columns) == ["id", "value", "name"]
        assert df.iloc[0]["name"] == "charlie"
        assert df.iloc[0]["id"] == 3


class TestGpuFilterChaining:
    """Test chaining multiple filters (sequential GPU filter applications)."""

    @pytest.fixture
    def table(self, tmp_path):
        csv = tmp_path / "data.csv"
        csv.write_text(
            "id,value,category\n"
            "1,100,a\n"
            "2,200,b\n"
            "3,300,a\n"
            "4,400,b\n"
            "5,500,a\n"
        )
        return LTSeq.read_csv(str(csv))

    def test_chained_filters(self, table):
        """Multiple filters applied in sequence."""
        result = table.filter(lambda r: r.value > 100).filter(lambda r: r.value < 500)
        assert len(result) == 3
        df = result.to_pandas()
        assert list(df["value"]) == [200, 300, 400]

    def test_filter_then_select(self, table):
        """Filter followed by select."""
        result = table.filter(lambda r: r.value > 200).select("id", "value")
        df = result.to_pandas()
        assert list(df.columns) == ["id", "value"]
        assert len(df) == 3

    def test_filter_with_string_predicate(self, table):
        """String filter goes through CPU (not GPU-compatible), should still work."""
        result = table.filter(lambda r: r.category == "a")
        assert len(result) == 3
        df = result.to_pandas()
        assert all(v == "a" for v in df["category"])


class TestGpuFilterFloat:
    """Test GPU filter for Float32 columns (if present in data)."""

    @pytest.fixture
    def table(self, tmp_path):
        """Create a table with Float64 columns (CSV default for decimals).

        Note: CSV inference creates Float64 (double), not Float32.
        The GPU filter only handles Float32, so these should fall through
        to CPU execution. This tests the fallback path.
        """
        csv = tmp_path / "data.csv"
        csv.write_text(
            "id,score\n"
            "1,85.5\n"
            "2,90.0\n"
            "3,78.3\n"
            "4,95.7\n"
            "5,88.1\n"
        )
        return LTSeq.read_csv(str(csv))

    def test_filter_float_gt(self, table):
        """Float64 filter (CPU fallback since GPU only handles f32)."""
        result = table.filter(lambda r: r.score > 89.0)
        assert len(result) == 2
        df = result.to_pandas()
        assert set(df["id"].tolist()) == {2, 4}

    def test_filter_float_empty(self, table):
        result = table.filter(lambda r: r.score > 100.0)
        assert len(result) == 0


class TestGpuFilterEdgeCases:
    """Test edge cases for GPU filter."""

    @pytest.fixture
    def table(self, tmp_path):
        csv = tmp_path / "data.csv"
        csv.write_text(
            "id,value\n"
            "1,0\n"
            "2,-100\n"
            "3,100\n"
        )
        return LTSeq.read_csv(str(csv))

    def test_filter_with_zero(self, table):
        result = table.filter(lambda r: r.value == 0)
        assert len(result) == 1
        df = result.to_pandas()
        assert df.iloc[0]["id"] == 1

    def test_filter_negative_values(self, table):
        result = table.filter(lambda r: r.value < 0)
        assert len(result) == 1
        df = result.to_pandas()
        assert df.iloc[0]["value"] == -100

    def test_filter_negative_literal(self, table):
        result = table.filter(lambda r: r.value > -50)
        assert len(result) == 2
        df = result.to_pandas()
        assert set(df["value"].tolist()) == {0, 100}

    def test_collect_empty_filtered(self, table):
        """Regression test: empty filter result + collect() should not crash."""
        empty = table.filter(lambda r: r.value > 9999)
        assert len(empty) == 0
        result = empty.collect()
        assert len(result) == 0

    def test_to_arrow_empty_filtered(self, table):
        """Regression test: empty filter result + to_arrow() should return valid table."""
        empty = table.filter(lambda r: r.value > 9999)
        arrow = empty.to_arrow()
        assert arrow.num_rows == 0
        assert "id" in arrow.schema.names
        assert "value" in arrow.schema.names


class TestGpuFilterDeriveInteraction:
    """Test GPU filter interacting with derive operations."""

    @pytest.fixture
    def table(self, tmp_path):
        csv = tmp_path / "data.csv"
        csv.write_text(
            "id,price,quantity\n"
            "1,10,5\n"
            "2,20,3\n"
            "3,30,7\n"
            "4,40,2\n"
        )
        return LTSeq.read_csv(str(csv))

    def test_derive_then_filter(self, table):
        """Derive a new column then filter on it."""
        result = table.derive(total=lambda r: r.price * r.quantity).filter(
            lambda r: r.total > 100
        )
        # price*quantity: 50, 60, 210, 80 → only 210 > 100
        assert len(result) == 1
        df = result.to_pandas()
        assert set(df["id"].tolist()) == {3}

    def test_filter_then_derive(self, table):
        """Filter first, then derive."""
        result = table.filter(lambda r: r.price > 15).derive(
            doubled=lambda r: r.price * 2
        )
        df = result.to_pandas()
        assert len(df) == 3
        assert list(df["doubled"]) == [40, 60, 80]


class TestGpuFilterCompoundPredicates:
    """Test GPU filter with compound predicates (AND, OR, NOT).

    Phase 3: Compound predicates are decomposed into a tree of GPU filter ops.
    Each leaf runs an independent GPU shader dispatch, and interior AND/OR/NOT
    nodes combine bitmasks on CPU.
    """

    @pytest.fixture
    def table(self, tmp_path):
        csv = tmp_path / "data.csv"
        csv.write_text(
            "id,value,score\n"
            "1,100,10\n"
            "2,200,20\n"
            "3,300,30\n"
            "4,400,40\n"
            "5,500,50\n"
        )
        return LTSeq.read_csv(str(csv))

    def test_filter_and(self, table):
        """AND predicate: both conditions must be true."""
        result = table.filter(lambda r: (r.value > 100) & (r.value < 500))
        assert len(result) == 3
        df = result.to_pandas()
        assert list(df["value"]) == [200, 300, 400]

    def test_filter_or(self, table):
        """OR predicate: either condition must be true."""
        result = table.filter(lambda r: (r.value == 100) | (r.value == 500))
        assert len(result) == 2
        df = result.to_pandas()
        assert list(df["value"]) == [100, 500]

    def test_filter_and_different_columns(self, table):
        """AND across two different columns."""
        result = table.filter(lambda r: (r.value > 200) & (r.score < 50))
        assert len(result) == 2
        df = result.to_pandas()
        assert list(df["id"]) == [3, 4]

    def test_filter_or_different_columns(self, table):
        """OR across two different columns."""
        result = table.filter(lambda r: (r.value == 100) | (r.score == 50))
        assert len(result) == 2
        df = result.to_pandas()
        assert list(df["id"]) == [1, 5]

    def test_filter_nested_and_or(self, table):
        """Nested: (A AND B) OR C."""
        result = table.filter(
            lambda r: ((r.value > 300) & (r.score < 50)) | (r.id == 1)
        )
        # (value>300 AND score<50) → id=4; OR id=1 → id=1,4
        assert len(result) == 2
        df = result.to_pandas()
        assert list(df["id"]) == [1, 4]

    def test_filter_nested_or_and(self, table):
        """Nested: (A OR B) AND C."""
        result = table.filter(
            lambda r: ((r.value == 100) | (r.value == 200)) & (r.score >= 20)
        )
        # (value=100 OR value=200) → id=1,2; AND score>=20 → id=2
        assert len(result) == 1
        df = result.to_pandas()
        assert list(df["id"]) == [2]

    def test_filter_and_all_pass(self, table):
        """AND where all rows pass both conditions."""
        result = table.filter(lambda r: (r.value > 0) & (r.score > 0))
        assert len(result) == 5

    def test_filter_and_none_pass(self, table):
        """AND where no rows pass (impossible condition)."""
        result = table.filter(lambda r: (r.value > 9999) & (r.score < 0))
        assert len(result) == 0
        arrow = result.to_arrow()
        assert arrow.num_rows == 0

    def test_filter_or_all_pass(self, table):
        """OR where every row passes at least one condition."""
        result = table.filter(lambda r: (r.value <= 300) | (r.value >= 300))
        assert len(result) == 5

    def test_filter_or_none_pass(self, table):
        """OR where no rows pass either condition."""
        result = table.filter(lambda r: (r.value > 9999) | (r.score > 9999))
        assert len(result) == 0

    def test_filter_triple_and(self, table):
        """Three conditions ANDed together."""
        result = table.filter(
            lambda r: (r.value > 100) & (r.value < 500) & (r.score > 20)
        )
        # value in (200,300,400) AND score>20 → score 30,40 → id=3,4
        assert len(result) == 2
        df = result.to_pandas()
        assert list(df["id"]) == [3, 4]

    def test_filter_triple_or(self, table):
        """Three conditions ORed together."""
        result = table.filter(
            lambda r: (r.value == 100) | (r.value == 300) | (r.value == 500)
        )
        assert len(result) == 3
        df = result.to_pandas()
        assert list(df["value"]) == [100, 300, 500]

    def test_filter_compound_preserves_columns(self, table):
        """Compound filter preserves all columns."""
        result = table.filter(lambda r: (r.value > 200) & (r.score < 50))
        df = result.to_pandas()
        assert list(df.columns) == ["id", "value", "score"]

    def test_filter_compound_chained(self, table):
        """Compound filter followed by another filter."""
        result = (
            table.filter(lambda r: (r.value > 100) & (r.value < 500))
            .filter(lambda r: r.score > 25)
        )
        # First: value in (200,300,400); Second: score>25 → score 30,40 → id=3,4
        assert len(result) == 2
        df = result.to_pandas()
        assert list(df["id"]) == [3, 4]


class TestGpuFilterCompoundWithUnsupportedTypes:
    """Test that compound predicates with unsupported types fall back to CPU."""

    @pytest.fixture
    def table(self, tmp_path):
        csv = tmp_path / "data.csv"
        csv.write_text(
            "id,value,name\n"
            "1,100,alice\n"
            "2,200,bob\n"
            "3,300,charlie\n"
        )
        return LTSeq.read_csv(str(csv))

    def test_and_with_string_column_falls_back(self, table):
        """AND with one string column — entire predicate falls back to CPU."""
        result = table.filter(lambda r: (r.value > 100) & (r.name == "charlie"))
        assert len(result) == 1
        df = result.to_pandas()
        assert df.iloc[0]["id"] == 3

    def test_or_with_string_column_falls_back(self, table):
        """OR with one string column — falls back to CPU."""
        result = table.filter(lambda r: (r.value == 100) | (r.name == "charlie"))
        assert len(result) == 2
        df = result.to_pandas()
        assert set(df["id"].tolist()) == {1, 3}


# ════════════════════════════════════════════════════════════════════════
#  Phase 4: GPU Projection (derive) Tests
# ════════════════════════════════════════════════════════════════════════


class TestGpuProjectionI64:
    """Test GPU projection for Int64 arithmetic expressions.

    CSV integer columns infer as Int64. The GPU projection rule replaces
    ProjectionExec with WgpuProjectionExec when arithmetic ops (+ - * / %)
    are present on i32/i64/f32 columns.

    Note: i64 div/mod are excluded from GPU (WGSL has no emulation), so
    those fall back to CPU via OutputColumnPlan::CpuPassthrough.
    """

    @pytest.fixture
    def table(self, tmp_path):
        csv = tmp_path / "data.csv"
        csv.write_text(
            "id,price,quantity\n"
            "1,10,5\n"
            "2,20,3\n"
            "3,30,7\n"
            "4,40,2\n"
            "5,50,1\n"
        )
        return LTSeq.read_csv(str(csv))

    def test_derive_add_literal(self, table):
        """Column + literal."""
        result = table.derive(price_plus=lambda r: r.price + 100)
        df = result.to_pandas()
        assert list(df["price_plus"]) == [110, 120, 130, 140, 150]

    def test_derive_sub_literal(self, table):
        """Column - literal."""
        result = table.derive(price_minus=lambda r: r.price - 5)
        df = result.to_pandas()
        assert list(df["price_minus"]) == [5, 15, 25, 35, 45]

    def test_derive_mul_literal(self, table):
        """Column * literal."""
        result = table.derive(price_doubled=lambda r: r.price * 2)
        df = result.to_pandas()
        assert list(df["price_doubled"]) == [20, 40, 60, 80, 100]

    def test_derive_add_columns(self, table):
        """Column + Column."""
        result = table.derive(total=lambda r: r.price + r.quantity)
        df = result.to_pandas()
        assert list(df["total"]) == [15, 23, 37, 42, 51]

    def test_derive_sub_columns(self, table):
        """Column - Column."""
        result = table.derive(diff=lambda r: r.price - r.quantity)
        df = result.to_pandas()
        assert list(df["diff"]) == [5, 17, 23, 38, 49]

    def test_derive_mul_columns(self, table):
        """Column * Column (total cost)."""
        result = table.derive(cost=lambda r: r.price * r.quantity)
        df = result.to_pandas()
        assert list(df["cost"]) == [50, 60, 210, 80, 50]

    def test_derive_complex_expr(self, table):
        """Multi-op expression: (price + quantity) * 2."""
        result = table.derive(val=lambda r: (r.price + r.quantity) * 2)
        df = result.to_pandas()
        assert list(df["val"]) == [30, 46, 74, 84, 102]

    def test_derive_preserves_existing_columns(self, table):
        """Derive adds new column without dropping existing ones."""
        result = table.derive(doubled=lambda r: r.price * 2)
        df = result.to_pandas()
        assert list(df.columns) == ["id", "price", "quantity", "doubled"]
        assert list(df["price"]) == [10, 20, 30, 40, 50]

    def test_derive_multiple_expressions(self, table):
        """Multiple derived columns in one call."""
        result = table.derive(
            doubled=lambda r: r.price * 2,
            total=lambda r: r.price + r.quantity,
        )
        df = result.to_pandas()
        assert list(df["doubled"]) == [20, 40, 60, 80, 100]
        assert list(df["total"]) == [15, 23, 37, 42, 51]


class TestGpuProjectionEdgeCases:
    """Edge cases for GPU projection."""

    @pytest.fixture
    def table(self, tmp_path):
        csv = tmp_path / "data.csv"
        csv.write_text(
            "id,value\n"
            "1,0\n"
            "2,-100\n"
            "3,100\n"
        )
        return LTSeq.read_csv(str(csv))

    def test_derive_with_zero(self, table):
        """Multiply by zero."""
        result = table.derive(zeroed=lambda r: r.value * 0)
        df = result.to_pandas()
        assert list(df["zeroed"]) == [0, 0, 0]

    def test_derive_with_negative(self, table):
        """Add negative literal (effectively subtraction)."""
        result = table.derive(shifted=lambda r: r.value + (-50))
        df = result.to_pandas()
        assert list(df["shifted"]) == [-50, -150, 50]

    def test_derive_identity(self, table):
        """Add zero (identity operation)."""
        result = table.derive(same=lambda r: r.value + 0)
        df = result.to_pandas()
        assert list(df["same"]) == [0, -100, 100]

    def test_derive_negative_multiply(self, table):
        """Negate via multiply by -1."""
        result = table.derive(negated=lambda r: r.value * (-1))
        df = result.to_pandas()
        assert list(df["negated"]) == [0, 100, -100]


class TestGpuProjectionChaining:
    """Test GPU projection interacting with other operations."""

    @pytest.fixture
    def table(self, tmp_path):
        csv = tmp_path / "data.csv"
        csv.write_text(
            "id,price,quantity\n"
            "1,10,5\n"
            "2,20,3\n"
            "3,30,7\n"
            "4,40,2\n"
        )
        return LTSeq.read_csv(str(csv))

    def test_derive_then_filter(self, table):
        """Derive a computed column, then filter on it."""
        result = table.derive(total=lambda r: r.price * r.quantity).filter(
            lambda r: r.total > 100
        )
        # totals: 50, 60, 210, 80 → only 210 > 100
        assert len(result) == 1
        df = result.to_pandas()
        assert df.iloc[0]["id"] == 3

    def test_filter_then_derive(self, table):
        """Filter first, then derive on the filtered result."""
        result = table.filter(lambda r: r.price > 15).derive(
            doubled=lambda r: r.price * 2
        )
        df = result.to_pandas()
        assert len(df) == 3
        assert list(df["doubled"]) == [40, 60, 80]

    def test_derive_chain(self, table):
        """Chain multiple derives."""
        result = (
            table
            .derive(doubled=lambda r: r.price * 2)
            .derive(tripled=lambda r: r.doubled + r.price)
        )
        df = result.to_pandas()
        # doubled: 20, 40, 60, 80
        # tripled: 20+10=30, 40+20=60, 60+30=90, 80+40=120
        assert list(df["tripled"]) == [30, 60, 90, 120]

    def test_derive_then_select(self, table):
        """Derive then select subset of columns."""
        result = table.derive(total=lambda r: r.price * r.quantity).select(
            "id", "total"
        )
        df = result.to_pandas()
        assert list(df.columns) == ["id", "total"]
        assert list(df["total"]) == [50, 60, 210, 80]

    def test_derive_empty_table(self, table):
        """Derive on an empty filtered result."""
        empty = table.filter(lambda r: r.price > 9999)
        result = empty.derive(doubled=lambda r: r.price * 2)
        assert len(result) == 0
        df = result.to_pandas()
        assert "doubled" in df.columns


# ════════════════════════════════════════════════════════════════════════════
# GPU Aggregate tests (WgpuAggregateExec — Phase 5)
# ════════════════════════════════════════════════════════════════════════════

# Number of rows above GPU_ROW_THRESHOLD (4096) to ensure GPU path is taken.
_AGG_N = 8_192
# Number of distinct groups (must fit in MAX_GPU_GROUPS = 65535).
_AGG_GROUPS = 16


def _make_sorted_agg_table(n=_AGG_N, groups=_AGG_GROUPS, key_type="int32"):
    """Build a PyArrow table pre-sorted by 'group_id', with 'value' column.

    Layout: n rows, values cycling through 0..groups-1, sorted ascending.
    Each group has exactly n // groups rows with value = group_id * 10.
    """
    group_ids = [i // (n // groups) for i in range(n)]
    values_i32 = [g * 10 for g in group_ids]

    if key_type == "int32":
        key_col = pa.array(group_ids, type=pa.int32())
    else:  # int64
        key_col = pa.array(group_ids, type=pa.int64())

    val_col_i32 = pa.array(values_i32, type=pa.int32())
    val_col_i64 = pa.array([v * 1000 for v in values_i32], type=pa.int64())

    return pa.table({
        "group_id": key_col,
        "value_i32": val_col_i32,
        "value_i64": val_col_i64,
    })


class TestGpuAggregateBasic:
    """Test GPU-accelerated GROUP BY aggregation (WgpuAggregateExec).

    Tests verify that GPU aggregation produces correct results matching CPU.
    The GPU path is activated when:
    - Input is sorted by the group-by key (assume_sorted)
    - Group key is i32 or i64
    - Aggregates are sum/count/min/max/avg on i32 or i64 columns
    - Row count >= GPU_ROW_THRESHOLD (4096)

    For small tables (below threshold), WgpuAggregateExec transparently
    falls back to CPU, so correctness is still verified.
    """

    @pytest.fixture
    def sorted_table_i32(self):
        """Sorted table with i32 group key."""
        arrow = _make_sorted_agg_table(key_type="int32")
        return LTSeq.from_arrow(arrow).assume_sorted("group_id")

    @pytest.fixture
    def sorted_table_i64(self):
        """Sorted table with i64 group key."""
        arrow = _make_sorted_agg_table(key_type="int64")
        return LTSeq.from_arrow(arrow).assume_sorted("group_id")

    @pytest.fixture
    def small_sorted_table(self):
        """Small table (below GPU threshold) with i32 group key.

        Tests that CPU fallback inside WgpuAggregateExec still works.
        """
        rows_per_group = 4
        n_groups = 8
        n = rows_per_group * n_groups
        group_ids = [i // rows_per_group for i in range(n)]
        values = [g * 10 for g in group_ids]
        arrow = pa.table({
            "group_id": pa.array(group_ids, type=pa.int32()),
            "value_i32": pa.array(values, type=pa.int32()),
        })
        return LTSeq.from_arrow(arrow).assume_sorted("group_id")

    def test_sum_i32_key_i32_value(self, sorted_table_i32):
        """sum(i32) grouped by i32 key."""
        result = sorted_table_i32.agg(
            by=lambda r: r.group_id,
            total=lambda g: g.value_i32.sum(),
        )
        df = result.to_pandas()
        assert len(df) == _AGG_GROUPS
        # Each group_id g has n//groups rows each with value = g*10
        rows_per_group = _AGG_N // _AGG_GROUPS
        for _, row in df.iterrows():
            gid = int(row["group_id"])
            expected_sum = gid * 10 * rows_per_group
            assert int(row["total"]) == expected_sum, (
                f"group_id={gid}: expected {expected_sum}, got {row['total']}"
            )

    def test_count_i32_key(self, sorted_table_i32):
        """count grouped by i32 key."""
        result = sorted_table_i32.agg(
            by=lambda r: r.group_id,
            n=lambda g: g.value_i32.count(),
        )
        df = result.to_pandas()
        assert len(df) == _AGG_GROUPS
        rows_per_group = _AGG_N // _AGG_GROUPS
        for _, row in df.iterrows():
            assert int(row["n"]) == rows_per_group, (
                f"group_id={row['group_id']}: expected count {rows_per_group}, got {row['n']}"
            )

    def test_min_i32_key_i32_value(self, sorted_table_i32):
        """min(i32) grouped by i32 key."""
        result = sorted_table_i32.agg(
            by=lambda r: r.group_id,
            min_val=lambda g: g.value_i32.min(),
        )
        df = result.to_pandas()
        assert len(df) == _AGG_GROUPS
        for _, row in df.iterrows():
            gid = int(row["group_id"])
            assert int(row["min_val"]) == gid * 10

    def test_max_i32_key_i32_value(self, sorted_table_i32):
        """max(i32) grouped by i32 key."""
        result = sorted_table_i32.agg(
            by=lambda r: r.group_id,
            max_val=lambda g: g.value_i32.max(),
        )
        df = result.to_pandas()
        assert len(df) == _AGG_GROUPS
        for _, row in df.iterrows():
            gid = int(row["group_id"])
            assert int(row["max_val"]) == gid * 10

    def test_avg_i32_key_i32_value(self, sorted_table_i32):
        """avg(i32) grouped by i32 key."""
        result = sorted_table_i32.agg(
            by=lambda r: r.group_id,
            avg_val=lambda g: g.value_i32.avg(),
        )
        df = result.to_pandas()
        assert len(df) == _AGG_GROUPS
        for _, row in df.iterrows():
            gid = int(row["group_id"])
            expected_avg = float(gid * 10)
            assert abs(float(row["avg_val"]) - expected_avg) < 1e-6, (
                f"group_id={gid}: expected avg {expected_avg}, got {row['avg_val']}"
            )

    def test_multiple_aggs(self, sorted_table_i32):
        """Multiple aggregates in a single agg() call."""
        result = sorted_table_i32.agg(
            by=lambda r: r.group_id,
            total=lambda g: g.value_i32.sum(),
            n=lambda g: g.value_i32.count(),
            lo=lambda g: g.value_i32.min(),
            hi=lambda g: g.value_i32.max(),
        )
        df = result.to_pandas()
        assert len(df) == _AGG_GROUPS
        rows_per_group = _AGG_N // _AGG_GROUPS
        for _, row in df.iterrows():
            gid = int(row["group_id"])
            val = gid * 10
            assert int(row["total"]) == val * rows_per_group
            assert int(row["n"]) == rows_per_group
            assert int(row["lo"]) == val
            assert int(row["hi"]) == val

    def test_i64_key_i64_value(self, sorted_table_i64):
        """sum/count/min/max on i64 value, grouped by i64 key."""
        result = sorted_table_i64.agg(
            by=lambda r: r.group_id,
            total=lambda g: g.value_i64.sum(),
            n=lambda g: g.value_i64.count(),
            lo=lambda g: g.value_i64.min(),
            hi=lambda g: g.value_i64.max(),
        )
        df = result.to_pandas()
        assert len(df) == _AGG_GROUPS
        rows_per_group = _AGG_N // _AGG_GROUPS
        for _, row in df.iterrows():
            gid = int(row["group_id"])
            val = gid * 10 * 1000  # value_i64 = group_id * 10 * 1000
            assert int(row["total"]) == val * rows_per_group
            assert int(row["n"]) == rows_per_group
            assert int(row["lo"]) == val
            assert int(row["hi"]) == val

    def test_small_table_cpu_fallback(self, small_sorted_table):
        """Small table falls back to CPU inside WgpuAggregateExec; results still correct."""
        result = small_sorted_table.agg(
            by=lambda r: r.group_id,
            total=lambda g: g.value_i32.sum(),
            n=lambda g: g.value_i32.count(),
        )
        df = result.to_pandas()
        assert len(df) == 8
        for _, row in df.iterrows():
            gid = int(row["group_id"])
            # 4 rows per group, each with value = gid * 10
            assert int(row["total"]) == gid * 10 * 4
            assert int(row["n"]) == 4

    def test_unsorted_fallback_correctness(self):
        """Unsorted data never triggers WgpuAggregateExec; CPU agg always works."""
        # Without assume_sorted, InputOrderMode != Sorted → GpuAggregateRule skips it.
        n = _AGG_N
        groups = _AGG_GROUPS
        # Deliberately unsorted (shuffled) group_ids
        import random
        rng = random.Random(42)
        group_ids = list(range(groups)) * (n // groups)
        rng.shuffle(group_ids)
        values = [g * 10 for g in group_ids]
        arrow = pa.table({
            "group_id": pa.array(group_ids, type=pa.int32()),
            "value_i32": pa.array(values, type=pa.int32()),
        })
        t = LTSeq.from_arrow(arrow)
        # No assume_sorted → CPU aggregation via DataFusion hash agg
        result = t.agg(
            by=lambda r: r.group_id,
            total=lambda g: g.value_i32.sum(),
            n=lambda g: g.value_i32.count(),
        )
        df = result.to_pandas()
        assert len(df) == groups
        rows_per_group = n // groups
        for _, row in df.iterrows():
            gid = int(row["group_id"])
            assert int(row["total"]) == gid * 10 * rows_per_group
            assert int(row["n"]) == rows_per_group

    def test_single_group(self):
        """All rows belong to one group."""
        n = _AGG_N
        arrow = pa.table({
            "group_id": pa.array([0] * n, type=pa.int32()),
            "value_i32": pa.array([5] * n, type=pa.int32()),
        })
        t = LTSeq.from_arrow(arrow).assume_sorted("group_id")
        result = t.agg(
            by=lambda r: r.group_id,
            total=lambda g: g.value_i32.sum(),
            n=lambda g: g.value_i32.count(),
        )
        df = result.to_pandas()
        assert len(df) == 1
        assert int(df["total"].iloc[0]) == 5 * n
        assert int(df["n"].iloc[0]) == n

    def test_all_different_groups(self):
        """Every row is its own group (n groups, 1 row each)."""
        n = 4096  # exactly at GPU threshold
        arrow = pa.table({
            "group_id": pa.array(list(range(n)), type=pa.int32()),
            "value_i32": pa.array(list(range(n)), type=pa.int32()),
        })
        t = LTSeq.from_arrow(arrow).assume_sorted("group_id")
        result = t.agg(
            by=lambda r: r.group_id,
            total=lambda g: g.value_i32.sum(),
        )
        df = result.to_pandas().sort_values("group_id").reset_index(drop=True)
        assert len(df) == n
        for i in range(n):
            assert int(df["total"].iloc[i]) == i


class TestFusedGpuPipeline:
    """Test the fused GPU filter + projection pipeline (FusedGpuPipelineExec).

    When the GPU optimizer detects a WgpuProjectionExec immediately wrapping a
    WgpuFilterExec it replaces both with a single FusedGpuPipelineExec.
    These tests verify the fused path produces correct results matching the
    unfused (CPU) path across all supported types and expression shapes.

    The GPU_ROW_THRESHOLD (4096) means small-batch tests exercise the CPU
    fallback inside FusedGpuPipelineExec, while large-batch tests hit the
    actual GPU path (when a GPU is present) or the GPU-error CPU fallback.
    """

    @pytest.fixture
    def small_table(self):
        """Below GPU threshold — exercises cpu_fused_batch path."""
        n = 100
        arrow = pa.table({
            "a": pa.array(list(range(n)), type=pa.int64()),
            "b": pa.array([i * 2 for i in range(n)], type=pa.int64()),
        })
        return LTSeq.from_arrow(arrow)

    @pytest.fixture
    def large_table(self):
        """Above GPU threshold — exercises gpu_fused_batch path."""
        n = 8192
        arrow = pa.table({
            "a": pa.array(list(range(n)), type=pa.int64()),
            "b": pa.array([i * 2 for i in range(n)], type=pa.int64()),
        })
        return LTSeq.from_arrow(arrow)

    # ── Correctness on small (CPU fallback) batches ─────────────────────

    def test_filter_then_derive_small_i64(self, small_table):
        """filter(a > 50) then derive(c = a + b) on small batch."""
        result = small_table.filter(lambda r: r.a > 50).derive(
            c=lambda r: r.a + r.b
        )
        df = result.to_pandas()
        assert all(df["a"] > 50)
        assert list(df["c"]) == [a + b for a, b in zip(df["a"], df["b"])]

    def test_filter_then_derive_mul_small(self, small_table):
        """filter(a > 10) then derive(c = a * b) on small batch."""
        result = small_table.filter(lambda r: r.a > 10).derive(
            c=lambda r: r.a * r.b
        )
        df = result.to_pandas()
        assert all(df["a"] > 10)
        assert list(df["c"]) == [a * b for a, b in zip(df["a"], df["b"])]

    def test_filter_then_derive_sub_small(self, small_table):
        """filter(b > 50) then derive(c = b - a) on small batch."""
        result = small_table.filter(lambda r: r.b > 50).derive(
            c=lambda r: r.b - r.a
        )
        df = result.to_pandas()
        assert all(df["b"] > 50)
        assert list(df["c"]) == [b - a for a, b in zip(df["a"], df["b"])]

    def test_filter_none_pass_small(self, small_table):
        """filter that removes all rows, then derive — should return empty."""
        result = small_table.filter(lambda r: r.a > 10000).derive(
            c=lambda r: r.a + r.b
        )
        df = result.to_pandas()
        assert len(df) == 0

    def test_filter_all_pass_small(self, small_table):
        """filter that keeps all rows, then derive."""
        result = small_table.filter(lambda r: r.a >= 0).derive(
            c=lambda r: r.a + r.b
        )
        df = result.to_pandas()
        assert len(df) == 100
        assert list(df["c"]) == [a + b for a, b in zip(df["a"], df["b"])]

    def test_filter_then_derive_preserves_passthrough_cols(self, small_table):
        """Pass-through columns (not GPU-computed) are preserved correctly."""
        result = small_table.filter(lambda r: r.a > 20).derive(
            c=lambda r: r.a + r.b
        )
        df = result.to_pandas()
        # 'a' and 'b' should still be present and unchanged
        assert "a" in df.columns
        assert "b" in df.columns
        assert "c" in df.columns
        assert all(df["a"] > 20)

    # ── Correctness on large (GPU-path) batches ──────────────────────────

    def test_filter_then_derive_large_i64(self, large_table):
        """filter(a > 4000) then derive(c = a + b) on large batch."""
        result = large_table.filter(lambda r: r.a > 4000).derive(
            c=lambda r: r.a + r.b
        )
        df = result.to_pandas()
        assert all(df["a"] > 4000)
        assert list(df["c"]) == [a + b for a, b in zip(df["a"], df["b"])]

    def test_filter_then_derive_mul_large(self, large_table):
        """filter(a < 4096) then derive(c = a * 3) on large batch."""
        result = large_table.filter(lambda r: r.a < 4096).derive(
            c=lambda r: r.a * 3
        )
        df = result.to_pandas()
        assert all(df["a"] < 4096)
        assert list(df["c"]) == [a * 3 for a in df["a"]]

    def test_filter_none_pass_large(self, large_table):
        """filter that removes all rows on large batch — empty result."""
        result = large_table.filter(lambda r: r.a > 100000).derive(
            c=lambda r: r.a + r.b
        )
        df = result.to_pandas()
        assert len(df) == 0

    def test_filter_all_pass_large(self, large_table):
        """filter that keeps all rows on large batch."""
        n = 8192
        result = large_table.filter(lambda r: r.a >= 0).derive(
            c=lambda r: r.a + r.b
        )
        df = result.to_pandas()
        assert len(df) == n
        assert list(df["c"]) == [a + b for a, b in zip(df["a"], df["b"])]

    def test_compound_filter_then_derive_large(self, large_table):
        """Compound AND predicate fused with derive on large batch."""
        result = large_table.filter(
            lambda r: (r.a > 1000) & (r.b < 12000)
        ).derive(c=lambda r: r.a + r.b)
        df = result.to_pandas()
        assert all(df["a"] > 1000)
        assert all(df["b"] < 12000)
        assert list(df["c"]) == [a + b for a, b in zip(df["a"], df["b"])]

    # ── i32 type ─────────────────────────────────────────────────────────

    def test_filter_then_derive_i32(self):
        """Fused pipeline works on i32 columns."""
        n = 200
        arrow = pa.table({
            "x": pa.array(list(range(n)), type=pa.int32()),
            "y": pa.array([i + 1 for i in range(n)], type=pa.int32()),
        })
        t = LTSeq.from_arrow(arrow)
        result = t.filter(lambda r: r.x > 100).derive(z=lambda r: r.x + r.y)
        df = result.to_pandas()
        assert all(df["x"] > 100)
        assert list(df["z"]) == [x + y for x, y in zip(df["x"], df["y"])]

    # ── Chained operations ────────────────────────────────────────────────

    def test_derive_then_filter_then_derive(self, small_table):
        """derive → filter → derive chain exercises multiple fusion opportunities."""
        result = (
            small_table
            .derive(c=lambda r: r.a + r.b)
            .filter(lambda r: r.c > 50)
            .derive(d=lambda r: r.c * 2)
        )
        df = result.to_pandas()
        assert all(df["c"] > 50)
        assert list(df["d"]) == [c * 2 for c in df["c"]]
