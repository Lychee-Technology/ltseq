"""Tests for the unified ``.over()`` surface on sequence windows (issue #117).

Sequence windows (shift/diff/cum_sum/cum_max/cum_min, rolling(n).agg()) historically
took partition via the ``partition_by=`` kwarg and ordering from the table sort. #117
lets them also accept ``.over(partition_by=, order_by=, desc=)``, sharing one window-spec
entry with the ranking windows.

Rule: window expressions default to table order; ``.over()`` overrides partition/order.

These tests assert:
- parity: ``.over(partition_by=r.grp)`` == the equivalent ``partition_by=`` kwarg
- ``.over(order_by=..., desc=...)`` overrides the table sort
- coexistence of ``.over(partition_by=)`` and the kwarg raises ValueError
- ``.over()`` on a non-window call still raises
"""

import os
import tempfile

import pytest

from ltseq import LTSeq


@pytest.fixture
def grouped_table():
    """Two groups sorted by (grp, value): A[10,20,30] | B[100,200,300]."""
    csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    csv.write("grp,value\n")
    csv.write("A,10\n")
    csv.write("A,20\n")
    csv.write("A,30\n")
    csv.write("B,100\n")
    csv.write("B,200\n")
    csv.write("B,300\n")
    csv.close()
    t = LTSeq.read_csv(csv.name).sort("grp", "value")
    yield t
    os.unlink(csv.name)


@pytest.fixture
def single_group_asc():
    """One group sorted ascending by value: [10, 20, 30]."""
    csv = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    csv.write("value\n")
    csv.write("10\n")
    csv.write("20\n")
    csv.write("30\n")
    csv.close()
    t = LTSeq.read_csv(csv.name).sort("value")
    yield t
    os.unlink(csv.name)


def _sorted(df):
    return df.sort_values(["grp", "value"]).reset_index(drop=True)


class TestOverPartitionByParity:
    """`.over(partition_by=r.grp)` must match the `partition_by=` kwarg result."""

    def test_shift_parity(self, grouped_table):
        result = grouped_table.derive(
            lambda r: {
                "kw": r.value.shift(1, partition_by="grp"),
                "ov": r.value.shift(1).over(partition_by=r.grp),
            }
        )
        df = _sorted(result.to_pandas())
        assert df["kw"].equals(df["ov"])  # NaN-aware column comparison
        # spot-check the partition boundary: group B first row is NULL, not 30
        assert df["ov"].isna().iloc[3]

    def test_diff_parity(self, grouped_table):
        result = grouped_table.derive(
            lambda r: {
                "kw": r.value.diff(1, partition_by="grp"),
                "ov": r.value.diff(1).over(partition_by=r.grp),
            }
        )
        df = _sorted(result.to_pandas())
        assert df["kw"].equals(df["ov"])  # NaN-aware column comparison

    def test_cum_sum_parity(self, grouped_table):
        result = grouped_table.derive(
            lambda r: {
                "kw": r.value.cum_sum(partition_by="grp"),
                "ov": r.value.cum_sum().over(partition_by=r.grp),
            }
        )
        df = _sorted(result.to_pandas())
        assert df["kw"].equals(df["ov"])  # NaN-aware column comparison

    def test_cum_max_parity(self, grouped_table):
        result = grouped_table.derive(
            lambda r: {
                "kw": r.value.cum_max(partition_by="grp"),
                "ov": r.value.cum_max().over(partition_by=r.grp),
            }
        )
        df = _sorted(result.to_pandas())
        assert df["kw"].equals(df["ov"])  # NaN-aware column comparison

    def test_cum_min_parity(self, grouped_table):
        result = grouped_table.derive(
            lambda r: {
                "kw": r.value.cum_min(partition_by="grp"),
                "ov": r.value.cum_min().over(partition_by=r.grp),
            }
        )
        df = _sorted(result.to_pandas())
        assert df["kw"].equals(df["ov"])  # NaN-aware column comparison

    def test_rolling_mean_parity(self, grouped_table):
        result = grouped_table.derive(
            lambda r: {
                "kw": r.value.rolling(2, partition_by="grp").mean(),
                "ov": r.value.rolling(2).mean().over(partition_by=r.grp),
            }
        )
        df = _sorted(result.to_pandas())
        assert df["kw"].equals(df["ov"])  # NaN-aware column comparison


class TestOverOrderByOverride:
    """`.over(order_by=..., desc=...)` overrides the table sort."""

    def test_cum_sum_order_by_desc_overrides_table_sort(self, single_group_asc):
        # Table is sorted ascending [10,20,30]. cum_sum default (table order): 10,30,60.
        # over(order_by=value desc) accumulates largest->smallest, so each row's value
        # is the sum of all rows >= it: value=10 -> 60, value=20 -> 50, value=30 -> 30.
        result = single_group_asc.derive(
            lambda r: {
                "default": r.value.cum_sum(),
                "desc": r.value.cum_sum().over(order_by=r.value, desc=True),
            }
        )
        df = result.to_pandas().sort_values("value").reset_index(drop=True)
        assert df["default"].tolist() == [10, 30, 60]
        # value=10 row now sees the whole descending prefix
        assert df["desc"].iloc[0] == 60  # value=10
        assert df["desc"].iloc[1] == 50  # value=20
        assert df["desc"].iloc[2] == 30  # value=30

    def test_shift_order_by_desc_overrides_table_sort(self, single_group_asc):
        # Descending order sequence is 30,20,10; lag(1) -> 30:NULL, 20:30, 10:20.
        result = single_group_asc.derive(
            lambda r: {"prev": r.value.shift(1).over(order_by=r.value, desc=True)}
        )
        df = result.to_pandas().sort_values("value").reset_index(drop=True)
        # value=30 is first in desc order -> NULL
        assert df["prev"].isna().iloc[2]
        assert df["prev"].iloc[1] == 30  # value=20 -> prev is 30
        assert df["prev"].iloc[0] == 20  # value=10 -> prev is 20

    def test_over_partition_and_order_by(self, grouped_table):
        # partition by grp, order by value desc: cum_sum within each group, largest first.
        result = grouped_table.derive(
            lambda r: {"cs": r.value.cum_sum().over(partition_by=r.grp, order_by=r.value, desc=True)}
        )
        df = _sorted(result.to_pandas())
        # Group A [10,20,30] desc cumsum: value=30->30, 20->50, 10->60
        assert df["cs"].iloc[0] == 60  # A,10
        assert df["cs"].iloc[1] == 50  # A,20
        assert df["cs"].iloc[2] == 30  # A,30
        # Group B resets: value=300->300, 200->500, 100->600
        assert df["cs"].iloc[3] == 600  # B,100
        assert df["cs"].iloc[5] == 300  # B,300


class TestOverCoexistenceError:
    """Passing partition_by via both `.over()` and the kwarg is an error."""

    def test_shift_both_partition_by_raises(self, grouped_table):
        with pytest.raises(ValueError):
            grouped_table.derive(
                lambda r: {"x": r.value.shift(1, partition_by="grp").over(partition_by=r.grp)}
            )

    def test_cum_sum_both_partition_by_raises(self, grouped_table):
        with pytest.raises(ValueError):
            grouped_table.derive(
                lambda r: {"x": r.value.cum_sum(partition_by="grp").over(partition_by=r.grp)}
            )

    def test_rolling_both_partition_by_raises(self, grouped_table):
        # kwarg lives on the inner rolling() call
        with pytest.raises(ValueError):
            grouped_table.derive(
                lambda r: {
                    "x": r.value.rolling(2, partition_by="grp").mean().over(partition_by=r.grp)
                }
            )

    def test_over_order_by_without_kwarg_partition_ok(self, grouped_table):
        # order_by via over + partition via kwarg is NOT a partition conflict -> allowed
        result = grouped_table.derive(
            lambda r: {"x": r.value.shift(1, partition_by="grp").over(order_by=r.value)}
        )
        assert len(result.to_pandas()) == 6


class TestOverNonWindowGuard:
    """`.over()` on a non-window call still raises."""

    def test_over_on_abs_raises(self, grouped_table):
        with pytest.raises(ValueError):
            grouped_table.derive(lambda r: {"x": r.value.abs().over(partition_by=r.grp)})
