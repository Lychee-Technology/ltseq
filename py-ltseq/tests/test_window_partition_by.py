"""Tests for partition_by kwarg support on window functions (shift, diff, rolling, cum_sum).

Tests both string-literal form: shift(1, partition_by="grp")
and expression form: shift(1, partition_by=r.grp)

Note: partition_by may cause DataFusion to reorder rows by partition.
Tests sort by (grp, value) before asserting to ensure deterministic order.
"""

import os
import tempfile

import numpy as np
import pytest

from ltseq import LTSeq


@pytest.fixture
def grouped_table():
    """Create a table with two groups sorted by (grp, value).

    Group A: values 10, 20, 30
    Group B: values 100, 200, 300

    Sorted by grp, value so the rows are:
    A,10 | A,20 | A,30 | B,100 | B,200 | B,300
    """
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


def _sort_df(df):
    """Sort dataframe by grp and value for deterministic assertions."""
    return df.sort_values(["grp", "value"]).reset_index(drop=True)


class TestShiftPartitionBy:
    """Tests for shift() with partition_by kwarg."""

    def test_shift_positive_partition_by_string(self, grouped_table):
        """shift(1, partition_by='grp') should return NULL at group boundaries."""
        result = grouped_table.derive(
            lambda r: {"prev": r.value.shift(1, partition_by="grp")}
        )
        df = _sort_df(result.to_pandas())

        # Group A: NULL, 10, 20
        assert df["prev"].isna().iloc[0]
        assert df["prev"].iloc[1] == 10
        assert df["prev"].iloc[2] == 20

        # Group B: NULL (partition boundary!), 100, 200
        assert df["prev"].isna().iloc[3]
        assert df["prev"].iloc[4] == 100
        assert df["prev"].iloc[5] == 200

    def test_shift_positive_partition_by_expr(self, grouped_table):
        """shift(1, partition_by=r.grp) using expression form."""
        result = grouped_table.derive(
            lambda r: {"prev": r.value.shift(1, partition_by=r.grp)}
        )
        df = _sort_df(result.to_pandas())

        # Group B first row should be NULL (partition boundary), not 30 from group A
        assert df["prev"].isna().iloc[3]
        assert df["prev"].iloc[4] == 100

    def test_shift_negative_partition_by(self, grouped_table):
        """shift(-1, partition_by='grp') = LEAD with partition."""
        result = grouped_table.derive(
            lambda r: {"next_val": r.value.shift(-1, partition_by="grp")}
        )
        df = _sort_df(result.to_pandas())

        # Group A: 20, 30, NULL
        assert df["next_val"].iloc[0] == 20
        assert df["next_val"].iloc[1] == 30
        assert df["next_val"].isna().iloc[2]

        # Group B: 200, 300, NULL (not leaking into next group)
        assert df["next_val"].iloc[3] == 200
        assert df["next_val"].iloc[4] == 300
        assert df["next_val"].isna().iloc[5]

    def test_shift_without_partition_by_crosses_groups(self, grouped_table):
        """Without partition_by, shift crosses group boundaries (baseline test)."""
        result = grouped_table.derive(
            lambda r: {"prev": r.value.shift(1)}
        )
        df = result.to_pandas()
        # No sort needed: without partition_by, DataFusion preserves sort order

        # Group B first row gets group A's last value (30) without partition_by
        assert df["prev"].iloc[3] == 30

    def test_shift_with_default_and_partition_by(self, grouped_table):
        """shift(1, default=0, partition_by='grp') should fill with default at boundaries."""
        result = grouped_table.derive(
            lambda r: {"prev": r.value.shift(1, default=0, partition_by="grp")}
        )
        df = _sort_df(result.to_pandas())

        # First row of each group gets default=0
        assert df["prev"].iloc[0] == 0  # Group A first
        assert df["prev"].iloc[3] == 0  # Group B first


class TestDiffPartitionBy:
    """Tests for diff() with partition_by kwarg."""

    def test_diff_partition_by(self, grouped_table):
        """diff(1, partition_by='grp') should reset at group boundaries."""
        result = grouped_table.derive(
            lambda r: {"d": r.value.diff(1, partition_by="grp")}
        )
        df = _sort_df(result.to_pandas())

        # Group A: NULL, 10, 10
        assert df["d"].isna().iloc[0]
        assert df["d"].iloc[1] == 10
        assert df["d"].iloc[2] == 10

        # Group B: NULL (not 100-30=70), 100, 100
        assert df["d"].isna().iloc[3]
        assert df["d"].iloc[4] == 100
        assert df["d"].iloc[5] == 100

    def test_diff_without_partition_by_crosses_groups(self, grouped_table):
        """Without partition_by, diff crosses group boundaries."""
        result = grouped_table.derive(
            lambda r: {"d": r.value.diff(1)}
        )
        df = result.to_pandas()

        # Group B first row: 100 - 30 = 70 (crosses from group A)
        assert df["d"].iloc[3] == 70


class TestRollingPartitionBy:
    """Tests for rolling().agg() with partition_by kwarg."""

    def test_rolling_sum_partition_by(self, grouped_table):
        """rolling(2, partition_by='grp').sum() should not cross group boundaries."""
        result = grouped_table.derive(
            lambda r: {"rsum": r.value.rolling(2, partition_by="grp").sum()}
        )
        df = _sort_df(result.to_pandas())

        # Group A: 10, 30, 50
        assert df["rsum"].iloc[0] == 10  # Only 1 row in window
        assert df["rsum"].iloc[1] == 30  # 10 + 20
        assert df["rsum"].iloc[2] == 50  # 20 + 30

        # Group B: 100 (only 1 row, NOT 30+100=130), 300, 500
        assert df["rsum"].iloc[3] == 100  # Partition boundary resets
        assert df["rsum"].iloc[4] == 300  # 100 + 200
        assert df["rsum"].iloc[5] == 500  # 200 + 300

    def test_rolling_mean_partition_by(self, grouped_table):
        """rolling(2, partition_by='grp').mean() should reset at boundaries."""
        result = grouped_table.derive(
            lambda r: {"rmean": r.value.rolling(2, partition_by="grp").mean()}
        )
        df = _sort_df(result.to_pandas())

        # Group B first: 100.0 (only 1 row), not avg(30, 100)=65
        assert df["rmean"].iloc[3] == 100.0


class TestCumSumPartitionBy:
    """Tests for cum_sum(partition_by='grp')."""

    def test_cum_sum_partition_by(self, grouped_table):
        """cum_sum(partition_by='grp') should reset at group boundaries."""
        result = grouped_table.derive(
            lambda r: {"cs": r.value.cum_sum(partition_by="grp")}
        )
        df = _sort_df(result.to_pandas())

        # Group A: 10, 30, 60
        assert df["cs"].iloc[0] == 10
        assert df["cs"].iloc[1] == 30
        assert df["cs"].iloc[2] == 60

        # Group B: 100 (resets!), 300, 600
        assert df["cs"].iloc[3] == 100
        assert df["cs"].iloc[4] == 300
        assert df["cs"].iloc[5] == 600

    def test_cum_sum_without_partition_by(self, grouped_table):
        """Without partition_by, cum_sum is global."""
        result = grouped_table.derive(
            lambda r: {"cs": r.value.cum_sum()}
        )
        df = result.to_pandas()

        # Global: 10, 30, 60, 160, 360, 660
        assert df["cs"].iloc[0] == 10
        assert df["cs"].iloc[1] == 30
        assert df["cs"].iloc[2] == 60
        assert df["cs"].iloc[3] == 160
        assert df["cs"].iloc[4] == 360
        assert df["cs"].iloc[5] == 660
