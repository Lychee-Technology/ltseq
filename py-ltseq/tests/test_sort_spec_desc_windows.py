"""Regression tests for issue #95: window functions on DESC-sorted tables.

sort_exprs used to store bare column names, losing direction; window ORDER BY
was rebuilt with hardcoded asc=true, so shift/diff/cum_sum/rolling computed
ASC semantics on DESC-sorted tables.
"""

import os
import tempfile

import pandas as pd
import pytest

from ltseq import LTSeq


@pytest.fixture
def desc_table():
    df = pd.DataFrame({"k": [1, 2, 3, 4, 5], "v": [10.0, 20.0, 30.0, 40.0, 50.0]})
    return LTSeq.from_pandas(df).sort("k", desc=True)


@pytest.fixture
def mixed_table():
    """Multi-column sort: grp ASC, k DESC. Rows: (A,5),(A,3),(A,1),(B,4),(B,2)"""
    df = pd.DataFrame({
        "grp": ["A", "A", "A", "B", "B"],
        "k":   [5, 3, 1, 4, 2],
        "v":   [50.0, 30.0, 10.0, 40.0, 20.0],
    })
    return LTSeq.from_pandas(df).sort("grp", "k", desc=[False, True])


class TestDescShift:
    def test_desc_shift_1(self, desc_table):
        """shift(1) on DESC-sorted table: k=4→prev=50, k=3→prev=40"""
        result = desc_table.derive(lambda r: {"prev": r.v.shift(1)})
        df = result.to_pandas().sort_values("k", ascending=False).reset_index(drop=True)
        expected_prev = [None, 50.0, 40.0, 30.0, 20.0]
        assert df["v"].tolist() == [50.0, 40.0, 30.0, 20.0, 10.0]
        for i, (actual, expected) in enumerate(zip(df["prev"].tolist(), expected_prev)):
            if expected is None:
                assert pd.isna(actual), f"index {i}: expected NaN, got {actual}"
            else:
                assert actual == expected, f"index {i}: expected {expected}, got {actual}"

    def test_desc_diff_1(self, desc_table):
        """diff(1) on DESC: each step = -10"""
        result = desc_table.derive(lambda r: {"d": r.v.diff(1)})
        df = result.to_pandas().sort_values("k", ascending=False).reset_index(drop=True)
        expected_diff = [None, -10.0, -10.0, -10.0, -10.0]
        for i, (actual, expected) in enumerate(zip(df["d"].tolist(), expected_diff)):
            if expected is None:
                assert pd.isna(actual), f"index {i}: expected NaN, got {actual}"
            else:
                assert actual == expected, f"index {i}: expected {expected}, got {actual}"

    def test_desc_cum_sum(self, desc_table):
        """cum_sum on DESC: 50, 90, 120, 140, 150"""
        result = desc_table.cum_sum("v")
        df = result.to_pandas().sort_values("k", ascending=False).reset_index(drop=True)
        expected_v_cumsum = [50.0, 90.0, 120.0, 140.0, 150.0]
        assert df["v_cumsum"].tolist() == expected_v_cumsum

    def test_desc_rolling_sum(self, desc_table):
        """rolling(2).sum() on DESC: 50, 90, 70, 50, 30"""
        result = desc_table.derive(lambda r: {"rsum": r.v.rolling(2).sum()})
        df = result.to_pandas().sort_values("k", ascending=False).reset_index(drop=True)
        expected = [50.0, 90.0, 70.0, 50.0, 30.0]
        assert df["rsum"].tolist() == expected


class TestMultiColumnMixedDirection:
    def test_shift_on_mixed_direction(self, mixed_table):
        """shift(1) respects multi-col order: grp ASC, k DESC"""
        result = mixed_table.derive(lambda r: {"prev": r.v.shift(1)})
        df = result.to_pandas()
        # Sort to match table order: grp=A,k=5 first; then A,3; A,1; B,4; B,2
        df_sorted = df.sort_values(["grp", "k"], ascending=[True, False]).reset_index(drop=True)
        # Expected prev over the whole sequence (no partitioning): the row
        # before B,4 in table order is A,1 (v=10).
        expected_prev = [None, 50.0, 30.0, 10.0, 40.0]
        for i, (actual, expected) in enumerate(zip(df_sorted["prev"].tolist(), expected_prev)):
            if expected is None:
                assert pd.isna(actual), f"index {i}: expected NaN, got {actual}"
            else:
                assert actual == expected, f"index {i}: expected {expected}, got {actual}"


class TestAssumeSortedDesc:
    def test_assume_sorted_desc_shift(self):
        """assume_sorted('k', desc=True) followed by shift(1) works correctly"""
        import pyarrow as pa
        import pyarrow.parquet as pq

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        t = pa.table({"k": [5, 4, 3, 2, 1], "v": [50.0, 40.0, 30.0, 20.0, 10.0]})
        pq.write_table(t, path)
        try:
            table = LTSeq.read_parquet(path).assume_sorted("k", desc=True)
            result = table.derive(lambda r: {"prev": r.v.shift(1)})
            df = result.to_pandas().sort_values("k", ascending=False).reset_index(drop=True)
            expected_prev = [None, 50.0, 40.0, 30.0, 20.0]
            for i, (actual, expected) in enumerate(zip(df["prev"].tolist(), expected_prev)):
                if expected is None:
                    assert pd.isna(actual), f"index {i}: expected NaN, got {actual}"
                else:
                    assert actual == expected, f"index {i}: expected {expected}, got {actual}"
        finally:
            os.unlink(path)
