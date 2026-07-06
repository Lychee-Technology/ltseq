"""Behavior-locking tests for rvs() and step() (issue #91 PR 6).

These ops had zero test coverage; the assertions below were captured
against the legacy SQL implementation before the native migration, so the
migration must reproduce them exactly.
"""

import pandas as pd
import pytest

from ltseq import LTSeq


@pytest.fixture
def seq_table():
    df = pd.DataFrame({"i": [0, 1, 2, 3, 4, 5, 6], "v": list("abcdefg")})
    return LTSeq.from_pandas(df)


class TestRvs:
    def test_rvs_reverses_rows(self, seq_table):
        df = seq_table.rvs().to_pandas()
        assert df["i"].tolist() == [6, 5, 4, 3, 2, 1, 0]
        assert df["v"].tolist() == list("gfedcba")

    def test_rvs_twice_restores_order(self, seq_table):
        df = seq_table.rvs().rvs().to_pandas()
        assert df["i"].tolist() == [0, 1, 2, 3, 4, 5, 6]

    def test_rvs_preserves_schema(self, seq_table):
        result = seq_table.rvs()
        assert result.columns == ["i", "v"]

    def test_rvs_clears_sort_keys(self, seq_table):
        result = seq_table.sort("i").rvs()
        assert result.sort_keys is None

    def test_rvs_single_row(self):
        t = LTSeq.from_pandas(pd.DataFrame({"x": [42]}))
        assert t.rvs().to_pandas()["x"].tolist() == [42]


class TestStep:
    def test_step_takes_every_nth_zero_based(self, seq_table):
        """step(n) keeps rows 0, n, 2n, ... (0-based)."""
        df = seq_table.step(3).to_pandas()
        assert df["i"].tolist() == [0, 3, 6]

    def test_step_one_is_identity(self, seq_table):
        df = seq_table.step(1).to_pandas()
        assert df["i"].tolist() == [0, 1, 2, 3, 4, 5, 6]

    def test_step_larger_than_table(self, seq_table):
        df = seq_table.step(100).to_pandas()
        assert df["i"].tolist() == [0]

    def test_step_preserves_row_order(self, seq_table):
        df = seq_table.step(2).to_pandas()
        assert df["i"].tolist() == [0, 2, 4, 6]

    def test_step_invalid_n_raises(self, seq_table):
        with pytest.raises(Exception):
            seq_table.step(0)
