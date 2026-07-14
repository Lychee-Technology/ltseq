"""derive() replace-or-add semantics, rolling() argument validation, and
UTF-8-safe repr truncation (issue #125 findings 4/8/9).

derive() with an existing column name must REPLACE that column in place
(Polars with_columns / pandas assign semantics) instead of appending a
duplicate column; overwriting a sort key invalidates the declared order
from that key onward. rolling() must reject degenerate window sizes and
unknown kwargs instead of silently accepting them. show()/repr must not
panic on long multi-byte values.
"""

import pandas as pd
import pytest

from ltseq import LTSeq


@pytest.fixture
def t3():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [10.0, 20.0, 30.0], "c": ["x", "y", "z"]})
    return LTSeq.from_pandas(df)


class TestDeriveOverwritePlain:
    def test_overwrite_replaces_value(self, t3):
        result = t3.derive(b=lambda r: r.b * 2).to_pandas()
        assert result["b"].tolist() == [20.0, 40.0, 60.0]

    def test_overwrite_keeps_column_count_and_order(self, t3):
        result = t3.derive(b=lambda r: r.b * 2).to_pandas()
        assert list(result.columns) == ["a", "b", "c"]

    def test_mixed_overwrite_and_add(self, t3):
        result = t3.derive(b=lambda r: r.b + 1, d=lambda r: r.a * 10).to_pandas()
        assert list(result.columns) == ["a", "b", "c", "d"]
        assert result["b"].tolist() == [11.0, 21.0, 31.0]
        assert result["d"].tolist() == [10, 20, 30]

    def test_overwrite_referencing_own_old_value(self, t3):
        """The derived expression reads the ORIGINAL column (snapshot
        semantics, like Polars with_columns)."""
        result = t3.derive(a=lambda r: r.a + 100).to_pandas()
        assert result["a"].tolist() == [101, 102, 103]


class TestDeriveOverwriteWindow:
    def test_window_expression_overwrite(self, t3):
        """Fast path: single-layer window expression overwriting a column."""
        result = t3.sort("a").derive(b=lambda r: r.b.shift(1)).to_pandas()
        assert list(result.columns) == ["a", "b", "c"]
        assert pd.isna(result["b"].iloc[0])
        assert result["b"].tolist()[1:] == [10.0, 20.0]

    def test_staged_nested_window_overwrite(self, t3):
        """Staged path: nested window expression overwriting a column."""
        result = t3.sort("a").derive(b=lambda r: r.b.shift(1).shift(1)).to_pandas()
        assert list(result.columns) == ["a", "b", "c"]
        assert pd.isna(result["b"].iloc[0]) and pd.isna(result["b"].iloc[1])
        assert result["b"].iloc[2] == 10.0

    def test_mixed_window_overwrite_and_add(self, t3):
        result = (
            t3.sort("a")
            .derive(b=lambda r: r.b.shift(1), prev_a=lambda r: r.a.shift(1))
            .to_pandas()
        )
        assert list(result.columns) == ["a", "b", "c", "prev_a"]


class TestDeriveOverwriteSortKey:
    def test_overwriting_sort_key_truncates_specs(self, t3):
        """After overwriting a sort key its values changed — the declared
        order is no longer valid from that key onward."""
        t = t3.sort("a", "b").derive(a=lambda r: r.b * -1)
        assert t._inner.get_sort_keys() == []

    def test_overwriting_later_sort_key_keeps_prefix(self, t3):
        t = t3.sort("a", "b").derive(b=lambda r: r.a * -1.0)
        assert t._inner.get_sort_keys() == [("a", False)]

    def test_overwriting_non_sort_column_keeps_specs(self, t3):
        t = t3.sort("a", "b").derive(c=lambda r: r.c)
        assert t._inner.get_sort_keys() == [("a", False), ("b", False)]

    def test_window_overwrite_of_sort_key_truncates(self, t3):
        t = t3.sort("a").derive(a=lambda r: r.b.shift(1))
        assert t._inner.get_sort_keys() == []

    def test_adding_new_column_keeps_specs(self, t3):
        t = t3.sort("a", "b").derive(d=lambda r: r.a + 1)
        assert t._inner.get_sort_keys() == [("a", False), ("b", False)]


class TestRollingValidation:
    def test_rolling_zero_rejected(self, t3):
        with pytest.raises(ValueError, match="window size|window_size"):
            t3.sort("a").derive(m=lambda r: r.b.rolling(0).mean())

    def test_rolling_negative_rejected(self, t3):
        with pytest.raises(ValueError, match="window size|window_size"):
            t3.sort("a").derive(m=lambda r: r.b.rolling(-1).mean())

    def test_min_periods_rejected_with_specific_message(self, t3):
        with pytest.raises(ValueError, match="min_periods"):
            t3.sort("a").derive(m=lambda r: r.b.rolling(3, min_periods=2).mean())

    def test_unknown_kwarg_rejected_by_name(self, t3):
        with pytest.raises(ValueError, match="bogus"):
            t3.sort("a").derive(m=lambda r: r.b.rolling(3, bogus=1).mean())

    def test_valid_rolling_still_works(self, t3):
        result = t3.sort("a").derive(m=lambda r: r.b.rolling(2).sum()).to_pandas()
        assert result["m"].tolist() == [10.0, 30.0, 50.0]

    def test_valid_rolling_partition_by_still_works(self):
        df = pd.DataFrame({"g": ["A", "A", "B", "B"], "v": [1.0, 2.0, 3.0, 4.0]})
        t = LTSeq.from_pandas(df).sort("g")
        result = t.derive(
            s=lambda r: r.v.rolling(2, partition_by="g").sum()
        ).to_pandas()
        assert result["s"].tolist() == [1.0, 3.0, 3.0, 7.0]


class TestReprUtf8Safety:
    def test_long_chinese_value_does_not_panic(self):
        df = pd.DataFrame({"s": ["数据序列处理库" * 12]})
        t = LTSeq.from_pandas(df)
        t.show()  # printing path
        assert "..." in repr(t)

    def test_long_emoji_value_does_not_panic(self):
        df = pd.DataFrame({"s": ["🚀🎉🔥💡🌟" * 15]})
        t = LTSeq.from_pandas(df)
        repr(t)

    def test_long_combining_chars_do_not_panic(self):
        df = pd.DataFrame({"s": ["éàô" * 20]})
        t = LTSeq.from_pandas(df)
        repr(t)

    def test_truncation_still_applies_to_long_ascii(self):
        df = pd.DataFrame({"s": ["x" * 80]})
        t = LTSeq.from_pandas(df)
        assert "..." in repr(t)
