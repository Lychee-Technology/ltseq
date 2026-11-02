"""`/` on two integers is Python's true division on every path (#218).

DataFusion's `/` truncates integers toward zero and keeps the integer type,
so `7 / 2` was `3`. The kernel is in `src/transpiler/true_div.rs`; the
DataFusion plan, the `group_ordered(...).first().count()` kernel and the
`search_pattern` evaluator all go through it. These tests pin it end to end:
both signs, NULLs and zero divisors, for Int32, Int64 and UInt64 columns,
integer literals, and an integer column next to a float. `pct_change`, which
divides too, keeps IEEE division and matches pandas.
"""

import math
import random

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import pandas as pd
import pytest

from ltseq import LTSeq, if_else

# Every sign combination, inexact and exact quotients, and zero dividends.
XS = [7, -7, 7, -7, 1, 6, 0, 0]
YS = [2, 2, -2, -2, 3, 3, 5, -5]


def _table(xs=XS, ys=YS, type=pa.int64()):
    return pa.table(
        {
            "i": pa.array(range(len(xs)), pa.int64()),
            "x": pa.array(xs, type),
            "y": pa.array(ys, type),
        }
    )


def _col(t, name="v"):
    return t.to_arrow().column(name).to_pylist()


def _same_floats(got, expected):
    """Equal values, NULLs, NaNs and signs of zero."""
    assert len(got) == len(expected)
    for g, e in zip(got, expected):
        if e is None or g is None:
            assert g is e, (got, expected)
        elif math.isnan(e):
            assert math.isnan(g), (got, expected)
        else:
            assert g == e and math.copysign(1, g) == math.copysign(1, e), (got, expected)


class TestPythonSemantics:
    @pytest.mark.parametrize("type", [pa.int32(), pa.int64()], ids=str)
    @pytest.mark.parametrize(
        "fn, reference",
        [
            (lambda r: r.x / r.y, lambda x, y: x / y),
            (lambda r: r.x / 2, lambda x, y: x / 2),
            (lambda r: r.x / -2, lambda x, y: x / -2),
            (lambda r: 100 / r.y, lambda x, y: 100 / y),
            (lambda r: -100 / r.y, lambda x, y: -100 / y),
        ],
        ids=["col-col", "col-lit", "col-neg-lit", "lit-col", "neg-lit-col"],
    )
    def test_signed(self, type, fn, reference):
        result = LTSeq.from_arrow(_table(type=type)).derive(v=fn).to_arrow()
        assert result.schema.field("v").type == pa.float64()
        _same_floats(result.column("v").to_pylist(), [reference(x, y) for x, y in zip(XS, YS)])

    def test_issue_examples(self):
        t = LTSeq.from_arrow(_table([7, -7, 7, -7], [2, 2, -2, -2]))
        assert _col(t.derive(v=lambda r: r.x / r.y)) == [3.5, -3.5, -3.5, 3.5]

    def test_uint64(self):
        us = [2**64 - 1, 2**63, 7, 0]
        t = LTSeq.from_arrow(
            pa.table({"u": pa.array(us, pa.uint64()), "w": pa.array([3, 2, 2, 9], pa.uint64())})
        )
        for fn, reference in [
            (lambda r: r.u / r.w, lambda u, w: u / w),
            (lambda r: r.u / 3, lambda u, w: u / 3),
            (lambda r: r.u / -3, lambda u, w: u / -3),
        ]:
            result = t.derive(v=fn).to_arrow()
            assert result.schema.field("v").type == pa.float64()
            _same_floats(
                result.column("v").to_pylist(),
                [reference(u, w) for u, w in zip(us, [3, 2, 2, 9])],
            )

    @pytest.mark.parametrize(
        "left, right",
        [
            (pa.int8(), pa.int8()),
            (pa.int16(), pa.uint8()),
            (pa.int32(), pa.int64()),
            (pa.uint32(), pa.uint64()),
            (pa.uint64(), pa.int64()),
        ],
    )
    def test_every_integer_pair_gives_float64(self, left, right):
        t = LTSeq.from_arrow(pa.table({"a": pa.array([7, 9], left), "b": pa.array([2, 4], right)}))
        result = t.derive(v=lambda r: r.a / r.b).to_arrow()
        assert result.schema.field("v").type == pa.float64()
        assert result.column("v").to_pylist() == [3.5, 2.25]

    def test_schema_reports_float64(self):
        t = LTSeq.from_arrow(_table()).derive(v=lambda r: r.x / r.y)
        assert t.schema["v"] == "float64"

    def test_int_and_float_operands_keep_float_division(self):
        t = LTSeq.from_arrow(pa.table({"x": [7, -7, 1, 0], "f": [2.0, 2.0, 0.0, 0.0]}))
        result = t.derive(a=lambda r: r.x / r.f, b=lambda r: r.x / 2.0, c=lambda r: r.f / r.x.fill_null(1)).to_arrow()
        assert result.schema.field("a").type == pa.float64()
        # IEEE: a zero float divisor is inf or NaN, not an error.
        _same_floats(result.column("a").to_pylist(), [3.5, -3.5, math.inf, math.nan])
        assert result.column("b").to_pylist() == [3.5, -3.5, 0.5, 0.0]

    def test_column_free_constant(self):
        # Folded by DataFusion; it used to be the Int64 3 (#193).
        from ltseq.expr.types import LiteralExpr

        t = LTSeq.from_arrow(_table())
        assert _col(t.derive(v=lambda r: LiteralExpr(7) / 2))[0] == 3.5

    def test_dictionary_encoded_column(self):
        codes = pa.array([7, -7, 9], pa.int64()).dictionary_encode()
        t = LTSeq.from_arrow(pa.table({"x": codes}))
        assert _col(t.derive(v=lambda r: r.x / 2)) == [3.5, -3.5, 4.5]

    def test_large_operands_round_once(self):
        # Python divides the exact integers and rounds once. Converting
        # 2**54 + 3 to float64 first rounds it to 2**54 + 4, and then the
        # quotient by 3 rounds to a different float.
        xs = [2**54 + 3, 2**63 - 1, -(2**63), 2**53 + 1, -(2**62) - 7]
        ys = [3, 7, -3, 2, 9_007_199_254_740_993]
        assert (2**54 + 3) / 3 != float(2**54 + 3) / 3
        t = LTSeq.from_arrow(_table(xs, ys))
        assert _col(t.derive(v=lambda r: r.x / r.y)) == [x / y for x, y in zip(xs, ys)]

    def test_random_operands_match_python(self):
        rng = random.Random(218)
        xs, ys = [], []
        for _ in range(2000):
            xs.append(rng.randint(-(2**63), 2**63 - 1) >> rng.randrange(64))
            ys.append((rng.randint(-(2**63), 2**63 - 1) >> rng.randrange(64)) or 1)
        t = LTSeq.from_arrow(_table(xs, ys))
        assert _col(t.derive(v=lambda r: r.x / r.y)) == [x / y for x, y in zip(xs, ys)]

        us = [rng.randrange(2**64) >> rng.randrange(64) for _ in range(2000)]
        ws = [(rng.randrange(2**64) >> rng.randrange(64)) or 1 for _ in range(2000)]
        u = LTSeq.from_arrow(pa.table({"u": pa.array(us, pa.uint64()), "w": pa.array(ws, pa.uint64())}))
        assert _col(u.derive(v=lambda r: r.u / r.w)) == [a / b for a, b in zip(us, ws)]

    @pytest.mark.parametrize("type", [pa.int32(), pa.int64(), pa.uint64()], ids=str)
    def test_null_operands_give_null(self, type):
        # The NULL divisor's slot holds 0 in the Arrow buffer: it must give
        # NULL, not a divide-by-zero error.
        t = LTSeq.from_arrow(
            pa.table({"x": pa.array([None, 7, 7], type), "y": pa.array([2, None, 2], type)})
        )
        assert _col(t.derive(v=lambda r: r.x / r.y)) == [None, None, 3.5]
        result = t.derive(v=lambda r: r.x / None).to_arrow()
        assert result.column("v").to_pylist() == [None, None, None]
        assert result.schema.field("v").type == pa.float64()

    def test_filter_and_search_first(self):
        t = LTSeq.from_arrow(_table()).sort("i")
        pred = lambda r: r.x / r.y == -3.5  # noqa: E731
        assert _col(t.filter(pred), "i") == [1, 2]  # truncation gave -3
        assert _col(t.search_first(pred), "i") == [1]
        assert _col(t.filter(lambda r: r.x / r.y > 3), "i") == [0, 3]

    def test_readme_daily_return_on_integer_csv(self, tmp_path):
        # README "Financial metrics": `read_csv` infers int64 for whole
        # numbers. It used to give [None, 0, 0, 0].
        path = tmp_path / "daily_prices.csv"
        path.write_text("date,close\n2024-01-01,100\n2024-01-02,110\n2024-01-03,99\n2024-01-04,99\n")
        t = LTSeq.read_csv(str(path))
        assert t.to_arrow().schema.field("close").type == pa.int64()
        result = t.sort(lambda r: r.date).derive(
            lambda r: {"daily_return": (r.close - r.close.shift(1)) / r.close.shift(1)}
        )
        close = pacsv.read_csv(str(path)).column("close").to_pandas()
        expected = ((close - close.shift(1)) / close.shift(1)).tolist()
        got = _col(result, "daily_return")
        assert got[0] is None and math.isnan(expected[0])
        assert got[1:] == expected[1:] == [0.1, -0.1, 0.0]


class TestZeroDivisor:
    @pytest.mark.parametrize("type", [pa.int32(), pa.int64(), pa.uint64()], ids=str)
    @pytest.mark.parametrize(
        "fn",
        [lambda r: r.x / 0, lambda r: r.x / (r.y - r.y), lambda r: 0 / (r.y - r.y)],
        ids=["int-literal", "int-column", "zero-by-zero"],
    )
    def test_raises(self, type, fn):
        # Python raises ZeroDivisionError for int / 0. A float cast would
        # give inf or NaN.
        t = LTSeq.from_arrow(pa.table({"x": pa.array([7, 8], type), "y": pa.array([1, 2], type)}))
        with pytest.raises(ValueError, match="Divide by zero"):
            t.derive(v=fn).to_arrow()

    def test_raises_in_filter(self):
        t = LTSeq.from_arrow(_table([1, 2], [1, 0]))
        with pytest.raises(ValueError, match="Divide by zero"):
            t.filter(lambda r: r.x / r.y > 0).to_arrow()

    def test_if_else_guards_a_zero_divisor(self):
        t = LTSeq.from_arrow(pa.table({"x": [7, 8, 9], "y": [2, 0, 3]}))
        assert _col(t.derive(v=lambda r: if_else(r.y != 0, r.x / r.y, 0.0))) == [3.5, 0.0, 3.0]

    def test_cast_restores_ieee(self):
        t = LTSeq.from_arrow(pa.table({"x": [1, -1, 0], "y": [0, 0, 0]}))
        _same_floats(_col(t.derive(v=lambda r: r.x / r.y.cast("float64"))), [math.inf, -math.inf, math.nan])


# Row 0 always starts a group. x / x.shift(1) exceeds 1.5 at rows 1, 3 and 5
# (2/1, 10/3, 20/11; 3/2 is exactly 1.5): 4 groups, where truncation gives 2,
# 3 and 1, so 3 groups. x / 4 grows by more than 1.5 at rows 3 and 5 (0.75 to
# 2.5, 2.75 to 5.0): 3 groups, where truncation (0 to 2, 2 to 5) gives the
# same rows, so `jump` checks that the kernel computes in Float64 at all.
GROUP_XS = [1, 2, 3, 10, 11, 20, 21]


def _jump(r):
    return (r.x / 4) - (r.x.shift(1) / 4) > 1.5


def _ratio(r):
    return r.x / r.x.shift(1) > 1.5


class TestExecutionPaths:
    def test_window_operand(self):
        t = LTSeq.from_arrow(_table()).sort("i")
        _same_floats(
            _col(t.derive(v=lambda r: r.x.shift(1) / 2)),
            [None] + [x / 2 for x in XS[:-1]],
        )

    def test_window_over(self):
        t = LTSeq.from_arrow(_table())
        got = _col(t.derive(v=lambda r: r.x.shift(1).over(order_by="i") / r.y))
        _same_floats(got, [None] + [x / y for x, y in zip(XS[:-1], YS[1:])])

    def test_group_ordered_window_path(self):
        t = LTSeq.from_arrow(_table(GROUP_XS, [1] * len(GROUP_XS))).sort("i")
        firsts = t.group_ordered(_ratio).first().to_arrow().column("x").to_pylist()
        assert firsts == [1, 2, 10, 20]

    @pytest.mark.parametrize("pred, groups", [(_ratio, 4), (_jump, 3)], ids=["ratio", "jump"])
    def test_count_kernel_takes_integer_division(self, pred, groups):
        # `_inner.group_ordered_count` raises instead of falling back, so
        # this runs the linear-scan kernel itself.
        t = LTSeq.from_arrow(_table(GROUP_XS, [1] * len(GROUP_XS))).sort("i")
        kernel = t._inner.group_ordered_count(t._capture_expr(pred))
        reference = len(t.group_ordered(pred).first().to_pandas())
        assert kernel == reference == groups

    def test_count_sorted_parquet(self, tmp_path):
        path = str(tmp_path / "x.parquet")
        pq.write_table(_table(GROUP_XS, [1] * len(GROUP_XS)), path, row_group_size=2)
        t = LTSeq.read_parquet(path).assume_sorted("i")
        assert t.group_ordered(_ratio).first().count() == 4

    def test_count_kernel_zero_divisor_raises(self):
        t = LTSeq.from_arrow(_table([1, 0, 3], [1, 1, 1])).sort("i")
        with pytest.raises(Exception, match="Divide by zero"):
            t.group_ordered(_ratio).first().count()

    # Step 1 at row i, step 2 at row i + 1: x / y == -3.5 holds at rows 1
    # and 2; x / y == 3.5 only at rows 0 and 3. Truncation matches neither.
    STEPS = (lambda r: r.x / r.y == -3.5, lambda r: r.x / r.y == 3.5)

    def test_search_pattern(self):
        t = LTSeq.from_arrow(_table()).sort("i")
        assert _col(t.search_pattern(*self.STEPS), "i") == [2]
        assert t.search_pattern_count(*self.STEPS) == 1

    def test_search_pattern_count_sorted_parquet(self, tmp_path):
        path = str(tmp_path / "x.parquet")
        pq.write_table(_table(), path)
        t = LTSeq.read_parquet(path).assume_sorted("i")
        assert t.search_pattern_count(*self.STEPS) == 1

    def test_search_pattern_zero_divisor_raises(self):
        t = LTSeq.from_arrow(_table([1, 2], [1, 0])).sort("i")
        with pytest.raises(RuntimeError, match="Divide by zero"):
            t.search_pattern(lambda r: r.x / r.y > 0, lambda r: r.x > 0).to_arrow()


def _pattern_rows(t, *steps):
    """Step-1 rows of `search_pattern`, checked against `search_pattern_count`."""
    rows = _col(t.search_pattern(*steps), "i")
    assert t.search_pattern_count(*steps) == len(rows)
    return rows


class TestSearchPatternOperands:
    """The pattern evaluator reads a Float64 quotient as `filter` does (#218).

    Integer `/` now gives Float64, so its result meets integer literals and
    columns, and its signed zeros meet comparisons: the evaluator takes
    DataFusion's operand coercion and its float comparisons.
    """

    # `filter` is the reference: its predicate runs in a DataFusion plan.
    PREDICATES = {
        "int_threshold": lambda r: r.x / 2 > 3,
        "int_threshold_reversed": lambda r: 4 > r.x / 2,
        "int_column": lambda r: r.x / 2 >= r.i,
        "plus_int": lambda r: (r.x / 2) + 1 == 5,
        "minus_int": lambda r: (r.x / 2) - 1 < 3,
        "times_int": lambda r: (r.x / 2) * 2 == 8,
        "div_int": lambda r: (r.x / 2) / 2 == 2.0,
        "floor_div_int": lambda r: (r.x / 2) // 2 == 2,
        "mod_int": lambda r: (r.x / 2) % 2 == 0.5,
    }

    @pytest.mark.parametrize("type", [pa.int32(), pa.int64(), pa.uint64()], ids=str)
    @pytest.mark.parametrize("name", list(PREDICATES))
    def test_mixed_operands_match_filter(self, type, name):
        pred = self.PREDICATES[name]
        t = LTSeq.from_arrow(_table([7, 8, 9], [1, 1, 1], type)).sort("i")
        expected = _col(t.filter(pred), "i")
        assert expected, "the predicate selects some row"
        assert _pattern_rows(t, pred) == expected
        assert _pattern_rows(t, pred, lambda r: r.x > 0) == [i for i in expected if i < 2]

    def test_mixed_operands_sorted_parquet(self, tmp_path):
        path = str(tmp_path / "x.parquet")
        pq.write_table(_table([7, 8, 9], [1, 1, 1]), path, row_group_size=1)
        t = LTSeq.read_parquet(path).assume_sorted("i")
        assert t.search_pattern_count(lambda r: r.x / 2 > 3, lambda r: (r.x / 2) + 1 >= 5) == 2

    # 0 / -2 is -0.0, which DataFusion compares as 0.0; 1 / 2 is 0.5.
    SIGNED_ZERO = {
        "eq": (lambda r: r.x / r.y == 0.0, [0]),
        "ne": (lambda r: r.x / r.y != 0.0, [1]),
        "lt": (lambda r: r.x / r.y < 0.0, []),
        "le": (lambda r: r.x / r.y <= 0.0, [0]),
        "gt": (lambda r: r.x / r.y > 0.0, [1]),
        "ge": (lambda r: r.x / r.y >= 0.0, [0, 1]),
        "eq_int": (lambda r: r.x / r.y == 0, [0]),
        "eq_column": (lambda r: r.x / r.y == r.x / 2, [0, 1]),
    }

    @pytest.mark.parametrize("name", list(SIGNED_ZERO))
    def test_signed_zero_compares_as_zero(self, name):
        pred, expected = self.SIGNED_ZERO[name]
        t = LTSeq.from_arrow(_table([0, 1], [-2, 2])).sort("i")
        assert _col(t.filter(pred), "i") == expected
        assert _pattern_rows(t, pred) == expected

    def test_signed_zero_in_a_pattern(self):
        t = LTSeq.from_arrow(_table([0, 1], [-2, 2])).sort("i")
        assert _pattern_rows(t, lambda r: r.x / r.y == 0.0, lambda r: r.x > 0) == [0]
        assert _pattern_rows(t, lambda r: r.x / r.y < 0.0, lambda r: r.x > 0) == []
        assert _pattern_rows(t, lambda r: r.x / r.y != 0.0, lambda r: r.x > 0) == []

    @staticmethod
    def _encoded(xs, ys, encode):
        """`_table(xs, ys)` with the columns in `encode` dictionary-encoded."""
        table = _table(xs, ys)
        for name in encode:
            index = table.schema.get_field_index(name)
            table = table.set_column(index, name, table.column(name).dictionary_encode())
        return table

    # x = [7, 8, 9, None], y = [2, 4, -3, 2]. Each predicate selects some
    # rows and not others; truncating division selects different ones.
    ENCODED = {
        "column_by_literal": lambda r: r.x / 2 > 3,
        "literal_by_column": lambda r: 18 / r.x > 2.2,
        "column_by_column": lambda r: r.x / r.y == 3.5,
        "negative_quotient": lambda r: r.x / r.y < -2.9,
        "quotient_is_null": lambda r: (r.x / r.y).is_null(),
    }

    @pytest.mark.parametrize("encode", [["x"], ["y"], ["x", "y"]], ids="+".join)
    @pytest.mark.parametrize("name", list(ENCODED))
    def test_encoded_integers_match_filter(self, encode, name):
        # R2-F1: a dictionary-encoded integer operand is true division too.
        pred = self.ENCODED[name]
        t = LTSeq.from_arrow(self._encoded([7, 8, 9, None], [2, 4, -3, 2], encode)).sort("i")
        expected = _col(t.filter(pred), "i")
        assert expected, "the predicate selects some row"
        assert _pattern_rows(t, pred) == expected
        assert _pattern_rows(t, pred, lambda r: r.i >= 0) == [i for i in expected if i < 3]

    def test_encoded_integers_reported_case(self):
        t = LTSeq.from_arrow(self._encoded([7, 8, 9], [1, 1, 1], ["x"])).sort("i")
        assert _col(t.filter(lambda r: r.x / 2 > 3), "i") == [0, 1, 2]
        assert _pattern_rows(t, lambda r: r.x / 2 > 3) == [0, 1, 2]

    @pytest.mark.parametrize("encode", [["x"], ["y"]], ids="+".join)
    def test_encoded_zero_divisor_raises(self, encode):
        t = LTSeq.from_arrow(self._encoded([1, 2], [1, 0], encode)).sort("i")
        with pytest.raises(RuntimeError, match="Divide by zero"):
            t.search_pattern(lambda r: r.x / r.y > 0, lambda r: r.x > 0).to_arrow()
        with pytest.raises(RuntimeError, match="Divide by zero"):
            t.search_pattern_count(lambda r: r.x / r.y > 0, lambda r: r.x > 0)

    def test_encoded_null_divisor_is_null(self):
        # A NULL divisor whose dictionary key slot holds a zero is NULL.
        t = LTSeq.from_arrow(self._encoded([1, 2], [None, 2], ["y"])).sort("i")
        assert _pattern_rows(t, lambda r: (r.x / r.y).is_null()) == [0]
        assert _pattern_rows(t, lambda r: r.x / r.y == 1.0) == [1]

    # R2-F2: a `None` operand of integer `/` makes the quotient NULL, as in
    # `filter`, rather than an Int64 / Boolean error.
    NULL_OPERAND = {
        "x_by_none_is_null": lambda r: (r.x / None).is_null(),
        "none_by_x_is_null": lambda r: (None / r.x).is_null(),
        "x_by_none_is_not_null": lambda r: (r.x / None).is_not_null(),
        "none_by_x_compared": lambda r: None / r.x > 0,
        "x_by_none_or_true": lambda r: (r.x / None > 0) | (r.i >= 0),
        "x_by_none_and_false": lambda r: ~((r.x / None > 0) & (r.i < 0)),
        "x_by_none_plus": lambda r: ((r.x / None) + 1).is_null(),
    }

    @pytest.mark.parametrize("type", [pa.int32(), pa.int64(), pa.uint64()], ids=str)
    @pytest.mark.parametrize("name", list(NULL_OPERAND))
    def test_null_operand_matches_filter(self, type, name):
        pred = self.NULL_OPERAND[name]
        t = LTSeq.from_arrow(_table([7, 0, 9], [1, 1, 1], type)).sort("i")
        expected = _col(t.filter(pred), "i")
        assert expected in ([], [0, 1, 2])
        assert _pattern_rows(t, pred) == expected
        assert _pattern_rows(t, pred, lambda r: r.x >= 0) == [i for i in expected if i < 2]

    def test_null_operand_selects_every_row(self):
        t = LTSeq.from_arrow(_table([7, 0, 9], [1, 1, 1])).sort("i")
        for pred in (lambda r: (r.x / None).is_null(), lambda r: (None / r.x).is_null()):
            assert _col(t.filter(pred), "i") == [0, 1, 2]
            assert _pattern_rows(t, pred) == [0, 1, 2]

    def test_null_operand_encoded_column(self):
        t = LTSeq.from_arrow(self._encoded([7, 0, 9], [1, 1, 1], ["x"])).sort("i")
        assert _pattern_rows(t, lambda r: (r.x / None).is_null()) == [0, 1, 2]
        assert _pattern_rows(t, lambda r: (None / r.x).is_null()) == [0, 1, 2]

    def test_null_operand_sorted_parquet(self, tmp_path):
        path = str(tmp_path / "x.parquet")
        pq.write_table(_table([7, 0, 9], [1, 1, 1]), path, row_group_size=1)
        t = LTSeq.read_parquet(path).assume_sorted("i")
        assert t.search_pattern_count(lambda r: (r.x / None).is_null(), lambda r: r.x >= 0) == 2


class TestPctChange:
    def test_integer_matches_pandas(self):
        values = [1, 2, None, 4, 5, 6]
        t = LTSeq.from_arrow(
            pa.table({"i": range(len(values)), "x": pa.array(values, pa.int64())})
        ).sort("i")
        result = t.derive(v=lambda r: r.x.pct_change()).to_arrow()
        assert result.schema.field("v").type == pa.float64()
        got = result.column("v").to_pylist()
        expected = pd.Series(values, dtype="Int64").astype("float64").pct_change(fill_method=None).tolist()
        assert got[0] is None and got[2] is None and got[3] is None
        assert [round(v, 12) for v in (got[1], got[4], got[5])] == [1.0, 0.25, 0.2]
        assert got[1] == pytest.approx(expected[1])
        assert got[4:] == pytest.approx(expected[4:])

    @pytest.mark.parametrize("type", [pa.int32(), pa.int64(), pa.uint64()], ids=str)
    def test_zero_base_is_ieee(self, type):
        # pandas and v0.5 §10.1: inf, and NaN for 0 -> 0. Integer `/` would
        # raise; pct_change does not.
        values = [0, 5, 0, 0, 3]
        t = LTSeq.from_arrow(pa.table({"i": range(len(values)), "x": pa.array(values, type)})).sort("i")
        _same_floats(_col(t.derive(v=lambda r: r.x.pct_change())), [None, math.inf, -1.0, math.nan, math.inf])

    def test_unsigned_decrease_is_negative(self):
        # The difference is taken in float64; in uint64 it would wrap.
        values = [10, 4, 2**64 - 1, 0]
        t = LTSeq.from_arrow(pa.table({"i": range(4), "x": pa.array(values, pa.uint64())})).sort("i")
        got = _col(t.derive(v=lambda r: r.x.pct_change()))
        assert got[0] is None
        assert got[1:] == pytest.approx([-0.6, (2**64 - 5) / 4, -1.0])

    def test_float_column(self):
        values = [2.0, 3.0, 0.0, 1.5]
        t = LTSeq.from_arrow(pa.table({"i": range(len(values)), "x": values})).sort("i")
        _same_floats(_col(t.derive(v=lambda r: r.x.pct_change())), [None, 0.5, -1.0, math.inf])
