"""Floor division (`//`) runs with Python's semantics on every path (#147).

`r.x // y` used to serialize and then fail on every execution path with
"Unknown binary operator: FloorDiv". The kernel is in
`src/transpiler/floor_div.rs`; these tests pin its behavior end to end.
Truncating division (what `/` does on integers) would give -3 where Python
gives -4, so each fixture includes operands with opposite signs.
"""

import math

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq

# Every sign combination, an exact division and a zero dividend.
XS = [7, -7, 7, -7, -3, 3, 6, 0]
YS = [2, 2, -2, -2, 2, -2, 3, -5]


def _table(xs=XS, ys=YS):
    return pa.table(
        {
            "i": pa.array(range(len(xs)), pa.int64()),
            "x": pa.array(xs, pa.int64()),
            "y": pa.array(ys, pa.int64()),
        }
    )


@pytest.fixture
def ints():
    return LTSeq.from_arrow(_table()).sort("i")


def _col(t, name="v"):
    return t.to_arrow().column(name).to_pylist()


class TestPythonSemantics:
    @pytest.mark.parametrize(
        "fn, reference",
        [
            (lambda r: r.x // r.y, lambda x, y: x // y),
            (lambda r: r.x // 2, lambda x, y: x // 2),
            (lambda r: r.x // -2, lambda x, y: x // -2),
            (lambda r: 100 // r.y, lambda x, y: 100 // y),
            (lambda r: -100 // r.y, lambda x, y: -100 // y),
        ],
        ids=["col-col", "col-lit", "col-neg-lit", "lit-col", "neg-lit-col"],
    )
    def test_derive(self, ints, fn, reference):
        assert _col(ints.derive(v=fn)) == [reference(x, y) for x, y in zip(XS, YS)]

    def test_filter_and_search_first(self, ints):
        expected = [i for i, (x, y) in enumerate(zip(XS, YS)) if x // y == -4]
        assert expected == [1, 2]  # -7 // 2 and 7 // -2; truncation gives -3
        pred = lambda r: r.x // r.y == -4  # noqa: E731
        assert _col(ints.filter(pred), "i") == expected
        assert _col(ints.search_first(pred), "i") == expected[:1]

    def test_float(self):
        fs = [7.5, -7.5, 1.0, -1.0, -0.0, 1.0, -1.0]
        ds = [2.0, 2.0, 0.1, 0.1, 3.0, math.inf, math.inf]
        t = LTSeq.from_arrow(pa.table({"f": fs, "d": ds}))
        got = _col(t.derive(v=lambda r: r.f // r.d))
        expected = [f // d for f, d in zip(fs, ds)]
        # 1.0 // 0.1 is 9.0 in Python, where floor(1.0 / 0.1) is 10.0.
        assert expected == [3.0, -4.0, 9.0, -10.0, -0.0, 0.0, -1.0]
        assert got == expected
        # == does not see the sign of zero.
        assert [math.copysign(1, v) for v in got] == [math.copysign(1, v) for v in expected]

    def test_int_and_float_operands_give_float(self, ints):
        result = ints.derive(v=lambda r: r.x // 2.0).to_arrow()
        assert result.schema.field("v").type == pa.float64()
        assert result.column("v").to_pylist() == [x // 2.0 for x in XS]

    @pytest.mark.parametrize(
        "left, right, expected",
        [
            (pa.int64(), pa.int64(), pa.int64()),
            (pa.int32(), pa.int32(), pa.int64()),
            (pa.int16(), pa.uint8(), pa.int64()),
            (pa.uint32(), pa.uint64(), pa.uint64()),
            (pa.int64(), pa.float32(), pa.float64()),
        ],
    )
    def test_result_type(self, left, right, expected):
        t = LTSeq.from_arrow(pa.table({"a": pa.array([7, 9], left), "b": pa.array([2, 4], right)}))
        result = t.derive(v=lambda r: r.a // r.b).to_arrow()
        assert result.schema.field("v").type == expected
        assert result.column("v").to_pylist() == [3, 2]

    def test_integers_beyond_float_precision_stay_exact(self):
        big = 2**53 + 1  # not representable as a float64
        t = LTSeq.from_arrow(pa.table({"x": pa.array([big, -big], pa.int64())}))
        assert _col(t.derive(v=lambda r: r.x // 1)) == [big, -big]
        assert _col(t.derive(v=lambda r: r.x // 2)) == [big // 2, -big // 2]

    def test_null_operands_give_null(self):
        # The NULL divisor's slot holds 0 in the Arrow buffer: it must give
        # NULL, not a divide-by-zero error.
        t = LTSeq.from_arrow(
            pa.table({"x": pa.array([None, 7, 7], pa.int64()), "y": pa.array([2, None, 2], pa.int64())})
        )
        assert _col(t.derive(v=lambda r: r.x // r.y)) == [None, None, 3]
        assert _col(t.derive(v=lambda r: r.x // None)) == [None, None, None]


class TestErrors:
    @pytest.mark.parametrize(
        "fn",
        [lambda r: r.x // 0, lambda r: r.x // (r.y - r.y), lambda r: r.f // 0.0, lambda r: r.f // -0.0],
        ids=["int-literal", "int-column", "float", "float-negative-zero"],
    )
    def test_zero_divisor_raises(self, fn):
        # Python raises ZeroDivisionError for ints and floats alike; `/` on
        # floats returns inf instead.
        t = LTSeq.from_arrow(pa.table({"x": [7, 8], "y": [1, 2], "f": [7.0, 8.0]}))
        with pytest.raises(ValueError, match="Divide by zero"):
            t.derive(v=fn).to_arrow()

    def test_overflow_raises(self):
        # Python would return 2**63; Int64 cannot hold it, as for `/`.
        t = LTSeq.from_arrow(pa.table({"x": pa.array([-(2**63)], pa.int64())}))
        with pytest.raises(ValueError, match="Overflow"):
            t.derive(v=lambda r: r.x // -1).to_arrow()

    def test_non_numeric_operand_is_rejected(self):
        t = LTSeq.from_arrow(pa.table({"s": ["a"]}))
        with pytest.raises(RuntimeError, match="needs integer or float operands"):
            t.derive(v=lambda r: r.s // 2).to_arrow()


# x // 10 buckets: floor gives [-1, 0, 1, 1, -2, -3] (5 runs); truncation
# would give [0, 0, 1, 1, -1, -2] (4 runs).
GROUP_XS = [-5, 5, 12, 18, -15, -25]


def _bucket_change(r):
    return (r.x // 10) != (r.x.shift(1) // 10)


class TestExecutionPaths:
    def test_window_operand(self, ints):
        assert _col(ints.derive(v=lambda r: r.x.shift(1) // 2)) == [None] + [x // 2 for x in XS[:-1]]

    def test_group_ordered_window_path(self):
        t = LTSeq.from_arrow(_table(GROUP_XS, [0] * len(GROUP_XS))).sort("i")
        firsts = t.group_ordered(_bucket_change).first().to_arrow().column("x").to_pylist()
        assert firsts == [-5, 5, 12, -15, -25]

    def test_group_ordered_count_linear_scan(self):
        t = LTSeq.from_arrow(_table(GROUP_XS, [0] * len(GROUP_XS))).sort("i")
        # Called directly: first().count() falls back to the window path on
        # any kernel error, which would hide a missing linear-scan operator.
        assert t._inner.group_ordered_count(t._capture_expr(_bucket_change)) == 5
        assert t.group_ordered(_bucket_change).first().count() == 5

    def test_group_ordered_count_sorted_parquet(self, tmp_path):
        path = str(tmp_path / "x.parquet")
        pq.write_table(_table(GROUP_XS, [0] * len(GROUP_XS)), path)
        t = LTSeq.read_parquet(path).assume_sorted("i")
        assert t._inner.group_ordered_count(t._capture_expr(_bucket_change)) == 5

    # Step 1 at row i, step 2 at row i + 1. x // 2 == -4 holds at rows 1 and
    # 3 (x = -7); x % 4 == 3 holds at row 2 (7) but not at row 4 (-3).
    STEPS = (lambda r: r.x // 2 == -4, lambda r: r.x % 4 == 3)

    def test_search_pattern(self, ints):
        assert _col(ints.search_pattern(*self.STEPS), "i") == [1]
        assert ints.search_pattern_count(*self.STEPS) == 1

    def test_search_pattern_count_sorted_parquet(self, tmp_path):
        path = str(tmp_path / "x.parquet")
        pq.write_table(_table(), path)
        t = LTSeq.read_parquet(path).assume_sorted("i")
        assert t.search_pattern_count(*self.STEPS) == 1
