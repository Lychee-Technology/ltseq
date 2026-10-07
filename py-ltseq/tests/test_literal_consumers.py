"""Literal arguments of methods: what each consumer accepts (#145).

A numeric argument accepts any number kind (an integral Decimal where an
integer is needed), a string argument accepts only a string, and a string is
never read as a number (decision P5 on #145). An argument that is present but
wrong is an error naming the method, never the default.
"""

from datetime import date
from decimal import Decimal

import pyarrow as pa
import pytest

from ltseq import LTSeq, log, ntile
from ltseq.expr import LiteralExpr


@pytest.fixture
def nums():
    return LTSeq.from_arrow(
        pa.table(
            {
                "i": pa.array([1, 2, 3, 4], pa.int64()),
                "x": pa.array([2.0, 8.0, 4.0, 16.0], pa.float64()),
                "y": pa.array([1.0, 1.0, 2.0, 2.0], pa.float64()),
                "d": pa.array([date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]),
            }
        )
    ).sort("i")


def _col(t, name):
    return t.to_arrow().column(name).to_pylist()


# Each entry builds the same derived column with an argument of the given value.
DERIVES = {
    "shift": lambda v: lambda r: r.x.shift(v),
    "diff": lambda v: lambda r: r.x.diff(v),
    "rolling": lambda v: lambda r: r.x.rolling(v).sum(),
    "ntile": lambda v: lambda r: ntile(v).over(order_by=r.x),
    "log": lambda v: lambda r: log(r.x, v),
    "dt.add": lambda v: lambda r: r.d.dt.add(days=v),
}
INTEGER_ARGS = {"shift": 1, "diff": 1, "rolling": 2, "ntile": 2, "log": 2, "dt.add": 1}


@pytest.mark.parametrize("method", sorted(DERIVES))
def test_integral_decimal_argument_means_the_integer(nums, method):
    make, n = DERIVES[method], INTEGER_ARGS[method]
    as_int = _col(nums.derive(v=make(n)), "v")
    as_decimal = _col(nums.derive(v=make(Decimal(n))), "v")
    assert as_decimal == as_int


@pytest.mark.parametrize(
    "method, message",
    [
        ("shift", r"shift\(\) offset must be an integer, got a String literal '1'"),
        ("diff", r"diff\(\) periods must be an integer, got a String literal '1'"),
        ("rolling", r"rolling\(\) window size must be an integer, got a String literal '2'"),
        ("ntile", r"ntile\(\) bucket count must be an integer, got a String literal '2'"),
        ("log", r"log\(\) base must be a number, got a String literal '2'"),
        ("dt.add", r"dt_add days must be an integer, got a String literal '1'"),
    ],
)
def test_string_argument_is_not_a_number(nums, method, message):
    with pytest.raises(ValueError, match=message):
        nums.derive(v=DERIVES[method](str(INTEGER_ARGS[method])))


@pytest.mark.parametrize(
    "method, value, message",
    [
        ("shift", 1.0, r"shift\(\) offset must be an integer, got a Float64 literal 1.0"),
        ("shift", Decimal("1.5"), r"shift\(\) offset must be an integer, got a Decimal128 literal 1.5"),
        ("rolling", True, r"rolling\(\) window size must be an integer, got a Boolean literal True"),
        ("dt.add", 1.5, r"dt_add days must be an integer, got a Float64 literal 1.5"),
    ],
)
def test_non_integral_argument_is_rejected(nums, method, value, message):
    with pytest.raises(ValueError, match=message):
        nums.derive(v=DERIVES[method](value))


@pytest.mark.parametrize(
    "p, message",
    [
        ("0.5", r"percentile\(\) p must be a number, got a String literal '0.5'"),
        (date(2024, 1, 1), r"percentile\(\) p must be a number, got a Date32 literal"),
    ],
)
def test_percentile_p_must_be_a_number(nums, p, message):
    with pytest.raises(ValueError, match=message):
        nums.agg(v=lambda g: g.x.percentile(p))


@pytest.mark.parametrize(
    "k, message",
    [
        ("3", r"top_k\(\) k must be an integer, got a String literal '3'"),
        (2.5, r"top_k\(\) k must be an integer, got a Float64 literal 2.5"),
        (Decimal("2.5"), r"top_k\(\) k must be an integer, got a Decimal128 literal 2.5"),
    ],
)
def test_top_k_k_must_be_an_integer(nums, k, message):
    with pytest.raises(ValueError, match=message):
        nums.agg(v=lambda g: g.x.top_k(k))


def test_dt_diff_unit_must_be_a_string(nums):
    with pytest.raises(ValueError, match=r"dt_diff unit must be a string, got an Int64 literal 5"):
        nums.derive(v=lambda r: r.d.dt.diff(r.d, 5))


def test_cast_target_must_be_a_string(nums):
    with pytest.raises(ValueError, match=r"cast\(\) target type must be a string, got an Int64 literal 1"):
        nums.derive(v=lambda r: r.x.cast(1))


def test_shift_offset_must_be_a_literal(nums):
    with pytest.raises(ValueError, match=r"shift\(\) offset must be a literal integer"):
        nums.derive(v=lambda r: r.x.shift(r.i))


def test_shift_default_must_be_a_literal(nums):
    """A non-literal default used to be dropped, leaving the first row null."""
    with pytest.raises(ValueError, match=r"shift\(\) default must be a literal value"):
        nums.derive(v=lambda r: r.x.shift(1, default=r.y))


def test_shift_default_literal_fills_the_first_row(nums):
    assert _col(nums.derive(v=lambda r: r.x.shift(1, default=Decimal("0.5"))), "v")[0] == 0.5


# ---- constant folding (#193) ----


def test_constant_folding_keeps_integer_precision(nums):
    """`2**53 + 1` used to fold through f64 and lose its last unit."""
    out = nums.derive(v=lambda r: r.i * 0 + (LiteralExpr(2**53) + 1))
    assert _col(out, "v")[0] == 2**53 + 1


def test_constant_folding_keeps_a_float_sum_float(nums):
    """`2.0 + 3.0` used to fold to the integer 5."""
    out = nums.derive(v=lambda r: LiteralExpr(2.0) + 3.0)
    assert out.to_arrow().schema.field("v").type == pa.float64()
    assert _col(out, "v")[0] == 5.0


# ---- literals in the linear-scan predicate evaluator ----


def _reference(t, pred):
    return len(t.group_ordered(pred).first().to_pandas())


@pytest.mark.parametrize("literal", [None, date(2024, 1, 1), Decimal("1.5")])
def test_linear_scan_leaves_other_literal_kinds_to_datafusion(literal):
    t = LTSeq.from_arrow(pa.table({"x": pa.array([1, 1, 3], pa.int64())})).sort("x")
    pred = lambda r: (r.x - r.x.shift(1)) > literal  # noqa: E731
    with pytest.raises(ValueError, match="only supports shift-based boundary predicates"):
        t._inner.group_ordered_count(t._capture_expr(pred))


@pytest.mark.parametrize(
    "pred",
    [
        lambda r: (r.x - r.x.shift(1)) > None,
        lambda r: (r.x != r.x.shift(1)) & (r.x > None),
        lambda r: (r.x != r.x.shift(1)) & ((r.x - None) > 0),
    ],
)
def test_a_none_literal_is_counted_like_the_reference(pred):
    """`x > None` is NULL on every row, and the kernel's `&` counts
    `false AND NULL` as a boundary where DataFusion's is false (#189). On
    `main` `None` was the string "None", which the kernel refused, so these
    were always counted on the general path (review of b6cc39f on #225)."""
    t = LTSeq.from_arrow(pa.table({"k": [0, 1, 2], "x": pa.array([1, 1, 2], pa.int64())})).sort("k")
    assert t.group_ordered(pred).first().count() == _reference(t, pred)


@pytest.mark.parametrize(
    "pred",
    [
        lambda r: (r.x != r.x.shift(1)) | (r.x > 2.0),
        lambda r: ((r.x - r.x.shift(1)) > 1.0) | (r.x > 3),
    ],
)
def test_a_float_threshold_outside_the_fused_shape_is_declined_before_collecting(pred):
    """Only the fused evaluator reads an integral float as an integer; the
    multi-pass one has no integer/float comparison. Such a predicate used to
    be collected and then fail inside the kernel."""
    t = LTSeq.from_arrow(pa.table({"k": range(5), "x": pa.array([1, 1, 3, 3, 5], pa.int64())})).sort("k")
    with pytest.raises(ValueError, match="only supports shift-based boundary predicates"):
        t._inner.group_ordered_count(t._capture_expr(pred))
    assert t.group_ordered(pred).first().count() == _reference(t, pred)


def test_a_float_threshold_in_the_fused_shape_is_counted_by_the_kernel():
    t = LTSeq.from_arrow(pa.table({"k": range(5), "x": pa.array([1, 1, 3, 3, 5], pa.int64())})).sort("k")
    pred = lambda r: (r.x != r.x.shift(1)) | ((r.x - r.x.shift(1)) > 1.0)  # noqa: E731
    assert t._inner.group_ordered_count(t._capture_expr(pred)) == _reference(t, pred) == 3


# ---- literals in search_pattern predicates (design §5 row 20) ----


@pytest.fixture
def flags():
    return LTSeq.from_arrow(
        pa.table(
            {
                "i": pa.array([1, 2, 3, 4], pa.int64()),
                "flag": pa.array([True, False, True, True]),
                "s": pa.array(["a", "b", "a", "b"]),
            }
        )
    ).sort("i")


def test_search_pattern_accepts_boolean_literals(flags):
    matches = flags.search_pattern(lambda r: r.flag == True, lambda r: r.flag == False)  # noqa: E712
    assert _col(matches, "i") == [1]


def test_search_pattern_accepts_a_null_literal(flags):
    """`flag & None` is NULL for true rows and false for false rows: no row matches."""
    matches = flags.search_pattern(lambda r: r.flag & None)
    assert _col(matches, "i") == []
