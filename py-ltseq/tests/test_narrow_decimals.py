"""decimal32 and decimal64 widen to decimal128 where they meet another type (#241).

DataFusion 55 has a common decimal type for a decimal32 or decimal64 and an
integer only up to Int16 (decimal32) or Int32 (decimal64). Next to a wider
integer the decimal was cast to the integer, truncating it: `1.50 > 1` was
false, `1.50 + 1` was the int64 2 and `fill_null(r.i)` returned int64. Two
narrow decimals met at a narrow type whose precision was clamped, so their sum
held values the type does not. Contract §17.4 rule 2 widens a narrow decimal
to the decimal128 of the same precision and scale before any binary
arithmetic, sum or mean, and wherever it meets another integer or decimal
type. A decimal128 column was never affected, so each test compares a narrow
column with the decimal128 column of the same precision and scale, and checks
the values against Python's `Decimal` where they are exact.
"""

from decimal import Decimal

import pyarrow as pa
import pytest

from ltseq import LTSeq, coalesce, if_else, when

NARROW = [pa.decimal32(9, 2), pa.decimal64(18, 2)]
INTEGERS = [
    pa.int8(), pa.int16(), pa.int32(), pa.int64(),
    pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64(),
]

X = [Decimal("1.50"), Decimal("0.01"), Decimal("-2.50"), None, Decimal("7.00")]
I = [1, 3, 2, 5, 7]


def wide(narrow):
    return pa.decimal128(narrow.precision, narrow.scale)


def table(narrow, integer):
    """`x` narrow, `w` the decimal128 of the same precision and scale, `i` an integer."""
    return LTSeq.from_arrow(pa.table({
        "k": pa.array(range(len(X)), pa.int64()),
        "x": pa.array(X, narrow),
        "w": pa.array(X, wide(narrow)),
        "i": pa.array(I, integer),
    }))


def column(t, fn):
    out = t.derive(v=fn).select("v").to_arrow().column("v")
    return str(out.type), out.to_pylist()


# Each expression of a decimal `d` (r.x or r.w) and the integer column.
EXPRESSIONS = {
    "eq": lambda d, r: d == r.i,
    "ne": lambda d, r: d != r.i,
    "gt": lambda d, r: d > r.i,
    "ge": lambda d, r: d >= r.i,
    "lt_mirror": lambda d, r: r.i < d,
    "le": lambda d, r: d <= r.i,
    "between": lambda d, r: d.between(r.i, r.i + 1),
    "is_in": lambda d, r: d.is_in([1, 7]),
    "add": lambda d, r: d + r.i,
    "sub_mirror": lambda d, r: r.i - d,
    "mul": lambda d, r: d * r.i,
    "div": lambda d, r: d / r.i,
    "mod": lambda d, r: d % r.i,
    "add_literal": lambda d, r: d + 1,
    "fill_null": lambda d, r: d.fill_null(r.i),
    "coalesce_mirror": lambda d, r: coalesce(r.i, d),
    "if_else": lambda d, r: if_else(r.k > 1, d, r.i),
    "when": lambda d, r: when(r.k == 0, r.i).when(r.k == 1, d).otherwise(r.i),
}

# The values Python's Decimal gives, for the expressions that are exact.
EXACT = {
    "eq": lambda x, i: x == i,
    "gt": lambda x, i: x > i,
    "lt_mirror": lambda x, i: i < x,
    "between": lambda x, i: i <= x <= i + 1,
    "add": lambda x, i: x + i,
    "sub_mirror": lambda x, i: i - x,
    "mul": lambda x, i: x * i,
    "fill_null": lambda x, i: i if x is None else x,
}


@pytest.mark.parametrize("name", list(EXPRESSIONS))
@pytest.mark.parametrize("integer", INTEGERS, ids=str)
@pytest.mark.parametrize("narrow", NARROW, ids=str)
def test_a_narrow_decimal_next_to_an_integer_computes_as_decimal128(narrow, integer, name):
    t = table(narrow, integer)
    expression = EXPRESSIONS[name]
    kind, values = column(t, lambda r: expression(r.x, r))
    assert (kind, values) == column(t, lambda r: expression(r.w, r))
    if name in EXACT:
        exact = EXACT[name]
        assert values == [None if x is None and name != "fill_null" else exact(x, i) for x, i in zip(X, I)]


@pytest.mark.parametrize("narrow", NARROW, ids=str)
def test_the_issue_table(narrow):
    """The expressions of #241 on its table, as the decision record gives them."""
    t = LTSeq.from_arrow(pa.table({
        "x": pa.array([Decimal("1.50"), Decimal("0.01")], narrow),
        "i": pa.array([1, 0], pa.int64()),
    }))
    assert column(t, lambda r: r.x > r.i) == ("bool", [True, True])
    assert column(t, lambda r: r.x == r.i) == ("bool", [False, False])
    assert column(t, lambda r: r.x + 1) == ("decimal128(23, 2)", [Decimal("2.50"), Decimal("1.01")])
    assert column(t, lambda r: r.x + r.i) == ("decimal128(23, 2)", [Decimal("2.50"), Decimal("0.01")])
    # The product's precision is p + 20 + 1 digits, at most 38.
    product = f"decimal128({min(narrow.precision + 21, 38)}, 2)"
    assert column(t, lambda r: r.x * r.i) == (product, [Decimal("1.50"), Decimal("0.00")])
    assert column(t, lambda r: r.x.fill_null(r.i)) == ("decimal128(22, 2)", [Decimal("1.50"), Decimal("0.01")])
    assert t.filter(lambda r: r.x > r.i).to_arrow().num_rows == 2
    assert t.search_first(lambda r: r.x > r.i).to_arrow().column("x").to_pylist() == [Decimal("1.50")]


@pytest.mark.parametrize("narrow", NARROW, ids=str)
def test_two_narrow_decimals_add_in_decimal128(narrow):
    """9999999.99 + 0.01 needs ten digits, which decimal32(9, 2) does not have
    (contract acceptance N4); the sum used to be a decimal32(9, 2) holding it."""
    t = LTSeq.from_arrow(pa.table({
        "a": pa.array([Decimal("9999999.99")], narrow),
        "b": pa.array([Decimal("0.01")], narrow),
    }))
    p = narrow.precision
    assert column(t, lambda r: r.a + r.b) == (f"decimal128({p + 1}, 2)", [Decimal("10000000.00")])
    assert column(t, lambda r: r.a - r.b) == (f"decimal128({p + 1}, 2)", [Decimal("9999999.98")])


def test_narrow_decimals_of_another_scale_or_width_meet_in_decimal128():
    """decimal32(9, 2) and decimal32(9, 3) met at decimal32(9, 3), which does
    not hold 9999999.99; widened, they meet at decimal128(10, 3)."""
    t = LTSeq.from_arrow(pa.table({
        "a": pa.array([Decimal("9999999.99"), None], pa.decimal32(9, 2)),
        "b": pa.array([Decimal("1.500"), Decimal("1.500")], pa.decimal32(9, 3)),
        "c": pa.array([Decimal("9999999.99"), Decimal("1.50")], pa.decimal64(18, 2)),
    }))
    assert column(t, lambda r: r.a.fill_null(r.b)) == ("decimal128(10, 3)", [Decimal("9999999.990"), Decimal("1.500")])
    assert column(t, lambda r: r.a > r.b) == ("bool", [True, None])
    assert column(t, lambda r: r.a == r.c) == ("bool", [True, None])
    assert column(t, lambda r: coalesce(r.a, r.c)) == ("decimal128(18, 2)", [Decimal("9999999.99"), Decimal("1.50")])


@pytest.mark.parametrize("narrow", NARROW, ids=str)
def test_a_narrow_decimal_that_meets_no_other_type_keeps_its_type(narrow):
    """An untouched narrow column, one compared or combined with values of its
    own type, and one that shares a column with an integer literal stay
    narrow: a literal takes the type of the values beside it (§17.5 rule 1)."""
    t = table(narrow, pa.int64())
    for fn in (
        lambda r: r.x,
        lambda r: r.x.fill_null(1),
        lambda r: if_else(r.k > 1, r.x, 0),
        lambda r: r.x.fill_null(r.x),
        lambda r: r.x.abs(),
    ):
        kind, _ = column(t, fn)
        assert kind == str(narrow)
    assert column(t.sort("k"), lambda r: r.x.shift(1))[0] == str(narrow)
    assert column(t, lambda r: r.x == 1) == ("bool", [False, False, False, None, False])
    assert str(t.filter(lambda r: r.x > r.i).to_arrow().schema.field("x").type) == str(narrow)


@pytest.mark.parametrize("narrow", NARROW, ids=str)
def test_a_narrow_decimal_next_to_a_float_is_float_arithmetic(narrow):
    """A float with a decimal is float64 in arithmetic (§17.4 rule 1), whatever
    the decimal's width; the float pairing itself is #228's."""
    t = LTSeq.from_arrow(pa.table({
        "x": pa.array([Decimal("1.50")], narrow),
        "f": pa.array([0.25], pa.float64()),
    }))
    assert column(t, lambda r: r.x + r.f) == ("double", [1.75])


@pytest.mark.parametrize("integer", [pa.int32(), pa.int64(), pa.uint64()], ids=str)
@pytest.mark.parametrize("narrow", NARROW, ids=str)
def test_windows_compute_as_decimal128(narrow, integer):
    t = table(narrow, integer).sort("k")
    for fn in (
        lambda d, r: d.diff(),
        lambda d, r: d.rolling(2).sum(),
        lambda d, r: d.rolling(2).mean(),
        lambda d, r: d - r.i.shift(1),
        lambda d, r: d > r.i.shift(1),
        lambda d, r: d.shift(1).fill_null(r.i),
    ):
        assert column(t, lambda r: fn(r.x, r)) == column(t, lambda r: fn(r.w, r))
    assert column(t, lambda r: r.x - r.i.shift(1))[1] == [
        None, Decimal("-0.99"), Decimal("-5.50"), None, Decimal("2.00"),
    ]


@pytest.mark.parametrize("narrow", NARROW, ids=str)
def test_group_counts_compare_a_narrow_decimal_by_value(narrow):
    """`first().count()` counts groups with its own kernel where it can; the
    kernel declines decimal columns, and the count is the general path's."""
    t = table(narrow, pa.int64()).sort("k")
    for key in (lambda d, r: d > r.i, lambda d, r: d > r.i.shift(1)):
        narrow_count = t.group_ordered(lambda r: key(r.x, r)).first().count()
        assert narrow_count == t.group_ordered(lambda r: key(r.w, r)).first().count()
    # x > i is [True, False, False, None, False]: four runs, where the
    # truncated 1.50 > 1 was false and made three.
    assert t.group_ordered(lambda r: r.x > r.i).first().count() == 4


@pytest.mark.parametrize("narrow", NARROW, ids=str)
def test_group_expressions_compute_as_decimal128(narrow):
    t = LTSeq.from_arrow(pa.table({
        "k": pa.array(range(4), pa.int64()),
        "g": pa.array([1, 1, 2, 2], pa.int64()),
        "x": pa.array([Decimal("1.50"), Decimal("9999999.99"), Decimal("0.01"), Decimal("0.01")], narrow),
        "w": pa.array([Decimal("1.50"), Decimal("9999999.99"), Decimal("0.01"), Decimal("0.01")], wide(narrow)),
        "i": pa.array([1, 0, 0, 0], pa.int64()),
    })).sort("k")

    def derived(d):
        out = t.group_ordered(lambda r: r.g).derive(
            lambda g: {"s": g.sum(d), "m": g.mean(d), "diff": getattr(g.first(), d) - g.first().i}
        ).to_arrow()
        return [(str(out.schema.field(c).type), out.column(c).to_pylist()) for c in ("s", "m", "diff")]

    assert derived("x") == derived("w")
    sum_type = f"decimal128({narrow.precision + 10}, 2)"
    assert derived("x")[0] == (sum_type, [Decimal("10000001.49")] * 2 + [Decimal("0.02")] * 2)
    assert derived("x")[2][1] == [Decimal("0.50")] * 2 + [Decimal("0.01")] * 2
    kept = t.group_ordered(lambda r: r.g).filter(lambda g: g.first().x > g.first().i).flatten()
    assert kept.to_arrow().column("k").to_pylist() == [0, 1, 2, 3]
