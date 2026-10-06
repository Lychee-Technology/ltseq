"""Metamorphic properties of literal typing (#225).

Each test builds two expressions that must mean the same thing and
requires identical outcomes on the current build: the same values, Arrow
dtype and NULLs, or the same error class raised at the same stage. None
of these tests knows the right answer; each states a property of the
answer, so a rule that types a literal by anything other than the type
the expression executes as breaks one of them.

- branch swap: ``if_else(c, a, b)`` and ``if_else(~c, b, a)``
- mirror: ``e op lit`` and ``lit op' e`` with the literal really on the left
- coalesce permutation: arguments that are never both set on a row
- inline vs staged: ``f(e)`` and ``derive(c=e)`` then ``f(c)``
- row count: a derived column's dtype does not depend on how many rows remain
- dialect parity: the row result shifted by one equals the window
  result, and one-row groups keep exactly the rows the row result keeps
- folding: a constant folded before planning equals the same arithmetic on a column
- boolean literal: ``e & True`` (and ``|``, ``False``) equals ``e`` next to a constant Boolean column
- linear scan: ``first().count()`` equals the materialized group count
"""

import math
import operator
import traceback
from datetime import date, datetime, timezone
from decimal import Decimal

import pandas as pd
import pyarrow as pa
import pytest

from ltseq import LTSeq, coalesce, if_else
from ltseq.expr import LiteralExpr

D = Decimal
CUT = datetime(1970, 1, 1, 0, 0, 1, 500000)  # 1.5 s, microseconds
NS_FINE = pd.Timestamp(1_000_000_001, unit="ns")  # 1 s + 1 ns
FINE33 = D("1.230000000000000000000000000000000")
FINE20 = D("0.12345678900000000000")
NY = "America/New_York"


def _utc(seconds):
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def table():
    k = list(range(6))

    def lo(values):  # set on rows 0-2 only
        return [v if i < 3 else None for i, v in enumerate(values)]

    def hi(values):  # set on rows 3-5 only
        return [v if i >= 3 else None for i, v in enumerate(values)]

    i = [None, 1, -2, 2**53 + 1, 0, 3]
    f = [float("nan"), None, 1e-16, 2.5, -0.0, 1e20]
    p = [D("1.23"), None, D("-0.50"), D("0.00"), D("999.99"), D("2.50")]
    w = [D("1000000000000000000000000000"), D("0.1234567890"), None, D("-1.5"), D("0"), D("2.5")]
    s = [0, 1, 2, None, -1, 86400]
    u = [1_250_000, 1_000_001, None, 1_500_000, -500_000, 86_400_000_000]
    n = [1_500_000_000, 1_000_000_001, 5, None, -1, 86_400_000_000_000]
    return LTSeq.from_arrow(pa.table({
        "k": pa.array(k, pa.int64()),
        "i": pa.array(i, pa.int64()),
        "f": pa.array(f, pa.float64()),
        "p": pa.array(p, pa.decimal128(5, 2)),
        "w": pa.array(w, pa.decimal128(38, 10)),
        "s": pa.array(s, pa.timestamp("s")),
        "u": pa.array(u, pa.timestamp("us")),
        "n": pa.array(n, pa.timestamp("ns")),
        "d": pa.array([date(1970, 1, 1), date(1970, 1, 2), None, date(2024, 1, 1),
                       date(1969, 12, 31), date(2262, 1, 1)], pa.date32()),
        "z": pa.array([_utc(18000), _utc(18001), None, _utc(0), _utc(-1), _utc(86400)],
                      pa.timestamp("us", NY)),
        "i_lo": pa.array(lo(i), pa.int64()),
        "f_hi": pa.array(hi(f), pa.float64()),
        "p_lo": pa.array(lo(p), pa.decimal128(5, 2)),
        "w_hi": pa.array(hi(w), pa.decimal128(38, 10)),
        "s_lo": pa.array(lo(s), pa.timestamp("s")),
        "u_hi": pa.array(hi(u), pa.timestamp("us")),
    })).sort("k")


# ---------------------------------------------------------------------------
# Outcomes
# ---------------------------------------------------------------------------


def _norm(value):
    if isinstance(value, float) and math.isnan(value):
        return "NaN"
    return value


def _stage(error, phase):
    if phase == "collect":
        return "collect"
    frames = traceback.extract_tb(error.__traceback__)
    return "capture" if frames and "/ltseq/expr/" in frames[-1].filename else "plan"


def outcome(make, name="v"):
    """('ok', dtype, values) or ('error', class, stage) for the table ``make`` builds."""
    phase = "plan"
    try:
        result = make()
        phase = "collect"
        arrow = result.to_arrow()
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as error:  # a Rust panic is a BaseException
        return ("error", type(error).__name__, _stage(error, phase))
    col = arrow.column(name)
    return ("ok", str(col.type), [_norm(v) for v in col.to_pylist()])


def derived(expr):
    return outcome(lambda: table().derive(v=expr))


# ---------------------------------------------------------------------------
# Branch swap
# ---------------------------------------------------------------------------

LITERALS = {
    ("s", "u"): {"cut": CUT, "ns": NS_FINE, "day": datetime(1970, 1, 2)},
    ("u", "n"): {"cut": CUT, "ns": NS_FINE},
    ("i", "f"): {"dec": D("2.5"), "dec0": D("0"), "float": 2.5, "int": 1},
    ("p", "w"): {"fine": FINE20, "dec": D("2.5"), "big": 10**18},
    ("i", "p"): {"fine": FINE33, "int": 2, "dec": D("-0.5")},
    ("d", "u"): {"six": datetime(1970, 1, 1, 6), "midnight": datetime(1970, 1, 2), "date": date(1970, 1, 2)},
    ("f", "p"): {"dec": D("2.5"), "float": 2.5},
}
TEMPORAL = {("s", "u"), ("u", "n"), ("d", "u")}


def case_expr(r, a, b, swapped):
    if swapped:
        return if_else(r.k <= 2, getattr(r, b), getattr(r, a))
    return if_else(r.k > 2, getattr(r, a), getattr(r, b))


CONSUMERS = {
    "eq": lambda e, lit: e == lit,
    "lt": lambda e, lit: e < lit,
    "ge": lambda e, lit: e >= lit,
    "isin": lambda e, lit: e.is_in([lit]),
    "fill": lambda e, lit: e.fill_null(lit),
    "default": lambda e, lit: e.shift(1, default=lit),
    "lag_eq": lambda e, lit: e.shift(1) == lit,
}


def _branch_swap_cases():
    for (a, b), literals in LITERALS.items():
        for lit_id, lit in literals.items():
            for consumer in CONSUMERS:
                yield pytest.param(a, b, consumer, lit, id=f"{a}-{b}-{consumer}-{lit_id}")


@pytest.mark.parametrize("a, b, consumer, lit", list(_branch_swap_cases()))
def test_branch_swap(a, b, consumer, lit):
    apply = CONSUMERS[consumer]
    assert derived(lambda r: apply(case_expr(r, a, b, False), lit)) == derived(
        lambda r: apply(case_expr(r, a, b, True), lit)
    )


@pytest.mark.parametrize("a, b, lit", [
    pytest.param(a, b, lit, id=f"{a}-{b}-{lit_id}")
    for (a, b), literals in LITERALS.items() for lit_id, lit in literals.items()
])
def test_branch_swap_with_a_window_branch(a, b, lit):
    def expr(r, swapped):
        shifted = getattr(r, a).shift(1)
        if swapped:
            return if_else(r.k <= 2, getattr(r, b), shifted) == lit
        return if_else(r.k > 2, shifted, getattr(r, b)) == lit

    assert derived(lambda r: expr(r, False)) == derived(lambda r: expr(r, True))


@pytest.mark.parametrize("a, b", [pytest.param(a, b, id=f"{a}-{b}") for a, b in sorted(TEMPORAL)])
def test_branch_swap_in_dt_diff(a, b):
    assert derived(lambda r: case_expr(r, a, b, False).dt.diff(r.s, unit="second")) == derived(
        lambda r: case_expr(r, a, b, True).dt.diff(r.s, unit="second")
    )


# ---------------------------------------------------------------------------
# Mirror, with NULL rows kept NULL
# ---------------------------------------------------------------------------

MIRROR = {
    operator.eq: operator.eq,
    operator.ne: operator.ne,
    operator.lt: operator.gt,
    operator.le: operator.ge,
    operator.gt: operator.lt,
    operator.ge: operator.le,
}
OPERAND_LITERALS = {
    "i": {"int": 1, "float": 2.5, "dec": D("2.5"), "big": 2**53},
    "f": {"float": 2.5, "dec": D("2.5"), "int": 0},
    "p": {"fine": D("1.235"), "dec": D("2.5"), "int": 1, "float": 2.5},
    "w": {"fine": FINE20, "big": 10**18},
    "s": {"cut": CUT, "ns": NS_FINE},
    "u": {"cut": CUT, "ns": NS_FINE},
    "n": {"cut": CUT, "ns": NS_FINE},
    "d": {"six": datetime(1970, 1, 1, 6), "date": date(1970, 1, 2)},
    "z": {"naive": datetime(1970, 1, 1, 0, 0, 1), "date": date(1970, 1, 1), "aware": _utc(18000)},
}
OPERANDS = {name: (lambda r, name=name: getattr(r, name)) for name in list(OPERAND_LITERALS)}
OPERANDS.update({
    "case_s_u": lambda r: case_expr(r, "s", "u", False),
    "case_i_f": lambda r: case_expr(r, "i", "f", False),
    "case_p_w": lambda r: case_expr(r, "p", "w", False),
    "coalesce_s_u": lambda r: coalesce(r.s_lo, r.u_hi),
    "coalesce_p_w": lambda r: coalesce(r.p_lo, r.w_hi),
})
OPERAND_LITERALS.update({
    "case_s_u": OPERAND_LITERALS["u"],
    "case_i_f": OPERAND_LITERALS["f"],
    "case_p_w": OPERAND_LITERALS["w"],
    "coalesce_s_u": OPERAND_LITERALS["u"],
    "coalesce_p_w": OPERAND_LITERALS["w"],
})


def _mirror_cases():
    for operand, literals in OPERAND_LITERALS.items():
        for lit_id, lit in literals.items():
            for op in MIRROR:
                yield pytest.param(operand, op, lit, id=f"{operand}-{op.__name__}-{lit_id}")


@pytest.mark.parametrize("operand, op, lit", list(_mirror_cases()))
def test_mirror(operand, op, lit):
    expr = OPERANDS[operand]
    right = derived(lambda r: op(expr(r), lit))
    left = derived(lambda r: MIRROR[op](LiteralExpr(lit), expr(r)))
    assert right == left
    if right[0] == "ok":
        values = derived(expr)
        assert values[0] == "ok"
        nulls = [i for i, v in enumerate(values[2]) if v is None]
        assert [right[2][i] for i in nulls] == [None] * len(nulls)


# ---------------------------------------------------------------------------
# Membership is equality: is_in agrees with the == disjunction
# ---------------------------------------------------------------------------

MEMBERSHIP = {
    "i": [1, 2.0, D("2.5"), 2**53 + 1, D("123456789012345678901234567890")],
    "p": [D("1.23"), D("1.230000000000000000000000000000000"), D("123456789012345678901234567890123456"), 2],
    "w": [FINE20, 10**18, D("2.5")],
    "s": [CUT, NS_FINE, datetime(1970, 1, 1, 0, 0, 2)],
    "u": [CUT, NS_FINE, datetime(2300, 1, 1)],
    "d": [datetime(1970, 1, 1, 6), date(1970, 1, 2), datetime(2024, 1, 1)],
    "z": [datetime(1970, 1, 1, 0, 0, 1), date(1970, 1, 1), _utc(18000)],
}


@pytest.mark.parametrize("name", list(MEMBERSHIP))
def test_is_in_agrees_with_the_equality_disjunction(name):
    items = MEMBERSHIP[name]

    def disjunction(r):
        column = getattr(r, name)
        out = column == items[0]
        for item in items[1:]:
            out = out | (column == item)
        return out

    assert derived(lambda r: getattr(r, name).is_in(items)) == derived(disjunction)


# ---------------------------------------------------------------------------
# Coalesce permutation (the arguments are never both set on a row)
# ---------------------------------------------------------------------------

PERMUTED = {
    ("s_lo", "u_hi"): {"cut": CUT, "ns": NS_FINE},
    ("i_lo", "f_hi"): {"dec": D("2.5"), "float": 2.5},
    ("p_lo", "w_hi"): {"fine": FINE20, "dec": D("2.5"), "big": 10**18},
}


@pytest.mark.parametrize("a, b, lit", [
    pytest.param(a, b, lit, id=f"{a}-{b}-{lit_id}")
    for (a, b), literals in PERMUTED.items() for lit_id, lit in literals.items()
])
def test_coalesce_permutation(a, b, lit):
    first = derived(lambda r: coalesce(getattr(r, a), getattr(r, b), lit))
    assert first == derived(lambda r: coalesce(getattr(r, b), getattr(r, a), lit))


# ---------------------------------------------------------------------------
# Inline vs staged, and the dtype of an empty result
# ---------------------------------------------------------------------------

STAGED_CONSUMERS = {
    "eq": lambda e, lit: e == lit,
    "isin": lambda e, lit: e.is_in([lit]),
    "fill": lambda e, lit: e.fill_null(lit),
}


def _staged_cases():
    for (a, b), literals in LITERALS.items():
        lit_id, lit = next(iter(literals.items()))
        for consumer in STAGED_CONSUMERS:
            yield pytest.param(a, b, consumer, lit, id=f"{a}-{b}-{consumer}-{lit_id}")


@pytest.mark.parametrize("a, b, consumer, lit", list(_staged_cases()))
def test_inline_vs_staged(a, b, consumer, lit):
    apply = STAGED_CONSUMERS[consumer]
    inline = derived(lambda r: apply(case_expr(r, a, b, False), lit))
    staged = outcome(
        lambda: table().derive(c=lambda r: case_expr(r, a, b, False)).derive(v=lambda r: apply(r.c, lit))
    )
    assert inline == staged


@pytest.mark.parametrize("a, b", [pytest.param(a, b, id=f"{a}-{b}") for a, b in sorted(TEMPORAL)])
def test_inline_vs_staged_dt_diff(a, b):
    inline = derived(lambda r: case_expr(r, a, b, False).dt.diff(r.s, unit="second"))
    staged = outcome(
        lambda: table().derive(c=lambda r: case_expr(r, a, b, False)).derive(v=lambda r: r.c.dt.diff(r.s, unit="second"))
    )
    assert inline == staged


# (f, p) is left out: its CASE fails at collect on the NaN row whatever the
# row count, because DataFusion casts the float branch to Decimal(30, 15) (#228).
@pytest.mark.parametrize("a, b", [pytest.param(a, b, id=f"{a}-{b}") for a, b in LITERALS if (a, b) != ("f", "p")])
def test_dtype_does_not_depend_on_row_count(a, b):
    some = derived(lambda r: case_expr(r, a, b, False))
    none = outcome(lambda: table().derive(v=lambda r: case_expr(r, a, b, False)).filter(lambda r: r.k > 100))
    assert some[0] == none[0] == "ok"
    assert some[1] == none[1]


# ---------------------------------------------------------------------------
# Dialect parity
# ---------------------------------------------------------------------------

PARITY_OPS = [operator.eq, operator.lt, operator.ge]


COLUMNS = ["i", "f", "p", "w", "s", "u", "n", "d", "z"]


def _parity_cases():
    for name in COLUMNS:
        for lit_id, lit in OPERAND_LITERALS[name].items():
            for op in PARITY_OPS:
                yield pytest.param(name, op, lit, id=f"{name}-{op.__name__}-{lit_id}")


@pytest.mark.parametrize("name, op, lit", list(_parity_cases()))
def test_window_dialect_matches_shifted_row_result(name, op, lit):
    row = derived(lambda r: op(getattr(r, name), lit))
    window = derived(lambda r: op(getattr(r, name).shift(1), lit))
    if row[0] == "error":
        assert window[0] == "error" and window[1] == row[1]
        return
    assert window == ("ok", row[1], [None] + row[2][:-1])


@pytest.mark.parametrize("name, op, lit", list(_parity_cases()))
def test_group_dialect_keeps_the_rows_the_row_result_keeps(name, op, lit):
    row = derived(lambda r: op(getattr(r, name), lit))
    groups = outcome(
        lambda: table().group_ordered(lambda r: r.k).filter(lambda g: op(g.max(name), lit)).flatten(),
        name="k",
    )
    if row[0] == "error":
        assert groups[0] == "error" and groups[1] == row[1]
        return
    # Flattened groups are not in input order (#196), so compare the kept rows as a set.
    assert groups[:2] == ("ok", "int64")
    assert sorted(groups[2]) == [k for k, kept in enumerate(row[2]) if kept is True]


# ---------------------------------------------------------------------------
# Folded constants equal the same arithmetic on a column
# ---------------------------------------------------------------------------

FOLDS = [
    (2**53, operator.add, 1),
    (7, operator.truediv, 2),
    (-7, operator.floordiv, 2),
    (-7, operator.mod, 2),
    (2**62, operator.mul, 4),
    (1, operator.truediv, 0),
    (2.0, operator.add, 3.0),
    (1, operator.sub, 0.5),
    (float("nan"), operator.eq, float("nan")),
    (float("nan"), operator.gt, 1.0),
    (-0.0, operator.eq, 0.0),
    (2**53, operator.eq, 2**53 + 1),
    (2**53 + 1, operator.gt, 2.0**53),
]


@pytest.mark.parametrize("a, op, b", [
    pytest.param(a, op, b, id=f"{a!r}-{op.__name__}-{b!r}") for a, op, b in FOLDS
])
def test_folded_constant_matches_column_arithmetic(a, op, b):
    zero = 0.0 if isinstance(a, float) else 0
    t = LTSeq.from_arrow(pa.table({"z": pa.array([zero, zero])}))
    folded = outcome(lambda: t.derive(v=lambda r: op(LiteralExpr(a), b)))
    unfolded = outcome(lambda: t.derive(v=lambda r: op(r.z + a, b)))
    assert folded == unfolded


# ---------------------------------------------------------------------------
# A boolean literal equals a constant Boolean column (#209)
# ---------------------------------------------------------------------------


def logic_table():
    return LTSeq.from_arrow(pa.table({
        "k": pa.array([0, 1, 2], pa.int64()),
        "b": pa.array([True, False, None], pa.bool_()),
        "a": pa.array([1, 5, None], pa.int64()),
        "s": pa.array(["x", "y", None], pa.string()),
        "T": pa.array([True] * 3, pa.bool_()),
        "F": pa.array([False] * 3, pa.bool_()),
    })).sort("k")


LOGIC_OPERANDS = {
    "bool": lambda r: r.b,
    "int": lambda r: r.a,
    "string": lambda r: r.s,
    "null": lambda r: LiteralExpr(None),
}


@pytest.mark.parametrize("consumer", ["derive", "filter"])
@pytest.mark.parametrize("op", [operator.and_, operator.or_], ids=["and", "or"])
@pytest.mark.parametrize("lit", [True, False])
@pytest.mark.parametrize("operand", list(LOGIC_OPERANDS))
@pytest.mark.parametrize("side", ["right", "left"])
def test_boolean_literal_matches_constant_column(side, operand, lit, op, consumer):
    def build(other):
        def expr(r):
            e, o = LOGIC_OPERANDS[operand](r), other(r)
            return op(e, o) if side == "right" else op(o, e)
        if consumer == "derive":
            return lambda: logic_table().derive(v=expr)
        return lambda: logic_table().filter(expr)

    name = "v" if consumer == "derive" else "k"
    literal = outcome(build(lambda r: LiteralExpr(lit)), name)
    column = outcome(build(lambda r: r.T if lit else r.F), name)
    assert literal == column


# ---------------------------------------------------------------------------
# Linear scan equals the materialized reference
# ---------------------------------------------------------------------------

SCAN_PREDICATES = {
    "gt_prev": lambda c: lambda r: getattr(r, c) > getattr(r, c).shift(1),
    "ne_prev": lambda c: lambda r: getattr(r, c) != getattr(r, c).shift(1),
    "gt_prev_half": lambda c: lambda r: getattr(r, c) > getattr(r, c).shift(1) + 0.5,
    "step_gt_1": lambda c: lambda r: getattr(r, c) - getattr(r, c).shift(1) > 1,
    "step_gt_half": lambda c: lambda r: getattr(r, c) - getattr(r, c).shift(1) > 0.5,
    "step_gt_dec": lambda c: lambda r: getattr(r, c) - getattr(r, c).shift(1) > D("0.5"),
    "div_gt": lambda c: lambda r: getattr(r, c) / 2 > getattr(r, c).shift(1) / 2,
    "mod_gt": lambda c: lambda r: getattr(r, c) % 2 > getattr(r, c).shift(1) % 2,
    "floordiv_gt": lambda c: lambda r: getattr(r, c) // 2 > getattr(r, c).shift(1) // 2,
    "big_gt": lambda c: lambda r: getattr(r, c) - getattr(r, c).shift(1) > 2**53,
    # shift() arguments the kernel does not implement.
    "ne_prev_default_int": lambda c: lambda r: getattr(r, c) != getattr(r, c).shift(1, default=7),
    "ne_prev_default_dec": lambda c: lambda r: getattr(r, c) != getattr(r, c).shift(1, default=D("1.5")),
    "ne_prev_default_str": lambda c: lambda r: getattr(r, c) != getattr(r, c).shift(1, default="a"),
    "ne_prev_partitioned": lambda c: lambda r: getattr(r, c) != getattr(r, c).shift(1, partition_by="g"),
}


SCAN_COLUMNS = {
    "x": pa.array([1, 2, 4, 3, 2**53 + 1, 2**53, 7, 7], pa.int64()),
    "y": pa.array([1.0, 2.5, 4.0, 3.0, 1e16, 1e16 + 2.0, -0.5, -0.5], pa.float64()),
    "i32": pa.array([2**31 - 1, -(2**31), 0, 5, 3, 3, 9, 1], pa.int32()),
    "u32": pa.array([5, 3, 10, 10, 2**32 - 1, 0, 7, 7], pa.uint32()),
    "u64": pa.array([2**63, 2**63 + 1, 5, 5, 2**64 - 1, 1, 7, 7], pa.uint64()),
    "ts_s": pa.array([0, 100, 3000, 3100, 9000, 9100, 9100, 20000], pa.timestamp("s")),
    "ts_us": pa.array([v * 10**6 for v in [0, 100, 3000, 3100, 9000, 9100, 9100, 20000]], pa.timestamp("us")),
}


def _count_outcome(compute):
    try:
        return ("ok", compute())
    except Exception as error:  # the reference refusing a predicate is an outcome too
        return ("error", type(error).__name__)


@pytest.mark.parametrize("column", list(SCAN_COLUMNS))
@pytest.mark.parametrize("predicate", list(SCAN_PREDICATES))
def test_linear_scan_count_matches_reference(column, predicate):
    g = ["a", "b", "a", "b", "a", "a", "b", "b"]
    t = LTSeq.from_arrow(pa.table({"k": list(range(8)), "g": g, **SCAN_COLUMNS})).sort("k")
    pred = SCAN_PREDICATES[predicate](column)
    reference = _count_outcome(lambda: t.group_ordered(pred).first().to_arrow().num_rows)
    assert _count_outcome(lambda: t.group_ordered(pred).first().count()) == reference
