"""The counting kernel's NULL semantics against the DataFusion path (#189).

`group_ordered(pred).first().count()` counts with the linear-scan kernel
when `can_linear_scan` admits the predicate, and otherwise materializes the
grouped table. The materialized count is the reference: the predicate is
evaluated in SQL three-valued logic, and a row whose predicate is NULL starts
a group. The kernel must decline a predicate before executing it or return
the reference's count. `count()` falls back only when the kernel raises, so
a wrong kernel count reaches the user.

The predicates are generated over NULL-bearing columns from leaves that
cover every operator the kernel admits, combined with `&`, `|` and `~`, so
that NULL meets each of them.
"""

from __future__ import annotations

import random

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq

DECLINED = "only supports shift-based boundary predicates"

# NULLs sit in the first and last rows, next to each other, and in different
# rows per column, so a comparison or arithmetic result is NULL on some rows
# where its partner in `&` or `|` is TRUE, and on others where it is FALSE.
# `x` holds no zero, so dividing by it cannot fail.
ROWS = {
    "x": (pa.int64(), [None, 1, 1, 4, None, None, 5, 9, 9, 2, 3, None, 3, 7]),
    "y": (pa.int64(), [1, 1, None, 1, 2, 2, None, 2, 3, 3, 3, 3, None, 3]),
    "n": (pa.int32(), [2, None, 2, 3, 3, 1, 1, None, 4, 4, 8, 8, 6, None]),
    "f": (pa.float64(), [0.5, None, 1.0, 1.5, None, 4.0, 4.0, 2.5, None, None, 6.0, 6.25, 6.5, 1.0]),
    "b": (pa.bool_(), [True, None, False, True, None, None, False, True, True, False, None, True, False, None]),
    "s": (pa.string(), ["a", None, "a", "b", None, "b", "b", None, "c", "c", "a", "a", None, "a"]),
}

COMPARISONS = {
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
    ">": lambda a, b: a > b,
    "<": lambda a, b: a < b,
    ">=": lambda a, b: a >= b,
    "<=": lambda a, b: a <= b,
}


def leaves() -> dict:
    """Boolean leaves by name. Most contain a `shift(1)`; those that do not
    count only inside a tree with one that does. The last group is declined
    by the kernel; the reference counts them, and they are here so that the
    kernel cannot start taking them without its count being compared."""
    out = {}
    for sym, cmp in COMPARISONS.items():
        out[f"x {sym} x.shift(1)"] = lambda r, cmp=cmp: cmp(r.x, r.x.shift(1))
        out[f"x.shift(1) {sym} 3"] = lambda r, cmp=cmp: cmp(r.x.shift(1), 3)
        out[f"(x - x.shift(1)) {sym} 1"] = lambda r, cmp=cmp: cmp(r.x - r.x.shift(1), 1)
        out[f"(x.shift(1) - x) {sym} -1"] = lambda r, cmp=cmp: cmp(r.x.shift(1) - r.x, -1)
        out[f"n.shift(1) {sym} x"] = lambda r, cmp=cmp: cmp(r.n.shift(1), r.x)
    out.update({
        "y == y.shift(1)": lambda r: r.y == r.y.shift(1),
        "(x + y.shift(1)) > 6": lambda r: (r.x + r.y.shift(1)) > 6,
        "(x * 2) >= (x.shift(1) + 3)": lambda r: (r.x * 2) >= (r.x.shift(1) + 3),
        "(x / 2) != (x.shift(1) / 2)": lambda r: (r.x / 2) != (r.x.shift(1) / 2),
        "(y.shift(1) / x) < 1": lambda r: (r.y.shift(1) / r.x) < 1,
        "(n - 1) > n.shift(1)": lambda r: (r.n - 1) > r.n.shift(1),
        "x.shift(1) == None": lambda r: r.x.shift(1) == None,  # noqa: E711
        "y != None": lambda r: r.y != None,  # noqa: E711
        "(x != x.shift(1)).is_null()": lambda r: (r.x != r.x.shift(1)).is_null(),
        "(x.shift(1) > 3).is_not_null()": lambda r: (r.x.shift(1) > 3).is_not_null(),
        "f != f.shift(1)": lambda r: r.f != r.f.shift(1),
        "f == f.shift(1)": lambda r: r.f == r.f.shift(1),
        "f.shift(1) > 1.5": lambda r: r.f.shift(1) > 1.5,
        "(f - f.shift(1)) > 0.5": lambda r: (r.f - r.f.shift(1)) > 0.5,
        "f.shift(1) == None": lambda r: r.f.shift(1) == None,  # noqa: E711
        "b": lambda r: r.b,
        "b.shift(1)": lambda r: r.b.shift(1),
        "s != s.shift(1)": lambda r: r.s != r.s.shift(1),
        # Declined: mixed integer and float operands (#189 part 2), Float64
        # `<`, string `==`.
        "(x - x.shift(1)) > -0.5": lambda r: (r.x - r.x.shift(1)) > -0.5,
        "x.shift(1) > 1.5": lambda r: r.x.shift(1) > 1.5,
        "(f - f.shift(1)) > 5": lambda r: (r.f - r.f.shift(1)) > 5,
        "f < f.shift(1)": lambda r: r.f < r.f.shift(1),
        "s == s.shift(1)": lambda r: r.s == r.s.shift(1),
    })
    return out


LEAVES = leaves()


def build(tree):
    """The predicate for `tree`: a leaf name, `("~", t)`, or `("&" | "|", a, b)`
    where `b` may be a Boolean literal."""
    if isinstance(tree, str):
        return LEAVES[tree]
    if tree[0] == "~":
        inner = build(tree[1])
        return lambda r: ~inner(r)
    left = build(tree[1])
    right = (lambda r, value=tree[2]: value) if isinstance(tree[2], bool) else build(tree[2])
    if tree[0] == "&":
        return lambda r: left(r) & right(r)
    return lambda r: left(r) | right(r)


def show(tree) -> str:
    if isinstance(tree, (str, bool)):
        return str(tree)
    if tree[0] == "~":
        return f"~({show(tree[1])})"
    return f"({show(tree[1])}) {tree[0]} ({show(tree[2])})"


def random_tree(rng: random.Random, depth: int):
    if depth == 0 or rng.random() < 0.25:
        return rng.choice(sorted(LEAVES))
    roll = rng.random()
    if roll < 0.2:
        return ("~", random_tree(rng, depth - 1))
    op = "&" if roll < 0.6 else "|"
    right = rng.random() < 0.5 if rng.random() < 0.1 else random_tree(rng, depth - 1)
    return (op, random_tree(rng, depth - 1), right)


def trees() -> list:
    """Each leaf alone, negated, and next to a partner that is NULL, TRUE
    and FALSE on different rows; then seeded random trees of depth 3."""
    out = []
    for name in LEAVES:
        out += [name, ("~", name), ("&", name, "y == y.shift(1)"), ("|", name, "x.shift(1) > 3")]
    rng = random.Random(189)
    return out + [random_tree(rng, 3) for _ in range(150)]


def reference(t: LTSeq, pred) -> int:
    return len(t.group_ordered(pred).first().to_pandas())


def kernel(t: LTSeq, pred):
    """The kernel's count, or None when it declines the predicate before
    executing. Any other error fails the test: the kernel admitted a
    predicate it cannot evaluate."""
    try:
        return t._inner.group_ordered_count(t._capture_expr(pred))
    except ValueError as error:
        if DECLINED in str(error):
            return None
        raise


def operators(node, found: set) -> set:
    """Every operator and function a captured expression uses."""
    if isinstance(node, dict):
        kind = node.get("type")
        if kind in ("BinOp", "UnaryOp"):
            found.add(node["op"])
        elif kind == "Call":
            found.add(node["func"])
        for value in node.values():
            operators(value, found)
    elif isinstance(node, list):
        for value in node:
            operators(value, found)
    return found


def test_kernel_count_matches_reference_or_declines():
    size = len(ROWS["x"][1])
    columns = {"i": pa.array(range(size), pa.int64())}
    columns.update({name: pa.array(values, dtype) for name, (dtype, values) in ROWS.items()})
    t = LTSeq.from_arrow(pa.table(columns)).sort("i")
    mismatches = []
    evaluated = set()
    for tree in trees():
        pred = build(tree)
        fast = kernel(t, pred)
        if fast is None:
            continue
        operators(t._capture_expr(pred), evaluated)
        expected = reference(t, pred)
        if fast != expected:
            mismatches.append(f"{show(tree)}: kernel={fast} reference={expected}")
    assert not mismatches, "\n".join(mismatches)
    # The kernel counted predicates with every operator it admits, so the
    # comparison was not vacuous for any of them.
    admitted = {"Eq", "Ne", "Gt", "Lt", "Ge", "Le", "And", "Or", "Add", "Sub", "Mul", "Div"}
    assert admitted | {"Not", "shift", "is_null", "is_not_null"} <= evaluated


# The repro in #189 and shapes found while fixing it, with their counts in
# three-valued logic. `x` is NULL at row 2 and `f` at rows 2 and 6.
NULL_X = [1, 2, None, 10, 11, 30, 31, 32]
NULL_Y = [1, 1, 1, 2, 2, 2, 3, 3]
NULL_F = [1.0, 2.0, None, 10.0, 11.0, 30.0, None, 32.0]

REGRESSIONS = {
    # The `>` arm read a NULL operand as false when the right side was constant.
    "swapped_gap": (lambda r: (r.x.shift(1) - r.x) > -5, 7),
    "prev_over": (lambda r: r.x.shift(1) > 5, 6),
    "prev_over_or_false": (lambda r: (r.x.shift(1) > 1) | False, 7),
    "prev_over_is_null": (lambda r: (r.x.shift(1) > 1) == None, 2),  # noqa: E711
    # `&` and `|` were not Kleene: NULL AND FALSE was NULL, a boundary.
    "swapped_gap_and_same_y": (lambda r: ((r.x.shift(1) - r.x) < -5) & (r.y == r.y.shift(1)), 3),
    "float_gap_and_same_y": (lambda r: ((r.f - r.f.shift(1)) > 0.5) & (r.y == r.y.shift(1)), 6),
    # The integer `!=` arm read a NULL operand as true, so `~` made it false.
    "not_changes": (lambda r: ~(r.x != r.x.shift(1)), 3),
    "changes_is_null": (lambda r: (r.x != r.x.shift(1)).is_null(), 3),
}


def regression_rows() -> pa.Table:
    return pa.table({
        "i": pa.array(range(len(NULL_X)), pa.int64()),
        "x": pa.array(NULL_X, pa.int64()),
        "y": pa.array(NULL_Y, pa.int64()),
        "f": pa.array(NULL_F, pa.float64()),
    })


@pytest.mark.parametrize("name", REGRESSIONS)
def test_regressions_count_in_three_valued_logic(data_layout, name):
    pred, expected = REGRESSIONS[name]
    t = data_layout(regression_rows()).sort("i")
    assert reference(t, pred) == expected
    assert kernel(t, pred) == expected
    assert t.group_ordered(pred).first().count() == expected


@pytest.mark.parametrize("row_group_size", [1, 3, 1000])
@pytest.mark.parametrize("name", REGRESSIONS)
def test_regressions_on_sorted_parquet(tmp_path, row_group_size, name):
    # The parallel Parquet count takes only fused predicates and hands these
    # to the general linear-scan path.
    pred, expected = REGRESSIONS[name]
    path = str(tmp_path / "events.parquet")
    pq.write_table(regression_rows(), path, row_group_size=row_group_size)
    t = LTSeq.read_parquet(path).assume_sorted("i")
    assert kernel(t, pred) == expected
    assert reference(t, pred) == expected
