"""Compound membership tests against SQL's three-valued logic (#296).

DataFusion 55's simplifier combined two IN lists on one value by set
algebra on their items, compared as expressions: `x IN A AND x IN B`
became `x IN (A ∩ B)`, `x IN A AND x NOT IN B` became `x IN (A \\ B)`, and
an empty result became a constant. That agrees with `=` only for literals
that are neither NULL nor floats, so a NULL item turned a NULL result into
TRUE or FALSE, a column item or a staged constant was lost, and the
simplifier merges `==` joined by `|` into a list, so equalities met it too.
DataFusion 56 applies the algebra only to lists of such literals
(apache/datafusion#24952) and keeps a nullable operand's NULL when the
result folds to a constant (apache/datafusion#24258).

Each case below combines two membership tests of one value under `&`, `|`
and `~`, each written as `is_in`, as `==` joined by `|`, or as `~` of `!=`
joined by `&`, with lists below and above DataFusion's three-item
unfolding threshold that hold NULL, a column, or a constant staged in a
derived column. Every case is asked as a projection and as a filter, on an
unsorted table and on the same table sorted, and compared with SQL's
three-valued logic, the current semantics of `is_in` until contract §8.5's
never-NULL membership lands (v0.5), which these tests do not implement.
`r.x == None` reads as `IS NULL` in the DSL, so a `None` item is written
only through `is_in`. `is_in` admits literal items only (#294, #295), so
a column item is written with `==`.

On DataFusion 55.0.0 the NULL-item and staged-item cases fail (the
#411 spike's matrix measured 302 such cases); the plan tests check that
the fix is the simplifier keeping both tests, not a plan shape that keeps
them apart (PR #402).
"""

from __future__ import annotations

import functools
import operator

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from ltseq import LTSeq
from ltseq.expr import LiteralExpr, if_else
from ltseq.expr.base import rand


# --- SQL's three-valued logic ------------------------------------------------


def and3(a, b):
    if a is False or b is False:
        return False
    if a is None or b is None:
        return None
    return True


def or3(a, b):
    if a is True or b is True:
        return True
    if a is None or b is None:
        return None
    return False


def not3(a):
    return None if a is None else not a


def eq3(a, b):
    return None if a is None or b is None else a == b


def member3(x, items):
    """`x IN (items)`: the disjunction of `x = item`."""
    return functools.reduce(or3, [eq3(x, item) for item in items], False)


# --- the data ----------------------------------------------------------------

# x, the column item y and k, per type. The staged constant z is the first
# of `STAGED` and is the value of x on row 0.
DATA = {
    "int64": {
        "type": pa.int64(),
        "x": [1, 2, 3, None, 5, 42, 6],
        "y": [1, 0, 3, 3, None, 42, 7],
        "staged": 1,
        "short": [1, 2, 5],
        "long": [1, 2, 5, 9],
        "null": [None, 3, 4, 6],
        "plain": [3, 4, 6, 1],
        "column": ["y", 5, 50, 500],
        "staged_list": ["z", 2, 10, 11],
    },
    "string": {
        "type": pa.string(),
        "x": ["a", "b", "c", None, "e", "zz", "f"],
        "y": ["a", "", "c", "c", None, "zz", "g"],
        "staged": "a",
        "short": ["a", "b", "e"],
        "long": ["a", "b", "e", "i"],
        "null": [None, "c", "d", "f"],
        "plain": ["c", "d", "f", "a"],
        "column": ["y", "e", "ee", "eee"],
        "staged_list": ["z", "b", "j", "k"],
    },
}


def table(kind: str) -> LTSeq:
    d = DATA[kind]
    n = len(d["x"])
    return LTSeq.from_arrow(
        pa.table(
            {
                "k": pa.array(range(n), pa.int64()),
                "x": pa.array(d["x"], d["type"]),
                "y": pa.array(d["y"], d["type"]),
            }
        )
    ).derive(z=lambda r: LiteralExpr(d["staged"]))


def item_value(item, row):
    return row[item] if item in ("y", "z") else item


def item_expr(r, item):
    return getattr(r, item) if item in ("y", "z") else item


# --- the membership tests ------------------------------------------------------
#
# A test is (name, items, form). `is_in` items are literals; `eq` and `ne`
# items may also name the column `y` or the staged constant `z`.


def written(r, items, form):
    if form == "is_in":
        return r.x.is_in(items)
    if form == "eq":
        return functools.reduce(operator.or_, [r.x == item_expr(r, i) for i in items])
    return ~functools.reduce(operator.and_, [r.x != item_expr(r, i) for i in items])


def membership_tests(kind):
    d = DATA[kind]
    first = [
        ("is_in short", d["short"], "is_in"),
        ("is_in long", d["long"], "is_in"),
        ("eq long", d["long"], "eq"),
        ("eq column", d["column"], "eq"),
        ("eq staged", d["staged_list"], "eq"),
    ]
    second = [
        ("is_in null", d["null"], "is_in"),
        ("is_in [None]", [None], "is_in"),
        ("is_in plain", d["plain"], "is_in"),
        ("eq column", d["column"], "eq"),
        ("ne column", d["column"], "ne"),
        ("ne staged", d["staged_list"], "ne"),
    ]
    return first, second


def cases(kind):
    """Every `A op B`, `A op ~B` and their negations, with the reference."""
    first, second = membership_tests(kind)
    out = []
    for a in first:
        for b in second:
            for op_name in ("&", "|"):
                for negate_b in (False, True):
                    for negate_all in (False, True):
                        name = f"{'~' if negate_all else ''}({a[0]} {op_name} {'~' if negate_b else ''}{b[0]})"
                        out.append((name, a, b, op_name, negate_b, negate_all))
    return out


def build(case):
    _, a, b, op_name, negate_b, negate_all = case

    def predicate(r):
        left = written(r, a[1], a[2])
        right = written(r, b[1], b[2])
        if negate_b:
            right = ~right
        combined = (left & right) if op_name == "&" else (left | right)
        return ~combined if negate_all else combined

    return predicate


def reference(case, row):
    _, a, b, op_name, negate_b, negate_all = case
    x = row["x"]
    left = member3(x, [item_value(i, row) for i in a[1]])
    right = member3(x, [item_value(i, row) for i in b[1]])
    if negate_b:
        right = not3(right)
    combined = and3(left, right) if op_name == "&" else or3(left, right)
    return not3(combined) if negate_all else combined


def rows(kind):
    d = DATA[kind]
    return [
        {"k": k, "x": x, "y": y, "z": d["staged"]}
        for k, (x, y) in enumerate(zip(d["x"], d["y"]))
    ]


def keyed(result, column):
    """A result column in the order of `k`: an unsorted table at several
    partitions returns its rows in no defined order."""
    pairs = sorted(zip(result.column("k").to_pylist(), result.column(column).to_pylist()))
    return [value for _, value in pairs]


LAYOUTS = {
    "unsorted": lambda t: t,
    "sorted": lambda t: t.sort("k"),
}


@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("kind", DATA)
def test_compound_membership_follows_three_valued_logic(kind, layout):
    """Projections and filters of every compound case, against the
    reference. A projection gives the three-valued answer per row; a filter
    keeps the rows where it is TRUE, compared as a set of keys since a
    filter of an unsorted table declares no order."""
    t = LAYOUTS[layout](table(kind))
    all_cases = cases(kind)
    reference_rows = rows(kind)
    names = {f"c{i}": case for i, case in enumerate(all_cases)}
    projected = t.derive(**{name: build(case) for name, case in names.items()}).to_arrow()
    order = projected.column("k").to_pylist()
    by_key = {row["k"]: row for row in reference_rows}
    wrong = {}
    for name, case in names.items():
        expected = [reference(case, by_key[k]) for k in order]
        got = projected.column(name).to_pylist()
        if got != expected:
            wrong[f"projection {case[0]}"] = (got, expected)
        kept = sorted(t.filter(build(case)).to_arrow().column("k").to_pylist())
        expected_keys = sorted(k for k in by_key if reference(case, by_key[k]) is True)
        if kept != expected_keys:
            wrong[f"filter {case[0]}"] = (kept, expected_keys)
    assert wrong == {}


@pytest.mark.parametrize("kind", DATA)
def test_the_issue_examples_on_each_type(engine_layout, kind):
    """#296's own examples through the LTSeq API, at every engine layout:
    with `x = [v1, v2, v3, None]`, `is_in([v1, v2]) & ~is_in([None])` was
    `[True, True, False, None]` on 55, `is_in([v1, v2]) & is_in([None])`
    all FALSE and `~is_in([v1, v2]) | ~is_in([None])` all TRUE."""
    d = DATA[kind]
    v1, v2, v3 = d["x"][:3]
    t = LTSeq.from_arrow(
        pa.table({"k": pa.array(range(4), pa.int64()), "x": pa.array([v1, v2, v3, None], d["type"])})
    )
    derived = t.derive(
        a=lambda r: r.x.is_in([v1, v2]) & ~r.x.is_in([None]),
        b=lambda r: r.x.is_in([v1, v2]) & r.x.is_in([None]),
        c=lambda r: ~r.x.is_in([v1, v2]) | ~r.x.is_in([None]),
    ).to_arrow()
    assert keyed(derived, "a") == [None, None, False, None]
    assert keyed(derived, "b") == [None, None, False, None]
    assert keyed(derived, "c") == [None, None, True, None]
    # The filter that turns the test into a value keeps the NULL rows.
    kept = t.filter(lambda r: (r.x.is_in([v1, v2]) & ~r.x.is_in([None])).is_null())
    assert keyed(kept.to_arrow(), "x") == [v1, v2, None]
    assert t.filter(lambda r: r.x.is_in([v1, v2]) & ~r.x.is_in([None])).to_arrow().num_rows == 0


@pytest.mark.parametrize("kind", DATA)
def test_a_null_item_keeps_both_lists_in_the_plan(kind):
    """Plan evidence for the NULL rewrite: 55 planned
    `is_in(long) & ~is_in(null)` as the first list alone (the difference
    dropped the `NOT IN`), 56 keeps both lists in the projection's
    optimized plan."""
    d = DATA[kind]
    t = table(kind)
    projected = t.derive(v=lambda r: r.x.is_in(d["long"]) & ~r.x.is_in(d["null"]))
    logical, _ = projected.explain_plan()
    line = next(l for l in logical.splitlines() if " AS v" in l)
    assert " IN (" in line and " NOT IN (" in line, logical
    assert "NULL" in line, logical


@pytest.mark.parametrize("kind", DATA)
def test_a_staged_item_keeps_the_difference_in_the_filter_plan(kind):
    """Plan evidence for a staged constant: DataFusion writes the constant
    in place of `z` when it moves the filter, and 55 then subtracted the
    lists, keeping `x IN (z, ...)` and dropping the `NOT IN` that holds
    NULL. 56 either keeps both or folds the filter, which is never TRUE,
    into an empty relation; either way no list stands alone."""
    d = DATA[kind]
    t = table(kind)
    items = d["staged_list"]
    kept = t.filter(lambda r: written(r, items, "eq") & ~r.x.is_in(d["null"]))
    logical, _ = kept.explain_plan()
    filters = [l for l in logical.splitlines() if "Filter:" in l]
    assert "EmptyRelation" in logical or all(" NOT IN (" in l for l in filters), logical
    assert kept.to_arrow().num_rows == 0


# --- run-end encoded values (B1 of the DataFusion 56 spike) ------------------

RUN_END = {
    "int64": (pa.int64(), [1, 42, None], [None, 1, 2, 3]),
    "float64": (pa.float64(), [1.0, 42.0, None], [None, 1.0, 2.0, 3.0]),
    "string": (pa.string(), ["a", "zz", None], [None, "a", "b", "c"]),
}


@pytest.mark.parametrize("kind", RUN_END)
def test_a_run_end_encoded_value_keeps_the_null_of_a_list(engine_layout, kind):
    """DataFusion 56 compares a run-end encoded value with an IN list at the
    encoded type, and its set filter for four items or more misses the
    list's NULL there: `42 NOT IN (NULL, 1, 2, 3)` was TRUE, not NULL. LTSeq
    tests the value decoded. Checked as a projection, as a bare filter and
    as filters that turn the test into a value, where the wrong answer
    keeps wrong rows."""
    value_type, values, items = RUN_END[kind]
    x = pc.run_end_encode(pa.array(values, value_type))
    t = LTSeq.from_arrow(pa.table({"k": pa.array(range(len(values)), pa.int64()), "x": x}))
    derived = t.derive(
        member=lambda r: r.x.is_in(items), outside=lambda r: ~r.x.is_in(items)
    ).to_arrow()
    assert keyed(derived, "member") == [True, None, None]
    assert keyed(derived, "outside") == [False, None, None]
    kept = lambda predicate: sorted(t.filter(predicate).to_arrow().column("k").to_pylist())
    assert kept(lambda r: ~r.x.is_in(items)) == []
    assert kept(lambda r: r.x.is_in(items)) == [0]
    assert kept(lambda r: if_else(~r.x.is_in(items), 1, 2) == 1) == []
    assert kept(lambda r: (~r.x.is_in(items)).is_null()) == [1, 2]


# --- volatile values -----------------------------------------------------------


def test_two_draws_tested_against_two_lists_are_two_draws():
    """#296's volatile case: `rand().round(0)` written twice and tested
    against two lists is two draws, so about a quarter of the rows hold
    `0` in the first and `1` in the second. Combining the lists over one
    draw would keep none. DataFusion 56 keeps volatile operands out of the
    set rewrites; LTSeq also stages each draw in a column of its own.
    LTSeq's own lowering of a value it names twice is #298, not tested
    here."""
    n = 20_000
    t = LTSeq.from_arrow(pa.table({"k": pa.array(range(n), pa.int64())}))
    kept = t.filter(
        lambda r: rand().round(0).is_in([0.0, 5.0, 6.0, 7.0])
        & rand().round(0).is_in([1.0, 5.0, 6.0, 7.0])
    )
    share = kept.count() / n
    assert 0.22 < share < 0.28, share
