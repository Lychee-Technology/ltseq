"""Float membership compares -0.0 equal to 0.0, as `==` does (#245).

DataFusion compares the items of a float IN list bit for bit: a list of
constants through a hash set or a branchless filter, any other list through
Arrow's total-order `eq`. Its `=` takes -0.0 to equal 0.0. Which of the two
a membership test met depended on the list's length and receiver:
DataFusion unfolds a list of up to three items on a plain column into `=`,
and its simplifier folds `x == a | x == b | ...` on a column, or on a
repeated expression it has made a column of, back into one IN list. So
`r.x.is_in([-0.0, 1.0, 2.0, 3.0])` missed the 0.0 rows, and so did the
same four equalities joined by `|`.

A zero that becomes a literal only in the optimizer is merged the same
way: a value of `now()`, which DataFusion reads when the query runs, a
constant ltseq did not take for one, or a derived column, whose value
DataFusion writes in its place when it moves a filter below the
projection. `r.x.is_in([now().dt.diff(now(), unit="second"), 1.0, 2.0,
3.0])` missed the -0.0 rows. Negating `!=` gives `==`, so `~((r.x != -0.0)
& (r.x != 1.0) & ...)` did too.

The lists cross the three-item threshold and hold zeros of both signs, an
integer zero, NULL, NaN, a column and zeros DataFusion folds late; the
receivers are a column, a CASE and a computed value, of each float type,
and values of `rand()`, which must be named once.
The reference is SQL's IN over Python's `==` in three-valued logic, with
NaN equal to NaN: every NaN here has the same bits, and NaNs of different
bits are #290's.
"""

from __future__ import annotations

import collections
import functools
import itertools
import math
import operator

import pyarrow as pa
import pytest

from ltseq import LTSeq, if_else, now, rand
from ltseq.expr.core_types import LiteralExpr

NAN = math.nan
INF = math.inf

X = [0.0, -0.0, 1.0, None, NAN, 2.5, -0.0, 0.0, INF, 3.0]
Y = [-0.0, 0.0, 0.0, 1.0, NAN, None, -0.0, 2.5, INF, -0.0]

# The column `y`, as a list item.
Y_ITEM = "r.y"


def clock_zero():
    """Zero, from a stable function DataFusion evaluates when the query runs."""
    return now().dt.diff(now(), unit="second")


# Items that are expressions, with their value on row `k`. Besides `y`,
# zeros DataFusion folds after lowering: from the clock, and a constant
# holding an IN.
ITEMS = {
    Y_ITEM: (lambda r: r.y, lambda k: Y[k]),
    "now 0": (lambda r: clock_zero(), lambda k: 0.0),
    "now -0.0": (lambda r: clock_zero() * -1.0, lambda k: -0.0),
    "constant -0.0": (lambda r: if_else(LiteralExpr(1).is_in([1]), -0.0, 1.0), lambda k: -0.0),
}
FOLDED_LATE = ["now 0", "now -0.0", "constant -0.0"]

X_TYPES = {
    "float64": pa.float64(),
    "float32": pa.float32(),
    "float16": pa.float16(),
    "dictionary": pa.dictionary(pa.int32(), pa.float64()),
    "run_end_encoded": pa.run_end_encoded(pa.int32(), pa.float64()),
}

RECEIVERS = {
    "column": (lambda r: r.x, lambda k: X[k]),
    "case": (lambda r: if_else(r.k % 2 == 0, r.x, r.y), lambda k: X[k] if k % 2 == 0 else Y[k]),
    "computed": (lambda r: r.x * 1.0, lambda k: X[k]),
}

POOL = [-0.0, 0.0, 0, None, NAN, Y_ITEM]
FILLERS = [1.0, 2.5, 7.0]


def item_lists() -> list[list]:
    """Every nonempty subset of the pool, each item folded late alone, with
    one item of the pool and with another, all alone and padded past three
    items, in both orders; and two lists without a zero."""
    out = [[1.0], [1.0, 2.5, 7.0, 3.0]]
    subsets = [list(s) for n in range(1, len(POOL) + 1) for s in itertools.combinations(POOL, n)]
    subsets += [[late, *other] for late in FOLDED_LATE for other in ([], *([item] for item in POOL))]
    subsets += [list(pair) for pair in itertools.combinations(FOLDED_LATE, 2)]
    for subset in subsets:
        for pad in (0, 1, 3):
            items = subset + FILLERS[:pad]
            out.append(items)
            if len(items) > 1:
                out.append(items[::-1])
    return out


LISTS = item_lists()


def table(x_type: pa.DataType) -> LTSeq:
    x = pa.array(X, pa.float64())
    if pa.types.is_dictionary(x_type):
        x = x.dictionary_encode()
    elif pa.types.is_run_end_encoded(x_type):
        x = pa.RunEndEncodedArray.from_arrays(pa.array(range(1, len(X) + 1), pa.int32()), x)
    else:
        x = x.cast(x_type)
    return LTSeq.from_arrow(
        pa.table({"k": pa.array(range(len(X)), pa.int64()), "x": x, "y": pa.array(Y, pa.float64())})
    ).sort("k")


def written(r, items: list) -> list:
    return [ITEMS[item][0](r) if isinstance(item, str) else item for item in items]


def values(k: int, items: list) -> list:
    return [ITEMS[item][1](k) if isinstance(item, str) else item for item in items]


def equal(a, b):
    if a is None or b is None:
        return None
    if math.isnan(a) and math.isnan(b):
        return True
    return a == b


def member(value, items: list):
    """SQL's `value IN (items)`: the disjunction of the equalities."""
    out = False
    for item in items:
        same = equal(value, item)
        if same:
            return True
        if same is None:
            out = None
    return out


def negation(value):
    return None if value is None else not value


def expected(receiver: str, items: list) -> list:
    value = RECEIVERS[receiver][1]
    return [member(value(k), values(k, items)) for k in range(len(X))]


def any_equal(value, items: list):
    return functools.reduce(operator.or_, [value == item for item in items])


def all_unequal(value, items: list):
    return functools.reduce(operator.and_, [value != item for item in items])


def outcomes(x_type: str, receiver: str, lists: list[list]) -> tuple[dict, dict]:
    """This build's `is_in`, its negation, the `|` of `==` and the `&` of
    `!=` and its negation over each list, all in one projection, so
    DataFusion also shares a computed receiver; and the reference's."""
    recv = RECEIVERS[receiver][0]
    columns, wanted = {}, {}
    for items in lists:
        reference = expected(receiver, items)
        name = f"{items}"
        columns[f"in {name}"] = lambda r, items=items: recv(r).is_in(written(r, items))
        wanted[f"in {name}"] = reference
        columns[f"not in {name}"] = lambda r, items=items: ~recv(r).is_in(written(r, items))
        wanted[f"not in {name}"] = [negation(v) for v in reference]
        # `== None` is IS NULL (#154), not an equality with NULL.
        if None in items:
            continue
        columns[f"or {name}"] = lambda r, items=items: any_equal(recv(r), written(r, items))
        wanted[f"or {name}"] = reference
        # Negating `!=` gives `==`, which the simplifier merges into a list
        # it keeps past three items.
        if len(items) > 3:
            columns[f"and {name}"] = lambda r, items=items: all_unequal(recv(r), written(r, items))
            wanted[f"and {name}"] = [negation(v) for v in reference]
            columns[f"not and {name}"] = lambda r, items=items: ~all_unequal(recv(r), written(r, items))
            wanted[f"not and {name}"] = reference
    got = table(X_TYPES[x_type]).derive(**columns).to_arrow()
    return {name: got.column(name).to_pylist() for name in columns}, wanted


ENCODED = ["dictionary", "run_end_encoded"]


def compared_by_column_equality(x_type: str, items: list) -> bool:
    """DataFusion's `=` between an encoded float column and a float column
    compares zeros by sign (#292), and an item that is a column is compared
    with `=`."""
    return x_type in ENCODED and Y_ITEM in items


@pytest.mark.parametrize("receiver", RECEIVERS)
@pytest.mark.parametrize("x_type", X_TYPES)
def test_membership_and_equalities_follow_the_reference(x_type, receiver):
    lists = [items for items in LISTS if not compared_by_column_equality(x_type, items)]
    got, wanted = outcomes(x_type, receiver, lists)
    assert {name: got[name] for name in wanted if got[name] != wanted[name]} == {}


@pytest.mark.parametrize("x_type", ENCODED)
def test_an_encoded_column_meets_a_column_item_as_equality_does(x_type):
    """Where `==` itself is wrong (#292), `is_in` still agrees with it."""
    lists = [items for items in LISTS if compared_by_column_equality(x_type, items) and None not in items]
    got, _ = outcomes(x_type, "column", lists)
    assert {f"{items}" for items in lists if got[f"in {items}"] != got[f"or {items}"]} == set()


@pytest.mark.xfail(strict=True, reason="#292: `=` on an encoded float column compares zeros by sign")
@pytest.mark.parametrize("x_type", ENCODED)
def test_an_encoded_column_meets_a_column_item_as_the_reference_does(x_type):
    lists = [items for items in LISTS if compared_by_column_equality(x_type, items)]
    got, wanted = outcomes(x_type, "column", lists)
    assert {name: got[name] for name in wanted if got[name] != wanted[name]} == {}


@pytest.mark.parametrize(
    "items",
    [
        [-0.0, 1.0, 2.5, 7.0],
        [0.0, 1.0, 2.5, 7.0],
        [0, 1, 2, 3],
        [-0.0],
        [Y_ITEM, 1.0, 2.5, 7.0],
        *([late, 1.0, 2.5, 7.0] for late in FOLDED_LATE),
    ],
    ids=repr,
)
@pytest.mark.parametrize("receiver", ["column", "case"])
def test_filter_keeps_both_zeros(data_layout, receiver, items):
    """Each predicate alone, as a filter DataFusion pushes into the scan and
    prunes Parquet row groups with."""
    rows = pa.table({"k": pa.array(range(len(X)), pa.int64()), "x": X, "y": Y})
    t = data_layout(rows).sort("k")
    recv = RECEIVERS[receiver][0]
    reference = expected(receiver, items)
    keep = [k for k, v in enumerate(reference) if v]

    def kept(predicate):
        return t.filter(predicate).to_arrow().column("k").to_pylist()

    assert kept(lambda r: recv(r).is_in(written(r, items))) == keep
    if None not in items:
        assert kept(lambda r: any_equal(recv(r), written(r, items))) == keep
        assert kept(lambda r: ~all_unequal(recv(r), written(r, items))) == keep


COMBINED_LISTS = {
    "literal": [[0.0, 1.0, 2.5, 7.0], [-0.0, 2.5, 8.0, 9.0], [-0.0, 3.0, 4.0, 5.0]],
    "folded late": [
        ["now 0", 1.0, 2.5, 7.0],
        ["now -0.0", 2.5, 8.0, 9.0],
        ["constant -0.0", 3.0, 4.0, 5.0],
        [0.0, 3.0, 8.0, 9.0],
    ],
}


@pytest.mark.parametrize("lists", COMBINED_LISTS)
@pytest.mark.parametrize("form", ["is_in", "|"])
@pytest.mark.parametrize("combine", ["and", "or"])
@pytest.mark.parametrize("negate", [(False, False), (False, True), (True, True)], ids=str)
def test_lists_the_optimizer_combines(combine, negate, form, lists):
    """Two membership tests on one column, written with `is_in` or with `|`
    of `==`. DataFusion's simplifier merges `==` joined by `|` into lists,
    folds the zeros that come late, and intersects, unites and subtracts
    lists on one column comparing their items bitwise; none of it may decide
    a zero by its sign."""
    test = {
        "is_in": lambda r, items: r.x.is_in(written(r, items)),
        "|": lambda r, items: any_equal(r.x, written(r, items)),
    }[form]
    join = {"and": operator.and_, "or": operator.or_}[combine]
    # The dominant value of `and` or `or` in three-valued logic.
    dominant = combine == "or"
    columns, wanted = {}, {}
    for a, b in itertools.permutations(COMBINED_LISTS[lists], 2):
        sides = list(zip((a, b), negate))
        columns[f"{a} {b}"] = lambda r, sides=sides: join(
            *[~test(r, items) if neg else test(r, items) for items, neg in sides]
        )
        reference = []
        for k, v in enumerate(X):
            each = [member(v, values(k, items)) for items, _ in sides]
            each = [negation(m) if neg else m for m, (_, neg) in zip(each, sides)]
            reference.append(dominant if dominant in each else None if None in each else not dominant)
        wanted[f"{a} {b}"] = reference
    got = table(pa.float64()).derive(**columns).to_arrow()
    assert {name: got.column(name).to_pylist() for name in columns} == wanted


def both(a, b):
    """SQL's AND in three-valued logic."""
    if a is False or b is False:
        return False
    return None if a is None or b is None else True


def either(a, b):
    """SQL's OR in three-valued logic."""
    if a is True or b is True:
        return True
    return None if a is None or b is None else False


# Tests of a value against a zero: a comparison with a value that is or
# folds into a zero, or a membership test whose list holds a zero; and
# their reference on a value.
ZERO_TESTS = {
    "== 0.0": (lambda x: x == 0.0, lambda v: equal(v, 0.0)),
    "== -0.0": (lambda x: x == -0.0, lambda v: equal(v, 0.0)),
    "!= 0.0": (lambda x: x != 0.0, lambda v: negation(equal(v, 0.0))),
    "~(!= -0.0)": (lambda x: ~(x != -0.0), lambda v: equal(v, 0.0)),
    "== now 0": (lambda x: x == clock_zero(), lambda v: equal(v, 0.0)),
    "!= now -0.0": (lambda x: x != clock_zero() * -1.0, lambda v: negation(equal(v, 0.0))),
    "in [0.0, 5.0]": (lambda x: x.is_in([0.0, 5.0]), lambda v: member(v, [0.0, 5.0])),
    "in [-0.0, 5.0, 6.0, 7.0]": (
        lambda x: x.is_in([-0.0, 5.0, 6.0, 7.0]),
        lambda v: member(v, [-0.0, 5.0, 6.0, 7.0]),
    ),
}
# Lists that meet the zeros in nothing, in a NULL, in a zero, or in both.
LISTS_BESIDE = [[1.0, 2.5, 7.0, 8.0], [None, 1.0, 2.5, 7.0], [-0.0, 1.0, 2.5, 7.0], [0.0, None, 2.5, 7.0]]
JOINS = {"&": (operator.and_, both), "|": (operator.or_, either)}


def beside(receiver: str, zero_test: str, items: list, join: str, negate_list: bool, negate_all: bool):
    """A test against a zero joined to the membership test of `items` on the
    same receiver, the latter or the whole negated; and the reference."""
    recv, recv_value = RECEIVERS[receiver]
    compare, truth = ZERO_TESTS[zero_test]
    join_exprs, join_values = JOINS[join]

    def predicate(r):
        test = recv(r).is_in(items)
        joined = join_exprs(compare(recv(r)), ~test if negate_list else test)
        return ~joined if negate_all else joined

    reference = []
    for k in range(len(X)):
        value = recv_value(k)
        test = member(value, items)
        joined = join_values(truth(value), negation(test) if negate_list else test)
        reference.append(negation(joined) if negate_all else joined)
    return predicate, reference


@pytest.mark.parametrize("receiver", RECEIVERS)
@pytest.mark.parametrize("x_type", X_TYPES)
def test_a_zero_beside_a_list(x_type, receiver):
    """DataFusion's simplifier intersects and subtracts two lists on one
    value, and loses NULL results doing so (#296): `x IN (0, -0) AND x NOT
    IN (NULL, 1, ...)` became `x IN (0, -0)`, and `x IN (0, -0) AND x IN
    (1, ...)` false, also where `x` is NULL. A zero is in no list, so a test
    against one, a comparison or a list that holds it, is never combined
    that way, also when its zero folds late."""
    columns, wanted = {}, {}
    for zero_test, items, join, negate_list, negate_all in itertools.product(
        ZERO_TESTS, LISTS_BESIDE, JOINS, [False, True], [False, True]
    ):
        name = f"{'~' * negate_all}({zero_test} {join} {'~' * negate_list}in {items})"
        columns[name], wanted[name] = beside(receiver, zero_test, items, join, negate_list, negate_all)
    got = table(X_TYPES[x_type]).derive(**columns).to_arrow()
    assert {name: got.column(name).to_pylist() for name in wanted if got.column(name).to_pylist() != wanted[name]} == {}


@pytest.mark.parametrize("negate_all", [False, True], ids=["kept", "negated"])
@pytest.mark.parametrize(
    "zero_test, items, join, negate_list",
    [
        ("== 0.0", [None, 1.0, 2.5, 7.0], "&", True),
        ("== 0.0", [1.0, 2.5, 7.0, 8.0], "&", False),
        ("!= 0.0", [None, 1.0, 2.5, 7.0], "&", False),
        ("== now 0", [None, 1.0, 2.5, 7.0], "&", True),
        ("in [0.0, 5.0]", [None, 1.0, 2.5, 7.0], "&", True),
    ],
    ids=str,
)
def test_filter_with_a_zero_beside_a_list(data_layout, zero_test, items, join, negate_list, negate_all):
    rows = pa.table({"k": pa.array(range(len(X)), pa.int64()), "x": X, "y": Y})
    t = data_layout(rows).sort("k")
    predicate, reference = beside("column", zero_test, items, join, negate_list, negate_all)
    kept = t.filter(predicate).to_arrow().column("k").to_pylist()
    assert kept == [k for k, v in enumerate(reference) if v]


# Zeros a derived column `z` holds. DataFusion moves a filter below the
# projection that computes `z` and writes `z`'s value in its place, so a
# zero that lowering saw as a column reaches the simplifier's merge of `=`
# into lists as a literal.
STAGED = {
    "-0.0": lambda r: LiteralExpr(-0.0),
    "0.0": lambda r: LiteralExpr(0.0),
    "now -0.0": lambda r: clock_zero() * -1.0,
}
STAGED_ITEMS = [1.0, 2.5, 7.0]
STAGED_FORMS = {
    "x.is_in([z, ...])": lambda r: r.x.is_in([r.z, *STAGED_ITEMS]),
    "x == z | ...": lambda r: any_equal(r.x, [r.z, *STAGED_ITEMS]),
    "z == x | ...": lambda r: functools.reduce(operator.or_, [r.z == r.x, *(r.x == v for v in STAGED_ITEMS)]),
    "~(x != z & ...)": lambda r: ~all_unequal(r.x, [r.z, *STAGED_ITEMS]),
}


def filtered_by_staged_zero(t: LTSeq, staged: str, form: str) -> dict[str, tuple[list, list]]:
    """The rows a staged-zero predicate keeps and the rows its negation
    keeps, each with the reference's."""
    t = t.derive(z=STAGED[staged])
    predicate = STAGED_FORMS[form]

    def kept(predicate):
        return t.filter(predicate).to_arrow().column("k").to_pylist()

    reference = [member(v, [0.0, *STAGED_ITEMS]) for v in X]
    return {
        "kept": (kept(predicate), [k for k, v in enumerate(reference) if v]),
        "negated": (kept(lambda r: ~predicate(r)), [k for k, v in enumerate(reference) if v is False]),
    }


@pytest.mark.parametrize("form", STAGED_FORMS)
@pytest.mark.parametrize("staged", STAGED)
def test_filter_with_a_zero_a_column_holds(data_layout, staged, form):
    """The list DataFusion builds itself is split after its other rules,
    also where the filter reaches the scan."""
    rows = pa.table({"k": pa.array(range(len(X)), pa.int64()), "x": X, "y": Y})
    for got, want in filtered_by_staged_zero(data_layout(rows).sort("k"), staged, form).values():
        assert got == want


@pytest.mark.parametrize("x_type", X_TYPES)
def test_a_zero_a_column_holds_on_each_float_type(x_type):
    """The bounds that replace a zero keep the list's type, since no type
    coercion follows the optimizer: a dictionary or run-end encoded column
    is compared with encoded bounds. Negated, the simplifier moves the zero
    out of the list into `x != -0.0`, which on an encoded column decides a
    zero by its sign (#292), so there only the kept rows are checked."""
    wrong = {}
    for staged, form in itertools.product(STAGED, STAGED_FORMS):
        for side, (got, want) in filtered_by_staged_zero(table(X_TYPES[x_type]), staged, form).items():
            if got != want and not (side == "negated" and x_type in ENCODED):
                wrong[f"{side} {staged} {form}"] = got
    assert wrong == {}


@pytest.mark.xfail(strict=True, reason="#296: DataFusion intersects lists comparing their items as expressions")
def test_two_columns_holding_zeros_of_either_sign():
    """Two lists the optimizer builds, each with a zero, are intersected by
    the simplifier before they are split, and `-0.0` and `0.0` are different
    items there."""
    t = table(pa.float64()).derive(a=lambda r: LiteralExpr(-0.0), b=lambda r: LiteralExpr(0.0))
    kept = t.filter(lambda r: r.x.is_in([r.a, 1.0, 2.5, 7.0]) & r.x.is_in([r.b, 3.0, 4.0, 5.0]))
    assert kept.to_arrow().column("k").to_pylist() == [k for k, v in enumerate(X) if v == 0.0]


# Values of stable functions that a float has no common type with.
STABLE_OF_ANOTHER_TYPE = {
    "boolean": lambda r: now().dt.year() > 2000,
    "timestamp": lambda r: now(),
}
SPELLINGS = {
    "x == v": lambda x, v: x == v,
    "v == x": lambda x, v: v == x,
    "x != v": lambda x, v: x != v,
    "v != x": lambda x, v: v != x,
    "x.is_in([v, ...])": lambda x, v: x.is_in([v, 1.0, 2.5, 7.0]),
}


@pytest.mark.parametrize("spelling", SPELLINGS)
@pytest.mark.parametrize("kind", STABLE_OF_ANOTHER_TYPE)
@pytest.mark.parametrize("x_type", ["float64", "float32", "float16"])
def test_a_stable_value_of_another_type_is_refused_as_its_column_is(x_type, kind, spelling):
    """A float has no common type with a Boolean or a timestamp, so comparing
    them fails at planning, whether the value comes from `now()` inline or
    from a column it was staged in."""
    t = table(X_TYPES[x_type])
    value, compare = STABLE_OF_ANOTHER_TYPE[kind], SPELLINGS[spelling]
    with pytest.raises(RuntimeError, match="Cannot infer common argument type") as staged:
        t.derive(v=value).derive(c=lambda r: compare(r.x, r.v)).to_arrow()
    with pytest.raises(RuntimeError, match="Cannot infer common argument type") as inline:
        t.derive(c=lambda r: compare(r.x, value(r))).to_arrow()
    assert str(inline.value) == str(staged.value)


# Values of `rand()`, each with every value it can take. DataFusion
# evaluates each copy of a volatile expression, so each copy draws again. A
# test that names its receiver once has one answer over such a domain; one
# that names it twice tests two draws, which need not give that answer.
VOLATILE = {
    "-0.5 or 0.5": (lambda: rand().round(0) - 0.5, [-0.5, 0.5]),
    "-0.0 or 0.0": (lambda: (rand() - 0.5) * 0.0, [-0.0, 0.0]),
    "0.0 or 1.0": (lambda: rand().round(0), [0.0, 1.0]),
    "-0.0, 0.0 or 1.0": (lambda: if_else(rand() < 0.5, (rand() - 0.5) * 0.0, 1.0), [-0.0, 0.0, 1.0]),
}
# Items, with their value: float columns holding one value each, and zeros
# DataFusion folds after lowering.
VOLATILE_ITEMS = {
    "r.nz": (lambda r: r.nz, -0.0),
    "r.z": (lambda r: r.z, 0.0),
    "r.one": (lambda r: r.one, 1.0),
    **{late: (ITEMS[late][0], ITEMS[late][1](0)) for late in FOLDED_LATE},
}
VOLATILE_COMPARISONS = {
    "== 0.0": (lambda r, x: x == 0.0, lambda v: equal(v, 0.0)),
    "== -0.0": (lambda r, x: x == -0.0, lambda v: equal(v, 0.0)),
    "!= 0.0": (lambda r, x: x != 0.0, lambda v: negation(equal(v, 0.0))),
    "~(!= -0.0)": (lambda r, x: ~(x != -0.0), lambda v: equal(v, 0.0)),
    "== now 0": (lambda r, x: x == clock_zero(), lambda v: equal(v, 0.0)),
    "!= now -0.0": (lambda r, x: x != clock_zero() * -1.0, lambda v: negation(equal(v, 0.0))),
    "== r.nz": (lambda r, x: x == r.nz, lambda v: equal(v, 0.0)),
}
VOLATILE_LISTS = [
    [0.0],
    [-0.0, 1.0],
    [0.0, 1.0, 2.5, 7.0],
    [-0.0, 1.0, 2.5, 7.0],
    [0.0, 5.0, 6.0, 7.0],
    [None, 0.0, 1.0, 2.5],
    ["r.nz", "r.one"],
    ["r.z", "r.one", 2.5, 7.0],
    ["r.nz", 1.0, 2.5, 7.0],
    *([late, 1.0, 2.5, 7.0] for late in FOLDED_LATE),
]


def volatile_tests() -> dict:
    """Each test of a value, naming it once, and its reference on a value."""
    tests = dict(VOLATILE_COMPARISONS)
    for items in VOLATILE_LISTS:

        def in_list(r, x, items=items):
            return x.is_in([VOLATILE_ITEMS[i][0](r) if isinstance(i, str) else i for i in items])

        value_items = [VOLATILE_ITEMS[i][1] if isinstance(i, str) else i for i in items]
        tests[f"in {items}"] = (in_list, lambda v, items=value_items: member(v, items))
        tests[f"not in {items}"] = (
            lambda r, x, in_list=in_list: ~in_list(r, x),
            lambda v, items=value_items: negation(member(v, items)),
        )
    return tests


VOLATILE_TESTS = volatile_tests()
VOLATILE_ROWS = 400


def decided_by_sign(receiver: str, name: str) -> bool:
    """A list on a volatile value keeps an item that varies by row as
    written, so it compares that item with a zero by sign (#298)."""
    _, domain = VOLATILE[receiver]
    return 0.0 in domain and ("'r.nz'" in name or "'r.z'" in name)


def constant_tests(receiver: str, *, by_sign: bool = False) -> dict:
    """The tests whose answer is the same for every value `receiver` can
    take, with that answer; those `decided_by_sign` only when `by_sign`."""
    _, domain = VOLATILE[receiver]
    out = {}
    for name, (test, truth) in VOLATILE_TESTS.items():
        answers = {truth(v) for v in domain}
        if len(answers) == 1 and decided_by_sign(receiver, name) == by_sign:
            out[name] = (test, answers.pop())
    return out


def volatile_table() -> LTSeq:
    n = VOLATILE_ROWS
    return LTSeq.from_arrow(
        pa.table({"k": range(n), "nz": [-0.0] * n, "z": [0.0] * n, "one": [1.0] * n})
    )


def wrong_answers(receiver: str, tests: dict, forms: tuple[str, ...]) -> dict:
    """The answers other than the one the receiver's domain allows, counted,
    per test with the receiver inline or staged in a column."""
    draw, _ = VOLATILE[receiver]
    built = {
        "inline": lambda: volatile_table().derive(
            **{name: lambda r, test=test: test(r, draw()) for name, (test, _) in tests.items()}
        ),
        "staged": lambda: volatile_table()
        .derive(v=lambda r: draw())
        .derive(**{name: lambda r, test=test: test(r, r.v) for name, (test, _) in tests.items()}),
    }
    wrong = {}
    for form in forms:
        got = built[form]().to_arrow()
        for name, (_, answer) in tests.items():
            counts = {v: n for v, n in collections.Counter(got.column(name).to_pylist()).items() if v != answer}
            if counts:
                wrong[f"{form} {name}"] = counts
    return wrong


@pytest.mark.parametrize("receiver", VOLATILE)
def test_a_volatile_value_is_tested_once(engine_layout, receiver):
    """A comparison or `is_in` names a volatile receiver once, so each row
    gives the one answer its domain allows; a range or a test beside the
    list would name it again and test another draw (`-0.5 or 0.5` is then
    between -0.0 and 0.0 on a quarter of the rows). The receiver staged in
    a column, which DataFusion computes once, is the control."""
    assert wrong_answers(receiver, constant_tests(receiver), ("inline", "staged")) == {}


@pytest.mark.xfail(strict=True, reason="#298: a list on a volatile value compares a column item's zeros by sign")
@pytest.mark.parametrize("receiver", [name for name, (_, domain) in VOLATILE.items() if 0.0 in domain])
def test_a_volatile_value_meets_a_column_item_as_equality_does(receiver):
    assert wrong_answers(receiver, constant_tests(receiver, by_sign=True), ("inline",)) == {}


@pytest.mark.parametrize("receiver", VOLATILE)
def test_a_volatile_value_filters_all_rows_or_none(receiver):
    """The same tests as filters: each keeps every row or none."""
    draw, _ = VOLATILE[receiver]
    t = volatile_table()
    tests = constant_tests(receiver)
    kept = {name: t.filter(lambda r, test=test: test(r, draw())).count() for name, (test, _) in tests.items()}
    assert kept == {name: VOLATILE_ROWS if answer else 0 for name, (_, answer) in tests.items()}


def test_constant_receiver():
    """A membership test of no column is folded by DataFusion before it
    runs, also when the clock gives its value or an item."""
    t = table(pa.float64())
    got = t.derive(
        a=lambda r: LiteralExpr(0.0).is_in([-0.0, 1.0]),
        b=lambda r: LiteralExpr(-0.0).is_in([0.0, 1.0, 2.0, 3.0]),
        c=lambda r: LiteralExpr(0.0) == -0.0,
        d=lambda r: (clock_zero() * -1.0).is_in([0.0, 1.0, 2.0, 3.0]),
        e=lambda r: LiteralExpr(-0.0).is_in([clock_zero() * 1.0, 1.0, 2.0, 3.0]),
        f=lambda r: (clock_zero() * -1.0) != 0.0,
    ).to_arrow()
    assert got.column("f").to_pylist() == [False] * len(X)
    for name in "abcde":
        assert got.column(name).to_pylist() == [True] * len(X), name
