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

The lists cross the three-item threshold and hold zeros of both signs, an
integer zero, NULL, NaN and an item that is not a literal; the receivers
are a column, a CASE and a computed value, of each float type. The
reference is SQL's IN over Python's `==` in three-valued logic, with NaN
equal to NaN: every NaN here has the same bits, and NaNs of different bits
are #290's.
"""

from __future__ import annotations

import functools
import itertools
import math
import operator

import pyarrow as pa
import pytest

from ltseq import LTSeq, if_else
from ltseq.expr.core_types import LiteralExpr

NAN = math.nan
INF = math.inf

X = [0.0, -0.0, 1.0, None, NAN, 2.5, -0.0, 0.0, INF, 3.0]
Y = [-0.0, 0.0, 0.0, 1.0, NAN, None, -0.0, 2.5, INF, -0.0]

# The column `y`, as a list item.
Y_ITEM = "r.y"

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
    """Every nonempty subset of the pool, alone and padded past three items,
    in both orders, and two lists without a zero."""
    out = [[1.0], [1.0, 2.5, 7.0, 3.0]]
    for n in range(1, len(POOL) + 1):
        for subset in itertools.combinations(POOL, n):
            for pad in (0, 1, 3):
                items = list(subset) + FILLERS[:pad]
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
    return [r.y if item == Y_ITEM else item for item in items]


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
    return [
        member(value(k), [Y[k] if item == Y_ITEM else item for item in items])
        for k in range(len(X))
    ]


def outcomes(x_type: str, receiver: str, lists: list[list]) -> tuple[dict, dict]:
    """This build's `is_in`, its negation and the `|` of `==` over each list,
    all in one projection, so DataFusion also shares a computed receiver;
    and the reference's."""
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
        if None not in items:
            columns[f"or {name}"] = lambda r, items=items: functools.reduce(
                operator.or_, [recv(r) == item for item in written(r, items)]
            )
            wanted[f"or {name}"] = reference
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
    [[-0.0, 1.0, 2.5, 7.0], [0.0, 1.0, 2.5, 7.0], [0, 1, 2, 3], [-0.0], [Y_ITEM, 1.0, 2.5, 7.0]],
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
    assert t.filter(lambda r: recv(r).is_in(written(r, items))).to_arrow().column("k").to_pylist() == keep
    if None not in items:
        either = lambda r: functools.reduce(operator.or_, [recv(r) == item for item in written(r, items)])
        assert t.filter(either).to_arrow().column("k").to_pylist() == keep


@pytest.mark.parametrize("combine", ["and", "or"])
@pytest.mark.parametrize("negate", [(False, False), (False, True), (True, True)], ids=str)
def test_lists_the_optimizer_combines(combine, negate):
    """DataFusion's simplifier intersects, unites and subtracts two lists on
    one column, comparing their items bitwise."""
    lists = [[0.0, 1.0, 2.5, 7.0], [-0.0, 2.5, 8.0, 9.0], [-0.0, 3.0, 4.0, 5.0]]
    join = {"and": operator.and_, "or": operator.or_}[combine]
    # The dominant value of `and` or `or` in three-valued logic.
    dominant = combine == "or"
    columns, wanted = {}, {}
    for a, b in itertools.permutations(lists, 2):
        sides = list(zip((a, b), negate))
        columns[f"{a} {b}"] = lambda r, sides=sides: join(
            *[~r.x.is_in(items) if neg else r.x.is_in(items) for items, neg in sides]
        )
        reference = []
        for v in X:
            values = [negation(member(v, items)) if neg else member(v, items) for items, neg in sides]
            reference.append(dominant if dominant in values else None if None in values else not dominant)
        wanted[f"{a} {b}"] = reference
    got = table(pa.float64()).derive(**columns).to_arrow()
    assert {name: got.column(name).to_pylist() for name in columns} == wanted


def test_constant_receiver():
    """A constant membership test is folded by DataFusion before it runs."""
    t = table(pa.float64())
    got = t.derive(
        a=lambda r: LiteralExpr(0.0).is_in([-0.0, 1.0]),
        b=lambda r: LiteralExpr(-0.0).is_in([0.0, 1.0, 2.0, 3.0]),
        c=lambda r: LiteralExpr(0.0) == -0.0,
    ).to_arrow()
    for name in "abc":
        assert got.column(name).to_pylist() == [True] * len(X), name
