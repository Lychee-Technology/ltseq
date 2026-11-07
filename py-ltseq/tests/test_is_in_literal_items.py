"""`is_in` takes literal values only (#294, contract §8.5, D7).

An item that is an expression (a column, arithmetic on one, a function
call, a constant expression) raises TypeError when the lambda is captured,
naming the item and the `==` spelling that tests a value of the same row.
Lists of literals, `None` included, behave as before, NULL results
included: SQL's IN, where a NULL value, or a value matched by no item of a
list that holds NULL, is NULL.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from ltseq import LTSeq, now
from ltseq.expr import LiteralExpr

X = [1, 2, 3, None]
Y = [1, None, 5, 7]


def table() -> LTSeq:
    return LTSeq.from_arrow(
        pa.table({"k": [0, 1, 2, 3], "x": pa.array(X, pa.int64()), "y": pa.array(Y, pa.int64())})
    ).sort("k")


def column(t: LTSeq, name: str = "m") -> list:
    return t.to_arrow().column(name).to_pylist()


# Each refused list, the item the message names, and the rewrite it offers.
REFUSED = {
    "column": (lambda r: [r.y], "item 0 is the expression r.y", "(r.x == r.y)."),
    "column and literal": (
        lambda r: [r.y, 3],
        "item 0 is the expression r.y",
        "(r.x == r.y) | r.x.is_in([3])",
    ),
    "literal before column": (
        lambda r: [3, None, r.y],
        "item 2 is the expression r.y",
        "(r.x == r.y) | r.x.is_in([3, None])",
    ),
    "arithmetic": (lambda r: [r.y + 1], "item 0 is the expression (r.y + 1)", "(r.x == (r.y + 1))."),
    "two expressions": (
        lambda r: [1, r.y, r.y * 2],
        "item 1 is the expression r.y",
        "(r.x == r.y) | (r.x == (r.y * 2)) | r.x.is_in([1])",
    ),
    "function": (lambda r: [r.y.abs()], "item 0 is the expression r.y.abs()", "(r.x == r.y.abs())"),
    "clock": (lambda r: [now()], "item 0 is the expression now()", "(r.x == now())"),
    "constant expression": (
        lambda r: [LiteralExpr(1) + 1],
        "item 0 is the expression (1 + 1)",
        "(r.x == (1 + 1))",
    ),
}


@pytest.mark.parametrize("case", REFUSED)
@pytest.mark.parametrize("method", ["derive", "filter", "negated"])
def test_an_expression_item_is_refused_at_capture(case, method):
    items, named, rewrite = REFUSED[case]
    t = table()
    call = {
        "derive": lambda: t.derive(m=lambda r: r.x.is_in(items(r))),
        "filter": lambda: t.filter(lambda r: r.x.is_in(items(r))),
        "negated": lambda: t.filter(lambda r: ~r.x.is_in(items(r))),
    }[method]
    with pytest.raises(TypeError) as raised:
        call()
    message = str(raised.value)
    assert "is_in() takes literal values only" in message
    assert named in message
    assert rewrite in message
    assert "semi_join()" in message


def test_the_message_spells_the_receiver():
    rewrite = "(getattr(r, 'a b') == r.y) | getattr(r, 'a b').is_in([3])"
    with pytest.raises(TypeError) as raised:
        table().derive(**{"a b": lambda r: r.x}).derive(m=lambda r: getattr(r, "a b").is_in([r.y, 3]))
    assert rewrite in str(raised.value)


def test_the_suggested_rewrite_runs():
    """The spelling the message gives for a value of the same row, run."""
    got = column(table().derive(m=lambda r: (r.x == r.y) | r.x.is_in([3])))
    assert got == [True, None, True, None]


@pytest.mark.parametrize(
    "items, expected",
    [
        ([1, 3], [True, False, True, None]),
        ([2], [False, True, False, None]),
        ([9], [False, False, False, None]),
        ([1, None], [True, None, None, None]),
        ([None], [None, None, None, None]),
        ([1.0, 3.5], [True, False, False, None]),
        ([LiteralExpr(2), 3], [False, True, True, None]),
    ],
    ids=repr,
)
def test_a_literal_list_is_unchanged(items, expected):
    t = table()
    assert column(t.derive(m=lambda r: r.x.is_in(items))) == expected
    assert column(t.derive(m=lambda r: ~r.x.is_in(items))) == [None if v is None else not v for v in expected]
    kept = t.filter(lambda r: r.x.is_in(items)).to_arrow().column("k").to_pylist()
    assert kept == [k for k, v in enumerate(expected) if v]


def test_values_may_be_any_iterable():
    t = table()
    assert column(t.derive(m=lambda r: r.x.is_in(v for v in (1, 3)))) == [True, False, True, None]
    assert column(t.derive(m=lambda r: r.x.is_in((2,)))) == [False, True, False, None]
