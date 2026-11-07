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
        "item 0 is the expression (LiteralExpr(1) + 1)",
        "(r.x == (LiteralExpr(1) + 1))",
    ),
}

# What each refused list's rewrite gives on table(), run: SQL's three-valued
# OR of the equalities and the membership of the literal items. The clock
# compares an integer with a timestamp, so it is only built.
REWRITTEN = {
    "column": [True, None, False, None],
    "column and literal": [True, None, True, None],
    "literal before column": [True, None, True, None],
    "arithmetic": [False, None, False, None],
    "two expressions": [True, None, False, None],
    "function": [True, None, False, None],
    "clock": None,
    "constant expression": [False, True, False, None],
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


def rewrite_in(message: str) -> str:
    """The Python source of the rewrite a refusal offers."""
    assert "compare it with ==: " in message, message
    return message.split("compare it with ==: ", 1)[1].split(". For membership", 1)[0]


def run_rewrite(t: LTSeq, message: str) -> list:
    """Evaluate the rewrite the message offers, as a user would paste it."""
    rewrite = eval(f"lambda r: {rewrite_in(message)}", {"LiteralExpr": LiteralExpr, "now": now})
    return column(t.derive(m=rewrite))


def refusal(t: LTSeq, fn) -> str:
    with pytest.raises(TypeError) as raised:
        t.derive(m=fn)
    return str(raised.value)


@pytest.mark.parametrize("case", REFUSED)
def test_each_offered_rewrite_runs(case):
    items, _, _ = REFUSED[case]
    t = table()
    message = refusal(t, lambda r: r.x.is_in(items(r)))
    if REWRITTEN[case] is None:
        eval(f"lambda r: {rewrite_in(message)}", {"now": now})(SchemaProxyLike())
        return
    assert run_rewrite(t, message) == REWRITTEN[case]


class SchemaProxyLike:
    """Columns by attribute, for building a rewrite without running it."""

    def __getattr__(self, name):
        from ltseq.expr import ColumnExpr

        return ColumnExpr(name)


def bools() -> LTSeq:
    x = [True, True, False, False]
    y = [True, False, True, False]
    return LTSeq.from_arrow(pa.table({"k": [0, 1, 2, 3], "x": x, "y": y})).sort("k")


def test_a_negated_receiver_keeps_its_parentheses():
    """`~r.x.is_in(...)` negates the membership; the receiver here is `~r.x`
    (R1-F1 of #294's review)."""
    t = bools()
    message = refusal(t, lambda r: (~r.x).is_in([r.y, True, False]))
    assert rewrite_in(message) == "((~r.x) == r.y) | (~r.x).is_in([True, False])"
    # ~x is True or False on every row, so it is always in [y, True, False].
    assert run_rewrite(t, message) == [True, True, True, True]


def test_a_negated_conjunction_keeps_its_parentheses():
    t = bools()
    message = refusal(t, lambda r: (~(r.x & r.y)).is_in([r.y]))
    assert rewrite_in(message) == "((~(r.x & r.y)) == r.y)"
    # ~(x & y) is [False, True, True, True]; y is [True, False, True, False].
    assert run_rewrite(t, message) == [False, False, True, False]


def test_a_literal_receiver_is_spelled_as_an_expression():
    """`1.is_in(...)` is not Python; the receiver is `LiteralExpr(1)`."""
    t = table()
    message = refusal(t, lambda r: LiteralExpr(1).is_in([r.y, 3]))
    assert rewrite_in(message) == "(LiteralExpr(1) == r.y) | LiteralExpr(1).is_in([3])"
    assert run_rewrite(t, message) == [True, None, False, False]


@pytest.mark.parametrize(
    "receiver, spelled, expected",
    [
        (lambda r: r.x + 1, "(r.x + 1)", [False, True, False, None]),
        (lambda r: 1 + r.x, "(1 + r.x)", [False, True, False, None]),
        (lambda r: r.x.abs(), "r.x.abs()", [True, None, True, None]),
        (lambda r: LiteralExpr(2) + 1, "(LiteralExpr(2) + 1)", [True, True, True, True]),
    ],
    ids=["arithmetic", "reflected arithmetic", "method", "constant"],
)
def test_the_rewrite_keeps_the_receiver(receiver, spelled, expected):
    t = table()
    message = refusal(t, lambda r: receiver(r).is_in([r.y, 3]))
    assert rewrite_in(message) == f"({spelled} == r.y) | {spelled}.is_in([3])"
    assert run_rewrite(t, message) == expected


def test_a_non_finite_float_item_is_spelled_as_python():
    message = refusal(table(), lambda r: r.x.is_in([r.y, float("nan"), float("-inf")]))
    assert rewrite_in(message) == "(r.x == r.y) | r.x.is_in([float('nan'), float('-inf')])"


def test_a_receiver_without_a_spelling_gets_the_generic_rewrite():
    from ltseq.expr import UnaryOpExpr

    message = refusal(table(), lambda r: UnaryOpExpr("Neg", r.x).is_in([r.y, 3]))
    assert "compare it with == and join the tests with |, as in (r.x == r.y) | r.x.is_in([3])." in message
    assert "Neg" not in message.split("To test", 1)[1]


def test_an_item_without_a_spelling_gets_the_generic_rewrite():
    from ltseq.expr import UnaryOpExpr

    message = refusal(table(), lambda r: r.x.is_in([UnaryOpExpr("Neg", r.y), 3]))
    assert "item 0 is the expression UnaryOpExpr" in message
    assert "as in (r.x == r.y) | r.x.is_in([3])." in message


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
