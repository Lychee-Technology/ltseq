"""`is_in` takes literal values only (#294, contract §8.5, D7).

An item that is an expression (a column, arithmetic on one, a function
call, a constant expression) raises TypeError when the lambda is captured,
naming the item and the `==` spelling that tests a value of the same row.
Lists of literals, `None` included, behave as before, NULL results
included: SQL's IN, where a NULL value, or a value matched by no item of a
list that holds NULL, is NULL.
"""

from __future__ import annotations

import datetime
import enum
import zoneinfo
from decimal import Decimal

import numpy as np
import pandas as pd
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


def floats(n: int = 200) -> LTSeq:
    return LTSeq.from_arrow(pa.table({"k": list(range(n)), "y": [0.0] * n})).sort("k")


def derived_in(message: str) -> tuple[str, str, str]:
    """The column, the derivation and the rewrite a refusal offers for a
    receiver that holds rand()."""
    marker = "each copy of it would draw its own value: t.derive("
    assert marker in message, message
    rest = message.split(marker, 1)[1]
    name, rest = rest.split("=lambda r: ", 1)
    derivation, rest = rest.split("), then compare that column with ==: ", 1)
    return name, derivation, rest.split(". For membership", 1)[0]


def test_a_volatile_receiver_is_derived_once():
    """Two copies of `rand()` are two draws (R2-F1 of #294's review): with
    `y` 0, `rand().round(0)` is 0 or 1, so it is always in `[y, 1.0]`, but
    `(rand().round(0) == r.y) | rand().round(0).is_in([1.0])` is not."""
    from ltseq import rand

    t = floats()
    message = refusal(t, lambda r: rand().round(0).is_in([r.y, 1.0]))
    assert "(rand().round(0) == r.y)" not in message
    name, derivation, rewrite = derived_in(message)
    assert (name, derivation) == ("v", "rand().round(0)")
    assert rewrite == "(r.v == r.y) | r.v.is_in([1.0])"
    scope = {"rand": rand}
    derived = t.derive(**{name: eval(f"lambda r: {derivation}", scope)})
    got = column(derived.derive(m=eval(f"lambda r: {rewrite}", scope)))
    assert got == [True] * len(got)


def test_the_derived_column_takes_a_name_no_item_reads():
    from ltseq import rand

    t = floats().derive(v=lambda r: r.y, v1=lambda r: r.y)
    message = refusal(t, lambda r: (rand() + r.v).is_in([r.v1, 2.0]))
    name, derivation, rewrite = derived_in(message)
    assert (name, derivation) == ("v2", "(rand() + r.v)")
    assert rewrite == "(r.v2 == r.v1) | r.v2.is_in([2.0])"


def test_a_volatile_receiver_written_once_is_kept():
    from ltseq import rand

    message = refusal(floats(), lambda r: rand().is_in([r.y]))
    assert rewrite_in(message) == "(rand() == r.y)"


def test_a_nested_membership_receiver_keeps_its_list():
    """`is_in` takes one list, so a nested membership is spelled with it
    (R2-F2 of #294's review)."""
    t = bools()
    message = refusal(t, lambda r: r.x.is_in([True]).is_in([r.y, True]))
    rewrite = "(r.x.is_in([True]) == r.y) | r.x.is_in([True]).is_in([True])"
    assert rewrite_in(message) == rewrite
    # x.is_in([True]) is x: [True, True, False, False]; y is [True, False, True, False],
    # so the value is y or True on all rows but the third.
    assert run_rewrite(t, message) == [True, True, False, True]


def test_a_nested_membership_item_keeps_its_list():
    t = table()
    message = refusal(t, lambda r: r.x.is_in([r.y.is_in([1, 5]), 3]))
    assert "item 0 is the expression r.y.is_in([1, 5])" in message
    assert rewrite_in(message) == "(r.x == r.y.is_in([1, 5])) | r.x.is_in([3])"
    bools_message = refusal(bools(), lambda r: r.x.is_in([r.y.is_in([True]), False]))
    assert rewrite_in(bools_message) == "(r.x == r.y.is_in([True])) | r.x.is_in([False])"
    # x == y on [T, T, F, F] and [T, F, T, F], or x is False.
    assert run_rewrite(bools(), bools_message) == [True, False, True, True]


def test_the_derived_column_takes_a_name_the_table_does_not_have():
    """An unread column `v` is not overwritten by the offered derivation
    (R3-F1 of #294's review)."""
    from ltseq import rand

    t = floats().derive(v=lambda r: r.y + 5)
    message = refusal(t, lambda r: rand().round(0).is_in([r.y, 1.0]))
    name, derivation, rewrite = derived_in(message)
    assert (name, derivation) == ("v1", "rand().round(0)")
    assert rewrite == "(r.v1 == r.y) | r.v1.is_in([1.0])"
    scope = {"rand": rand}
    derived = t.derive(**{name: eval(f"lambda r: {derivation}", scope)})
    assert column(derived, "v") == [5.0] * 200
    got = column(derived.derive(m=eval(f"lambda r: {rewrite}", scope)))
    assert got == [True] * len(got)


def test_outside_a_capture_the_message_asks_for_a_fresh_name():
    """Built outside a row lambda, the expression has no table, so the
    message cannot know `v` is free and says to pick a name that is."""
    from ltseq import rand
    from ltseq.expr import ColumnExpr

    with pytest.raises(TypeError) as raised:
        rand().round(0).is_in([ColumnExpr("y"), 1.0])
    message = str(raised.value)
    assert (
        "t.derive(v=lambda r: rand().round(0)) (if the table already has a column v, "
        "use a name it does not have), then compare that column with ==: "
        "(r.v == r.y) | r.v.is_in([1.0])."
    ) in message


class Small(enum.IntEnum):
    THREE = 3


class Text(str):
    pass


UTC = datetime.timezone.utc


def pair(kind: pa.DataType, x: list, y: list) -> LTSeq:
    return LTSeq.from_arrow(
        pa.table({"k": list(range(len(x))), "x": pa.array(x, kind), "y": pa.array(y, kind)})
    ).sort("k")


# Literal items of each supported kind beside an expression item: the table,
# the literals, and how the rewrite spells them (R3-F2 of #294's review). A
# numpy number reads as the Python number it is encoded as.
SPELLED = {
    "numpy float32 nan": (
        lambda: pair(pa.float64(), [0.0, float("nan"), 1.0, None], [1.0, None, 2.0, 3.0]),
        [np.float32(float("nan"))],
        "float('nan')",
    ),
    "numpy infinities": (
        lambda: pair(pa.float64(), [float("inf"), float("-inf"), 1.0, None], [1.0, None, 2.0, 3.0]),
        [np.float64(float("inf")), np.float16(float("-inf"))],
        "float('inf'), float('-inf')",
    ),
    "numpy float32": (
        lambda: pair(pa.float32(), [1.1, 2.0, 1.0, None], [2.0, None, 1.0, 3.0]),
        [np.float32(1.1)],
        "1.100000023841858",
    ),
    "numpy integers": (lambda: table(), [np.int16(3), np.uint8(2)], "3, 2"),
    "int enum": (lambda: table(), [Small.THREE], "3"),
    "numpy bool": (
        lambda: pair(pa.bool_(), [True, True, False, None], [True, False, False, True]),
        [np.True_],
        "True",
    ),
    "numpy and subclassed str": (
        lambda: pair(pa.string(), ["a", "b", "c", None], ["c", None, "c", "a"]),
        [np.str_("a"), Text("b")],
        "'a', 'b'",
    ),
    "decimal": (
        lambda: pair(
            pa.decimal128(5, 2),
            [Decimal("1.50"), Decimal("2.00"), Decimal("3.25"), None],
            [Decimal("2.00"), None, Decimal("1.00"), Decimal("1.50")],
        ),
        [Decimal("1.50")],
        "Decimal('1.50')",
    ),
    "date": (
        lambda: pair(
            pa.date32(),
            [datetime.date(2020, 1, 1), datetime.date(2020, 1, 2), datetime.date(2020, 1, 3), None],
            [datetime.date(2020, 1, 2), None, datetime.date(2020, 1, 3), datetime.date(2020, 1, 1)],
        ),
        [datetime.date(2020, 1, 1)],
        "datetime.date(2020, 1, 1)",
    ),
    "datetime": (
        lambda: pair(
            pa.timestamp("us"),
            [datetime.datetime(2020, 1, 1), datetime.datetime(2020, 1, 2), None],
            [datetime.datetime(2020, 1, 2), None, datetime.datetime(2020, 1, 1)],
        ),
        [datetime.datetime(2020, 1, 1)],
        "datetime.datetime(2020, 1, 1, 0, 0)",
    ),
    "datetime in UTC": (
        lambda: pair(
            pa.timestamp("us", tz="UTC"),
            [datetime.datetime(2020, 1, 1, tzinfo=UTC), datetime.datetime(2020, 1, 2, tzinfo=UTC), None],
            [datetime.datetime(2020, 1, 2, tzinfo=UTC), None, datetime.datetime(2020, 1, 1, tzinfo=UTC)],
        ),
        [datetime.datetime(2020, 1, 1, tzinfo=UTC)],
        "datetime.datetime(2020, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)",
    ),
}


@pytest.mark.parametrize("case", SPELLED)
def test_each_supported_literal_is_spelled_as_python_that_runs(case):
    make, literals, spelled = SPELLED[case]
    t = make()
    message = refusal(t, lambda r: r.x.is_in([r.y, *literals]))
    rewrite = rewrite_in(message)
    assert rewrite == f"(r.x == r.y) | r.x.is_in([{spelled}])"
    # Each spelling, evaluated, encodes as the literal it spells.
    scope = {"Decimal": Decimal, "datetime": datetime}
    for read, value in zip(eval(f"[{spelled}]", scope), literals, strict=True):
        assert repr(LiteralExpr(read).serialize()) == repr(LiteralExpr(value).serialize())
    # The rewrite, run, gives what the refused list means.
    got = column(t.derive(m=eval(f"lambda r: {rewrite}", scope)))
    assert got == column(t.derive(m=lambda r: (r.x == r.y) | r.x.is_in(literals)))


@pytest.mark.parametrize(
    "literal",
    [
        np.datetime64("2020-01-01T00:00:00"),
        pd.Timestamp("2020-01-01 00:00:00.000000001"),
        datetime.datetime(2020, 1, 1, tzinfo=zoneinfo.ZoneInfo("Europe/Paris")),
    ],
    ids=["numpy datetime64", "pandas Timestamp", "zoneinfo datetime"],
)
def test_a_literal_without_a_faithful_spelling_gets_the_generic_rewrite(literal):
    t = pair(pa.timestamp("ns", tz="UTC"), [None], [None])
    message = refusal(t, lambda r: r.x.is_in([r.y, literal]))
    assert "compare it with == and join the tests with |, as in (r.x == r.y) | r.x.is_in([3])." in message


def test_an_item_with_an_unspellable_literal_gets_the_generic_rewrite():
    message = refusal(table(), lambda r: r.x.is_in([r.y + LiteralExpr(np.datetime64("2020-01-01")), 3]))
    assert "as in (r.x == r.y) | r.x.is_in([3])." in message


def strings() -> LTSeq:
    x = ["a", "b ", " c", "d"]
    y = ["  a", "b  ", " c ", None]
    return LTSeq.from_arrow(pa.table({"k": [0, 1, 2, 3], "x": x, "y": y})).sort("k")


@pytest.mark.parametrize(
    "method, spelled, expected",
    [
        ("lstrip", "r.y.s.lstrip()", [True, False, False, True]),
        ("rstrip", "r.y.s.rstrip()", [False, False, True, True]),
    ],
)
def test_an_accessor_item_is_spelled_with_its_public_method(method, spelled, expected):
    """`.s.lstrip()` builds `str_ltrim`: the rewrite names the method a user
    calls, and runs (R5-F1 of #294's review)."""
    t = strings()
    message = refusal(t, lambda r: r.x.is_in([getattr(r.y.s, method)(), "d"]))
    rewrite = rewrite_in(message)
    assert rewrite == f"(r.x == {spelled}) | r.x.is_in(['d'])"
    got = column(t.derive(m=eval(f"lambda r: {rewrite}", {})))
    assert got == expected
    assert got == column(t.derive(m=lambda r: (r.x == getattr(r.y.s, method)()) | r.x.is_in(["d"])))


# A call of each accessor method, as an is_in item.
ACCESSOR_CALLS = {
    **{
        f"s.{name}": (lambda r, name=name, args=args: getattr(r.y.s, name)(*args))
        for name, args in [
            ("contains", ("a",)),
            ("starts_with", ("a",)),
            ("ends_with", ("a",)),
            ("lower", ()),
            ("upper", ()),
            ("strip", ()),
            ("len", ()),
            ("slice", (1, 2)),
            ("regex_match", ("a+",)),
            ("replace", ("a", "b")),
            ("concat", ("a", "b")),
            ("pad_left", (5, "*")),
            ("pad_right", (5,)),
            ("split", (",", 1)),
            ("split_part", (",", 1)),
            ("like", ("a%",)),
            ("isalpha", ()),
            ("isdigit", ()),
            ("islower", ()),
            ("isupper", ()),
            ("pos", ("a",)),
            ("find", ("a",)),
            ("left", (2,)),
            ("right", (2,)),
            ("lstrip", ()),
            ("rstrip", ()),
            ("asc", ()),
            ("ord", ()),
        ]
    },
    **{
        f"dt.{name}": (lambda r, name=name, args=args: getattr(r.y.dt, name)(*args))
        for name, args in [
            ("year", ()),
            ("month", ()),
            ("day", ()),
            ("hour", ()),
            ("minute", ()),
            ("second", ()),
            ("millisecond", ()),
            ("weekday", ()),
            ("age", ()),
            ("add", (1, 2)),
            ("diff", (datetime.date(2020, 1, 1), "month")),
        ]
    },
}


def test_every_accessor_method_is_listed():
    from ltseq.expr.accessors import StringAccessor, TemporalAccessor

    public = {
        f"{prefix}.{name}"
        for prefix, cls in (("s", StringAccessor), ("dt", TemporalAccessor))
        for name in vars(cls)
        if not name.startswith("_") and callable(getattr(cls, name))
    }
    assert public == set(ACCESSOR_CALLS)


@pytest.mark.parametrize("case", ACCESSOR_CALLS)
def test_an_accessor_item_is_spelled_as_source_that_builds_it(case):
    """The rewrite for each accessor call, evaluated, builds the same
    expression the refused item was (R5-F1 of #294's review)."""
    item = ACCESSOR_CALLS[case]
    message = refusal(table(), lambda r: r.x.is_in([item(r)]))
    rewrite = rewrite_in(message)
    scope = {"datetime": datetime}
    built = eval(f"lambda r: {rewrite}", scope)(SchemaProxyLike())
    expected = (lambda r: r.x == item(r))(SchemaProxyLike())
    assert built.serialize() == expected.serialize()


def test_an_accessor_function_without_a_method_gets_the_generic_rewrite():
    from ltseq.expr import CallExpr

    message = refusal(table(), lambda r: r.x.is_in([CallExpr("str_ltrim_all", (), {}, on=r.y), 3]))
    assert "item 0 is the expression CallExpr" in message
    assert "as in (r.x == r.y) | r.x.is_in([3])." in message
