"""A string is never rounded to a decimal of negative scale (#296, B4).

DataFusion 56 casts a string to ``decimal128(10, -2)`` by rounding it to a
multiple of 100, so ``'1'`` compares equal to 0. LTSeq refuses such a cast
where the query is built, whether the string is a literal of the lambda,
a column ``derive`` staged, or a literal of a ``filter_where`` clause
(R1-F3 of the review on PR 423). A decimal cast to a string is a string
comparison and is kept. At the lowest scale, -128, the refusal is an
error, not a panic (R1-F1).
"""

from __future__ import annotations

from decimal import Decimal

import pyarrow as pa
import pytest

from ltseq import LTSeq
from ltseq.expr import LiteralExpr, if_else

REFUSED = "a decimal of negative scale"


def table(scale: int = -2) -> LTSeq:
    values = [Decimal(0), Decimal(100), None] if scale > -10 else [Decimal(0), None, None]
    return LTSeq.from_arrow(
        pa.table(
            {
                "k": pa.array([0, 1, 2], pa.int64()),
                "d": pa.array(values, type=pa.decimal128(10, scale)),
            }
        )
    )


def where(t: LTSeq, clause: str) -> LTSeq:
    return LTSeq._from_inner(t._inner.filter_where(clause))


def keys(t: LTSeq) -> list:
    return sorted(t.to_arrow().column("k").to_pylist())


@pytest.mark.parametrize("scale", [-128, -38, -2])
def test_a_string_literal_is_refused_at_every_negative_scale(scale):
    t = table(scale)
    for predicate in (
        lambda r: r.d == "1",
        lambda r: r.d.is_in(["1"]),
        lambda r: r.d.is_in(["1", "2", "3", "4"]),
    ):
        with pytest.raises(ValueError, match=REFUSED):
            t.filter(predicate)
    with pytest.raises(ValueError, match=REFUSED):
        t.derive(v=lambda r: r.d.fill_null("1"))


def test_a_staged_string_column_is_refused():
    t = table().derive(s=lambda r: LiteralExpr("1"))
    for predicate in (
        lambda r: r.d == r.s,
        lambda r: r.s != r.d,
        lambda r: r.d > r.s,
    ):
        with pytest.raises(ValueError, match=REFUSED):
            t.filter(predicate)
    with pytest.raises(ValueError, match=REFUSED):
        t.derive(e=lambda r: r.d == r.s)


@pytest.mark.parametrize(
    "clause",
    [
        "d = '1'",
        "'1' = d",
        "d IN ('1', '2')",
        "d IN ('1', '2', '3', '4', '5')",
        "d BETWEEN '1' AND '200'",
        "k = 0 OR d <> '1'",
    ],
)
def test_a_where_clause_string_is_refused(clause):
    with pytest.raises(Exception, match=REFUSED):
        where(table(), clause)


def test_a_string_the_decimal_unifies_to_is_kept():
    t = table()
    assert t.derive(z=lambda r: if_else(r.d.is_null(), "1", r.d)).to_arrow().column(
        "z"
    ).to_pylist() == ["0", "100", "1"]
    assert keys(where(t, "CAST(d AS VARCHAR) = '0'")) == [0]
    assert keys(t.filter(lambda r: r.d == Decimal(100))) == [1]
    assert keys(where(t, "d = 100")) == [1]
