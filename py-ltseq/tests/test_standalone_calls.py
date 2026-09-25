"""Standalone function calls (issue #145).

A standalone function such as ``abs(r.x)`` or ``coalesce(r.a, r.b)`` has no
receiver: it serializes with ``"on": None`` and Rust keeps that as a missing
receiver (``Option::None``), never as a column with an empty name. These tests
exercise every standalone channel end to end, and check that a function that
needs a receiver reports that instead of looking up a column named ``''``.
"""

import math
from datetime import date

import pyarrow as pa
import pytest

from ltseq import LTSeq, atan2, coalesce, concat_agg, corr, count_if, log, power, skew, sqrt, today
from ltseq.expr import ColumnExpr, concat_ws, gcd, str_char


@pytest.fixture
def t():
    return LTSeq.from_arrow(
        pa.table(
            {
                "g": ["a", "a", "b", "b"],
                "x": pa.array([-4.0, 9.0, 16.0, -1.0]),
                "y": pa.array([1.0, None, 3.0, None]),
                "n": pa.array([12, 18, 65, 66], pa.int64()),
                "s": ["p", "q", "r", "s"],
            }
        )
    )


def _col(t, expr):
    return t.derive(out=expr).to_arrow().column("out").to_pylist()


def test_standalone_serializes_without_receiver():
    assert abs(ColumnExpr("x")).serialize()["on"] is None
    assert coalesce(ColumnExpr("x"), 0).serialize()["on"] is None


def test_abs_standalone_matches_method(t):
    assert _col(t, lambda r: abs(r.x)) == [4.0, 9.0, 16.0, 1.0]
    assert _col(t, lambda r: abs(r.x)) == _col(t, lambda r: r.x.abs())


def test_coalesce_standalone(t):
    assert _col(t, lambda r: coalesce(r.y, r.x, 0.0)) == [1.0, 9.0, 3.0, -1.0]


def test_math_standalone_forms(t):
    assert _col(t, lambda r: sqrt(abs(r.x))) == [2.0, 3.0, 4.0, 1.0]
    assert _col(t, lambda r: power(r.x, 2)) == [16.0, 81.0, 256.0, 1.0]
    assert _col(t, lambda r: log(abs(r.x), 2)) == pytest.approx([2.0, math.log2(9), 4.0, 0.0])
    assert _col(t, lambda r: atan2(r.x, abs(r.x))) == pytest.approx(
        [-math.pi / 4, math.pi / 4, math.pi / 4, -math.pi / 4]
    )
    assert _col(t, lambda r: gcd(r.n, 6)) == [6, 6, 1, 6]


def test_string_standalone_forms(t):
    assert _col(t, lambda r: str_char(r.n)) == ["\x0c", "\x12", "A", "B"]
    assert _col(t, lambda r: concat_ws("-", r.g, r.s)) == ["a-p", "a-q", "b-r", "b-s"]


def test_temporal_standalone_takes_no_receiver(t):
    assert _col(t, lambda r: today()) == [date.today()] * 4


def test_aggregate_standalone_forms(t):
    out = (
        t.agg(
            by=lambda r: r.g,
            c=lambda g: count_if(g.x > 0),
            r=lambda g: corr(g.x, g.n),
            k=lambda g: skew(g.x),
            j=lambda g: concat_agg(g.s, "|"),
        )
        .sort("g")
        .to_arrow()
    )
    assert out.column("c").to_pylist() == [1, 1]
    assert out.column("r").to_pylist() == pytest.approx([1.0, -1.0])
    assert out.column("k").to_pylist() == pytest.approx([0.0, 0.0])
    assert sorted(out.column("j").to_pylist()[0].split("|")) == ["p", "q"]


@pytest.mark.parametrize("func", ["is_null", "fill_null", "str_lower", "dt_year", "cast"])
def test_receiver_function_called_standalone_names_the_problem(t, func):
    """Rust used to substitute an empty column name and report
    ``Column '' not found``."""
    call = {
        "type": "Call",
        "func": func,
        "args": [{"type": "Column", "name": "s"}],
        "kwargs": {},
        "on": None,
    }
    with pytest.raises(Exception, match=f"{func} must be called as a method") as excinfo:
        t._inner.derive({"out": call})
    assert "Column ''" not in str(excinfo.value)


def test_search_pattern_keeps_the_unsupported_function_message_for_standalone_calls(t):
    """search_pattern's evaluator supports a handful of functions. A standalone
    function it does not support must still be reported as unsupported, not as
    "must be called as a method" (coalesce has no method form)."""
    sorted_t = t.sort("n")
    with pytest.raises(Exception, match="Unsupported function in search_pattern predicate: 'coalesce'"):
        sorted_t.search_pattern(lambda r: coalesce(r.y, r.x) > 0, lambda r: r.x > 0)
    # A function the evaluator does support still needs its receiver.
    with pytest.raises(Exception, match="is_null must be called as a method"):
        sorted_t._inner.search_pattern(
            [{"type": "Call", "func": "is_null", "args": [{"type": "Column", "name": "y"}], "kwargs": {}, "on": None}],
            None,
        )
