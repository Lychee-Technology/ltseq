"""``show()`` prints every column type ``to_arrow()`` returns (#312).

Decimals, durations, times and lists printed as ``[unsupported type]``.
"""

import datetime as dt
from decimal import Decimal

import pyarrow as pa

from ltseq import LTSeq


def _cells(capsys, table, column):
    """The cells of ``column`` as ``show()`` printed them, one per row."""
    table.show()
    lines = [line for line in capsys.readouterr().out.splitlines() if line.startswith("|")]
    header = [cell.strip() for cell in lines[0].strip("|").split("|")]
    index = header.index(column)
    return [[cell.strip() for cell in line.strip("|").split("|")][index] for line in lines[1:]]


def test_decimal_from_integer_arithmetic_with_a_decimal(capsys):
    # An Int64 column times a `Decimal` literal is a decimal computed by the
    # engine, not one read from the input.
    t = LTSeq.from_arrow(pa.table({"v": [3, 1, 2, 4]})).sort("v")
    out = t.derive(q=lambda r: r.v * Decimal("1.5"))
    assert pa.types.is_decimal(out.to_arrow().schema.field("q").type)
    assert _cells(capsys, out, "q") == ["1.5", "3.0", "4.5", "6.0"]


def test_decimal_keeps_its_scale(capsys):
    values = pa.array([Decimal("1.50"), None, Decimal("-0.05")], pa.decimal128(10, 2))
    assert _cells(capsys, LTSeq.from_arrow(pa.table({"d": values})), "d") == ["1.50", "None", "-0.05"]


def test_duration_from_timestamp_subtraction(capsys):
    t = LTSeq.from_rows([{"a": dt.datetime(2026, 1, 1, 9), "b": dt.datetime(2026, 1, 1, 9, 30)}])
    out = t.derive(g=lambda r: r.b - r.a)
    assert _cells(capsys, out, "g") == ["0 days 0 hours 30 mins 0.000000 secs"]


def test_list_from_fold(capsys):
    t = LTSeq.from_rows([{"a": 1}, {"a": 2}]).sort("a")
    out = t.fold(lambda s, r: s + [r["a"]], init=[], into="l")
    assert _cells(capsys, out, "l") == ["[1]", "[1, 2]"]


def test_list_null_items_print_as_none(capsys):
    values = pa.array([[1, None], None, []], pa.list_(pa.int64()))
    assert _cells(capsys, LTSeq.from_arrow(pa.table({"l": values})), "l") == ["[1, None]", "None", "[]"]


def test_time_of_day(capsys):
    values = pa.array([dt.time(9, 30), None])
    assert _cells(capsys, LTSeq.from_arrow(pa.table({"t": values})), "t") == ["09:30:00", "None"]


def test_repr_previews_a_decimal_column():
    values = pa.array([Decimal("1.50")], pa.decimal128(10, 2))
    text = repr(LTSeq.from_arrow(pa.table({"d": values})))
    assert "1.50" in text
    assert "[unsupported type]" not in text
