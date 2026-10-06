"""Every literal kind compared with every column type, against an oracle (#145).

Each cell compares one column with one literal and checks the result against
a Python oracle that states the intended semantics independently of the
transpiler:

- numbers compare exactly, except that a float column compares as floats
  (a Decimal literal becomes the column's float type);
- timestamps compare as instants: a naive literal against a zoned column is
  wall-clock time in the column's zone, a date literal is that zone's local
  midnight, a date column is read at UTC midnight, and a naive column cannot
  be compared with an aware literal;
- a string column compares only with strings; a Decimal, date or datetime
  literal against it is an error.

Cells whose result is DataFusion's own coercion between unrelated kinds (an
integer against a date, a number against a string) are not part of this
contract and are left out.
"""

import operator
from datetime import date, datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from ltseq import LTSeq

UTC = timezone.utc
NS = 10**9
EPOCH = date(1970, 1, 1)


def _day(d: date) -> int:
    return (d - EPOCH).days


# Timestamp columns are built from nanosecond ticks so the oracle compares
# exact integers. Naive ticks are wall-clock time, zoned ones UTC instants.
TS_TICKS = [0, 1 * NS, None, 1 * NS + 500, 1_704_088_800 * NS]  # .., 1970-01-01T00:00:01.0000005, 2024-01-01T06:00Z
TS_COLUMNS = {
    "ts_s": ("s", None),
    "ts_ms": ("ms", None),
    "ts_us": ("us", None),
    "ts_ns": ("ns", None),
    "ts_utc": ("us", "UTC"),
    "ts_ny": ("us", "America/New_York"),
    "ts_tok": ("ns", "Asia/Tokyo"),
}
UNIT_NS = {"s": NS, "ms": 10**6, "us": 10**3, "ns": 1}
DATES = [date(2023, 12, 31), date(2024, 1, 1), None, date(2024, 1, 2), date(1970, 1, 1)]


def _ts_ticks(unit):
    """TS_TICKS floored to `unit` (the column holds only what its unit can)."""
    return [None if t is None else t // UNIT_NS[unit] * UNIT_NS[unit] for t in TS_TICKS]


COLUMNS = {
    "i64": pa.array([1, 2, None, -3, 0], pa.int64()),
    "f64": pa.array([1.5, float("inf"), None, float("-inf"), 2.0], pa.float64()),
    "f32": pa.array([0.1, 2.5, None, 1.5, 2.0], pa.float32()),
    "dec": pa.array([Decimal("1.23"), Decimal("1.50"), None, Decimal("2.00"), Decimal("-1.00")], pa.decimal128(5, 2)),
    "decw": pa.array(
        [Decimal(1), Decimal(10) ** 27, None, Decimal("0.1234567891"), Decimal(2)], pa.decimal128(38, 10)
    ),
    # Negative scales: 39+ digits coarser than a scale-38 literal, and one
    # narrow enough that DataFusion's own coercion stays within 38 digits.
    "decneg": pa.array(
        [Decimal(10), Decimal(-10), None, Decimal(0), Decimal("9E+37")], pa.decimal128(38, -1)
    ),
    "decneg_narrow": pa.array(
        [Decimal(100), Decimal(-200), None, Decimal(0), Decimal(9_999_900)], pa.decimal128(5, -2)
    ),
    "utf8": pa.array(["a", "b", None, "c", "B"]),
    "large_utf8": pa.array(["a", "b", None, "c", "B"], pa.large_string()),
    "bool": pa.array([True, False, None, True, False]),
    "d32": pa.array(DATES, pa.date32()),
    "d64": pa.array(DATES, pa.date64()),
    **{
        name: pa.array(
            [None if t is None else t // UNIT_NS[unit] for t in _ts_ticks(unit)], pa.int64()
        ).cast(pa.timestamp(unit, tz=tz))
        for name, (unit, tz) in TS_COLUMNS.items()
    },
}

LITERALS = {
    "int": 2,
    "float": 1.5,
    "decimal": Decimal("1.5"),
    "decimal-0.1": Decimal("0.1"),
    "decimal-hiscale": Decimal("0.1234567890123456789"),
    "decimal-tiny": Decimal("1E-38"),
    "decimal-tiny-neg": Decimal("-1E-38"),
    "decimal-zero-scale38": Decimal("0E-38"),
    "np-int": np.int64(2),
    "bool": True,
    "np-bool": np.bool_(True),
    "str": "b",
    "date": date(2024, 1, 1),
    "datetime": datetime(2024, 1, 1, 6),
    "datetime-1.5s": datetime(1970, 1, 1, 0, 0, 1, 500000),
    "aware-utc": datetime(2024, 1, 1, 6, tzinfo=UTC),
    "aware-ny": datetime(2024, 1, 1, 1, tzinfo=ZoneInfo("America/New_York")),
    "pd-ns": pd.Timestamp("1970-01-01 00:00:01.000000500"),
    "np-datetime64": np.datetime64("2024-01-01T06:00:00"),
}

OPS = {"gt": operator.gt, "eq": operator.eq}
SKIP = object()


class Error(str):
    """An expected error: the message must contain this text."""


# ---- the oracle ----


def _literal_ns(lit):
    """(ticks in ns, aware) for a datetime-like literal; None otherwise."""
    if isinstance(lit, np.datetime64):
        return int(lit.astype("datetime64[ns]").astype("int64")), False
    if isinstance(lit, pd.Timestamp):
        return (lit.value, lit.tzinfo is not None)
    if isinstance(lit, datetime):
        if lit.tzinfo is None:
            naive = lit - datetime(1970, 1, 1)
            return (naive.days * 86_400 + naive.seconds) * NS + naive.microseconds * 1000, False
        utc = lit - datetime(1970, 1, 1, tzinfo=UTC)
        return (utc.days * 86_400 + utc.seconds) * NS + utc.microseconds * 1000, True
    return None


def _wall_clock_to_instant(ticks_ns: int, zone: str) -> int:
    wall = datetime(1970, 1, 1) + pd.Timedelta(ticks_ns, "ns").to_pytimedelta()
    aware = wall.replace(tzinfo=ZoneInfo(zone))
    return ticks_ns - int(aware.utcoffset().total_seconds()) * NS


def _numeric(column: str, value, lit):
    if isinstance(lit, (bool, np.bool_, str, date, np.datetime64)):
        return SKIP
    lit = lit.item() if isinstance(lit, np.generic) else lit
    if column == "f32" and isinstance(lit, Decimal):
        return value, float(np.float32(lit))
    if column in ("f64", "f32") or isinstance(lit, float):
        return float(value), float(lit)
    return Decimal(value), Decimal(lit)


def _temporal(column: str, value, lit):
    """(column ns, literal ns) as comparable instants, or an Error / SKIP."""
    if column in ("d32", "d64"):
        column_ns = _day(value) * 86_400 * NS  # a date column is read at UTC midnight
        if isinstance(lit, date) and not isinstance(lit, datetime):
            return column_ns, _day(lit) * 86_400 * NS
        parsed = _literal_ns(lit)
        return SKIP if parsed is None else (column_ns, parsed[0])
    unit, zone = TS_COLUMNS[column]
    if isinstance(lit, date) and not isinstance(lit, datetime):
        midnight = _day(lit) * 86_400 * NS
        return value, midnight if zone is None else _wall_clock_to_instant(midnight, zone)
    parsed = _literal_ns(lit)
    if parsed is None:
        return SKIP
    ticks, aware = parsed
    if zone is None and aware:
        return Error(f"column '{column}' is timezone-naive")
    if zone is not None and not aware:
        ticks = _wall_clock_to_instant(ticks, zone)
    return value, ticks


def oracle(column: str, op: str, lit):
    """The expected column of booleans (None for NULL), an Error, or SKIP."""
    out = []
    for value in _column_values(column):
        if column in ("utf8", "large_utf8"):
            if isinstance(lit, (Decimal, date, np.datetime64)):
                return Error(f"column '{column}' is a string")
            if not isinstance(lit, str):
                return SKIP
            pair = (value, lit)
        elif column == "bool":
            if not isinstance(lit, (bool, np.bool_)):
                return SKIP
            pair = (value, bool(lit))
        elif column in ("d32", "d64") or column in TS_COLUMNS:
            if value is None:
                pair = None
            else:
                pair = _temporal(column, value, lit)
        else:
            pair = None if value is None else _numeric(column, value, lit)
        if pair is SKIP or isinstance(pair, Error):
            return pair
        out.append(None if value is None else OPS[op](*pair))
    return out


def _column_values(column: str):
    """Values the oracle compares: ns ticks for timestamps, Python values otherwise."""
    if column in TS_COLUMNS:
        return _ts_ticks(TS_COLUMNS[column][0])
    return COLUMNS[column].to_pylist()


CELLS = [
    (column, lname, op, expected)
    for column in COLUMNS
    for lname, lit in LITERALS.items()
    for op in OPS
    if (expected := oracle(column, op, lit)) is not SKIP
]


@pytest.fixture(scope="module")
def table():
    return LTSeq.from_arrow(pa.table({"k": pa.array(range(5), pa.int64()), **COLUMNS}))


def _param(column, lname, op, expected):
    return pytest.param(column, lname, op, expected, id=f"{column}-{op}-{lname}")


@pytest.mark.parametrize("column, lname, op, expected", [_param(*cell) for cell in CELLS])
def test_comparison_matches_oracle(table, column, lname, op, expected):
    lit = LITERALS[lname]
    swapped = {"gt": operator.lt, "eq": operator.eq}[op]
    for fn in (lambda r: OPS[op](getattr(r, column), lit), lambda r: swapped(lit, getattr(r, column))):
        if isinstance(expected, Error):
            with pytest.raises(ValueError, match=str(expected)):
                table.derive(v=fn).to_arrow()
        else:
            assert table.derive(v=fn).to_arrow().column("v").to_pylist() == expected


def test_grid_covers_every_column_and_literal():
    assert {c for c, *_ in CELLS} == set(COLUMNS)
    assert {lname for _, lname, *_ in CELLS} == set(LITERALS)
