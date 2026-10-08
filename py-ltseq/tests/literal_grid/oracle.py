"""An exact model of the literal grid, independent of ltseq and DataFusion.

Column values and literals become exact numbers (``Fraction``) or instants
(nanoseconds since the epoch). A float literal is its binary64 value (D-j), a
naive datetime next to a zoned column is wall-clock time in that zone and a
date next to one is local midnight, as ``docs/api.md`` documents. From those,
``expectation`` says what a cell must produce when the decisions D-b and D-i
to D-m hold, and ``judge`` grades an outcome against it:

- ``ok``: the outcome is what the decisions require;
- ``skip``: the oracle has no opinion (a string or Boolean context or
  literal reads as DataFusion reads it, D-h);
- ``violation``: the outcome contradicts a decision; the reason says which.

An expectation is one of: the exact value of every row in one type, which
is DataFusion's own result type for the cell (the model ports the coercion
rules of DataFusion 55 that the grid exercises, so an exact result in a type
DataFusion would not propose is a violation); an error at one stage, of an
ordinary class, whose message names the cause that justifies it; or, for
arithmetic with a NULL literal, every row NULL in whatever type a NULL
operand gets, or DataFusion's planning error for the pair. A refusal is
accepted only where the model finds no exact type, so an unjustified
``ValueError`` is a violation like a wrong value. Before any of that, every
outcome must have the shape the grid records (an error with a class and a
stage, or a known type with one value of that type per row), and no outcome
is a Rust panic: no expectation authorizes one. A
dictionary-encoded result is allowed only on a dictionary-encoded context:
``fill_null`` and ``coalesce`` keep the encoding and CASE drops it, which is
DataFusion's business, so types compare modulo the wrapper there.

Type-level holding is decided here from the type's range and scale alone, so
the Rust ``holds`` and the Python model are two independent readings of the
same question.
"""

import datetime as dt
import math
import re
import struct
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from typing import Optional
from zoneinfo import ZoneInfo

from . import grid

NS = 10**9
DAY_NS = 86_400 * NS
TICKS = {"s": NS, "ms": 10**6, "us": 10**3, "ns": 1}
UNITS = ["s", "ms", "us", "ns"]
I64 = (-(2**63), 2**63 - 1)

# Tagged exact values: ("num", Fraction), ("inst", nanoseconds), ("special", "NaN"|"inf"|"-inf"),
# ("str", text), ("bool", flag), ("dur", nanoseconds); None is NULL.


def num(value):
    return ("num", Fraction(value))


def inst(nanoseconds):
    return ("inst", int(nanoseconds))


def float32(value):
    return struct.unpack("f", struct.pack("f", value))[0]


_FLOAT = {16: (11, -24, 16), 32: (24, -149, 128)}  # significand bits, smallest exponent, overflow exponent


def to_float(value, bits=64):
    """The nearest binary float of `bits` to an exact rational, ties to even, as a Python float."""
    value = Fraction(value)
    if bits == 64:
        try:
            return float(value)
        except OverflowError:
            return math.inf if value > 0 else -math.inf
    significand, least, overflow = _FLOAT[bits]
    if value == 0:
        return 0.0
    magnitude = abs(value)
    exponent = magnitude.numerator.bit_length() - magnitude.denominator.bit_length()
    if Fraction(2) ** exponent > magnitude:
        exponent -= 1
    quantum = max(exponent - (significand - 1), least)
    n = round(magnitude / Fraction(2) ** quantum)  # Fraction rounds half to even
    result = math.inf if n * Fraction(2) ** quantum >= Fraction(2) ** overflow else n * 2.0**quantum
    return -result if value < 0 else result


# --- what each context executes as ------------------------------------------

CONTEXT_TYPE = {
    "i8": "int8", "i16": "int16", "i32": "int32", "i64": "int64",
    "u8": "uint8", "u32": "uint32", "u64": "uint64",
    "f32": "float", "f64": "double",
    "d32": "decimal32(9, 2)", "d64": "decimal64(18, 3)", "d5": "decimal128(5, 2)",
    "d38": "decimal128(38, 10)", "dneg": "decimal128(10, -2)", "d256": "decimal256(50, 5)",
    "date32": "date32[day]", "date64": "date64[ms]",
    "ts_s": "timestamp[s]", "ts_ms": "timestamp[ms]", "ts_us": "timestamp[us]", "ts_ns": "timestamp[ns]",
    "ts_ny": "timestamp[us, tz=America/New_York]", "ts_utc": "timestamp[us, tz=UTC]",
    "str": "string", "bool": "bool",
    "dict_i64": "dictionary<values=int64, indices=int32, ordered=0>",
    # DataFusion's unification of the two branches
    "case_i64_f64": "double", "case_f64_i64": "double", "case_d5_d38": "decimal128(38, 10)",
    "case_us_ns": "timestamp[ns]", "case_i32_i64": "int64", "case_date_ts": "timestamp[ns]",
}

TEMPORAL = {"date", "ts", "ts_tz"}
EXACT_NUMERIC = {"int", "uint", "decimal"}
NUMERIC = EXACT_NUMERIC | {"float"}
TEMPORAL_LITERALS = {"date", "dt_naive", "dt_aware"}


def context_kind(ctx):
    t = parse_type(CONTEXT_TYPE[ctx])
    if t[0] == "int":
        return "int" if t[2] else "uint"
    if t[0] == "ts":
        return "ts_tz" if t[2] else "ts"
    return t[0]


def context_zone(ctx):
    t = parse_type(CONTEXT_TYPE[ctx])
    return t[2] if t[0] == "ts" else None


def literal_kind(lit):
    value = grid.literals()[lit]
    if value is None:
        return "none"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "special" if math.isnan(value) or math.isinf(value) else "float"
    if isinstance(value, Decimal):
        return "decimal"
    if isinstance(value, str):
        return "str"
    if type(value).__name__ == "Timestamp":
        return "dt_aware" if value.tz is not None else "dt_naive"
    if isinstance(value, dt.datetime):
        return "dt_aware" if value.tzinfo is not None else "dt_naive"
    if isinstance(value, dt.date):
        return "date"
    raise TypeError(lit)


def literal_exact(lit, zone=None):
    """The literal's exact value, read next to a column of `zone` (None for naive)."""
    value = grid.literals()[lit]
    kind = literal_kind(lit)
    if kind == "none":
        return None
    if kind == "bool":
        return ("bool", value)
    if kind in ("int", "decimal"):
        return num(value)
    if kind == "float":
        return num(value)  # Fraction(float) is the binary64 value
    if kind == "special":
        return ("special", grid.normalize(value))
    if kind == "str":
        return ("str", value)
    if kind == "date":
        if zone:
            local = dt.datetime(value.year, value.month, value.day, tzinfo=ZoneInfo(zone))
            return inst(_aware_ns(local))
        return inst((value - dt.date(1970, 1, 1)).days * DAY_NS)
    if type(value).__name__ == "Timestamp":  # pandas
        if value.tz is not None:
            return inst(value.value)
        if zone:
            return inst(value.tz_localize(zone).value)
        return inst(value.value)
    if value.tzinfo is not None:
        return inst(_aware_ns(value))
    if zone:
        return inst(_aware_ns(value.replace(tzinfo=ZoneInfo(zone))))
    return inst(_naive_ns(value))


def literal_wire(lit):
    """The Arrow type a numeric literal is sent as (``core_types.py``), parsed; None for other kinds."""
    kind = literal_kind(lit)
    if kind == "int":
        return ("int", 64, True)
    if kind in ("float", "special"):
        return ("float", 64)
    if kind == "decimal":
        _, digits, exponent = grid.literals()[lit].as_tuple()
        scale = max(-exponent, 0)
        return ("decimal", max(len(digits) + max(exponent, 0), scale), scale, 128)
    return None


def literal_unit(lit, ctx_type):
    """The unit a temporal literal carries: a datetime is microseconds, pandas its own unit, a date the column's."""
    value = grid.literals()[lit]
    if type(value).__name__ == "Timestamp":  # pandas, a datetime subclass
        return value.unit
    if isinstance(value, dt.datetime):
        return "us"
    t = parse_type(ctx_type)
    return t[1] if t[0] == "ts" else None


def _naive_ns(value):
    delta = value - dt.datetime(1970, 1, 1)
    return (delta.days * 86_400 + delta.seconds) * NS + delta.microseconds * 1000


def _aware_ns(value):
    delta = value - dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    return (delta.days * 86_400 + delta.seconds) * NS + delta.microseconds * 1000


def _column_exact(name):
    values, typ = grid.COLUMNS[name]
    if name.startswith("f32"):
        return [None if v is None else num(float32(v)) for v in values]
    if name.startswith("f64"):
        return [None if v is None else num(v) for v in values]
    if name.startswith("date"):
        return [None if v is None else inst((v - dt.date(1970, 1, 1)).days * DAY_NS) for v in values]
    if name.startswith("ts_"):
        unit = typ.unit
        return [None if v is None else inst(_aware_ns(v) if isinstance(v, dt.datetime) else v * TICKS[unit]) for v in values]
    if name == "str":
        return [None if v is None else ("str", v) for v in values]
    if name == "bool":
        return [None if v is None else ("bool", v) for v in values]
    return [None if v is None else num(v) for v in values]


def _cast_exact(value, type_text):
    """`value` carried into a CASE's unified type: floats round, everything else is exact."""
    t = parse_type(type_text)
    if value is not None and value[0] == "num" and t[0] == "float":
        return num(to_float(value[1], t[1]))
    return value


def context_exact(ctx):
    """The context's exact value in each row."""
    if ctx == "dict_i64":
        return _column_exact("i64")
    if ctx in grid.CASES:
        a, b = (_column_exact(name) for name in grid.CASES[ctx])
        unified = CONTEXT_TYPE[ctx]
        return [_cast_exact(a[i] if grid.K[i] > 2 else b[i], unified) for i in range(grid.ROWS)]
    return _column_exact(ctx)


# --- Arrow types, as pyarrow prints them ---------------------------------------

_DICT = re.compile(r"dictionary<values=(.+), indices=[^,>]+, ordered=\d>")
_DECIMAL = re.compile(r"decimal(32|64|128|256)\((-?\d+), (-?\d+)\)")
_TS = re.compile(r"timestamp\[(s|ms|us|ns)(?:, tz=([^\]]+))?\]")
_DUR = re.compile(r"duration\[(s|ms|us|ns)\]")
_INTS = {"int8": (8, True), "int16": (16, True), "int32": (32, True), "int64": (64, True),
         "uint8": (8, False), "uint16": (16, False), "uint32": (32, False), "uint64": (64, False)}
_FLOATS = {"halffloat": 16, "float": 32, "double": 64}


def parse_type(text):
    """("int", bits, signed) | ("float", bits) | ("decimal", precision, scale, width) | ("date", bits)
    | ("ts", unit, zone) | ("dur", unit) | ("bool",) | ("str",) | ("other", text)

    A dictionary wrapper is dropped: the value type is what the policy fixes."""
    inner = _DICT.fullmatch(text)
    if inner:
        return parse_type(inner.group(1))
    if text in _INTS:
        return ("int",) + _INTS[text]
    if text in _FLOATS:
        return ("float", _FLOATS[text])
    m = _DECIMAL.fullmatch(text)
    if m:
        return ("decimal", int(m.group(2)), int(m.group(3)), int(m.group(1)))
    if text == "date32[day]":
        return ("date", 32)
    if text == "date64[ms]":
        return ("date", 64)
    m = _TS.fullmatch(text)
    if m:
        return ("ts", m.group(1), m.group(2))
    m = _DUR.fullmatch(text)
    if m:
        return ("dur", m.group(1))
    if text == "bool":
        return ("bool",)
    if text in ("string", "large_string", "string_view"):
        return ("str",)
    return ("other", text)


def type_text(t):
    """A parsed type as pyarrow prints it."""
    kind = t[0]
    if kind == "int":
        return f"{'' if t[2] else 'u'}int{t[1]}"
    if kind == "float":
        return {bits: name for name, bits in _FLOATS.items()}[t[1]]
    if kind == "decimal":
        return f"decimal{t[3]}({t[1]}, {t[2]})"
    if kind == "date":
        return "date32[day]" if t[1] == 32 else "date64[ms]"
    if kind == "ts":
        return f"timestamp[{t[1]}]" if t[2] is None else f"timestamp[{t[1]}, tz={t[2]}]"
    if kind == "dur":
        return f"duration[{t[1]}]"
    if kind == "bool":
        return "bool"
    if kind == "str":
        return "string"
    return t[1]


def _parsed(t):
    return parse_type(t) if isinstance(t, str) else t


def _int_range(t):
    bits, signed = t[1], t[2]
    return (-(2 ** (bits - 1)), 2 ** (bits - 1) - 1) if signed else (0, 2**bits - 1)


def _representable(ns, ticks):
    return ns % ticks == 0 and I64[0] <= ns // ticks <= I64[1]


def holds(type_, value):
    """Whether the type (text or parsed) holds `value` exactly, from its range and scale alone."""
    if value is None:
        return True
    t = _parsed(type_)
    tag = value[0]
    if tag == "num":
        v = value[1]
        if t[0] == "int":
            lo, hi = _int_range(t)
            return v.denominator == 1 and lo <= v <= hi
        if t[0] == "float":
            f = to_float(v, t[1])
            return math.isfinite(f) and Fraction(f) == v
        if t[0] == "decimal":
            scaled = v * Fraction(10) ** t[2]
            return scaled.denominator == 1 and abs(scaled) < 10 ** t[1]
        return False
    if tag == "special":
        return t[0] == "float"
    if tag == "inst":
        ns = value[1]
        if t[0] == "date":
            return ns % DAY_NS == 0 and (t[1] == 64 or -(2**31) <= ns // DAY_NS <= 2**31 - 1)
        if t[0] == "ts":
            return _representable(ns, TICKS[t[1]])
        return False
    if tag == "dur":
        return t[0] == "dur" and _representable(value[1], TICKS[t[1]])
    if tag == "str":
        return t[0] == "str"
    if tag == "bool":
        return t[0] == "bool"
    return False


def finer_units(type_text_):
    """The timestamp types at the same zone with a finer unit, nearest first."""
    t = parse_type(type_text_)
    if t[0] != "ts":
        return []
    return [type_text(("ts", unit, t[2])) for unit in UNITS[UNITS.index(t[1]) + 1:]]


def _finer(unit, other):
    return unit if UNITS.index(unit) >= UNITS.index(other) else other


def nearest(type_, value):
    """`value` as the float type holds it (the nearest float), for a float result."""
    t = _parsed(type_)
    if value is None or value[0] != "num" or t[0] != "float":
        return value
    return num(to_float(value[1], t[1]))


# --- DataFusion 55's numeric coercion, as far as the grid exercises it ---------------
#
# `binary_numeric_coercion` (comparisons, and the values of coalesce and CASE):
# equal types unify as themselves; otherwise `decimal_coercion` where a side is
# a decimal, else `numerical_coercion`. The ports below follow
# datafusion-expr-common's type_coercion/binary.rs arm by arm.

_DECIMAL_CAP = {32: 9, 64: 18, 128: 38, 256: 76}
_INT_DIGITS = {8: 3, 16: 5, 32: 10, 64: 20}  # coerce_numeric_type_to_decimal: the decimal an int becomes


def _as_decimal(t, width):
    """`coerce_numeric_type_to_decimal{width}`: the decimal of `width` a number becomes, or None."""
    if t[0] == "decimal":
        return t
    if t[0] == "int":
        digits = _INT_DIGITS[t[1]]
        return ("decimal", digits, 0, width) if digits <= {32: 5, 64: 10, 128: 20, 256: 20}[width] else None
    if t[0] == "float":
        precision_scale = {16: (6, 3), 32: (14, 7), 64: (30, 15)}[t[1]]
        return ("decimal", *precision_scale, width) if t[1] <= {32: 16, 64: 32, 128: 64, 256: 64}[width] else None
    return None


def _wider_decimal(a, b, width):
    scale = max(a[2], b[2])
    precision = min(_DECIMAL_CAP[width], max(a[1] - a[2], b[1] - b[2]) + scale)
    return ("decimal", precision, scale, width)


def _decimal_coercion(a, b):
    if a[0] != "decimal" and b[0] != "decimal":
        return None
    if a[0] == "decimal" and b[0] == "decimal":
        if a[3] == b[3]:
            return _wider_decimal(a, b, a[3])
        width = 256 if 256 in (a[3], b[3]) else 128 if 128 in (a[3], b[3]) else 64
        required = max(a[1] - a[2], b[1] - b[2]) + max(a[2], b[2])
        return ("decimal", required, max(a[2], b[2]), width) if required <= _DECIMAL_CAP[width] else None
    decimal, other = (a, b) if a[0] == "decimal" else (b, a)
    coerced = _as_decimal(other, decimal[3])
    return _wider_decimal(decimal, coerced, decimal[3]) if coerced else None


def _numerical_coercion(a, b):
    if a[0] not in ("int", "float", "decimal") or b[0] not in ("int", "float", "decimal"):
        return None
    if a == b:
        return a

    def has(*types):
        return a in types or b in types

    def i(bits, signed=True):
        return ("int", bits, signed)

    for bits in (64, 32, 16):
        if has(("float", bits)):
            return ("float", bits)
    if has(i(64, False)) and has(i(64), i(32), i(16), i(8)):
        return ("decimal", 20, 0, 128)
    if has(i(64, False)):
        return i(64, False)
    if has(i(64)) or (has(i(32, False)) and has(i(32), i(16), i(8))):
        return i(64)
    if has(i(32, False)):
        return i(32, False)
    if has(i(32)) or (has(i(16, False)) and has(i(16), i(8))):
        return i(32)
    if has(i(16, False)):
        return i(16, False)
    if has(i(16)) or (has(i(8, False)) and has(i(8))):
        return i(16)
    if has(i(8)):
        return i(8)
    if has(i(8, False)):
        return i(8, False)
    return None


def unify(a, b):
    """DataFusion's common type for two numeric types in a comparison or a shared value, or None."""
    a, b = _parsed(a), _parsed(b)
    if a == b:
        return a
    return _decimal_coercion(a, b) or _numerical_coercion(a, b)


_SIGNIFICAND = {16: 11, 32: 24, 64: 53}
_RANGE_DIGITS = {(8, True): 3, (16, True): 5, (32, True): 10, (64, True): 19,
                 (8, False): 3, (16, False): 5, (32, False): 10, (64, False): 20}


def widens_exactly(frm, to):
    """Whether every value of `frm` is a value of `to`, from ranges and scales alone
    (the Python reading of ``cast_class == Exact``)."""
    frm, to = _parsed(frm), _parsed(to)
    if frm == to:
        return True
    if frm[0] == "int":
        if to[0] == "int":
            return _int_range(to)[0] <= _int_range(frm)[0] and _int_range(frm)[1] <= _int_range(to)[1]
        if to[0] == "float":
            return (frm[1] - 1 if frm[2] else frm[1]) <= _SIGNIFICAND[to[1]]
        if to[0] == "decimal":
            return to[2] >= 0 and to[1] - to[2] >= _RANGE_DIGITS[(frm[1], frm[2])]
        return False
    if frm[0] == "decimal":
        if to[0] == "decimal":
            return to[2] >= frm[2] and to[1] - to[2] >= frm[1] - frm[2]
        if to[0] == "float":
            return frm[2] <= 0 and 10 ** (frm[1] - frm[2]) <= 2 ** _SIGNIFICAND[to[1]]
        return False
    if frm[0] == "float":
        return to[0] == "float" and to[1] >= frm[1]
    if frm[0] == "date":
        return to[0] == "date" and to[1] >= frm[1]
    return False


# --- reading an outcome's values -------------------------------------------------

_TIMESTAMP = re.compile(r"Timestamp\((-?\d+)(?:, tz=(.*))?\)")
_DATETIME = re.compile(r"datetime\((.*)\)")
_DATE = re.compile(r"date\((.*)\)")
_DECIMAL_VALUE = re.compile(r"Decimal\((.*)\)")
_TIMEDELTA = re.compile(r"timedelta\((-?\d+), (\d+), (\d+)\)")
_PD_TIMEDELTA = re.compile(r"Timedelta\((-?\d+)\)")
STAGES = {"capture", "plan", "collect"}


def result_exact(value, type_text_):
    """A normalized outcome value as an exact value.

    Raises ValueError when the value is not one the type produces: a Boolean
    column holds Booleans, an integer column integers, a timestamp column
    ``datetime(...)`` or ``Timestamp(...)`` renderings whose awareness is the
    column's, and so on."""
    if value is None:
        return None
    t = parse_type(type_text_)
    kind = t[0]
    text = value if isinstance(value, str) else None
    try:
        exact = _result_exact(t, kind, value, text)
    except (ValueError, ArithmeticError):  # an unparseable rendering, such as NaT
        exact = None
    if exact is None:
        raise ValueError(f"{value!r} is not a value of {type_text_}")
    return exact


def _result_exact(t, kind, value, text):
    if kind == "bool":
        if isinstance(value, bool):
            return ("bool", value)
    elif kind == "int":
        if isinstance(value, int) and not isinstance(value, bool):
            return num(value)
    elif kind == "float":
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return num(value)
        if text in ("NaN", "inf", "-inf"):
            return ("special", text)
        if text == "-0.0":
            return num(0)
    elif kind == "decimal":
        m = text and _DECIMAL_VALUE.fullmatch(text)
        if m:
            return num(Decimal(m.group(1)))
    elif kind == "date":
        m = text and _DATE.fullmatch(text)
        if m:
            return inst((dt.date.fromisoformat(m.group(1)) - dt.date(1970, 1, 1)).days * DAY_NS)
    elif kind == "ts":
        m = text and _TIMESTAMP.fullmatch(text)
        if m and (m.group(2) is not None) == (t[2] is not None):
            return inst(int(m.group(1)))
        m = text and _DATETIME.fullmatch(text)
        if m:
            parsed = dt.datetime.fromisoformat(m.group(1))
            if (parsed.tzinfo is not None) == (t[2] is not None):
                return inst(_aware_ns(parsed) if parsed.tzinfo else _naive_ns(parsed))
    elif kind == "dur":
        m = text and _TIMEDELTA.fullmatch(text)
        if m:
            days, seconds, micros = (int(g) for g in m.groups())
            return ("dur", (days * 86_400 + seconds) * NS + micros * 1000)
        m = text and _PD_TIMEDELTA.fullmatch(text)
        if m:
            return ("dur", int(m.group(1)))
    elif kind == "str":
        if text is not None:
            return ("str", text)
    return None


def outcome_values(outcome):
    """The outcome's values as exact values; ValueError when one is not a value of its type."""
    return [result_exact(v, outcome["type"]) for v in outcome["values"]]


def check_structure(ctx, outcome):
    """Raise ValueError unless the outcome has the shape the grid records.

    An error has a class and a stage; values have a known type, one value of
    that type per row, and a dictionary wrapper only on a dictionary-encoded
    context."""
    if "error" in outcome:
        error = outcome["error"]
        if not isinstance(error, dict) or not error.get("class") or error.get("stage") not in STAGES:
            raise ValueError(f"an error needs a class and a stage in {sorted(STAGES)}: {error!r}")
        return
    if "type" not in outcome or "values" not in outcome:
        raise ValueError("neither an error nor a type with values")
    text, values = outcome["type"], outcome["values"]
    if not isinstance(text, str) or parse_type(text)[0] == "other":
        raise ValueError(f"unknown result type {text!r}")
    if _DICT.fullmatch(text) and not _DICT.fullmatch(CONTEXT_TYPE[ctx]):
        raise ValueError(f"dictionary-encoded result {text} on a plain context")
    if not isinstance(values, list) or len(values) != grid.ROWS:
        raise ValueError(f"{len(values) if isinstance(values, list) else values!r} values for {grid.ROWS} rows")
    outcome_values(outcome)


def same(a, b):
    if a is None or b is None:
        return a is None and b is None
    return a == b


# --- the expectations -------------------------------------------------------------


@dataclass(frozen=True)
class Expectation:
    """What a cell must produce.

    ``kind`` is ``values`` (``type`` is the parsed result type and ``rows``
    the exact value of every row; ``type`` is None only where the authorized
    type is itself an open issue, #241), ``error`` (raised at ``stage``, of
    a class in ``cls``, with a message ``msg`` matches: the cause),
    ``null_arithmetic`` (every row NULL in whatever type DataFusion gives a
    NULL operand, or the planning error ``stage``, ``cls`` and ``msg``
    describe) or ``skip`` (no opinion). ``reason`` names the decision the
    expectation rests on."""

    kind: str
    reason: str
    type: Optional[tuple] = None
    rows: Optional[tuple] = None
    stage: Optional[str] = None
    cls: Optional[tuple] = None
    msg: Optional[str] = None


def _values(t, rows, reason):
    rows = tuple(rows)
    assert len(rows) == grid.ROWS, (reason, rows)
    return Expectation("values", reason, type=None if t is None else _parsed(t), rows=rows)


# What justifies an error. ltseq refuses a literal at plan with a ValueError
# that words the decision behind it; DataFusion's planning errors reach Python
# as RuntimeErrors, and a failure while collecting as a ValueError carrying
# DataFusion's cause. Each pattern matches the message a debug build and a
# build without overflow checks give alike.
NOT_A_NUMBER_FOR_A_DATE = (
    r"is a date or timestamp \(.*\); use a date or datetime, not "
    r"|dt_diff cannot subtract .*; both sides must be dates or timestamps"
)
NOT_A_DATE_FOR_A_NUMBER = r"is numeric \(.*\); use a number, not a date or datetime"
AWARE_NEXT_TO_NAIVE = r"is timezone-naive, but the literal is timezone-aware"
NOT_FOR_A_STRING = r"is a string"
NO_NAN_OR_INFINITY = r"which has no value for -?(NaN|inf); NaN and infinity meet only float columns"
NO_EXACT_FIT = r"does not fit .* (exactly|without rounding)"
NO_EXACT_SHIFT_DEFAULT = r"cannot hold the shift\(\) default .* exactly"
OUT_OF_RANGE = r"is outside the range of"
NOT_A_WHOLE_DAY = r"is a date; the .*literal .* (has a time of day|is not a midnight)"
NO_ARITHMETIC_TYPE = (
    r"Error during planning: Cannot (coerce arithmetic expression"
    r"|get result type for (arithmetic|temporal) operation) "
)
CAST_OUT_OF_RANGE = r"Cannot cast \w+(\(.*?\))? value -?\d+ to Timestamp\(.*?\): converted value exceeds the representable i64 range"
DECIMAL_OVERFLOW = r"Arithmetic overflow"


def _error(reason, stage, cls, msg):
    return Expectation("error", reason, stage=stage, cls=(cls,) if isinstance(cls, str) else tuple(cls), msg=msg)


def _plan_value_error(reason, msg):
    """ltseq's own refusal."""
    return _error(reason, "plan", "ValueError", msg)


def _planning_error(reason, msg):
    """DataFusion's refusal to plan the expression."""
    return _error(reason, "plan", "RuntimeError", msg)


def _collect_error(reason, msg):
    """A failure while DataFusion computes the values."""
    return _error(reason, "collect", "ValueError", msg)


def _skip(reason):
    return Expectation("skip", reason)


def _bools(flags):
    return [None if flag is None else ("bool", bool(flag)) for flag in flags]


LITERAL_ROWS = {  # rows that show the literal, per value position
    "fill": [1, 4], "coal": [1, 4], "coal_rev": [0, 1, 2, 3, 4, 5],
    "ifelse_t": [0, 2, 4], "ifelse_f": [0, 2, 4], "shift_def": [0],
}


def expected_values(ctx, lit, pos, zone):
    """Which exact value each row must show in a value position or a shift."""
    column = context_exact(ctx)
    literal = literal_exact(lit, zone)
    return _placed(column, literal, pos)


def _placed(column, literal, pos):
    if pos == "shift_def":
        return [literal] + column[:-1]
    return [literal if i in LITERAL_ROWS[pos] else column[i] for i in range(grid.ROWS)]


def exact_rows(ctx, lit, pos):
    """The cell's exact reading whether or not a type holds it: the comparison of the exact
    values, or the literal's exact value placed among the column's. It is what a value
    this branch refuses would have had to be, so a baseline that returned anything else
    was wrong. None where the operands are of different kinds, a string, Boolean, NULL or
    non-finite float is involved, or the position is arithmetic, whose exactness only
    ``expectation`` models."""
    ck, lk = context_kind(ctx), literal_kind(lit)
    if pos in grid.ARITHMETIC_POSITIONS or pos == "dtdiff":
        return None
    if ck in ("str", "bool") or lk not in {"int", "float", "decimal"} | TEMPORAL_LITERALS:
        return None
    if (ck in TEMPORAL) != (lk in TEMPORAL_LITERALS) or (ck == "ts" and lk == "dt_aware"):
        return None
    column, literal = context_exact(ctx), literal_exact(lit, context_zone(ctx))
    if pos in grid.COMPARISON_POSITIONS:
        return tuple(_bools([compare(pos, x, literal) for x in column]))
    return tuple(_placed(column, literal, pos))


def compare(pos, x, literal):
    """SQL's answer for the comparison position, exactly."""
    if x is None:
        return None
    a, b = x[1], literal[1]
    if pos == "eq" or pos == "isin":
        return a == b
    if pos == "isin2":
        return a == b or (x[0] == "num" and a == 1)
    return a > b  # gt, and lt_mirror (literal < x)


def expectation(ctx, lit, pos):
    """What the decisions require of the cell."""
    ck, lk = context_kind(ctx), literal_kind(lit)
    # D-l: a number or Boolean next to a date or timestamp is a plan-time type
    # error in every position; `isin2` carries the int 1.
    if ck in TEMPORAL and (lk in ("int", "float", "special", "decimal", "bool") or pos == "isin2"):
        cause = NOT_A_NUMBER_FOR_A_DATE
        if lk in TEMPORAL_LITERALS:
            # `isin2` refuses the 1, or first the literal where it alone is refused.
            alone = expectation(ctx, lit, "isin")
            cause = cause if alone.kind != "error" else f"{cause}|{alone.msg}"
        return _plan_value_error("D-l: a number or Boolean next to a date or timestamp", cause)
    if ck in NUMERIC and lk in TEMPORAL_LITERALS:
        return _plan_value_error("D-l (mirrored): a date or datetime next to a number", NOT_A_DATE_FOR_A_NUMBER)
    if ck == "ts" and lk == "dt_aware":
        return _plan_value_error("D5: an aware datetime next to a naive timestamp", AWARE_NEXT_TO_NAIVE)
    if ck == "str" and lk in {"decimal"} | TEMPORAL_LITERALS and pos not in grid.ARITHMETIC_POSITIONS:
        return _plan_value_error("D-h: a Decimal, date or datetime next to a string column", NOT_FOR_A_STRING)
    if lk == "none":
        return _null_expectation(ctx, pos)
    if ck in ("str", "bool") or lk in ("str", "bool"):
        return _skip("D-h: strings and Booleans keep DataFusion's reading")
    if lk == "special" and ck in EXACT_NUMERIC and pos not in grid.ARITHMETIC_POSITIONS:
        return _plan_value_error("D-j: NaN and infinity have no value in an integer or decimal column", NO_NAN_OR_INFINITY)
    if ck == "float":
        return _float_expectation(ctx, lit, pos)
    if ck in EXACT_NUMERIC:
        return _exact_expectation(ctx, lit, pos)
    return _temporal_expectation(ctx, lit, pos)


def _null_expectation(ctx, pos):
    """``None`` is NULL: ``== None`` is ``IS NULL`` (#154), any other comparison is NULL, a
    shared NULL keeps the context's type and arithmetic with NULL is DataFusion's."""
    column = context_exact(ctx)
    t = parse_type(CONTEXT_TYPE[ctx])
    if pos == "eq":
        return _values(("bool",), _bools([x is None for x in column]), "#154: `== None` is IS NULL")
    if pos in ("gt", "lt_mirror", "isin"):
        return _values(("bool",), [None] * grid.ROWS, "a comparison with NULL is NULL")
    if pos == "isin2":
        if t[0] in ("str", "bool"):
            return _skip("D-h: the int 1 next to a string or Boolean column is DataFusion's reading")
        flags = [None if x is None else (True if x[0] == "num" and x[1] == 1 else None) for x in column]
        return _values(("bool",), _bools(flags), "x IN (NULL, 1) is x = 1 OR NULL")
    if pos in ("fill", "coal", "coal_rev"):
        return _values(t, column, "a NULL filled with NULL, or coalesced with it, is the column")
    if pos in ("ifelse_t", "ifelse_f"):
        rows = [None if i in LITERAL_ROWS[pos] else column[i] for i in range(grid.ROWS)]
        return _values(t, rows, "a NULL branch of a CASE keeps the column's type")
    if pos == "shift_def":
        return _values(t, [None] + column[:-1], "a NULL shift default keeps the column's type")
    return Expectation(
        "null_arithmetic", "arithmetic with NULL is NULL in DataFusion's type, or its planning error",
        stage="plan", cls=("RuntimeError",), msg=NO_ARITHMETIC_TYPE,
    )


def _special_result(special, pos):
    if special == "NaN":
        return ("special", "NaN")
    return ("special", "inf" if (special == "inf") == (pos == "add") else "-inf")


def _float_value(exact, bits):
    f = to_float(exact, bits)
    return ("special", "inf" if f > 0 else "-inf") if math.isinf(f) else num(f)


def _compare_float(pos, x, read):
    if x is None:
        return None
    if read[0] != "special":
        return compare(pos, x, read)
    if pos == "isin2":
        return x[1] == 1
    if read[1] == "-inf":
        return pos in ("gt", "lt_mirror")  # every float is above -inf
    return False  # nothing equals NaN or infinity, nothing is above infinity


def _float_expectation(ctx, lit, pos):
    """A float column keeps float semantics: an int or Decimal literal is the nearest
    float of the column's width, a float literal its binary64 value, so the result is
    the wider of the two floats (D-b, D-i, D-j)."""
    bits = parse_type(CONTEXT_TYPE[ctx])[1]
    lk = literal_kind(lit)
    column = context_exact(ctx)
    literal = literal_exact(lit)
    if lk in ("int", "decimal"):
        read, result_bits = num(to_float(literal[1], bits)), bits
    else:
        read, result_bits = literal, 64
    if pos in grid.COMPARISON_POSITIONS:
        rows = _bools([_compare_float(pos, x, read) for x in column])
        return _values(("bool",), rows, "a float comparison, exact on the floats that meet")
    if pos in grid.VALUE_POSITIONS:
        return _values(("float", result_bits), _placed(column, read, pos), "a shared value at the wider float")
    if pos == "shift_def":
        held = read if read[0] == "special" else num(to_float(read[1], bits))
        return _values(("float", bits), _placed(column, held, pos), "D-c: a shift default is the column's nearest float")
    rows = []
    for x in column:
        if x is None:
            rows.append(None)
        elif read[0] == "special":
            rows.append(_special_result(read[1], pos))
        else:
            rows.append(_float_value(x[1] + read[1] if pos == "add" else x[1] - read[1], result_bits))
    return _values(("float", result_bits), rows, "IEEE arithmetic at the wider float")


def _exact_expectation(ctx, lit, pos):
    """An integer or decimal column: comparisons by exact math (D-e, D-j), shared values
    in DataFusion's unification when it is exact for the column and holds the literal,
    else in the column's type when it holds the literal, else refused (D-i), shift
    defaults in the column's type or refused (D-c), arithmetic as DataFusion computes it."""
    t = parse_type(CONTEXT_TYPE[ctx])
    column = context_exact(ctx)
    literal = literal_exact(lit)
    if pos in grid.COMPARISON_POSITIONS:
        return _values(("bool",), _bools([compare(pos, x, literal) for x in column]), "D-e, D-j: an exact comparison")
    if pos in grid.ARITHMETIC_POSITIONS:
        return _exact_arithmetic(t, lit, pos, column, literal)
    if pos == "shift_def":
        if holds(t, literal):
            return _values(t, _placed(column, literal, pos), "D-c: a shift default in the column's type")
        return _plan_value_error("D-c: the column cannot hold the shift default exactly", NO_EXACT_SHIFT_DEFAULT)
    proposed = unify(t, literal_wire(lit))
    if proposed and widens_exactly(t, proposed) and holds(proposed, literal):
        return _values(proposed, _placed(column, literal, pos), "D-i: DataFusion's unification is exact for the column and holds the literal")
    if holds(t, literal):
        return _values(t, _placed(column, literal, pos), "D-i: the literal-free context type holds the literal")
    return _plan_value_error("D-i: no exact type for the shared value", NO_EXACT_FIT)


def _wrap_i64(value):
    return ((value + 2**63) % 2**64) - 2**63


def _powi(base, n):
    """Rust's f64::powi: repeated squaring, then the reciprocal for a negative exponent."""
    result, recip, n = 1.0, n < 0, abs(n)
    while True:
        if n & 1:
            result *= base
        n >>= 1
        if n == 0:
            break
        base *= base
    return 1.0 / result if recip else result


def _i256_to_f64(n):
    """arrow's i256::to_f64: the top 64 bits, scaled back."""
    shift = (n.bit_length() if n >= 0 else (-n - 1).bit_length()) - 63
    return float(n >> shift) * 2.0**shift if shift > 0 else float(n)


def _as_f64(t, value):
    """The column's value cast to Float64 as Arrow casts it: exact from an integer type,
    unscaled-then-divided (lossy) from a decimal."""
    if t[0] != "decimal":
        return float(value)
    unscaled = int(value * Fraction(10) ** t[2])
    f = _i256_to_f64(unscaled) if t[3] == 256 else float(unscaled)
    return f / _powi(10.0, t[2])


def _exact_arithmetic(t, lit, pos, column, literal):
    lk = literal_kind(lit)
    sign = 1 if pos == "add" else -1
    if lk in ("float", "special"):
        rows = []
        for x in column:
            if x is None:
                rows.append(None)
            elif literal[0] == "special":
                rows.append(_special_result(literal[1], pos))
            else:
                rows.append(_float_value(Fraction(_as_f64(t, x[1])) + sign * literal[1], 64))
        return _values(("float", 64), rows, "arithmetic with a float is Float64 (DataFusion's coercion), the column cast as Arrow casts it")
    if lk == "int":
        if t[0] == "int" and not (t[1] == 64 and not t[2]):
            rows = [None if x is None else num(_wrap_i64(x[1] + sign * literal[1])) for x in column]
            return _values(("int", 64, True), rows, "integer arithmetic at Int64, wrapping (DataFusion's coercion)")
        if t[0] == "int":  # UInt64 next to Int64: both become Decimal128(20, 0)
            return _decimal_arithmetic(("decimal", 20, 0, 128), ("decimal", 20, 0, 128), column, literal, sign)
        if t[3] in (32, 64):
            rows = [None if x is None else num(x[1] + sign * literal[1]) for x in column]
            return _values(None, rows, "#241: the exact sum; DataFusion truncates a decimal32/64 column to Int64 next to an integer")
        return _decimal_arithmetic(t, ("decimal", 20, 0, t[3]), column, literal, sign)
    wire = literal_wire(lit)
    if t[0] == "int":
        return _decimal_arithmetic(("decimal", _INT_DIGITS[t[1]], 0, 128), wire, column, literal, sign)
    if t[3] == 128:
        return _decimal_arithmetic(t, wire, column, literal, sign)
    width = 256 if t[3] == 256 else 128
    required = max(t[1] - t[2], wire[1] - wire[2]) + max(t[2], wire[2])
    if required > _DECIMAL_CAP[width]:
        return _planning_error("no decimal type of one width holds both sides", NO_ARITHMETIC_TYPE)
    common = ("decimal", required, max(t[2], wire[2]), width)
    return _decimal_arithmetic(common, common, column, literal, sign)


def _decimal_arithmetic(a, b, column, literal, sign):
    """Arrow's `decimal_op`: both sides rescaled to the larger scale by checked multiplication,
    then added or subtracted with a check, in the width's native integer; the result's
    precision is DataFusion's, capped, and not validated against the values."""
    width = a[3]
    cap, native = _DECIMAL_CAP[width], 2 ** (width - 1)
    scale = max(a[2], b[2])
    precision = min(scale + max(a[1] - a[2], b[1] - b[2]) + 1, cap)
    l_mul, r_mul = 10 ** (scale - a[2]), 10 ** (scale - b[2])
    right = int(literal[1] * Fraction(10) ** b[2])
    rows = []
    overflow = l_mul >= native or r_mul >= native
    for x in column:
        if x is None:
            rows.append(None)
            continue
        left = int(x[1] * Fraction(10) ** a[2])
        terms = (left * l_mul, right * r_mul)
        total = terms[0] + sign * terms[1]
        if any(not -native <= v < native for v in terms + (total,)):
            overflow = True
        rows.append(num(Fraction(total, 10**scale)))
    if overflow:
        return _collect_error("Arrow's checked decimal arithmetic overflows the native integer", DECIMAL_OVERFLOW)
    return _values(("decimal", precision, scale, width), rows, "decimal arithmetic, exact at DataFusion's result scale")


def _temporal_expectation(ctx, lit, pos):
    """A date or timestamp column next to a date or datetime literal: comparisons of
    instants are exact (#200), a shared value is the column's type or (D-m) the
    literal's finer unit in the column's zone, a shift default never widens, and
    subtraction and `dt.diff` are exact where DataFusion's computing unit holds both."""
    t = parse_type(CONTEXT_TYPE[ctx])
    zone = t[2] if t[0] == "ts" else None
    lk = literal_kind(lit)
    column = context_exact(ctx)
    literal = literal_exact(lit, zone)
    unit = literal_unit(lit, CONTEXT_TYPE[ctx])
    if pos in grid.COMPARISON_POSITIONS:
        return _values(("bool",), _bools([compare(pos, x, literal) for x in column]), "#200: an exact comparison of instants")
    if pos == "add":
        return _planning_error("an instant plus an instant has no type", NO_ARITHMETIC_TYPE)
    if pos == "sub":
        return _temporal_sub(t, lk, unit, column, literal)
    if pos == "dtdiff":
        return _temporal_diff(t, lk, unit, column, literal)
    rows = _placed(column, literal, pos)
    if t[0] == "date":
        if holds(t, literal):
            return _values(t, rows, "a midnight instant is the date")
        cause = NO_EXACT_SHIFT_DEFAULT if pos == "shift_def" else NOT_A_WHOLE_DAY
        return _plan_value_error("a date column holds whole days only and never becomes a timestamp (D-m)", cause)
    if holds(t, literal):
        return _values(t, rows, "the column's unit holds the instant")
    widened = ("ts", unit, zone)
    if pos != "shift_def" and _finer(t[1], unit) == unit and unit != t[1] and holds(widened, literal):
        return _values(widened, rows, "D-m: a shared value widens to the literal's finer unit, keeping the zone")
    if pos == "shift_def":
        return _plan_value_error("D-c, D-m: a shift default never widens the column", NO_EXACT_SHIFT_DEFAULT)
    return _plan_value_error("D-m: no unit of the column's zone holds the instant", OUT_OF_RANGE)


def _computing_unit(t, lk, unit, for_diff):
    """The unit DataFusion subtracts at: a date column meets a datetime at nanoseconds (`sub`)
    or at the literal's unit (`dt.diff`), a timestamp column meets a date at its own unit
    (naive `sub` excepted: nanoseconds) and a datetime at the finer of the two."""
    if t[0] == "date":
        return "day" if lk == "date" else (unit if for_diff else "ns")
    if lk == "date":
        return t[1] if (for_diff or t[2]) else "ns"
    return _finer(t[1], unit)


def _unrepresentable(computing, column, literal):
    if computing == "day":
        return False
    ticks = TICKS[computing]
    return not _representable(literal[1], ticks) or any(x is not None and not _representable(x[1], ticks) for x in column)


def _temporal_sub(t, lk, unit, column, literal):
    computing = _computing_unit(t, lk, unit, for_diff=False)
    if computing == "day":
        rows = [None if x is None else num(Fraction(x[1] - literal[1], DAY_NS)) for x in column]
        return _values(("int", 64, True), rows, "a date minus a date is a day count")
    if _unrepresentable(computing, column, literal):
        return _collect_error(f"DataFusion subtracts at {computing}, where an operand is out of range", CAST_OUT_OF_RANGE)
    rows = [None if x is None else ("dur", x[1] - literal[1]) for x in column]
    return _values(("dur", computing), rows, f"an exact difference at {computing}")


def _temporal_diff(t, lk, unit, column, literal):
    computing = _computing_unit(t, lk, unit, for_diff=True)
    if _unrepresentable(computing, column, literal):
        return _collect_error(f"dt.diff subtracts at {computing}, where an operand is out of range", CAST_OUT_OF_RANGE)
    ticks = DAY_NS if computing == "day" else TICKS[computing]
    factor = ticks / NS  # the elapsed seconds per tick, applied in Float64
    rows = []
    for x in column:
        if x is None:
            rows.append(None)
            continue
        elapsed = float((x[1] - literal[1]) // ticks)
        rows.append(num(elapsed * factor if ticks >= NS else elapsed / (NS / ticks)))
    return _values(("float", 64), rows, f"dt.diff in seconds from the difference at {computing}")


# --- the judge ----------------------------------------------------------------------


def _ok():
    return ("ok", None)


def _violation(reason):
    return ("violation", reason)


def _show(outcome):
    if "error" in outcome:
        e = outcome["error"]
        return f"{e['class']} at {e['stage']}: {e.get('msg', '')}"
    return f"{outcome['type']} {outcome['values']}"


def _fmt(value):
    if value is None:
        return "NULL"
    tag, v = value
    if tag == "num":
        return str(v) if v.denominator == 1 else f"{v} (~{float(v)!r})"
    if tag in ("inst", "dur"):
        return f"{v} ns"
    return repr(v)


def _describe(e):
    return f"{' or '.join(e.cls)} at {e.stage} matching /{e.msg}/"


def _expected_error(e, error):
    return error["stage"] == e.stage and error["class"] in e.cls and re.search(e.msg, error.get("msg", "")) is not None


def judge(ctx, lit, pos, outcome):
    """("ok" | "skip" | "violation", reason) for the outcome of a cell."""
    try:
        check_structure(ctx, outcome)
    except ValueError as error:
        return _violation(f"malformed outcome: {error}")
    # Before any expectation, and whatever the cell: a panic is an internal
    # failure, which no decision authorizes (review of d1eb7b3 on #225, R2).
    if grid.exposed_panic(outcome):
        return _violation(f"an exposed panic is never an authorized outcome: {_show(outcome)}")
    e = expectation(ctx, lit, pos)
    if e.kind == "skip":
        return ("skip", e.reason)
    if e.kind == "error":
        if "error" not in outcome or not _expected_error(e, outcome["error"]):
            return _violation(f"{e.reason}: expected {_describe(e)}, got {_show(outcome)}")
        return _ok()
    if e.kind == "null_arithmetic":
        if "error" in outcome:
            if _expected_error(e, outcome["error"]):
                return _ok()
            return _violation(f"{e.reason}: expected NULL values or {_describe(e)}, got {_show(outcome)}")
        if any(v is not None for v in outcome["values"]):
            return _violation(f"{e.reason}: expected NULL in every row, got {_show(outcome)}")
        return _ok()
    if "error" in outcome:
        return _violation(f"{e.reason}: expected {type_text(e.type) if e.type else 'exact'} values, got {_show(outcome)}")
    if e.type is not None and parse_type(outcome["type"]) != e.type:
        return _violation(f"{e.reason}: expected {type_text(e.type)}, got {outcome['type']}")
    got = outcome_values(outcome)
    for i, (want, have) in enumerate(zip(e.rows, got)):
        if not same(want, have):
            return _violation(f"{e.reason}: row {i} expected {_fmt(want)}, got {_fmt(have)} in {outcome['type']}")
    return _ok()
