"""An exact model of the literal grid, independent of ltseq and DataFusion.

Column values and literals become exact numbers (``Fraction``) or instants
(nanoseconds since the epoch). A float literal is its binary64 value (D-j), a
naive datetime next to a zoned column is wall-clock time in that zone and a
date next to one is local midnight, as ``docs/api.md`` documents. From those
the oracle says what a cell must produce when the decisions D-b and D-i to
D-m hold, and ``judge`` grades an outcome against it:

- ``ok``: the outcome is what the decisions require;
- ``skip``: the oracle has no opinion (string and Boolean readings are
  DataFusion's by D-h, and float arithmetic is DataFusion's);
- ``violation``: the outcome contradicts a decision; the reason says which.

Type-level holding is decided here from the type's range and scale alone, so
the Rust ``holds`` and the Python model are two independent readings of the
same question.
"""

import datetime as dt
import math
import re
import struct
from decimal import Decimal
from fractions import Fraction
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
        return num(float(value[1]) if t[1] == 64 else float32(float(value[1])))
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

_DICT = re.compile(r"dictionary<values=([^,>]+)")
_DECIMAL = re.compile(r"decimal(32|64|128|256)\((-?\d+), (-?\d+)\)")
_TS = re.compile(r"timestamp\[(s|ms|us|ns)(?:, tz=([^\]]+))?\]")
_INTS = {"int8": (8, True), "int16": (16, True), "int32": (32, True), "int64": (64, True),
         "uint8": (8, False), "uint16": (16, False), "uint32": (32, False), "uint64": (64, False)}


def parse_type(text):
    """("int", bits, signed) | ("float", bits) | ("decimal", precision, scale) | ("date", bits)
    | ("ts", unit, zone) | ("bool",) | ("str",) | ("other", text)"""
    inner = _DICT.match(text)
    if inner:
        return parse_type(inner.group(1))
    if text in _INTS:
        return ("int",) + _INTS[text]
    if text in ("float", "double", "halffloat"):
        return ("float", {"halffloat": 16, "float": 32, "double": 64}[text])
    m = _DECIMAL.fullmatch(text)
    if m:
        return ("decimal", int(m.group(2)), int(m.group(3)))
    if text.startswith("date32"):
        return ("date", 32)
    if text.startswith("date64"):
        return ("date", 64)
    m = _TS.fullmatch(text)
    if m:
        return ("ts", m.group(1), m.group(2))
    if text == "bool":
        return ("bool",)
    if text in ("string", "large_string", "string_view"):
        return ("str",)
    return ("other", text)


def holds(type_text, value):
    """Whether the type holds `value` exactly, from its range and scale alone."""
    if value is None:
        return True
    t = parse_type(type_text)
    tag = value[0]
    if tag == "num":
        v = value[1]
        if t[0] == "int":
            bits, signed = t[1], t[2]
            lo, hi = (-(2 ** (bits - 1)), 2 ** (bits - 1) - 1) if signed else (0, 2**bits - 1)
            return v.denominator == 1 and lo <= v <= hi
        if t[0] == "float":
            return Fraction(float(v) if t[1] == 64 else float32(float(v))) == v
        if t[0] == "decimal":
            precision, scale = t[1], t[2]
            scaled = v * Fraction(10) ** scale
            return scaled.denominator == 1 and abs(scaled) < 10**precision
        return False
    if tag == "special":
        return t[0] == "float"
    if tag == "inst":
        ns = value[1]
        if t[0] == "date":
            return ns % DAY_NS == 0 and (t[1] == 64 or -(2**31) <= ns // DAY_NS <= 2**31 - 1)
        if t[0] == "ts":
            ticks = TICKS[t[1]]
            return ns % ticks == 0 and I64[0] <= ns // ticks <= I64[1]
        return False
    if tag == "str":
        return t[0] == "str"
    if tag == "bool":
        return t[0] == "bool"
    return False


def finer_units(type_text):
    """The timestamp types at the same zone with a finer unit, nearest first."""
    t = parse_type(type_text)
    if t[0] != "ts":
        return []
    zone = f", tz={t[2]}" if t[2] else ""
    return [f"timestamp[{unit}{zone}]" for unit in UNITS[UNITS.index(t[1]) + 1:]]


def nearest(type_text, value):
    """`value` as the float type holds it (the nearest float), for a float result."""
    t = parse_type(type_text)
    if value is None or value[0] != "num" or t[0] != "float":
        return value
    return num(float(value[1]) if t[1] == 64 else float32(float(value[1])))


# --- reading an outcome's values -------------------------------------------------

_TIMESTAMP = re.compile(r"Timestamp\((-?\d+)(?:, tz=(.*))?\)")
_DATETIME = re.compile(r"datetime\((.*)\)")
_DATE = re.compile(r"date\((.*)\)")
_DECIMAL_VALUE = re.compile(r"Decimal\((.*)\)")
_TIMEDELTA = re.compile(r"timedelta\((-?\d+), (\d+), (\d+)\)")
_PD_TIMEDELTA = re.compile(r"Timedelta\((-?\d+)\)")


def result_exact(value, type_text):
    """A normalized outcome value as an exact value."""
    if value is None:
        return None
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, (int, float)):
        return num(value)
    if parse_type(type_text)[0] == "str":
        return ("str", value)
    if value in ("NaN", "inf", "-inf"):
        return ("special", value)
    if value == "-0.0":
        return num(0)
    m = _DECIMAL_VALUE.fullmatch(value)
    if m:
        return num(Decimal(m.group(1)))
    m = _TIMESTAMP.fullmatch(value)
    if m:
        return inst(int(m.group(1)))
    m = _DATETIME.fullmatch(value)
    if m:
        parsed = dt.datetime.fromisoformat(m.group(1))
        return inst(_aware_ns(parsed) if parsed.tzinfo else _naive_ns(parsed))
    m = _DATE.fullmatch(value)
    if m:
        return inst((dt.date.fromisoformat(m.group(1)) - dt.date(1970, 1, 1)).days * DAY_NS)
    m = _TIMEDELTA.fullmatch(value)
    if m:
        days, seconds, micros = (int(g) for g in m.groups())
        return ("dur", (days * 86_400 + seconds) * NS + micros * 1000)
    m = _PD_TIMEDELTA.fullmatch(value)
    if m:
        return ("dur", int(m.group(1)))
    return ("str", value)


# --- the expectations -------------------------------------------------------------

LITERAL_ROWS = {  # rows that show the literal, per value position
    "fill": [1, 4], "coal": [1, 4], "coal_rev": [0, 1, 2, 3, 4, 5],
    "ifelse_t": [0, 2, 4], "ifelse_f": [0, 2, 4], "shift_def": [0],
}


def expected_values(ctx, lit, pos, zone):
    """Which exact value each row must show in a value position or a shift."""
    column = context_exact(ctx)
    literal = literal_exact(lit, zone)
    if pos == "shift_def":
        return [literal] + column[:-1]
    return [literal if i in LITERAL_ROWS[pos] else column[i] for i in range(grid.ROWS)]


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


def _ok():
    return ("ok", None)


def _skip(reason):
    return ("skip", reason)


def _violation(reason):
    return ("violation", reason)


def _is_plan_value_error(outcome):
    error = outcome.get("error")
    return bool(error) and error["class"] == "ValueError" and error["stage"] == "plan"


def _expect_plan_value_error(outcome, decision):
    if _is_plan_value_error(outcome):
        return _ok()
    return _violation(f"{decision}: expected a plan-time ValueError, got {_show(outcome)}")


def _show(outcome):
    if "error" in outcome:
        e = outcome["error"]
        return f"{e['class']} at {e['stage']}: {e.get('msg', '')}"
    return f"{outcome['type']} {outcome['values']}"


def outcome_values(outcome):
    return [result_exact(v, outcome["type"]) for v in outcome["values"]]


def same(a, b):
    if a is None or b is None:
        return a is None and b is None
    return a == b


def judge(ctx, lit, pos, outcome):
    """("ok" | "skip" | "violation", reason) for the outcome of a cell."""
    ck, lk = context_kind(ctx), literal_kind(lit)
    zone = context_zone(ctx)
    ctx_type = CONTEXT_TYPE[ctx]

    # D-l: a number or Boolean next to a date or timestamp is a plan-time type
    # error in every position; `isin2` carries the int 1.
    if ck in TEMPORAL and (lk in ("int", "float", "special", "decimal", "bool") or pos == "isin2"):
        return _expect_plan_value_error(outcome, "D-l")
    # D-l, mirrored: a date or datetime next to a numeric column.
    if ck in NUMERIC and lk in ("date", "dt_naive", "dt_aware"):
        return _expect_plan_value_error(outcome, "D-l (mirrored)")
    # D-h: strings and Booleans keep DataFusion's reading.
    if ck in ("str", "bool") or lk in ("str", "bool", "none"):
        return _skip("D-h: DataFusion's reading")

    # D-j: NaN and infinity have no value in an exact domain; arithmetic
    # with them is float arithmetic.
    if lk == "special" and ck in EXACT_NUMERIC:
        if pos in grid.ARITHMETIC_POSITIONS:
            if "error" in outcome:
                return _violation(f"D-j: float arithmetic expected, got {_show(outcome)}")
            if parse_type(outcome["type"])[0] != "float":
                return _violation(f"D-j: float arithmetic expected, got {outcome['type']}")
            return _ok()
        return _expect_plan_value_error(outcome, "D-j")

    if ck == "float":
        return _judge_float_context(ctx, lit, pos, outcome)
    if ck in EXACT_NUMERIC:
        return _judge_exact_numeric(ctx, lit, pos, outcome, ctx_type)
    return _judge_temporal(ctx, lit, pos, outcome, ctx_type, zone, lk)


def _judge_float_context(ctx, lit, pos, outcome):
    """A float column keeps float semantics; a literal is the nearest float (D-b, D-i)."""
    if pos in grid.ARITHMETIC_POSITIONS:
        return _skip("float arithmetic is DataFusion's")
    if "error" in outcome:
        return _violation(f"float context: expected float semantics, got {_show(outcome)}")
    literal = literal_exact(lit)
    if pos in grid.COMPARISON_POSITIONS:
        if literal[0] == "special":
            return _skip("NaN and infinity compare as DataFusion compares them")
        got = [None if v is None else v[1] for v in outcome_values(outcome)]
        column = context_exact(ctx)
        exact = [compare(pos, x, literal) for x in column]
        if got == exact:
            return _ok()
        for bits in (64, 32):
            as_float = lambda v: num(float(v[1]) if bits == 64 else float32(float(v[1])))  # noqa: E731
            if got == [compare(pos, None if x is None else as_float(x), as_float(literal)) for x in column]:
                return _ok()
        return _violation(f"float comparison: expected {exact}, got {got}")
    if parse_type(outcome["type"])[0] != "float":
        return _violation(f"float context: expected a float result, got {outcome['type']}")
    expected = [nearest(outcome["type"], v) for v in expected_values(ctx, lit, pos, None)]
    got = outcome_values(outcome)
    bad = [(i, e, g) for i, (e, g) in enumerate(zip(expected, got)) if not same(e, g)]
    if bad:
        return _violation(f"float values: row {bad[0][0]} expected {bad[0][1]}, got {bad[0][2]}")
    return _ok()


def _judge_exact_numeric(ctx, lit, pos, outcome, ctx_type):
    """An integer or decimal column: comparisons by exact math (D-j), shared
    values exact or refused (D-i), shift defaults in the column's type (D-c)."""
    literal = literal_exact(lit)
    column = context_exact(ctx)
    if pos in grid.COMPARISON_POSITIONS:
        if "error" in outcome:
            return _violation(f"exact comparison: expected a Boolean result, got {_show(outcome)}")
        got = [None if v is None else v[1] for v in outcome_values(outcome)]
        expected = [compare(pos, x, literal) for x in column]
        if got != expected:
            return _violation(f"exact comparison: expected {expected}, got {got}")
        return _ok()
    if pos in grid.ARITHMETIC_POSITIONS:
        if "error" in outcome:
            return _skip("arithmetic overflow and widths are DataFusion's")
        result_type = outcome["type"]
        if parse_type(result_type)[0] not in ("int", "decimal"):
            return _skip("float arithmetic is DataFusion's")
        got = outcome_values(outcome)
        for i, x in enumerate(column):
            if x is None:
                continue
            expected = num(x[1] + literal[1] if pos == "add" else x[1] - literal[1])
            if holds(result_type, expected) and not same(expected, got[i]):
                return _violation(f"exact arithmetic: row {i} expected {expected[1]}, got {got[i]}")
        return _ok()
    # fill, coal, coal_rev, ifelse_t, ifelse_f, shift_def
    if "error" in outcome:
        if not _is_plan_value_error(outcome):
            return _violation(f"shared value: expected a value or a plan-time ValueError, got {_show(outcome)}")
        if holds(ctx_type, literal):
            return _violation(f"shared value: {ctx_type} holds {literal[1]} exactly, but it was refused")
        return _ok()
    if pos == "shift_def" and outcome["type"] != ctx_type:
        return _violation(f"D-c: a shift default keeps the column type {ctx_type}, got {outcome['type']}")
    expected = expected_values(ctx, lit, pos, None)
    got = outcome_values(outcome)
    bad = [(i, e, g) for i, (e, g) in enumerate(zip(expected, got)) if not same(e, g)]
    if bad:
        i, e, g = bad[0]
        return _violation(f"shared value: row {i} expected {None if e is None else e[1]}, got {None if g is None else g[1]} in {outcome['type']}")
    return _ok()


def _in_ns_range(ns):
    return I64[0] <= ns <= I64[1]


def _needs_ns(ctx_type, lit):
    """Whether DataFusion computes at nanoseconds: a nanosecond context or literal."""
    value = grid.literals()[lit]
    return parse_type(ctx_type)[:2] == ("ts", "ns") or getattr(value, "unit", None) == "ns"


def _judge_temporal(ctx, lit, pos, outcome, ctx_type, zone, lk):
    """A date or timestamp column next to a date or datetime literal."""
    ck = context_kind(ctx)
    if lk == "dt_aware" and ck == "ts":
        return _expect_plan_value_error(outcome, "aware literal next to a naive timestamp column")
    literal = literal_exact(lit, zone)
    column = context_exact(ctx)
    if pos in grid.ARITHMETIC_POSITIONS:
        return _judge_temporal_arithmetic(pos, outcome, ctx_type, lit, literal, column)
    if pos in grid.COMPARISON_POSITIONS:
        if "error" in outcome:
            return _violation(f"instant comparison: expected a Boolean result, got {_show(outcome)}")
        got = [None if v is None else v[1] for v in outcome_values(outcome)]
        expected = [compare(pos, x, literal) for x in column]
        if got != expected:
            return _violation(f"instant comparison: expected {expected}, got {got}")
        return _ok()
    if pos == "dtdiff":
        if "error" in outcome:
            overflow = not _in_ns_range(literal[1]) or any(x is not None and not _in_ns_range(x[1]) for x in column)
            if _needs_ns(ctx_type, lit) and overflow:
                return _skip("dt.diff at nanoseconds with a value outside their range is DataFusion's error")
            return _violation(f"dt.diff: expected elapsed seconds, got {_show(outcome)}")
        got = outcome["values"]
        for i, x in enumerate(column):
            if x is None:
                if got[i] is not None:
                    return _violation(f"dt.diff: row {i} expected NULL, got {got[i]}")
                continue
            expected = (x[1] - literal[1]) / NS
            if not isinstance(got[i], (int, float)) or not math.isclose(got[i], expected, rel_tol=1e-9, abs_tol=1e-6):
                return _violation(f"dt.diff: row {i} expected {expected}, got {got[i]}")
        return _ok()
    # fill, coal, coal_rev, ifelse_t, ifelse_f, shift_def
    held = holds(ctx_type, literal)
    widened = pos != "shift_def" and any(holds(finer, literal) for finer in finer_units(ctx_type))
    if "error" in outcome:
        if not _is_plan_value_error(outcome):
            return _violation(f"temporal value: expected a value or a plan-time ValueError, got {_show(outcome)}")
        if held:
            return _violation(f"temporal value: {ctx_type} holds the literal exactly, but it was refused")
        if widened:
            return _violation(f"D-m: a finer unit of {ctx_type} holds the literal, but it was refused")
        return _ok()
    result_type = outcome["type"]
    if held and result_type != ctx_type:
        return _violation(f"temporal value: {ctx_type} holds the literal, but the result is {result_type}")
    if not held and result_type not in finer_units(ctx_type):
        return _violation(f"D-m: expected a finer unit of {ctx_type} in its zone, got {result_type}")
    expected = expected_values(ctx, lit, pos, zone)
    got = outcome_values(outcome)
    bad = [(i, e, g) for i, (e, g) in enumerate(zip(expected, got)) if not same(e, g)]
    if bad:
        i, e, g = bad[0]
        return _violation(f"temporal value: row {i} expected {e}, got {g} in {result_type}")
    return _ok()


def _judge_temporal_arithmetic(pos, outcome, ctx_type, lit, literal, column):
    """`add` of two instants is DataFusion's error; `sub` is the exact
    difference, as a duration or (date minus date) in days, computed at
    nanoseconds when DataFusion unifies there."""
    if pos == "add":
        return _skip("an instant plus an instant is DataFusion's error")
    overflow = not _in_ns_range(literal[1]) or any(x is not None and not _in_ns_range(x[1]) for x in column)
    if "error" in outcome:
        if overflow:
            return _skip("subtraction at nanoseconds with a value outside their range is DataFusion's error")
        return _violation(f"temporal sub: expected a duration, got {_show(outcome)}")
    days = parse_type(outcome["type"])[0] == "int"
    got = outcome_values(outcome)
    for i, x in enumerate(column):
        if x is None:
            if got[i] is not None:
                return _violation(f"temporal sub: row {i} expected NULL, got {got[i]}")
            continue
        diff = x[1] - literal[1]
        expected = num(Fraction(diff, DAY_NS)) if days else ("dur", diff)
        if not same(expected, got[i]):
            return _violation(f"temporal sub: row {i} expected {expected}, got {got[i]} in {outcome['type']}")
    return _ok()
