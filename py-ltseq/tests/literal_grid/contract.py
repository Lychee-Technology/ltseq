"""The literal grid graded by the v0.5 contract (#275), independent of ltseq and DataFusion.

``oracle.py`` grades a cell by the decisions of ADR 0018 (D-b, D-i to D-m)
as the code on ``main`` implements them. v0.5 revises several of them: a
literal never widens a column's type ([§17.5] rule 1), integer results take
the types of [§17.3] and are checked ([§17.1]), a naive datetime next to a
zoned column is a type error ([§19.2]), and errors have one of the fifteen
classes of [§20.1] at a stage of [§20.2]. This module says what v0.5
requires of each cell, and each requirement is a ``Rule`` that names the one
contract section it rests on.

A cell's v0.5 expectation lists the rules it rests on. The gate grades the
cell by it only when every one of those rules is in ``IN_FORCE``; otherwise
``oracle.py`` grades it by ADR 0018 as before. A rule enters ``IN_FORCE`` in
the change that brings the build to it (the ``owner`` M item of
``docs/proposals/v0.5/review/impact-map.md``, or a rule the build already
meets), so each M item's gate checks the cells it changes against v0.5 and
the cells it does not change against what ``main`` does today.

One rule is about errors rather than cells. Until ``error_classes``
([§20.1], M15) is in force, a v0.5 refusal that ADR 0018's oracle expects
too keeps that oracle's class, stage and cause (a ``ValueError`` alone
cannot tell a justified refusal from any other), and one it does not
expect accepts the v0.5 class or the one ltseq raises today for it
(``PRE_M15``). Once it is in force, the classes change on every cell,
not only on those whose rules are in force: M15 lands before M8, so a cell
ADR 0018 still grades keeps its semantics and takes v0.5's class for its
refusal (``_classes_only``). Stages map onto the grid's: v0.5's plan stage
is the grid's ``capture`` or ``plan``, and execution is ``collect``.

    PYTHONPATH=py-ltseq/tests python -m literal_grid.contract RUN.jsonl [--classes today] [--cells RULE]

grades a recorded run (``grid.py --commit``) by every rule at once, counts
the cells that meet v0.5 and those that do not yet per set of rules, and
lists the rules the run could put in force: the work each M item has left
on the grid, and the classification run its gate rests on.
"""

import sys
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Optional

from . import grid, oracle
from .oracle import Expectation


@dataclass(frozen=True)
class Rule:
    name: str
    section: str
    owner: Optional[str]  # the impact-map item whose scope covers the rule; None when no item names it
    note: str


_RULES = [
    # --- kinds and the types of literals ---
    Rule("number_with_temporal", "§19.2", "M20",
         "A number or bool next to a date or timestamp, on either side, raises LTSeqTypeError at plan time; "
         "an integer factor or divisor of a duration is the only exception, and the grid has no duration context"),
    Rule("bool_column_with_temporal", "§19.2", None,
         "A bool column next to a date or datetime literal raises LTSeqTypeError at plan time; the build fails in "
         "DataFusion while collecting"),
    Rule("no_implicit_kind_change", "§17.4 rule 4", None,
         "A number or bool next to a string, a bool next to a number, a string next to a number or temporal "
         "value: LTSeqTypeError at plan time. No string is parsed, and no bool is a number"),
    Rule("aware_literal_naive_column", "§19.2", "M8",
         "An aware datetime next to a naive timestamp column raises LTSeqTypeError"),
    Rule("naive_literal_aware_column", "§19.2", "M8",
         "A naive datetime or a date next to an aware column, and an aware datetime next to a date column, "
         "raise LTSeqTypeError; the v0.4 reading as wall-clock time or local midnight in the column's zone is gone"),
    Rule("date_timestamp_arithmetic", "§19.2", "M20",
         "A date and a timestamp have no arithmetic: `date - timestamp` and `timestamp - date` raise LTSeqTypeError"),
    Rule("date_timestamp_shared", "§17.5", "M8",
         "A date and a timestamp never share a column, as shared values or into a fixed target: LTSeqTypeError"),
    Rule("context_date_ts", "§17.5", "M8",
         "`if_else(c, r.ts_us, r.date32)` makes a date and a timestamp share a column, so every cell over it raises"),
    Rule("date64_read_as_date32", "§6.2", "M18",
         "A date64 column is date32 when read, so a value it shares has type date32"),
    # --- comparisons ---
    Rule("none_is_null", "§8.2", None, "`x == None` is `x.is_null()`"),
    Rule("none_ordering", "§8.2", None, "An ordering comparison with `None` raises LTSeqTypeError"),
    Rule("isin_never_null", "§8.5", "M7",
         "`is_in` never returns NULL: a NULL input is TRUE when None is among the values and FALSE otherwise"),
    Rule("nan_literal_exact_domain", "§17.4 rule 3", "M7",
         "A NaN or infinite float literal compared with an integer or decimal operand raises LTSeqValueError at plan time"),
    Rule("exact_comparison", "§17.4 rule 3", "M7",
         "An integer or decimal column compares with a number literal by exact value"),
    Rule("float_comparison_exact", "§17.4 rule 3", "M7",
         "A float column compares with an integer or Decimal literal by exact value, never through the nearest float"),
    Rule("float_order", "§18", "M7",
         "A float column against a float literal: binary64 values, NaN equal to NaN and above +inf, -0.0 equal to 0.0"),
    Rule("temporal_comparison", "§19.2", "M8",
         "Dates and timestamps compare exactly across units, aware values by instant, a date with a naive timestamp at midnight"),
    Rule("string_comparison", "§8.2", None, "Strings compare with strings by code point"),
    Rule("bool_comparison", "§8.2", None,
         "bools compare with bools for equality; no section orders them, so `>` has no expectation"),
    # --- arithmetic ---
    Rule("integer_result_types", "§17.3", "M5",
         "Integer `+` and `-` take the common type of §17.4, int64 for an int literal beside any narrower or 32-bit "
         "type, decimal128(21, 0) beside uint64; the result is checked"),
    Rule("decimal_result_types", "§17.3", "M5",
         "Decimal `+` and `-` take decimal(min(max(p1 - s1, p2 - s2) + s + 1, P), s), an integer counting as "
         "decimal(3/5/10/20, 0); the result is checked"),
    Rule("checked_arithmetic", "§17.1", "M5",
         "An integer or decimal result that does not fit its type raises ArithmeticOverflowError during execution; "
         "nothing wraps"),
    Rule("float_arithmetic", "§17.4", "M7",
         "An integer column with a float literal, or a float64 column with a number literal, is float64 arithmetic on "
         "the nearest float64 of each operand"),
    Rule("float32_widened", "§17.4", "M7",
         "A float32 column with an int or Decimal literal is float64 arithmetic: an integer with any float gives float64"),
    Rule("decimal_with_float", "§17.4 rule 1", "M7",
         "A decimal with a float is float64 arithmetic on the nearest float64 of the decimal"),
    Rule("string_arithmetic", "§8.2", None,
         "`str + str` concatenates; `str - str` has no rule and raises LTSeqTypeError"),
    Rule("bool_not_number", "§18", None, "bools are not numbers: `bool + bool` raises LTSeqTypeError"),
    Rule("null_operand", "§18", None,
         "Arithmetic with a NULL literal is NULL on every row; §17.3 gives no type for a `null` operand, so any "
         "result type, or LTSeqTypeError at plan time, is accepted"),
    Rule("temporal_sum", "§19.2", "M20", "An instant plus an instant raises LTSeqTypeError at plan time"),
    Rule("date_difference", "§19.2", "M20", "`date - date` is duration[s]"),
    Rule("timestamp_difference", "§19.2", "M8",
         "`timestamp - timestamp` is a duration at the finer unit, of instants for aware operands; an operand or "
         "difference that unit cannot hold raises ArithmeticOverflowError during execution"),
    # --- shared values and fixed targets ---
    Rule("shared_value_no_widening", "§17.5", "M8",
         "A literal among shared values never widens the column's type: it is exact in that type, or CastError at plan time"),
    Rule("temporal_shared_value", "§17.5", "M8",
         "A temporal literal among shared values keeps the column's unit and zone, contributes its instant, and is exact "
         "at that unit or raises CastError at plan time"),
    Rule("fixed_target", "§17.5", "M8",
         "A shift default goes into the column's type exactly, or raises CastError at plan time"),
    Rule("fixed_target_float", "§17.5", "M8",
         "A shift default goes into a float column exactly too: an int or Decimal the float cannot hold raises "
         "CastError instead of rounding"),
    Rule("same_kind_shared_value", "§17.5", None, "A string among strings, or a bool among bools, keeps the column's type"),
    Rule("null_shared_value", "§17.6", None, "A NULL literal becomes a NULL of the column's type (the `null` row)"),
    # --- removed API, errors ---
    Rule("dt_diff_removed", "§19.4", "M4",
         "`dt.diff` is removed (M4 and M20 both list it); an unknown accessor method raises AttributeError (§8.7)"),
    Rule("error_classes", "§20.1", "M15", "Errors have the v0.5 classes, not the ones the build raises today"),
]
RULES = {rule.name: rule for rule in _RULES}

# The rules the gate grades by. A rule enters in the change that makes the
# build meet it on every cell it governs (whose other rules are in force too);
# `python -m literal_grid.contract RUN.jsonl --classes today` lists the rules a
# recorded run could put in force. Those below are the ones the build of
# b8456b9 already meets; the gate in `test_literal_grid.py` holds it to them.
IN_FORCE = frozenset({
    "aware_literal_naive_column",
    "bool_comparison",
    "bool_not_number",
    "exact_comparison",
    "fixed_target",
    "float_arithmetic",
    "float_order",
    "nan_literal_exact_domain",
    "none_is_null",
    "null_operand",
    "null_shared_value",
    "number_with_temporal",
    "same_kind_shared_value",
    "string_comparison",
    "temporal_comparison",
    "temporal_sum",
    "timestamp_difference",
})

PLAN = ("capture", "plan")
EXECUTION = ("collect",)

# The class the build raises today for what v0.5 calls each class: ltseq's own
# refusals are ValueErrors, DataFusion's planning errors RuntimeErrors, and a
# failure while collecting a ValueError.
PRE_M15 = {
    "LTSeqTypeError": ("ValueError", "RuntimeError", "TypeError"),
    "LTSeqValueError": ("ValueError",),
    "CastError": ("ValueError",),
    "ArithmeticOverflowError": ("ValueError",),
    "AttributeError": ("AttributeError",),
}


# --- the types v0.5 gives the operands ------------------------------------------------


def literal_type(lit):
    """The literal's type by the table of §17.2, parsed as ``oracle.parse_type`` parses."""
    value = grid.literals()[lit]
    kind = oracle.literal_kind(lit)
    if kind == "none":
        return ("null",)
    if kind == "bool":
        return ("bool",)
    if kind == "int":
        return ("int", 64, True) if value < 2**63 else ("int", 64, False)
    if kind in ("float", "special"):
        return ("float", 64)
    if kind == "decimal":
        return oracle.literal_wire(lit)
    if kind == "str":
        return ("str",)
    if kind == "date":
        return ("date", 32)
    if type(value).__name__ == "Timestamp":  # pandas: the value's unit and zone
        return ("ts", value.unit, None if value.tz is None else str(value.tz))
    if value.tzinfo is None:
        return ("ts", "us", None)
    return ("ts", "us", getattr(value.tzinfo, "key", None) or "UTC")


def context_type(ctx, in_force):
    """The context's type as v0.5 reads it: a date64 column is date32 (§6.2)."""
    t = oracle.parse_type(oracle.CONTEXT_TYPE[ctx])
    if t == ("date", 64) and "date64_read_as_date32" in in_force:
        return ("date", 32)
    return t


def _kind(t):
    """number | bool | str | date | naive | aware | null"""
    if t[0] in ("int", "float", "decimal"):
        return "number"
    if t[0] == "ts":
        return "naive" if t[2] is None else "aware"
    return t[0]


TEMPORAL_KINDS = {"date", "naive", "aware"}
_INT_DECIMAL_DIGITS = {8: 3, 16: 5, 32: 10, 64: 20}


def _int_common(a, b):
    """The smallest integer type holding both (§17.3), or decimal128(20, 0) for a signed type with uint64."""
    if a == b:
        return a
    if a[2] == b[2]:
        return a if a[1] >= b[1] else b
    signed, unsigned = (a, b) if a[2] else (b, a)
    if signed[1] > unsigned[1]:
        return signed
    if unsigned[1] < 64:
        return ("int", unsigned[1] * 2, True)
    return ("decimal", 20, 0, 128)


def _as_decimal(t):
    """An integer operand as the decimal of its width (§17.3); a decimal32/64 as decimal128 (§17.4 rule 2)."""
    if t[0] == "int":
        return ("decimal", _INT_DECIMAL_DIGITS[t[1]], 0, 128)
    return ("decimal", t[1], t[2], max(t[3], 128))


def _decimal_sum_type(a, b):
    a, b = _as_decimal(a), _as_decimal(b)
    width = 256 if 256 in (a[3], b[3]) else 128
    cap = 76 if width == 256 else 38
    scale = max(a[2], b[2])
    return ("decimal", min(max(a[1] - a[2], b[1] - b[2]) + scale + 1, cap), scale, width)


# --- building expectations ------------------------------------------------------------


def _reason(rules):
    return "; ".join(f"{RULES[name].section} ({name})" for name in rules)


def _values(t, rows, *rules):
    rows = tuple(rows)
    assert len(rows) == grid.ROWS, (rules, rows)
    return Expectation("values", _reason(rules), type=t, rows=rows, rules=tuple(rules))


def _error(cls, stage, *rules):
    return Expectation("error", _reason(rules), stage=stage, cls=(cls,), rules=tuple(rules))


def _skip(*rules):
    return Expectation("skip", _reason(rules), rules=tuple(rules))


def _bools(flags):
    return [None if flag is None else ("bool", bool(flag)) for flag in flags]


def _key(value):
    """The order of §18: -inf < numbers < +inf < NaN, -0.0 equal to 0.0 (a Fraction has no sign of zero)."""
    tag, v = value
    if tag == "special":
        return {"-inf": (0,), "inf": (2,), "NaN": (3,)}[v]
    return (1, v)  # numbers, and instants, strings or bools, each compared within its kind


def _equal(x, y):
    return _key(x) == _key(y)


def _greater(x, y):
    a, b = _key(x), _key(y)
    if a[0] != b[0]:
        return a[0] > b[0]
    return len(a) > 1 and a[1] > b[1]


def _compare(pos, column, literal):
    """The comparison of each row with the literal; `is_in` never returns NULL (§8.5)."""
    one = oracle.num(1)
    rows = []
    for x in column:
        if x is None:
            rows.append(False if pos in ("isin", "isin2") else None)
        elif pos in ("eq", "isin"):
            rows.append(_equal(x, literal))
        elif pos == "isin2":
            rows.append(_equal(x, literal) or _equal(x, one))
        else:  # gt, and lt_mirror: `L < x` is `x > L`
            rows.append(_greater(x, literal))
    return _bools(rows)


# --- the expectation of a cell ----------------------------------------------------------


def expectation(ctx, lit, pos, in_force=None):
    """What v0.5 requires of the cell, and the rules that requirement rests on.

    ``in_force`` matters only where a rule changes a type another rule uses
    (a date64 column read as date32); it defaults to ``IN_FORCE``."""
    in_force = IN_FORCE if in_force is None else in_force
    if ctx == "case_date_ts":
        return _error("LTSeqTypeError", PLAN, "context_date_ts")
    if pos == "dtdiff":
        return _error("AttributeError", PLAN, "dt_diff_removed")
    t, lt = context_type(ctx, in_force), literal_type(lit)
    ck, lk = _kind(t), _kind(lt)
    # Every value of an `is_in` list must suit the column (§8.5), the int 1 of
    # `isin2` included, before a None among them decides anything.
    if pos == "isin2" and ck != "number":
        return _error("LTSeqTypeError", PLAN, "number_with_temporal" if ck in TEMPORAL_KINDS else "no_implicit_kind_change")
    if lk == "null":
        return _null(ctx, t, pos)
    mismatch = _kind_mismatch(ck, lk, pos)
    if mismatch:
        return _error("LTSeqTypeError", PLAN, mismatch)
    if pos in grid.COMPARISON_POSITIONS:
        return _comparison(ctx, lit, pos, t, lt)
    if pos in grid.ARITHMETIC_POSITIONS:
        return _arithmetic(ctx, lit, pos, t, lt)
    return _shared(ctx, lit, pos, t, lt)


def _kind_mismatch(ck, lk, pos):
    """The rule a pair of kinds breaks in this position, or None when the pair has a rule."""
    if ck == lk:
        return None
    pair = {ck, lk}
    if ck == "bool" and lk in TEMPORAL_KINDS:
        return "bool_column_with_temporal"
    if pair & {"number", "bool"} and pair & TEMPORAL_KINDS:
        return "number_with_temporal"
    if "str" in pair or pair == {"number", "bool"}:
        return "no_implicit_kind_change"
    if ck == "naive" and lk == "aware":
        return "aware_literal_naive_column"
    if "aware" in pair:  # a naive literal or a date beside an aware column, an aware literal beside a date
        return "naive_literal_aware_column"
    # a date with a naive timestamp: compared at midnight, never computed or shared
    if pos in grid.COMPARISON_POSITIONS:
        return None
    return "date_timestamp_arithmetic" if pos in grid.ARITHMETIC_POSITIONS else "date_timestamp_shared"


def _null(ctx, t, pos):
    column = oracle.context_exact(ctx)
    if pos == "eq":
        return _values(("bool",), _bools([x is None for x in column]), "none_is_null")
    if pos in ("gt", "lt_mirror"):
        return _error("LTSeqTypeError", PLAN, "none_ordering")
    if pos == "isin":
        return _values(("bool",), _bools([x is None for x in column]), "isin_never_null")
    if pos == "isin2":
        rows = [x is None or _equal(x, oracle.num(1)) for x in column]
        return _values(("bool",), _bools(rows), "isin_never_null")
    if pos in grid.ARITHMETIC_POSITIONS:
        return Expectation(
            "null_arithmetic", _reason(["null_operand"]), stage=PLAN, cls=("LTSeqTypeError",), rules=("null_operand",),
        )
    rules = ["null_shared_value"] + (["date64_read_as_date32"] if t[0] == "date" and ctx == "date64" else [])
    if pos == "shift_def":
        rows = [None] + column[:-1]
    elif pos in ("ifelse_t", "ifelse_f"):
        rows = [None if i in oracle.LITERAL_ROWS[pos] else column[i] for i in range(grid.ROWS)]
    else:  # a NULL filled with NULL, or coalesced with it in either order, is the column
        rows = column
    return _values(t, rows, *rules)


def _comparison(ctx, lit, pos, t, lt):
    ck, lk = _kind(t), oracle.literal_kind(lit)
    column = oracle.context_exact(ctx)
    literal = oracle.literal_exact(lit)
    isin = ["isin_never_null"] if pos in ("isin", "isin2") else []
    if ck == "number":
        if lk == "special" and t[0] != "float":
            return _error("LTSeqValueError", PLAN, "nan_literal_exact_domain")
        if t[0] != "float":
            rule = "exact_comparison"
        elif lk in ("int", "decimal"):
            rule = "float_comparison_exact"
        else:
            rule = "float_order"
        return _values(("bool",), _compare(pos, column, literal), rule, *isin)
    if ck == "str":
        return _values(("bool",), _compare(pos, column, literal), "string_comparison", *isin)
    if ck == "bool":
        if pos in ("gt", "lt_mirror"):
            return _skip("bool_comparison")
        return _values(("bool",), _compare(pos, column, literal), "bool_comparison", *isin)
    return _values(("bool",), _compare(pos, column, literal), "temporal_comparison", *isin)


def _float_operand(value):
    """An operand of float arithmetic: the nearest float64 of its exact value (§17.4)."""
    return value if value[0] == "special" else oracle.num(oracle.to_float(value[1], 64))


def _arithmetic(ctx, lit, pos, t, lt):
    ck = _kind(t)
    column = oracle.context_exact(ctx)
    literal = oracle.literal_exact(lit)
    if ck == "str":
        if pos == "sub":
            return _error("LTSeqTypeError", PLAN, "string_arithmetic")
        return _values(("str",), [None if x is None else ("str", x[1] + literal[1]) for x in column], "string_arithmetic")
    if ck == "bool":
        return _error("LTSeqTypeError", PLAN, "bool_not_number")
    if ck in TEMPORAL_KINDS:
        if pos == "add":
            return _error("LTSeqTypeError", PLAN, "temporal_sum")
        return _difference(ctx, t, lt, column, literal)
    sign = 1 if pos == "add" else -1
    if t[0] == "float" or lt[0] == "float":
        if t[0] == "decimal":
            rule = "decimal_with_float"
        elif t == ("float", 32) and lt[0] != "float":
            rule = "float32_widened"
        else:
            rule = "float_arithmetic"
        rows = []
        read = _float_operand(literal)
        for x in column:
            if x is None:
                rows.append(None)
            elif read[0] == "special":
                rows.append(oracle._special_result(read[1], pos))
            else:
                rows.append(oracle._float_value(_float_operand(x)[1] + sign * read[1], 64))
        return _values(("float", 64), rows, rule)
    if t[0] == "int" and lt[0] == "int":
        result, rule = _int_common(t, lt), "integer_result_types"
        if result[0] == "decimal":  # a signed type with uint64: decimal128(20, 0) operands
            result = _decimal_sum_type(result, result)
    else:
        result, rule = _decimal_sum_type(t, lt), "decimal_result_types"
    rows = [None if x is None else oracle.num(x[1] + sign * literal[1]) for x in column]
    if any(not oracle.holds(result, v) for v in rows):
        return _error("ArithmeticOverflowError", EXECUTION, rule, "checked_arithmetic")
    return _values(result, rows, rule, "checked_arithmetic")


def _difference(ctx, t, lt, column, literal):
    """`date - date` is duration[s]; `timestamp - timestamp` is a duration at the finer unit, checked."""
    if t[0] == "date":
        rows = [None if x is None else ("dur", x[1] - literal[1]) for x in column]
        rules = ["date_difference"] + (["date64_read_as_date32"] if ctx == "date64" else [])
        return _values(("dur", "s"), rows, *rules)
    unit = oracle._finer(t[1], lt[1])
    ticks = oracle.TICKS[unit]
    operands = [literal] + [x for x in column if x is not None]
    differences = [x[1] - literal[1] for x in column if x is not None]
    if any(not oracle._representable(v[1], ticks) for v in operands) or any(not oracle._representable(d, ticks) for d in differences):
        return _error("ArithmeticOverflowError", EXECUTION, "timestamp_difference")
    rows = [None if x is None else ("dur", x[1] - literal[1]) for x in column]
    return _values(("dur", unit), rows, "timestamp_difference")


def _shared(ctx, lit, pos, t, lt):
    ck = _kind(t)
    column = oracle.context_exact(ctx)
    literal = oracle.literal_exact(lit)
    if ck in ("str", "bool"):
        return _values(t, oracle._placed(column, literal, pos), "same_kind_shared_value")
    if pos == "shift_def":
        rule = "fixed_target_float" if t[0] == "float" else "fixed_target"
    else:
        rule = "temporal_shared_value" if ck in TEMPORAL_KINDS else "shared_value_no_widening"
    rules = [rule] + (["date64_read_as_date32"] if ctx == "date64" else [])
    if not oracle.holds(t, literal):
        return _error("CastError", PLAN, *rules)
    return _values(t, oracle._placed(column, literal, pos), *rules)


# --- grading in force, and everything at once -------------------------------------------


def in_force(e, rules=None):
    rules = IN_FORCE if rules is None else rules
    return all(name in rules for name in e.rules)


def for_the_gate(ctx, lit, pos, legacy):
    """The v0.5 expectation when every rule it rests on is in force, adapted to the
    build's error classes until M15; None when ADR 0018's still grades the cell.
    Once M15 is in force, a cell ADR 0018 still grades takes v0.5's error classes too
    (``_classes_only``), since M15 lands before the items that change those cells."""
    e = expectation(ctx, lit, pos)
    if not in_force(e):
        return _classes_only(e, legacy()) if "error_classes" in IN_FORCE else None
    return _adapted(e, legacy, IN_FORCE)


# What v0.5 calls each class the build raises today: the inverse of PRE_M15.
V05_CLASSES = {
    today: tuple(cls for cls, classes in PRE_M15.items() if today in classes)
    for today in dict.fromkeys(c for classes in PRE_M15.values() for c in classes)
}


def _classes_only(e, before):
    """ADR 0018's expectation for a cell whose v0.5 rules are not all in force, with
    M15's classes (§20.1): its semantics stay, its refusals change class. A refusal
    v0.5 makes too, at the same stage, takes v0.5's class and stage; any other keeps
    its stage and accepts each v0.5 class of the class it has today. Messages stop
    counting either way (§20.1)."""
    if before.kind not in ("error", "null_arithmetic"):
        return before
    rules = ("error_classes",)
    reason = f"{before.reason}; {_reason(rules)}"
    if e.kind == before.kind and before.stage in e.stage:
        return replace(before, reason=reason, stage=e.stage, cls=e.cls, msg=None, rules=rules)
    classes = tuple(dict.fromkeys(v for c in before.cls for v in V05_CLASSES.get(c, (c,))))
    return replace(before, reason=reason, stage=(before.stage,), cls=classes, msg=None, rules=rules)


def _adapted(e, legacy, rules):
    """Before M15, a v0.5 refusal that ADR 0018's oracle also expects keeps that oracle's
    class, stage and cause, so a rule in force never loosens the gate; a refusal that
    oracle does not expect accepts the v0.5 class or the one ltseq raises today."""
    if e.kind not in ("error", "null_arithmetic") or "error_classes" in rules:
        return e
    classes = tuple(dict.fromkeys(c for cls in e.cls for c in (cls,) + PRE_M15[cls]))
    before = legacy()
    if before.kind == e.kind and before.stage in e.stage and set(before.cls) <= set(classes):
        return replace(e, stage=before.stage, cls=before.cls, msg=before.msg)
    return replace(e, cls=classes)


ALL = frozenset(RULES)


def graded(ctx, lit, pos, classes="v0.5"):
    """The cell's v0.5 expectation with every rule in force; with ``classes="today"``,
    errors are adapted to the build's classes as the gate adapts them before M15."""
    e = expectation(ctx, lit, pos, in_force=ALL)
    if classes == "today":
        return _adapted(e, lambda: oracle.legacy_expectation(ctx, lit, pos), ALL - {"error_classes"})
    return e


def report(outcomes, classes="v0.5"):
    """``{rules: (cells that meet v0.5, cells that do not yet)}`` for a recorded run,
    keyed by the set of rules each cell's expectation rests on. A cell that fails
    counts against its whole set: the oracle cannot tell which of them it breaks."""
    groups = {}
    for ctx, lit, pos in grid.cells():
        cid = grid.cell_id(ctx, lit, pos)
        e = graded(ctx, lit, pos, classes)
        verdict, reason = oracle.judge_by(e, ctx, outcomes[cid])
        met, unmet = groups.setdefault(e.rules, ([], []))
        (unmet if verdict == "violation" else met).append((cid, reason))
    return groups


def ready(groups):
    """A maximal set of rules that could be in force on this run: every cell whose
    rules all lie in the set meets v0.5. A cell that also rests on a rule outside the
    set stays with ADR 0018, so it does not hold a rule back; but a rule must grade
    some cell of its own, or it would be in force without the build meeting it.

    The set grows by whole rule sets of the report, not rule by rule: rules whose
    cells all rest on them together (``integer_result_types`` and
    ``checked_arithmetic``) can only enter together."""
    sets = {frozenset(rules): bool(unmet) for rules, (_, unmet) in groups.items()}

    def meets(candidate):
        return not any(failing for rules, failing in sets.items() if rules <= candidate)

    chosen, grown = frozenset(), True
    while grown:
        grown = False
        for rules in sorted(sets, key=lambda r: (len(r - chosen), sorted(r))):
            if not rules <= chosen and meets(chosen | rules):
                chosen, grown = chosen | rules, True
    return sorted(chosen)


def main(argv):
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("run", help="a recorded run, such as expected/main.jsonl")
    parser.add_argument("--classes", choices=["v0.5", "today"], default="v0.5",
                        help="grade errors by v0.5's classes, or by the classes the build raises before M15")
    parser.add_argument("--cells", help="list the failing cells of the rule sets that include this rule")
    args = parser.parse_args(argv)
    _, outcomes = grid.load(Path(args.run))
    groups = report(outcomes, args.classes)
    print(f"{'meet':>6s} {'not yet':>8s}  rules (section, owner; * in force)")
    for rules, (met, unmet) in sorted(groups.items(), key=lambda item: (-len(item[1][1]), item[0])):
        names = ", ".join(f"{n}{'*' if n in IN_FORCE else ''} ({RULES[n].section}, {RULES[n].owner or '-'})" for n in rules)
        print(f"{len(met):6d} {len(unmet):8d}  {names}")
    print(f"\ncould be in force: {', '.join(ready(groups))}")
    if args.cells:
        for rules, (_, unmet) in groups.items():
            if args.cells in rules:
                for cid, reason in unmet:
                    print(f"\n{cid}\n  {reason}\n  {oracle._show(outcomes[cid])[:300]}")


if __name__ == "__main__":
    main(sys.argv[1:])
