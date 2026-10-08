"""How each grid cell's outcome on this branch relates to main's (#243).

A cell whose outcome equals main's recorded one is UNCHANGED unless the
oracle says the shared outcome contradicts a decision; then a
PREEXISTING_BUG rule must name the issue that tracks it. A cell whose
outcome differs needs a rule from ``RULES``: the first rule whose ``when``
holds gives the status and the decision, issue or review finding it rests
on. A rule's status is fixed, or ``BY_MAIN``: BUG_FIXED when main returned
values the oracle's expectation for the cell says are wrong (a lost column
value, a rounded literal, a Boolean from a truncated comparison, a missing
row), INTENDED_CHANGE when main errored or was exact in another type.

The oracle has the last word on this branch's outcome: a changed cell it
calls a violation is a REGRESSION whatever rule matches, and one it calls
ok is accepted by its rule. A rule's ``expect`` is a further condition on
this branch's outcome that must hold whenever the rule matches; it is the
only check on a cell the oracle has no opinion on (a string or Boolean
reading), so such a cell under a rule without one is UNDECIDED. A changed
cell no rule covers is a REGRESSION when the oracle objects and UNDECIDED
otherwise. The merge gate in ``test_literal_grid.py`` fails every
REGRESSION and UNDECIDED cell.

Compare two recorded runs without ltseq installed:

    PYTHONPATH=py-ltseq/tests python -m literal_grid.classification \\
        py-ltseq/tests/literal_grid/expected/main.jsonl HEAD.jsonl [--rules] [--cells STATUS]
"""

import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from decimal import Decimal, InvalidOperation
from typing import Callable, Optional

from . import grid, oracle

STATUSES = {"UNCHANGED", "INTENDED_CHANGE", "BUG_FIXED", "PREEXISTING_BUG", "REGRESSION", "UNDECIDED"}
BY_MAIN = "BY_MAIN"
TEMPORAL_LITERALS = {"date", "dt_naive", "dt_aware"}
TYPED = "#145 (typed literals)"


@dataclass(frozen=True)
class Cell:
    ctx: str
    lit: str
    pos: str
    ck: str
    lk: str
    main: dict
    head: dict
    verdict: tuple
    same: bool

    @property
    def id(self):
        return grid.cell_id(self.ctx, self.lit, self.pos)

    @property
    def ctx_type(self):
        return oracle.CONTEXT_TYPE[self.ctx]


def cell(ctx, lit, pos, main, head):
    return Cell(
        ctx, lit, pos, oracle.context_kind(ctx), oracle.literal_kind(lit), main, head,
        oracle.judge(ctx, lit, pos, head), grid.comparable(main) == grid.comparable(head),
    )


@dataclass(frozen=True)
class Rule:
    """``when`` selects the cells; ``expect``, if given, must hold on this branch's
    outcome for every cell the rule classifies, over and above the oracle's verdict."""

    name: str
    status: str
    ref: str
    note: str
    when: Callable[[Cell], bool]
    expect: Optional[Callable[[Cell], bool]] = None


@dataclass(frozen=True)
class Classification:
    status: str
    rule: Optional[str]
    reason: str


# --- reading outcomes ----------------------------------------------------------


def _error(outcome, stage=None, cls=None, msg=None):
    e = outcome.get("error")
    if not e:
        return False
    if stage and e["stage"] != stage:
        return False
    if cls and e["class"] != cls:
        return False
    return not msg or re.search(msg, e.get("msg", "")) is not None


def _plan_value_error(outcome, msg=None):
    return _error(outcome, "plan", "ValueError", msg)


def _collect_error(outcome, msg=None):
    return _error(outcome, "collect", "ValueError", msg)


def _has_values(outcome):
    return "error" not in outcome


def _head_values(c, values):
    return _has_values(c.head) and c.head["values"] == values


def main_wrong(c):
    """Main returned values the oracle says are wrong: neither the cell's exact reading
    nor the expectation's rows, a missing row, or a value its type cannot render. Main's
    type is not held against it: exact values in another type are an intended change,
    and so is a value this branch rounds (an int in a float column) where main was exact."""
    if not _has_values(c.main):
        return False
    e = oracle.expectation(c.ctx, c.lit, c.pos)
    accepted = [rows for rows in (oracle.exact_rows(c.ctx, c.lit, c.pos), e.rows if e.kind == "values" else None) if rows is not None]
    if not accepted:
        return False
    try:
        got = oracle.outcome_values(c.main)
    except ValueError:
        return True
    return not any(len(got) == len(rows) and all(oracle.same(w, g) for w, g in zip(rows, got)) for rows in accepted)


def _in_list_with_string(c):
    """DataFusion's reading of ``is_in([text, 1])`` once ``1`` has the column's type:
    the text as that type where it holds, else its cast fails at collect."""
    try:
        parsed = oracle.num(Decimal(grid.literals()[c.lit]))
    except InvalidOperation:
        return _collect_error(c.head)
    if not oracle.holds(c.ctx_type, parsed):
        return _collect_error(c.head)
    column = oracle.context_exact(c.ctx)
    expected = [None if x is None else (x[1] == parsed[1] or x[1] == 1) for x in column]
    return _head_values(c, expected)


# --- the rules, first match wins -------------------------------------------------


def _is(**kinds):
    def when(c):
        for attr, allowed in kinds.items():
            value = getattr(c, attr)
            if value not in (allowed if isinstance(allowed, (set, list, tuple)) else {allowed}):
                return False
        return True

    return when


def _all(*preds):
    return lambda c: all(p(c) for p in preds)


RULES = [
    Rule(
        "negative_zero_in_list", "PREEXISTING_BUG", "#245",
        "DataFusion's static in-list filter hashes floats bitwise, so 0.0 in a CASE-derived "
        "float column is not found among [-0.0, 1]; main and this branch agree",
        _is(ck="float", lit="fm0", pos="isin2"),
    ),
    Rule(
        "decimal32_64_integer_arithmetic", "PREEXISTING_BUG", "#241",
        "DataFusion has no decimal32/64 type for an Int64, so it computes the sum at Int64 "
        "and truncates the column toward zero (9999999.99 + 1 is 10000000); main and this "
        "branch agree",
        lambda c: c.ctx in ("d32", "d64") and c.lk == "int" and c.pos in grid.ARITHMETIC_POSITIONS,
    ),
    Rule(
        "temporal_vs_number", "INTENDED_CHANGE", "D-l (U4)",
        "a number or Boolean next to a date or timestamp is a plan-time type error in every "
        "position; `isin2` carries the int 1. Main cast the number to an instant (5 reads as "
        "1970-01-06) or failed in the optimizer",
        lambda c: c.ck in oracle.TEMPORAL and (c.lk in ("int", "float", "special", "decimal", "bool") or c.pos == "isin2"),
    ),
    Rule(
        "number_vs_temporal", "INTENDED_CHANGE", "D-l (U4)",
        "mirrored: a date or datetime next to a numeric column is a plan-time type error; "
        "main compared the column with the literal's text or failed in the optimizer",
        lambda c: c.ck in oracle.NUMERIC and c.lk in TEMPORAL_LITERALS,
    ),
    Rule(
        "string_column", "INTENDED_CHANGE", "#145 design §2.6, plan change 4 (D-h)",
        "a Decimal, date or datetime next to a string column is refused at planning; main "
        "compared or stored the literal's text",
        _all(_is(ck="str", lk={"decimal"} | TEMPORAL_LITERALS), lambda c: c.pos not in grid.ARITHMETIC_POSITIONS),
    ),
    Rule(
        "nan_inf_in_exact_domain", "INTENDED_CHANGE", "D-j (U2)",
        "NaN and infinity have no value in an integer or decimal column: comparisons and "
        "shared values fail at planning, arithmetic is float arithmetic. Main compared "
        "through Float64 (every row False) or stored NaN in a widened float column",
        lambda c: c.lk == "special" and c.ck in oracle.EXACT_NUMERIC,
    ),
    Rule(
        "negative_scale_resolver_panic", "BUG_FIXED", "#242",
        "main's resolver panicked (attempt to multiply with overflow) unifying decimal(10, -2) "
        "with the literal; the literal now reads exactly, and a string in the in-list follows "
        "DataFusion's reading (D-h), whose cast to decimal(10, -2) fails at collect",
        lambda c: c.ctx == "dneg" and _error(c.main, cls="PanicException"),
        expect=lambda c: c.lk != "str" or _in_list_with_string(c),
    ),
    Rule(
        "aware_vs_naive_timestamp", "INTENDED_CHANGE", "D5",
        "an aware datetime next to a naive timestamp column is refused at planning; main "
        "compared or stored its UTC wall-clock time",
        _is(ck="ts", lk="dt_aware"),
    ),
    Rule(
        "shift_default_exact", BY_MAIN, "D-c",
        "a shift default is held in the column's type exactly or refused at planning; main "
        "truncated it to the column's width, scale or unit, took an aware instant's local "
        "date, converted 1e20 through a lossy path, or failed at collect",
        lambda c: c.pos == "shift_def" and (_plan_value_error(c.head) or (_has_values(c.head) and main_wrong(c))),
    ),
    Rule(
        "date_column_instant", BY_MAIN, "review of b6cc39f on #225",
        "an aware datetime next to a date column is the instant it names: a comparison is "
        "exact, and a value is refused unless the instant is midnight UTC. Main took the "
        "local date, dropping the time of day",
        _all(_is(ck="date", lk="dt_aware"), lambda c: c.pos not in grid.ARITHMETIC_POSITIONS),
    ),
    Rule(
        "date_column_time_of_day", BY_MAIN, "review of b6cc39f on #225",
        "a datetime with a time of day is refused for a date column, Date64 included; main "
        "stored the date part or had no common type",
        _all(_is(ck="date", lk="dt_naive"), lambda c: c.pos in grid.VALUE_POSITIONS, lambda c: _plan_value_error(c.head)),
    ),
    Rule(
        "timestamp_widened", "INTENDED_CHANGE", "D-m (U5)",
        "the literal needs a finer unit than the column's, so the shared value widens to "
        "that unit in the column's zone, a range-only change; main had no common type",
        _all(
            _is(ck={"ts", "ts_tz"}),
            lambda c: c.lk in TEMPORAL_LITERALS and c.pos in grid.VALUE_POSITIONS,
            lambda c: _has_values(c.head) and c.head["type"] in oracle.finer_units(c.ctx_type),
        ),
    ),
    Rule(
        "temporal_comparison_truncated", "BUG_FIXED", "#200",
        "main truncated the literal to the column's unit before comparing, so an instant "
        "between two ticks matched the earlier one",
        lambda c: c.ck in oracle.TEMPORAL and c.lk in TEMPORAL_LITERALS and c.pos in grid.COMPARISON_POSITIONS and main_wrong(c),
    ),
    Rule(
        "temporal_nanosecond_overflow", "INTENDED_CHANGE", TYPED,
        "DataFusion subtracts a date from a timestamp, or two timestamps with a nanosecond "
        "side, at nanoseconds; 1500-01-01 and 2300-01-01 lie outside their range, so the "
        "cast fails at collect. Main failed at planning, on the literal's text",
        lambda c: c.ck in oracle.TEMPORAL and c.lk in TEMPORAL_LITERALS and c.pos in ("sub", "dtdiff") and _collect_error(c.head),
        expect=lambda c: _collect_error(c.head, r"Cannot cast Date(32|64) value|simplify_expressions|overflow"),
    ),
    Rule(
        "zoned_column_reading", BY_MAIN, "#145 design §2.6 (a naive datetime or date next to a zoned column reads in its zone)",
        "a naive datetime next to a zoned column is wall-clock time in that zone, a date is "
        "local midnight, and an aware datetime is its instant in the column's zone; main "
        "passed the literal as a string, which DataFusion could not coerce",
        lambda c: c.ck == "ts_tz" and c.lk in TEMPORAL_LITERALS,
    ),
    Rule(
        "temporal_literal_typed", BY_MAIN, TYPED,
        "a date or datetime literal is typed next to the column, so the comparison, value, "
        "difference or subtraction is exact; main passed it as a string, which DataFusion "
        "could not coerce or cast",
        lambda c: c.ck in oracle.TEMPORAL and c.lk in TEMPORAL_LITERALS,
    ),
    Rule(
        "integer_in_list_exact", BY_MAIN, "D-b, #145 design §1.3",
        "an integer the decimal column cannot hold compares in a type that holds both "
        "exactly; main unified a mixed in-list at Int64 and cast the column to it, so 1.50 "
        "matched 1",
        lambda c: c.ck in oracle.EXACT_NUMERIC and c.lk == "int" and c.pos in grid.COMPARISON_POSITIONS,
    ),
    Rule(
        "float_comparison_exact", BY_MAIN, "D-j (U2)",
        "a float literal denotes its binary64 value and compares with an integer or decimal "
        "column exactly; main cast the column to Float64 (2**53 + 1 equalled 2.0**53) or the "
        "literal to decimal(30, 15) (#240), and hashed -0.0 bitwise in an in-list",
        lambda c: c.ck in oracle.EXACT_NUMERIC and c.lk == "float" and c.pos in grid.COMPARISON_POSITIONS,
    ),
    Rule(
        "shared_value_exact", BY_MAIN, "D-i (U1)",
        "a shared value is exact in DataFusion's proposed type, or in the literal-free "
        "context type when the literal holds there, or refused at planning. Main widened "
        "to Float64 (losing 2**63 - 1), truncated a decimal column to Int64, rounded the "
        "literal to the column's scale, or read a Decimal as a string",
        lambda c: c.ck in oracle.EXACT_NUMERIC and c.lk in ("int", "float", "decimal") and c.pos in grid.VALUE_POSITIONS,
    ),
    Rule(
        "decimal_next_to_float", BY_MAIN, "#145 design §2.6 (Decimal next to a float is that float)",
        "a Decimal literal next to a float column is the nearest float; main passed it as a "
        "string: CASE unified the branches at Utf8 and arithmetic had no common type",
        lambda c: c.ck == "float" and c.lk == "decimal",
    ),
    Rule(
        "decimal_literal_typed", BY_MAIN, TYPED,
        "a Decimal literal is typed next to an integer or decimal column, so comparisons, "
        "shift defaults and arithmetic are exact, and arithmetic that overflows the result "
        "type fails at collect; main passed it as a string, which failed in the optimizer "
        "or had no common type",
        lambda c: c.ck in oracle.EXACT_NUMERIC and c.lk == "decimal",
    ),
    Rule(
        "in_list_with_string", "INTENDED_CHANGE", TYPED + ", D-h",
        "the int 1 beside a string in `is_in` adopts the column's type, so DataFusion "
        "unifies the list there and reads the string as that type; main unified at Int64 "
        "or Utf8",
        lambda c: c.ck in oracle.EXACT_NUMERIC and c.lk == "str" and c.pos == "isin2",
        expect=_in_list_with_string,
    ),
]


def _resolve(rule, c):
    if rule.status == BY_MAIN:
        return "BUG_FIXED" if main_wrong(c) else "INTENDED_CHANGE"
    return rule.status


def classify(c):
    """The cell's status, the rule that gave it, and why."""
    kind, reason = c.verdict
    rule = next((r for r in RULES if r.when(c)), None)
    if c.same:
        if kind != "violation":
            return Classification("UNCHANGED", None, "main's outcome")
        if rule and rule.status == "PREEXISTING_BUG":
            return Classification("PREEXISTING_BUG", rule.name, reason)
        return Classification("UNDECIDED", None, f"main's outcome, which the oracle rejects: {reason}")
    if rule is None:
        if kind == "violation":
            return Classification("REGRESSION", None, reason)
        return Classification("UNDECIDED", None, f"no rule covers the change ({kind})")
    if rule.status == "PREEXISTING_BUG":
        return Classification("REGRESSION", rule.name, "the outcome changed under a pre-existing-bug rule")
    if kind == "violation":
        return Classification("REGRESSION", rule.name, reason)
    if rule.expect is not None and not rule.expect(c):
        return Classification("REGRESSION", rule.name, f"the rule's expectation does not hold ({reason or 'oracle ok'})")
    if kind == "skip" and rule.expect is None:
        return Classification("UNDECIDED", rule.name, f"the oracle has no opinion ({reason}) and the rule no expectation")
    return Classification(_resolve(rule, c), rule.name, rule.note)


def cells(main, head):
    """Every grid cell as a ``Cell`` from two outcome maps."""
    return [cell(ctx, lit, pos, main[grid.cell_id(ctx, lit, pos)], head[grid.cell_id(ctx, lit, pos)]) for ctx, lit, pos in grid.cells()]


def summary(classified):
    """Counts per status and per (rule, status)."""
    by_status = Counter(k.status for _, k in classified)
    by_rule = Counter((k.rule, k.status) for _, k in classified if k.rule)
    return by_status, by_rule


def show(outcome):
    if "error" in outcome:
        e = outcome["error"]
        return f"{e['class']} at {e['stage']}: {e.get('msg', '')[:90]}"
    return f"{outcome['type']} {outcome['values']}"[:120]


def main(argv):
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("main")
    parser.add_argument("head")
    parser.add_argument("--rules", action="store_true", help="count cells per rule and status")
    parser.add_argument("--cells", help="list the cells with this status")
    parser.add_argument("--rule", help="list the cells this rule classified")
    args = parser.parse_args(argv)
    _, main_outcomes = grid.load(Path(args.main))
    _, head_outcomes = grid.load(Path(args.head))
    classified = [(c, classify(c)) for c in cells(main_outcomes, head_outcomes)]
    by_status, by_rule = summary(classified)
    for status in ("UNCHANGED", "INTENDED_CHANGE", "BUG_FIXED", "PREEXISTING_BUG", "REGRESSION", "UNDECIDED"):
        print(f"{by_status.get(status, 0):6d} {status}")
    if args.rules:
        print()
        for rule in RULES:
            for status in ("INTENDED_CHANGE", "BUG_FIXED", "PREEXISTING_BUG", "REGRESSION", "UNDECIDED"):
                n = by_rule.get((rule.name, status), 0)
                if n:
                    print(f"{n:6d} {rule.name:34s} {status:16s} {rule.ref}")
    for c, k in classified:
        if (args.cells and k.status == args.cells) or (args.rule and k.rule == args.rule):
            print(f"\n{c.id} [{k.status}] {k.reason[:160]}\n  head={show(c.head)}\n  main={show(c.main)}")


if __name__ == "__main__":
    main(sys.argv[1:])
