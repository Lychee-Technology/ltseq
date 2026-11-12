"""The v0.5 model of the literal grid (#275), checked against the contract's own examples.

``contract.py`` says what v0.5 requires of each grid cell, and the gate
grades a cell by it once every rule it rests on is in force. These tests
check the model without ltseq: that each rule cites a section the contract
has and an M item the impact map has, that every cell gets an expectation,
that the cells the contract states outright come out as it states them
(§17.2-§17.5, §19.2, §20 and the rows N2, N4, N5, H1 and P12 of §24), and
that putting a rule in force, or the error classes of M15, changes what the
gate accepts, and that the rules a run could put in force include rules
that only ever grade cells together.
"""

import re
from pathlib import Path

import pytest

from . import classification, contract, grid, oracle

CONTRACT = Path(__file__).parents[3] / "docs" / "proposals" / "v0.5"
BASELINE = Path(__file__).parent / "expected" / "main.jsonl"
NULL = None


def values(type_, rows):
    return {"type": type_, "values": list(rows)}


def error(cls, stage="plan", msg="refused"):
    return {"error": {"class": cls, "stage": stage, "msg": msg}}


def full(cell):
    return contract.graded(*cell.split("/"))


def judged_by_v05(cell, outcome):
    ctx = cell.split("/")[0]
    return oracle.judge_by(full(cell), ctx, outcome)[0]


# --- the rules ------------------------------------------------------------------------


def _headings():
    text = "\n".join(path.read_text() for path in (CONTRACT / "contract").glob("*.md"))
    return set(re.findall(r"^#{2,3} (\d+(?:\.\d+)?)\.? ", text, re.MULTILINE))


def test_every_rule_cites_one_section_the_contract_has():
    headings = _headings()
    for rule in contract.RULES.values():
        m = re.fullmatch(r"§(\d+(?:\.\d+)?)(?: rule \d)?", rule.section)
        assert m, rule
        assert m.group(1) in headings, rule
        assert rule.note, rule


def test_every_owner_is_an_item_of_the_impact_map():
    items = set(re.findall(r"^\| (M\d+) \|", (CONTRACT / "review" / "impact-map.md").read_text(), re.MULTILINE))
    for rule in contract.RULES.values():
        assert rule.owner is None or rule.owner in items, rule


def test_in_force_names_rules():
    assert contract.IN_FORCE <= set(contract.RULES)
    assert "error_classes" not in contract.IN_FORCE  # M15 has not landed


def test_every_cell_has_a_v05_expectation_on_known_rules():
    used = set()
    for ctx, lit, pos in grid.cells():
        e = contract.graded(ctx, lit, pos)
        assert e.kind in ("values", "error", "skip", "null_arithmetic"), (ctx, lit, pos, e)
        assert e.rules and set(e.rules) <= set(contract.RULES), (ctx, lit, pos, e)
        if e.kind in ("error", "null_arithmetic"):
            assert set(e.stage) <= oracle.STAGES and e.cls and e.msg is None, (ctx, lit, pos, e)
            assert set(e.cls) <= set(contract.PRE_M15), (ctx, lit, pos, e)
        if e.kind == "values":
            assert len(e.rows) == grid.ROWS and e.type is not None, (ctx, lit, pos, e)
        used.update(e.rules)
    assert set(contract.RULES) - used == {"error_classes"}


def test_the_v05_model_skips_only_the_order_of_booleans():
    skipped = {(ctx, lit, pos) for ctx, lit, pos in grid.cells() if contract.graded(ctx, lit, pos).kind == "skip"}
    assert skipped == {("bool", "bT", "gt"), ("bool", "bT", "lt_mirror")}


# --- cells the contract states ------------------------------------------------------------

I32 = [2**31 - 1, NULL, -(2**31), 2**24 + 1, NULL, 0]
NY_2024_UTC = 1704067200000000000  # 2024-01-01T00:00Z, 2023-12-31 19:00 in New York


def _rows(cell):
    e = full(cell)
    assert e.kind == "values", (cell, e)
    return oracle.type_text(e.type), [None if v is None else v[1] for v in e.rows]


def _raises(cell):
    e = full(cell)
    assert e.kind == "error", (cell, e)
    return e.cls[0], "plan" if "plan" in e.stage else "execution"


@pytest.mark.parametrize("cell", ["i32/f0/fill", "i32/f0/coal", "i32/f0/ifelse_t"])
def test_a_literal_never_widens_the_column(cell):
    """§17.5 rule 1, N5: `int32.fill_null(0.0)` stays int32."""
    t, rows = _rows(cell)
    assert t == "int32"
    assert [r for i, r in enumerate(rows) if i not in oracle.LITERAL_ROWS[cell.split("/")[2]]] == [
        v for i, v in enumerate(I32) if i not in oracle.LITERAL_ROWS[cell.split("/")[2]]
    ]


@pytest.mark.parametrize("cell", ["i32/f1_5/fill", "i64/f1_5/fill", "f32/f0_1/fill", "f64/imax/coal", "d5/D1_236/ifelse_f",
                                  "i8/fnan/fill", "f32/i2p24p1/shift_def"])
def test_an_inexact_shared_literal_is_a_cast_error_at_plan(cell):
    """§17.5 rule 2, N5: `r.i32.fill_null(1.5)` and `r.i64.fill_null(1.5)` both raise; so
    does a value a float column cannot hold, where v0.4 rounded it."""
    assert _raises(cell) == ("CastError", "plan")


def test_an_aware_literal_takes_the_columns_zone_and_keeps_its_instant():
    """§17.5, #246: `r.ts_ny.fill_null(datetime(2024, 1, 1, tzinfo=timezone.utc))`."""
    t, rows = _rows("ts_ny/dt_utc/fill")
    assert t == "timestamp[us, tz=America/New_York]"
    assert rows[1] == rows[4] == NY_2024_UTC


def test_a_finer_temporal_literal_is_exact_at_the_columns_unit_or_refused():
    """§17.5, #247: no widening; `pd.Timestamp` with a fraction of a second into seconds raises."""
    assert _raises("ts_s/pd_ns/fill") == ("CastError", "plan")
    assert _raises("ts_s/dt_1_5s/coal") == ("CastError", "plan")
    assert _rows("ts_s/dt_mid/fill")[0] == "timestamp[s]"
    assert _raises("ts_ns/dt_1500/fill") == ("CastError", "plan")  # outside the nanosecond range


@pytest.mark.parametrize("cell", ["ts_ny/dt_mid/eq", "ts_utc/date2024/fill", "date32/dt_utc/gt", "ts_s/dt_utc/coal",
                                  "ts_ny/pd_ns/sub", "date32/dt_mid/fill", "ts_us/date2024/sub", "date32/dt_mid/sub",
                                  "ts_s/dt_mid/add", "date32/date2024/add", "case_date_ts/i1/eq", "case_date_ts/none/fill"])
def test_naive_with_aware_and_date_with_timestamp_are_type_errors(cell):
    """§19.2: naive with aware, a date with an aware timestamp, a date with a timestamp
    outside comparison, and an instant plus an instant raise LTSeqTypeError at plan time;
    a CASE of a date and a timestamp shares a column (§17.5)."""
    assert _raises(cell) == ("LTSeqTypeError", "plan")


def test_a_date_compares_with_a_naive_timestamp_at_midnight():
    """§19.2: `date32 == datetime(2024, 1, 1)` is TRUE on 2024-01-01."""
    assert _rows("date32/dt_mid/eq") == ("bool", [False, None, True, False, None, False])


def test_temporal_differences():
    """§19.2: `date - date` is duration[s]; `timestamp - timestamp` the finer unit, of
    instants across zones (#246), and an operand the unit cannot hold overflows."""
    t, rows = _rows("date32/date2024/sub")
    assert t == "duration[s]" and rows[2] == 0 and rows[0] == -19723 * oracle.DAY_NS
    assert _rows("ts_s/pd_ns/sub")[0] == "duration[ns]"
    t, rows = _rows("ts_ny/dt_utc/sub")
    assert t == "duration[us]" and rows[5] == 0
    assert _raises("ts_ns/dt_1500/sub") == ("ArithmeticOverflowError", "execution")


@pytest.mark.parametrize("cell,expected", [
    ("i8/i1/add", "int64"),  # §17.3: `r.i8 + 1` is int64
    ("u8/i1/sub", "int64"),
    ("u64/i1/sub", "decimal128(21, 0)"),  # §17.3: `r.u64 - 1` is decimal128(21, 0)
    ("d32/D1_5/add", "decimal128(10, 2)"),  # §17.4 rule 2, N4: decimal32(9, 2) widens first
    ("d256/i1/add", "decimal256(51, 5)"),
    ("i32/D1_5/add", "decimal128(12, 1)"),  # an int32 counts as decimal(10, 0)
    ("f32/i1/add", "double"),  # §17.4: an integer with any float is float64
    ("f32/D1_5/sub", "double"),
    ("d5/f1_5/add", "double"),  # §17.4 rule 1: a float with a decimal
])
def test_arithmetic_result_types(cell, expected):
    assert _rows(cell)[0] == expected


def test_unsigned_minus_one_takes_the_decimal_that_holds_it():
    """N1: `uint64` with `int64` under `-` is decimal128(21, 0), so 0 - 1 is -1."""
    assert _rows("u64/i1/sub")[1] == [2**64 - 2, None, -1, 2**63 - 1, None, 0]


@pytest.mark.parametrize("cell", ["i64/imax/add", "i64/imin/sub", "i8/imax/add", "d38/D38nines/add", "i64/Dfine33/add"])
def test_arithmetic_that_does_not_fit_its_type_overflows_during_execution(cell):
    """§17.1: checked, never wrapped; a decimal result beyond its precision fails too."""
    assert _raises(cell) == ("ArithmeticOverflowError", "execution")


def test_an_integer_literal_in_float_arithmetic_is_its_nearest_float64():
    """P12: the literal converts once to the nearest float64, as a column does."""
    _, rows = _rows("f32/i2p24p1/add")
    assert rows[0] == 2**25 + 1  # 2**24 + (2**24 + 1), exact in float64; float32 has no such value
    _, rows = _rows("f64/i2p53p1/add")
    assert rows[0] == oracle.to_float(oracle.Fraction(2**53 + 2) + 2**53)


@pytest.mark.parametrize("cell,expected", [
    ("f32/i2p24p1/eq", [False, None, False, False, None, False]),  # 2**24 is not 2**24 + 1
    ("f32/f0_1/eq", [False, None, False, False, None, False]),  # N9: `r.f32 == 0.1` is FALSE
    ("f64/D0_1/eq", [False, None, False, False, None, False]),  # binary64 0.1 is not Decimal("0.1")
    ("f64/f0_1/eq", [False, None, True, False, None, False]),
    ("i64/f2p53/gt", [True, None, False, True, None, False]),  # 2**53 + 1 > 2.0**53
    ("i32/f1_5/isin", [False] * 6),  # §8.5: `r.int_col.is_in([1.5])` is FALSE for every row
    ("i8/i1/isin", [False, False, False, True, False, False]),  # never NULL
    ("i8/none/isin", [False, True, False, False, True, False]),
    ("i8/none/isin2", [False, True, False, True, True, False]),
    ("i8/none/eq", [False, True, False, False, True, False]),  # §8.2: `== None` is is_null()
    ("f64/fnan/eq", [False, None, False, False, None, False]),
    ("f64/fnan/lt_mirror", [False, None, False, False, None, False]),  # NaN is above every float
    ("f64/fninf/gt", [True, None, True, True, None, True]),
    ("str/s1/eq", [True, None, False, False, None, False]),
])
def test_comparisons_are_exact(cell, expected):
    """§17.4 rule 3, §18: exact values, NaN above +inf."""
    assert _rows(cell) == ("bool", expected)


@pytest.mark.parametrize("cell,expected", [
    ("i8/fnan/eq", "LTSeqValueError"),  # §17.4 rule 3, N4: NaN literal vs integer
    ("d5/finf/isin2", "LTSeqValueError"),
    ("i8/none/gt", "LTSeqTypeError"),  # §8.2: an ordering comparison with None
    ("i8/s1/eq", "LTSeqTypeError"),  # §17.4 rule 4: a string next to a number
    ("str/i1/eq", "LTSeqTypeError"),
    ("str/s1/isin2", "LTSeqTypeError"),  # the int 1 among strings
    ("bool/i1/gt", "LTSeqTypeError"),  # §18: `r.flag > 0`
    ("i8/bT/add", "LTSeqTypeError"),  # §18: `True + 1`
    ("bool/bT/add", "LTSeqTypeError"),
    ("date32/i5/eq", "LTSeqTypeError"),  # §19.2: `r.d > 5`
    ("ts_s/D1_5/add", "LTSeqTypeError"),
    ("i8/date2024/fill", "LTSeqTypeError"),
    ("date32/sdate/eq", "LTSeqTypeError"),  # no implicit parsing
    ("str/s1/sub", "LTSeqTypeError"),
    ("ts_us/i5/dtdiff", "AttributeError"),  # §19.4: dt.diff is removed
])
def test_refusals_at_plan(cell, expected):
    assert _raises(cell) == (expected, "plan")


@pytest.mark.parametrize("cell,rule", [
    ("str/none/isin2", "no_implicit_kind_change"),  # `r.s.is_in([None, 1])`: the 1 is not a string
    ("bool/none/isin2", "no_implicit_kind_change"),
    ("date32/none/isin2", "number_with_temporal"),
    ("ts_ny/none/isin2", "number_with_temporal"),
    ("case_us_ns/none/isin2", "number_with_temporal"),
])
def test_every_item_of_a_membership_list_must_suit_the_column(cell, rule):
    """§8.5, §17.4 rule 4, §19.2: the int 1 of `isin2` is refused beside a string, bool or
    temporal column even when the other item is None, and no Boolean outcome passes."""
    assert _raises(cell) == ("LTSeqTypeError", "plan")
    assert full(cell).rules == (rule,)
    assert judged_by_v05(cell, values("bool", [False] * 6)) == "violation"
    assert judged_by_v05(cell, error("LTSeqTypeError")) == "ok"


@pytest.mark.parametrize("cell", ["str/none/isin", "bool/none/isin", "date32/none/isin", "i8/none/isin2", "f64/none/isin2"])
def test_none_alone_or_beside_a_number_is_membership_of_null(cell):
    """§8.5: `is_in([None])` on any column, and `is_in([None, 1])` on a number column, are Booleans."""
    assert full(cell).kind == "values" and full(cell).rules == ("isin_never_null",)


def test_strings_concatenate():
    """§8.2: `str + str` concatenates."""
    assert _rows("str/s1/add") == ("string", ["11", None, "1.51", "a1", None, "2024-01-011"])


# --- the gate ---------------------------------------------------------------------------


def test_a_cell_is_graded_by_v05_only_when_its_rules_are_in_force(monkeypatch):
    """`i32/f1_5/fill` is float64 under ADR 0018 D-i and CastError under §17.5 rule 1:
    putting `shared_value_no_widening` in force (M8) changes what the gate accepts."""
    cell = "i32/f1_5/fill"
    widened = values("double", [2147483647.0, 1.5, -2147483648.0, 16777217.0, 1.5, 0.0])
    refused = error("ValueError", msg="literal 1.5 does not fit column 'i32' exactly")
    assert oracle.judge("i32", "f1_5", "fill", widened)[0] == "ok"
    assert oracle.judge("i32", "f1_5", "fill", refused)[0] == "violation"
    monkeypatch.setattr(contract, "IN_FORCE", contract.IN_FORCE | {"shared_value_no_widening"})
    assert oracle.judge("i32", "f1_5", "fill", widened)[0] == "violation"
    assert oracle.judge("i32", "f1_5", "fill", refused)[0] == "ok"  # before M15, ltseq's ValueError
    assert oracle.judge("i32", "f1_5", "fill", error("CastError"))[0] == "ok"
    assert oracle.judge("i32", "f1_5", "fill", error("CastError", "collect"))[0] == "violation"
    assert "§17.5 (shared_value_no_widening)" in oracle.judge("i32", "f1_5", "fill", widened)[1]
    assert cell in grid.load(BASELINE)[1]


def test_m15_makes_the_classes_strict(monkeypatch):
    """Before M15 a v0.5 refusal that ADR 0018 expects too keeps that oracle's class,
    stage and cause; once `error_classes` is in force, only v0.5's class and stage count."""
    cell = ("date32", "i5", "eq")
    today = error("ValueError", msg="column 'date32' is a date or timestamp (Date32); use a date or datetime, not 5")
    assert "number_with_temporal" in contract.IN_FORCE
    assert oracle.judge(*cell, today)[0] == "ok"
    assert oracle.judge(*cell, error("ValueError", msg="something else"))[0] == "violation"
    assert oracle.judge(*cell, error("ValueError", "capture", msg=today["error"]["msg"]))[0] == "violation"
    monkeypatch.setattr(contract, "IN_FORCE", contract.IN_FORCE | {"error_classes"})
    assert oracle.judge(*cell, today)[0] == "violation"
    assert oracle.judge(*cell, error("LTSeqTypeError", msg="reworded"))[0] == "ok"
    assert oracle.judge(*cell, error("LTSeqTypeError", stage="collect"))[0] == "violation"


def test_m15_reclassifies_the_refusals_of_cells_whose_rules_are_deferred(monkeypatch):
    """M15 lands before M8: with `error_classes` in force and `shared_value_no_widening`
    not, `r.i64.fill_null(1.5)` takes v0.5's class and stage (§20.1) while its semantics
    stay ADR 0018's, and the widened outcome M8 brings is not accepted early."""
    cell = ("i64", "f1_5", "fill")
    today = error("ValueError", msg="literal 1.5 does not fit column 'i64' exactly")
    assert "shared_value_no_widening" not in contract.IN_FORCE
    assert oracle.legacy_expectation(*cell).kind == "error"
    assert oracle.judge(*cell, today)[0] == "ok"
    assert oracle.judge(*cell, error("CastError"))[0] == "violation"
    monkeypatch.setattr(contract, "IN_FORCE", contract.IN_FORCE | {"error_classes"})
    assert not contract.in_force(contract.expectation(*cell))
    assert oracle.expectation(*cell).rules == ("error_classes",)
    assert oracle.judge(*cell, error("CastError", msg="reworded"))[0] == "ok"
    assert oracle.judge(*cell, error("CastError", "capture"))[0] == "ok"
    assert oracle.judge(*cell, today)[0] == "violation"
    assert oracle.judge(*cell, error("CastError", "collect"))[0] == "violation"
    assert oracle.judge(*cell, error("LTSeqTypeError"))[0] == "violation"


def test_m15_keeps_the_stage_and_semantics_of_deferred_cells(monkeypatch):
    """A deferred cell whose refusal v0.5 does not share keeps its stage and takes the v0.5
    classes of today's class; a deferred cell with values keeps ADR 0018's values."""
    monkeypatch.setattr(contract, "IN_FORCE", contract.IN_FORCE | {"error_classes"})
    changed = 0
    for ctx, lit, pos in grid.cells():
        v05 = contract.expectation(ctx, lit, pos)
        if contract.in_force(v05):
            continue
        before, now = oracle.legacy_expectation(ctx, lit, pos), oracle.expectation(ctx, lit, pos)
        if before.kind not in ("error", "null_arithmetic"):
            assert now == before, (ctx, lit, pos)
            continue
        changed += 1
        assert now.kind == before.kind and now.msg is None and now.rules == ("error_classes",), (ctx, lit, pos)
        assert set(now.cls) <= set(contract.PRE_M15), (ctx, lit, pos, now)
        if v05.kind == before.kind and before.stage in v05.stage:
            assert (now.stage, now.cls) == (v05.stage, v05.cls), (ctx, lit, pos)
        else:
            assert now.stage == (before.stage,), (ctx, lit, pos)
            assert all(set(contract.V05_CLASSES[c]) <= set(now.cls) for c in before.cls), (ctx, lit, pos)
    assert changed


def test_a_cell_with_a_rule_not_in_force_keeps_adr_0018():
    for ctx, lit, pos in grid.cells():
        v05 = contract.expectation(ctx, lit, pos)
        if not contract.in_force(v05):
            assert oracle.expectation(ctx, lit, pos) == oracle.legacy_expectation(ctx, lit, pos), (ctx, lit, pos)
        else:
            assert oracle.expectation(ctx, lit, pos).rules == v05.rules, (ctx, lit, pos)


def test_v05_outcomes_pass_and_mutants_fail():
    """The v0.5 outcome of a cell is ok under the full model, and a changed row, type or
    class is a violation."""
    right = values("int32", [2147483647, 0, -2147483648, 16777217, 0, 0])
    assert judged_by_v05("i32/f0/fill", right) == "ok"
    assert judged_by_v05("i32/f0/fill", values("double", [float(v) for v in right["values"]])) == "violation"
    assert judged_by_v05("i32/f0/fill", values("int32", [2147483647, NULL, -2147483648, 16777217, 0, 0])) == "violation"
    assert judged_by_v05("i64/imax/add", error("ArithmeticOverflowError", "collect")) == "ok"
    assert judged_by_v05("i64/imax/add", values("int64", [-2, NULL, -1, -9214364837600034816, NULL, 2**63 - 1])) == "violation"
    assert judged_by_v05("i64/imax/add", error("ValueError", "collect")) == "violation"
    assert judged_by_v05("ts_ny/dt_mid/eq", error("LTSeqTypeError", "capture")) == "ok"


def test_the_report_groups_every_cell_once():
    _, outcomes = grid.load(BASELINE)
    groups = contract.report(outcomes)
    assert sum(len(met) + len(unmet) for met, unmet in groups.values()) == len(outcomes)
    assert set(contract.ready(groups)) <= set(contract.RULES)


def _groups(failing=lambda rules: False):
    """The report of a run in which the cells of the rule sets ``failing`` names do not meet v0.5."""
    groups = {}
    for ctx, lit, pos in grid.cells():
        rules = contract.graded(ctx, lit, pos).rules
        met, unmet = groups.setdefault(rules, ([], []))
        (unmet if failing(rules) else met).append(grid.cell_id(ctx, lit, pos))
    return groups


COUPLED = {"integer_result_types", "decimal_result_types", "checked_arithmetic"}


def test_coupled_rules_never_grade_a_cell_alone():
    """The premise of the next tests: integer and decimal result types rest on checked
    arithmetic in every cell, so no single rule can enter first."""
    for rules in _groups():
        if set(rules) & COUPLED:
            assert "checked_arithmetic" in rules and len(set(rules) & COUPLED) == 2, rules


def test_a_conforming_run_could_put_every_rule_in_force():
    """Coupled rules enter together when their cells meet v0.5."""
    assert set(contract.ready(_groups())) == set(contract.RULES) - {"error_classes"}


def test_a_failing_rule_set_keeps_only_its_coupled_rules_out():
    decimal_fails = _groups(lambda rules: "decimal_result_types" in rules)
    chosen = set(contract.ready(decimal_fails))
    assert {"integer_result_types", "checked_arithmetic"} <= chosen
    assert "decimal_result_types" not in chosen
    checked_fails = _groups(lambda rules: rules == ("integer_result_types", "checked_arithmetic"))
    chosen = set(contract.ready(checked_fails))
    assert {"decimal_result_types", "checked_arithmetic"} <= chosen and "integer_result_types" not in chosen
    assert not COUPLED & set(contract.ready(_groups(lambda rules: "checked_arithmetic" in rules)))


def test_ready_never_chooses_a_set_with_a_failing_cell():
    groups = _groups(lambda rules: "float_order" in rules or rules == ("integer_result_types", "checked_arithmetic"))
    chosen = frozenset(contract.ready(groups))
    assert "float_order" not in chosen and "integer_result_types" not in chosen
    assert not any(unmet for rules, (_, unmet) in groups.items() if frozenset(rules) <= chosen)
    for name in chosen:  # each rule grades a cell of its own
        assert any(name in rules and frozenset(rules) <= chosen for rules in groups)


def test_classification_still_reads_v05_expectations():
    """`main_wrong` reads an expectation's rows whichever oracle gave it."""
    _, main = grid.load(BASELINE)
    c = classification.cell("i8", "i1", "eq", main["i8/i1/eq"], main["i8/i1/eq"])
    assert classification.classify(c).status in classification.STATUSES
