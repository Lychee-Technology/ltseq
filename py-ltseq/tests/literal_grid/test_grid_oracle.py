"""Mutation tests for the grid oracle and the classification it drives (#243).

Each test invents an outcome for a cell, sets it beside the recorded main
baseline, and checks the status the gate would give it. Together they show
that every way a cell can go wrong is a REGRESSION: a wrong row, a lost
NULL, a type the policy does not authorize, a missing row, a refusal the
decisions do not justify, an error of the wrong shape, and an outcome the
grid could not have recorded. The positive controls show the same paths
accept a correct outcome. Nothing here runs ltseq.
"""

import math
import random
from fractions import Fraction
from pathlib import Path

import pytest

from . import classification, grid, oracle
from .classification import classify

BASELINE = Path(__file__).parent / "expected" / "main.jsonl"
_, MAIN = grid.load(BASELINE)

NULL = None


def values(type_, rows):
    return {"type": type_, "values": list(rows)}


def error(stage="plan", cls="ValueError", msg="refused"):
    return {"error": {"class": cls, "stage": stage, "msg": msg}}


def classified(cell, head):
    ctx, lit, pos = cell.split("/")
    return classify(classification.cell(ctx, lit, pos, MAIN[cell], head))


def judged(cell, head):
    ctx, lit, pos = cell.split("/")
    return oracle.judge(ctx, lit, pos, head)


# Correct outcomes, written by hand from the column values and the decisions.
I32_F1_5_FILL = values("double", [2147483647.0, 1.5, -2147483648.0, 16777217.0, 1.5, 0.0])
TS_S_DT_1_5S_FILL = values("timestamp[us]", [
    "datetime(1970-01-01T00:00:00)", "datetime(1970-01-01T00:00:01.500000)", "datetime(1970-01-01T00:00:01)",
    "datetime(1969-12-31T23:59:59)", "datetime(1970-01-01T00:00:01.500000)", "datetime(2024-01-01T00:00:00)",
])
I64_F0_FILL = values("int64", [2**63 - 1, 0, -(2**63), 2**53 + 1, 0, 0])
I8_I1_EQ = values("bool", [False, NULL, False, True, NULL, False])
I8_NONE_EQ = values("bool", [False, True, False, False, True, False])
I8_NONE_FILL = values("int8", [127, NULL, -128, 1, NULL, 0])
F32_I2P24P1_FILL = values("float", [16777216.0, 16777216.0, 0.10000000149011612, 1.5, 16777216.0, 1.0000000200408773e+20])
I64_IMAX_ADD = values("int64", [-2, NULL, -1, -9214364837600034816, NULL, 2**63 - 1])
D5_D1_5_ADD = values("decimal128(6, 2)", ["Decimal(1001.49)", NULL, "Decimal(1.00)", "Decimal(3.00)", NULL, "Decimal(1.50)"])
DATE32_DATE2024_SUB = values("int64", [-19723, NULL, 0, -19724, NULL, 100807])
TS_S_DT_MID_SUB = values("duration[us]", [
    "timedelta(-19723, 0, 0)", NULL, "timedelta(-19723, 1, 0)", "timedelta(-19724, 86399, 0)", NULL, "timedelta(0, 0, 0)",
])
TS_NY_PD_NS_FILL = values("timestamp[ns, tz=America/New_York]", [
    "Timestamp(18000000000000, tz=America/New_York)", "Timestamp(18001000000001, tz=America/New_York)",
    "Timestamp(0, tz=America/New_York)", "Timestamp(-1000000000, tz=America/New_York)",
    "Timestamp(18001000000001, tz=America/New_York)", "Timestamp(1704067200000000000, tz=America/New_York)",
])
I8_FINF_SUB = values("double", ["-inf", NULL, "-inf", "-inf", NULL, "-inf"])
I8_S1_ISIN2 = values("bool", [False, NULL, False, True, NULL, False])


def mutate(outcome, row, value):
    out = {"type": outcome["type"], "values": list(outcome["values"])}
    out["values"][row] = value
    return out


def retyped(outcome, type_):
    return values(type_, outcome["values"])


CONTROLS = [
    ("i32/f1_5/fill", I32_F1_5_FILL),
    ("ts_s/dt_1_5s/fill", TS_S_DT_1_5S_FILL),
    ("i64/f0/fill", I64_F0_FILL),
    ("i8/i1/eq", I8_I1_EQ),
    ("i8/none/eq", I8_NONE_EQ),
    ("i8/none/fill", I8_NONE_FILL),
    ("f32/i2p24p1/fill", F32_I2P24P1_FILL),
    ("i64/imax/add", I64_IMAX_ADD),
    ("d5/D1_5/add", D5_D1_5_ADD),
    ("date32/date2024/sub", DATE32_DATE2024_SUB),
    ("ts_s/dt_mid/sub", TS_S_DT_MID_SUB),
    ("ts_ny/pd_ns/fill", TS_NY_PD_NS_FILL),
    ("i8/finf/sub", I8_FINF_SUB),
    ("i8/s1/isin2", I8_S1_ISIN2),
    ("d38/D38nines/add", error("collect", msg="Arrow error: Arithmetic overflow: Overflow happened on: 9 * 10")),
    ("ts_ns/dt_1500/sub", error("collect", msg="Cannot cast Date32 value")),
    ("i64/Dfine33/fill", error(msg="does not fit column 'i64' (Int64) without rounding")),
    ("i8/i256/shift_def", error(msg="cannot hold the shift() default")),
    ("str/D1_5/eq", error(msg="column 'str' is a string; use a string literal")),
    ("date32/i5/eq", error(msg="is a date or timestamp")),
    ("ts_s/dt_utc/fill", error(msg="is timezone-naive, but the literal is timezone-aware")),
    ("i8/fnan/eq", error(msg="NaN and infinity meet only float columns")),
    ("i8/none/add", values("int8", [NULL] * 6)),
    ("date32/none/add", error("plan", "RuntimeError", "Cannot get result type")),
]


@pytest.mark.parametrize("cell,head", CONTROLS, ids=[c for c, _ in CONTROLS])
def test_a_correct_outcome_passes(cell, head):
    assert judged(cell, head)[0] in ("ok", "skip"), judged(cell, head)
    assert classified(cell, head).status not in ("REGRESSION", "UNDECIDED"), classified(cell, head)


MUTANTS = [
    # the six mutants of the review of f07b855
    ("i32/f1_5/fill", error(), "an unjustified refusal of a shared value DataFusion holds exactly"),
    ("i32/f1_5/fill", retyped(I32_F1_5_FILL, "decimal256(76, 1)"), "exact values in a type the policy does not propose"),
    ("i64/fnan/add", values("float", [99] * 6), "NaN arithmetic that returns numbers"),
    ("i8/i0/eq", values("int64", [0, NULL, 0, 0, NULL, 1]), "a comparison that is not Boolean"),
    ("i64/f0/fill", values("int64", []), "no rows"),
    ("ts_s/dt_1_5s/fill", values("timestamp[us]", []), "no rows in a temporal value"),
    # rows
    ("i8/i1/eq", mutate(I8_I1_EQ, 1, False), "a lost NULL in a comparison"),
    ("i8/i1/eq", mutate(I8_I1_EQ, 3, False), "a wrong Boolean"),
    ("i64/f0/fill", mutate(I64_F0_FILL, 0, 2**63 - 2), "one wrong value where main was wrong too"),
    ("i64/imax/add", mutate(I64_IMAX_ADD, 0, 2**63 - 1), "saturating where DataFusion wraps"),
    ("f32/i2p24p1/fill", retyped(F32_I2P24P1_FILL, "double"), "a float column widened for an int literal"),
    ("f32/i2p24p1/fill", mutate(F32_I2P24P1_FILL, 1, 16777217.0), "a float column holding more than its width"),
    ("f32/i2p24p1/eq", values("bool", [False, NULL, False, False, NULL, False]), "a float comparison ignoring the column's rounding"),
    ("i8/finf/sub", mutate(I8_FINF_SUB, 0, "inf"), "the wrong sign of infinity"),
    ("date32/date2024/sub", mutate(DATE32_DATE2024_SUB, 0, -19722), "a wrong day count"),
    ("i8/none/eq", values("bool", [False] * 6), "`== None` that is not IS NULL"),
    ("i8/none/fill", values("int8", [127, 0, -128, 1, 0, 0]), "a NULL fill that invents values"),
    ("i8/none/add", values("int64", [127, NULL, -128, 1, NULL, 0]), "NULL arithmetic that is not NULL"),
    # types
    ("i8/i1/fill", retyped(values("int8", [127, 1, -128, 1, 1, 0]), "int8"), "a shared value narrower than DataFusion's exact unification"),
    ("i8/D1_5/fill", values("decimal128(5, 2)", ["Decimal(127.00)", "Decimal(1.50)", "Decimal(-128.00)", "Decimal(1.00)", "Decimal(1.50)", "Decimal(0.00)"]), "a decimal scale DataFusion did not propose"),
    ("ts_s/dt_1_5s/fill", retyped(TS_S_DT_1_5S_FILL, "timestamp[ms]"), "a unit that holds the literal but is not its own"),
    ("ts_s/dt_1_5s/shift_def", values("timestamp[us]", ["datetime(1970-01-01T00:00:01.500000)", "datetime(1970-01-01T00:00:00)", NULL, "datetime(1970-01-01T00:00:01)", "datetime(1969-12-31T23:59:59)", NULL]), "a shift default that widens the column"),
    ("ts_ny/pd_ns/fill", retyped(TS_NY_PD_NS_FILL, "timestamp[ns, tz=UTC]"), "a widened timestamp that changes zone"),
    ("ts_ny/pd_ns/fill", values("timestamp[ns, tz=America/New_York]", [v.replace(", tz=America/New_York", "") for v in TS_NY_PD_NS_FILL["values"]]), "naive renderings in a zoned column"),
    # refusals and errors
    ("ts_s/dt_1_5s/fill", error(), "refusing a value the literal's unit holds (D-m)"),
    ("i8/i1/shift_def", error(), "refusing a shift default the column holds"),
    ("i64/Dfine33/fill", error("capture"), "a justified refusal at the wrong stage"),
    ("i64/Dfine33/fill", error(cls="RuntimeError"), "a justified refusal with the wrong class"),
    ("str/D1_5/eq", error(msg="cannot compare"), "a D-h refusal with another message"),
    ("ts_s/dt_mid/sub", error("collect", msg="overflow"), "an error where the computing unit holds both operands"),
    ("ts_ns/dt_1500/sub", values("duration[ns]", ["Timedelta(1)", NULL, "Timedelta(1)", "Timedelta(1)", NULL, "Timedelta(1)"]), "values where the computing unit cannot hold the literal"),
    ("d38/D38nines/add", values("decimal128(38, 10)", ["Decimal(1)", NULL, "Decimal(1)", "Decimal(1)", NULL, "Decimal(1)"]), "values where Arrow's checked arithmetic overflows"),
    ("d38/D38nines/add", error("plan", msg="overflow"), "an overflow reported at the wrong stage"),
    ("d5/D1_5/add", error("collect", msg="overflow"), "an overflow where none happens"),
    ("date32/i5/eq", values("bool", [True, NULL, True, True, NULL, True]), "a number compared with a date (D-l)"),
    ("i8/none/add", error("collect", "ValueError", "boom"), "NULL arithmetic failing after planning"),
    # rule expectations on cells the oracle has no opinion on
    ("i8/s1/isin2", mutate(I8_S1_ISIN2, 3, False), "a string in the in-list read wrongly"),
    ("dneg/s1/isin2", values("bool", [False, NULL, False, False, NULL, False]), "a string the column cannot hold, read as a value"),
    # shapes the grid could not have recorded
    ("i8/i1/fill", values("int128", [127, 1, -128, 1, 1, 0]), "an unknown type"),
    ("i8/i1/eq", values("bool", [0, NULL, 0, 1, NULL, 1]), "integers in a Boolean column"),
    ("i8/i1/fill", values("dictionary<values=int64, indices=int32, ordered=0>", [127, 1, -128, 1, 1, 0]), "a dictionary wrapper on a plain context"),
    ("i8/i1/eq", values("bool", [False, NULL, False, True, NULL, False, False]), "seven rows"),
    ("i32/f1_5/fill", values("double", I32_F1_5_FILL["values"][:5]), "five rows"),
    ("i64/Dfine33/fill", {"error": {"class": "ValueError", "msg": "x"}}, "an error without a stage"),
    ("i64/Dfine33/fill", {"error": {"class": "", "stage": "plan", "msg": "x"}}, "an error without a class"),
    ("ts_s/dt_1_5s/fill", values("timestamp[us]", ["datetime(NaT)"] + TS_S_DT_1_5S_FILL["values"][1:]), "a rendering the type cannot have produced"),
    ("i8/D1_5/fill", values("decimal128(4, 1)", [127.0, 1.5, -128.0, 1.0, 1.5, 0.0]), "floats in a decimal column"),
]


@pytest.mark.parametrize("cell,head,why", MUTANTS, ids=[f"{c}: {w}" for c, _, w in MUTANTS])
def test_a_wrong_outcome_is_a_regression(cell, head, why):
    k = classified(cell, head)
    assert k.status == "REGRESSION", (why, k)


SAME_AS_MAIN = [
    ("i64/f1_5/fill", MAIN["i64/f1_5/fill"], "main widened to Float64 and lost 2**63 - 1"),
    ("i64/Dfine33/fill", error("collect"), "main refused the unrepresentable literal only at collect"),
    ("date32/i5/eq", MAIN["date32/i5/eq"], "main compared a date with a number"),
    ("date32/i5/add", MAIN["date32/i5/add"], "main added a number to a date"),
    ("ts_s/i5/eq", MAIN["ts_s/i5/eq"], "main failed with DataFusion's RuntimeError, not the D-l ValueError"),
    ("i8/i1/eq", values("bool", [0, NULL, 0, 1, NULL, 0]), "integers that Python equality cannot tell from main's Booleans"),
]


@pytest.mark.parametrize("cell,head,why", SAME_AS_MAIN, ids=[f"{c}: {w}" for c, _, w in SAME_AS_MAIN])
def test_a_shared_wrong_outcome_without_a_pinned_issue_is_undecided(cell, head, why):
    """Nothing changed, so it is not a regression, but the gate still fails on UNDECIDED."""
    assert judged(cell, head)[0] == "violation", why
    k = classified(cell, head)
    assert (k.status, k.rule) == ("UNDECIDED", None), (why, k)


def test_a_corrected_value_where_main_was_wrong_is_a_fixed_bug():
    k = classified("i64/f0/fill", I64_F0_FILL)
    assert (k.status, k.rule) == ("BUG_FIXED", "shared_value_exact")


def test_an_exact_value_in_another_type_is_an_intended_change():
    head = values("int64", [127, 1, -128, 1, 1, 0])
    k = classified("i8/i1/fill", head)
    assert judged("i8/i1/fill", head)[0] == "ok"
    assert k.status in ("UNCHANGED", "INTENDED_CHANGE")
    # main held the same exact values at Int64 too
    assert classification.main_wrong(classification.cell("i8", "i1", "fill", MAIN["i8/i1/fill"], head)) is False


@pytest.mark.parametrize("cell", ["case_i64_f64/fm0/isin2", "d32/i1/add"])
def test_a_pinned_cell_is_preexisting_and_fixing_it_needs_unpinning(cell):
    assert judged(cell, MAIN[cell])[0] == "violation"
    assert classified(cell, MAIN[cell]).status == "PREEXISTING_BUG"
    corrected = {
        "case_i64_f64/fm0/isin2": values("bool", [False, NULL, False, False, NULL, True]),
        "d32/i1/add": values("decimal128(11, 2)", ["Decimal(10000000.99)", NULL, "Decimal(0.99)", "Decimal(2.50)", NULL, "Decimal(1.00)"]),
    }[cell]
    assert classified(cell, corrected).status == "REGRESSION"


def test_a_panicked_in_list_string_that_now_fails_at_collect_is_a_fixed_bug():
    k = classified("dneg/s1/isin2", error("collect", msg="Optimizer rule 'simplify_expressions' failed"))
    assert (k.status, k.rule) == ("BUG_FIXED", "negative_scale_resolver_panic")


def test_the_oracle_skips_only_strings_and_booleans():
    for ctx, lit, pos in grid.cells():
        e = oracle.expectation(ctx, lit, pos)
        involved = oracle.context_kind(ctx) in ("str", "bool") or oracle.literal_kind(lit) in ("str", "bool")
        if e.kind == "skip":
            assert involved, (ctx, lit, pos, e)


def test_the_baseline_is_readable_but_for_the_cells_main_rendered_as_nat():
    """Main's build placed i64::MIN as a nanosecond shift default, which pandas shows as NaT;
    the strict reader rejects it, and ``main_wrong`` counts it as a wrong value."""
    unreadable = set()
    for cell, outcome in MAIN.items():
        try:
            oracle.check_structure(cell.split("/")[0], outcome)
        except ValueError:
            unreadable.add(cell)
    assert unreadable == {"ts_ns/imin/shift_def", "case_us_ns/imin/shift_def", "case_date_ts/imin/shift_def"}


@pytest.mark.parametrize("a,b,expected", [
    ("int8", "int64", "int64"),
    ("int32", "double", "double"),
    ("uint64", "int64", "decimal128(20, 0)"),
    ("uint32", "int8", "int64"),
    ("uint8", "int8", "int16"),
    ("int8", "decimal128(2, 1)", "decimal128(4, 1)"),
    ("int64", "decimal128(4, 3)", "decimal128(23, 3)"),
    ("decimal32(9, 2)", "decimal128(4, 3)", "decimal128(10, 3)"),
    ("decimal128(10, -2)", "decimal128(2, 1)", "decimal128(13, 1)"),
    ("decimal256(50, 5)", "int64", "decimal256(50, 5)"),
    ("decimal128(5, 2)", "decimal128(38, 10)", "decimal128(38, 10)"),
])
def test_the_coercion_port_agrees_with_datafusion(a, b, expected):
    """Pinned to result types the live grid recorded under DataFusion 55."""
    assert oracle.type_text(oracle.unify(a, b)) == expected


@pytest.mark.parametrize("frm,to,expected", [
    ("int32", "double", True),
    ("int64", "double", False),
    ("int8", "decimal128(4, 1)", True),
    ("int64", "decimal128(19, 0)", True),
    ("uint64", "decimal128(19, 0)", False),
    ("decimal32(9, 2)", "decimal128(9, 2)", True),
    ("decimal128(5, 2)", "decimal128(5, 3)", False),
    ("decimal128(10, -2)", "decimal128(13, 1)", True),
    ("float", "double", True),
    ("double", "float", False),
])
def test_exact_widening(frm, to, expected):
    assert oracle.widens_exactly(frm, to) is expected


def test_the_binary32_rounding_matches_the_hardware():
    rng = random.Random(145)
    samples = [16777217.0, 16777219.0, 0.1, 1.5, 1e20, 2.0**-149, 2.0**-150, 3 * 2.0**-150, 3.4e38, -0.1]
    samples += [rng.uniform(-1, 1) * 10.0 ** rng.randint(-45, 38) for _ in range(2000)]
    for x in samples:
        assert oracle.to_float(Fraction(x), 32) == oracle.float32(x), x
    assert math.isinf(oracle.to_float(Fraction(10) ** 39, 32))
    assert oracle.to_float(Fraction(10) ** 400) == math.inf
