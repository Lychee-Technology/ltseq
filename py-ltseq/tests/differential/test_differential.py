"""Differential cases against the outcomes recorded on main (#203).

Every case in ``cases/`` runs on the current build. An unlisted case must
reproduce main's outcome from ``expected/``; a case listed in
``classification.toml`` must produce its ``expect`` outcome (an intended
change or a fixed bug) or main's outcome (a known pre-existing bug). No
case may expose a Rust panic, whatever its classification: errors compare
by class and stage only, so the panic is checked apart from them, and a pin
admits main's ordinary error, never a panic.
To see two builds side by side, use ``tools/diffprobe/diffprobe.py``.
"""

import json
import tomllib
from pathlib import Path

import pytest

from ..literal_grid.grid import exposed_panic
from . import harness

STATUSES = {"INTENDED_CHANGE", "BUG_FIXED", "PREEXISTING_BUG", "REGRESSION", "UNDECIDED"}
CLASSIFICATION = tomllib.loads(Path(__file__).with_name("classification.toml").read_text())


def _recorded():
    recorded = {}
    for path in sorted(harness.EXPECTED_DIR.glob("*.jsonl")):
        for line in path.read_text().splitlines():
            record = json.loads(line)
            if "id" in record:
                case_id = record.pop("id")
                recorded[f"{path.stem}::{case_id}"] = record
    return recorded


RECORDED = _recorded()


def _cases():
    cases = []
    for path in harness.case_files():
        module = harness.load_module(path)
        for case_id, fn in harness.case_functions(module):
            cases.append(pytest.param(f"{path.stem}::{case_id}", fn, id=f"{path.stem}::{case_id}"))
    return cases


CASES = _cases()


def test_classification_names_known_cases():
    known = {param.values[0] for param in CASES}
    assert sorted(set(CLASSIFICATION) - known) == []
    for name, entry in CLASSIFICATION.items():
        assert entry["status"] in STATUSES, name
        assert entry.get("ref"), name
        if entry["status"] in {"INTENDED_CHANGE", "BUG_FIXED"}:
            assert not exposed_panic(json.loads(entry["expect"])), name


def test_every_case_has_a_recorded_outcome():
    assert sorted({param.values[0] for param in CASES} - set(RECORDED)) == []


def check(name, outcome):
    """The gate: fail unless ``outcome``, the current build's, is right for case ``name``."""
    entry = CLASSIFICATION.get(name, {"status": "UNCHANGED"})
    status = entry["status"]
    if status in {"REGRESSION", "UNDECIDED"}:
        pytest.fail(f"{name} is classified {status}: {entry.get('note', '')}")
    assert not exposed_panic(outcome), f"{name} panicked: {outcome}"
    if status in {"INTENDED_CHANGE", "BUG_FIXED"}:
        expected = json.loads(entry["expect"])
    else:
        expected = harness.comparable(RECORDED[name])
    assert harness.comparable(outcome) == expected


@pytest.mark.parametrize("name, fn", CASES)
def test_case(name, fn):
    check(name, harness.run_case(fn))


def test_the_panic_check_recognizes_what_main_recorded():
    """Main panicked here; the check must see it, or a live panic would pass as an error."""
    assert exposed_panic(RECORDED["decimal_widths::int_negative_scale_d64"])
    assert not exposed_panic(RECORDED["decimal_widths::int_negative_scale_d64"] | {"error": {"class": "ValueError", "stage": "collect", "msg": "Cannot cast"}})


# The gate itself, fed outcomes no build produced. A panic caught as an
# ordinary error (by the Arrow stream at collect, or by the resolver at plan)
# compares equal to any error of its class and stage, so only the panic check
# can reject it.
NAMES = [param.values[0] for param in CASES]
CAUGHT_PANICS = ["Execution panicked: injected panic", "DataFusion panicked: injected panic"]
PINNED = [name for name in NAMES if CLASSIFICATION.get(name, {}).get("status") == "PREEXISTING_BUG"]


def _rejected_as_panic(name, outcome):
    try:
        check(name, outcome)
    except AssertionError as error:
        return f"{name} panicked" in str(error)
    return False


def _expected_error(name):
    """The error case ``name`` must raise, or None."""
    entry = CLASSIFICATION.get(name, {})
    return (json.loads(entry["expect"]) if "expect" in entry else RECORDED[name]).get("error")


def test_a_caught_panic_does_not_pass_as_a_pinned_error():
    name = "pairs::big_integer_column"
    panic = {"error": {"class": "ValueError", "stage": "collect", "msg": CAUGHT_PANICS[0]}}
    assert CLASSIFICATION[name]["status"] == "PREEXISTING_BUG"
    assert harness.comparable(panic) == harness.comparable(RECORDED[name])
    assert _rejected_as_panic(name, panic)


@pytest.mark.parametrize("msg", CAUGHT_PANICS)
def test_no_status_admits_a_caught_panic_of_the_expected_error(msg):
    raising = [name for name in NAMES if _expected_error(name)]
    statuses = {CLASSIFICATION.get(name, {"status": "UNCHANGED"})["status"] for name in raising}
    assert statuses == {"UNCHANGED", "INTENDED_CHANGE", "BUG_FIXED", "PREEXISTING_BUG"}
    shapes = {(e["class"], e["stage"]) for e in map(_expected_error, raising)}
    assert shapes >= {("ValueError", "collect"), ("RuntimeError", "plan")}  # where the stream and resolver catch one
    admitted = [name for name in raising if not _rejected_as_panic(name, {"error": {**_expected_error(name), "msg": msg}})]
    assert admitted == []


@pytest.mark.parametrize("stage", ["plan", "collect"])
def test_no_case_admits_a_panic_exception(stage):
    panic = {"error": {"class": "PanicException", "stage": stage, "msg": "injected panic"}}
    assert [name for name in NAMES if not _rejected_as_panic(name, panic)] == []


def test_a_pin_does_not_admit_even_the_panic_main_recorded(monkeypatch):
    name = "decimal_widths::int_negative_scale_d64"
    monkeypatch.setitem(CLASSIFICATION, name, {"status": "PREEXISTING_BUG", "ref": "test"})
    assert _rejected_as_panic(name, RECORDED[name])


@pytest.mark.parametrize("name", PINNED)
def test_a_pinned_case_passes_with_an_ordinary_error(name):
    check(name, RECORDED[name])
    check(name, {"error": {**RECORDED[name]["error"], "msg": "reworded by a DataFusion upgrade"}})
